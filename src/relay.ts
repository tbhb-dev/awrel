// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import { DurableObject } from "cloudflare:workers";
import {
  CAP_EVENTS,
  CAP_MINUTES,
  charge,
  delivered,
  drain,
  enqueue,
  forget,
  forgetOthers,
  refund,
  retain,
} from "./cap.ts";
import {
  gatesClient,
  handle,
  limit,
  type Subscription,
  unboundedCheck,
} from "./protocol.ts";

/** A webhook event, as the relay hands it to subscribed connections. */
export interface RelayEvent {
  readonly provider: string;
  readonly event: string;
  readonly delivery: string;
  readonly repository: string | null;
  readonly payload: unknown;
}

/** Serializes both published events and socket echoes. */
function eventFrame(
  event: RelayEvent | Omit<RelayEvent, "repository">,
): string {
  return JSON.stringify({ type: "event", ...event });
}

/**
 * A connection's WebSocket attachment. It is stored with the socket, so the
 * subscriptions survive the Durable Object hibernating.
 */
export interface Attachment {
  readonly subscriptions: readonly Subscription[];
  /** Names the connection's delivery counts and queued notices; set on its first matching event or removal. */
  readonly id?: string;
}

// A caller carries on when this fails: the event still goes out.
function store(socket: WebSocket, attachment: Attachment): boolean {
  try {
    socket.serializeAttachment(attachment);
    return true;
  } catch (error) {
    console.error("connection state could not be stored", error);
    return false;
  }
}

function thisMinute(): number {
  return Math.floor(Date.now() / 60_000);
}

// The frame that tells a connection the relay removed a subscription, and why.
function noticeFrame(
  subscription: Subscription,
  reason: "unfiltered" | "rate_cap",
  message: string,
): string {
  return eventFrame({
    provider: "awrel",
    event: "notice",
    delivery: crypto.randomUUID(),
    payload: {
      code: "subscription_removed",
      reason,
      message,
      subscription,
      limit: { events: CAP_EVENTS, minutes: CAP_MINUTES },
    },
  });
}

// Read only fixed fields on the event's own object, once per publication.
function property(value: unknown, key: string): unknown {
  return typeof value === "object" && value !== null
    ? (value as Record<string, unknown>)[key]
    : undefined;
}

function filterFields(event: RelayEvent) {
  const object = property(event.payload, event.event);
  return {
    action: property(event.payload, "action"),
    conclusion: property(object, "conclusion"),
    name: property(object, "name"),
    workflow_path: property(object, "path"),
  };
}

function matches(
  subscription: Subscription,
  event: RelayEvent,
  fields: ReturnType<typeof filterFields>,
): boolean {
  return (
    subscription.provider === event.provider &&
    (subscription.event === undefined || subscription.event === event.event) &&
    (subscription.repository === undefined ||
      subscription.repository.toLowerCase() ===
        event.repository?.toLowerCase()) &&
    (subscription.action === undefined ||
      subscription.action.some((value) => value === fields.action)) &&
    (subscription.conclusion === undefined ||
      subscription.conclusion.some((value) => value === fields.conclusion)) &&
    (subscription.name === undefined || subscription.name === fields.name) &&
    (subscription.workflow_path === undefined ||
      subscription.workflow_path === fields.workflow_path)
  );
}

/** The id of a connection, or "undefined" for one that never subscribed. */
export function connectionId(attachment: Attachment | null): string {
  return String(attachment?.id);
}

/** How long after a filter request the relay holds back queued notices. */
export const GATE_GRACE_MS = 5000;

export class Relay extends DurableObject {
  // When the last filter request arrived, on any connection. Not persisted:
  // a relay that was evicted has been idle for longer than the grace.
  gatedAt = 0;

  constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
    super(ctx, env);
    // The runtime answers a keepalive ping itself, so it never wakes the
    // object.
    ctx.setWebSocketAutoResponse(
      new WebSocketRequestResponsePair('{"type":"ping"}', '{"type":"pong"}'),
    );
    // A new version drops the subscriptions that an older one accepted and
    // this one refuses, and the counts of connections that are gone.
    const live = new Set<string>();
    for (const socket of ctx.getWebSockets()) {
      const attachment = this.screen(
        socket,
        socket.deserializeAttachment() as Attachment | null,
      );
      live.add(connectionId(attachment));
    }
    try {
      forgetOthers(ctx.storage.kv, live);
    } catch (error) {
      // Stale rows are harmless; a throw here would stop every publish.
      console.error("stale connection rows could not be dropped", error);
    }
  }

  // Removes the stored subscriptions that a check refuses and queues a
  // notice for each in storage, for `deliverNotices` to send once a client
  // message shows that its event gate is open. Returns the attachment as it
  // stands afterwards. A storage failure is logged and never stops the caller.
  private screen(
    socket: WebSocket,
    attachment: Attachment | null,
  ): Attachment | null {
    if (attachment === null) return null;
    const refusals = attachment.subscriptions.flatMap((subscription) => {
      const message = unboundedCheck(subscription);
      return message === null ? [] : [{ subscription, message }];
    });
    if (refusals.length === 0) return attachment;
    const id = attachment.id ?? crypto.randomUUID();
    const next: Attachment = {
      subscriptions: attachment.subscriptions.filter(
        (subscription) => unboundedCheck(subscription) === null,
      ),
      id,
    };
    try {
      socket.serializeAttachment(next);
    } catch (error) {
      console.error("removed subscriptions could not be stored", error);
      return attachment;
    }
    try {
      enqueue(
        this.ctx.storage.kv,
        id,
        refusals.map(({ subscription, message }) =>
          noticeFrame(subscription, "unfiltered", message),
        ),
      );
    } catch (error) {
      console.error("removal notices could not be queued", error);
    }
    return next;
  }

  // A filter request closes the client's event gate until its reply is
  // routed, and `list` and `echo` bypass the client's request queue, so they
  // can arrive while the gate is still shut. The relay sees neither the gate
  // nor the end of a request. It sends notices only for a message that isn't a
  // filter request, and only once the last filter request is GATE_GRACE_MS old.
  private release(
    ws: WebSocket,
    attachment: Attachment,
    message: string | ArrayBuffer,
  ): void {
    const now = Date.now();
    if (gatesClient(message)) this.gatedAt = now;
    else if (now - this.gatedAt >= GATE_GRACE_MS) {
      this.deliverNotices(ws, attachment);
    }
  }

  // Sends the queued removal notices. Called only for a message that proves
  // the client's event gate is open (see `gatesClient`), before its reply.
  private deliverNotices(ws: WebSocket, attachment: Attachment): void {
    if (attachment.id === undefined) return;
    try {
      for (const frame of drain(this.ctx.storage.kv, attachment.id)) {
        ws.send(frame);
      }
    } catch (error) {
      console.error("removal notices could not be sent", error);
    }
  }

  /** Forgets the delivery counts of a connection that closed. */
  override webSocketClose(ws: WebSocket): void {
    forget(
      this.ctx.storage.kv,
      (ws.deserializeAttachment() as Attachment | null)?.id,
    );
  }

  /** Forgets the delivery counts of a connection that failed. */
  override webSocketError(ws: WebSocket): void {
    this.webSocketClose(ws);
  }

  /** Accepts a connection the Worker has already checked (src/connect.ts). */
  override fetch(): Response {
    const pair = new WebSocketPair();
    this.ctx.acceptWebSocket(pair[1]);
    pair[1].serializeAttachment({ subscriptions: [] } satisfies Attachment);
    return new Response(null, {
      status: 101,
      webSocket: pair[0],
      headers: { "Sec-WebSocket-Protocol": "awrel.v1" },
    });
  }

  /** Answers one client message and stores the subscriptions it changed. */
  override webSocketMessage(
    ws: WebSocket,
    message: string | ArrayBuffer,
  ): void {
    const attachment = this.screen(
      ws,
      ws.deserializeAttachment() as Attachment,
    ) as Attachment;
    this.release(ws, attachment, message);
    const outcome = handle(attachment.subscriptions, message);
    if (outcome.next !== null) {
      try {
        ws.serializeAttachment({
          ...attachment,
          subscriptions: outcome.next,
        } satisfies Attachment);
      } catch {
        // The attachment refused the new list: nothing was stored.
        ws.send(JSON.stringify(limit(outcome.reply.id)));
        return;
      }
      if (attachment.id !== undefined) {
        try {
          retain(
            this.ctx.storage.kv,
            attachment.id,
            outcome.next,
            thisMinute(),
          );
        } catch (error) {
          // Stale counts are harmless; the reply must go out.
          console.error("delivery counts could not be trimmed", error);
        }
      }
    }
    if ("echo" in outcome) {
      ws.send(
        eventFrame({
          provider: "awrel",
          event: "echo",
          delivery: outcome.echo,
          payload: {
            echo: outcome.echo,
            relay_received_at: new Date().toISOString(),
          },
        }),
      );
    }
    const { reply } = outcome;
    ws.send(
      JSON.stringify(
        reply.type === "subscriptions"
          ? {
              ...reply,
              ...this.lastHour(attachment.id, reply.subscriptions),
            }
          : reply,
      ),
    );
  }

  // The counts are extra: a storage failure leaves them out and the reply
  // still goes.
  private lastHour(
    id: string | undefined,
    subscriptions: readonly Subscription[],
  ): { delivered_last_hour?: number[] } {
    try {
      return {
        delivered_last_hour: delivered(
          this.ctx.storage.kv,
          id,
          subscriptions,
          thisMinute(),
        ),
      };
    } catch (error) {
      console.error("delivery counts could not be read", error);
      return {};
    }
  }

  /**
   * Sends the event to every connection with a matching subscription, and
   * returns how many connections it reached. Each matching subscription is
   * charged one delivery; one that is over its cap is removed with a notice.
   */
  publish(event: RelayEvent): number {
    const message = eventFrame(event);
    const fields = filterFields(event);
    const minute = thisMinute();
    let reached = 0;
    for (const socket of this.ctx.getWebSockets()) {
      const screened = this.screen(
        socket,
        socket.deserializeAttachment() as Attachment | null,
      );
      if (screened === null) continue;
      const matched = screened.subscriptions.filter((s) =>
        matches(s, event, fields),
      );
      if (matched.length === 0) continue;
      const { id, within, tripped, counted } = this.charged(
        socket,
        screened,
        matched,
        minute,
      );
      this.announceCap(socket, id, tripped);
      if (within.length === 0) continue;
      // One socket that fails mid-close mustn't stop the others from
      // receiving the event, or fail the webhook.
      try {
        socket.send(message);
        reached += 1;
      } catch {
        // Not reached, so not counted. An event that was never charged has
        // nothing to take back.
        if (counted) this.uncount(id, within, minute);
      }
    }
    return reached;
  }

  // Gives a new connection its id, charges the matched subscriptions and
  // removes the ones that tripped. An id that cannot be stored would leave
  // orphan counts, so the event goes out uncounted instead.
  private charged(
    socket: WebSocket,
    screened: Attachment,
    matched: readonly Subscription[],
    minute: number,
  ): {
    id: string;
    within: Subscription[];
    tripped: Subscription[];
    counted: boolean;
  } {
    const id = screened.id ?? crypto.randomUUID();
    if (
      screened.id === undefined &&
      !store(socket, { subscriptions: screened.subscriptions, id })
    ) {
      return { id, within: [...matched], tripped: [], counted: false };
    }
    const result = this.count(id, matched, minute, screened.subscriptions);
    if (result.tripped.length === 0) return { id, ...result };
    const stored = store(socket, {
      subscriptions: screened.subscriptions.filter(
        (s) => !result.tripped.includes(s),
      ),
      id,
    });
    // A subscription that stays in the attachment trips again on the next
    // event, so it gets its notice only once its removal is stored.
    return { id, ...result, tripped: stored ? result.tripped : [] };
  }

  // Tells the connection about each removed subscription at once, and queues
  // the same frames for the resend: a client that only listens never sends a
  // message that would release the queue.
  private announceCap(
    socket: WebSocket,
    id: string,
    tripped: readonly Subscription[],
  ): void {
    if (tripped.length === 0) return;
    const frames = tripped.map((subscription) =>
      noticeFrame(
        subscription,
        "rate_cap",
        `removed: it matched more than ${String(CAP_EVENTS)} events in ${String(CAP_MINUTES)} minutes; subscribe again with narrower filters such as name, conclusion or action`,
      ),
    );
    for (const frame of frames) {
      try {
        socket.send(frame);
      } catch {
        // The socket is closing; it has no subscriptions left to lose.
      }
    }
    try {
      enqueue(this.ctx.storage.kv, id, frames);
    } catch (error) {
      console.error("cap notices could not be queued", error);
    }
  }

  // Charges the matched subscriptions. A storage failure must not stop the
  // event from reaching anyone, so it is logged and the event goes out
  // uncounted.
  private count(
    id: string,
    matched: readonly Subscription[],
    minute: number,
    live: readonly Subscription[],
  ): { within: Subscription[]; tripped: Subscription[]; counted: boolean } {
    try {
      return {
        ...charge(this.ctx.storage.kv, id, matched, minute, live),
        counted: true,
      };
    } catch (error) {
      console.error("delivery counts could not be stored", error);
      return { within: [...matched], tripped: [], counted: false };
    }
  }

  // Takes back the count for an event that a socket never got.
  private uncount(
    id: string,
    within: readonly Subscription[],
    minute: number,
  ): void {
    try {
      refund(this.ctx.storage.kv, id, within, minute);
    } catch (error) {
      console.error("delivery counts could not be corrected", error);
    }
  }
}
