// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import { DurableObject } from "cloudflare:workers";
import {
  CAP_EVENTS,
  CAP_MINUTES,
  charge,
  delivered,
  forget,
  forgetOthers,
  refund,
  retain,
} from "./cap.ts";
import {
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
  /** Names the connection's delivery counts; set on its first matching event. */
  readonly id?: string;
  /**
   * Removal notices (as event frames) waiting for the client's event gate to
   * open. A client ignores events while one of its filter requests is
   * pending, so a notice sent beside a reply would be lost.
   */
  readonly pending?: readonly string[];
}

/** How long a removal notice waits before it is sent, in milliseconds. */
const NOTICE_DELAY_MS = 5000;
/** The most notices kept for one connection; later ones are dropped. */
const MAX_PENDING = 3;

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

// Sends the notice at once.
function notice(
  socket: WebSocket,
  subscription: Subscription,
  reason: "unfiltered" | "rate_cap",
  message: string,
): void {
  try {
    socket.send(noticeFrame(subscription, reason, message));
  } catch {
    // The socket is closing; it has no subscriptions left to lose.
  }
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

/**
 * The relay between webhook providers and MCP connections. One instance
 * holds every connection, as hibernatable WebSockets.
 */
function announceCap(
  socket: WebSocket,
  tripped: readonly Subscription[],
): void {
  for (const subscription of tripped) {
    notice(
      socket,
      subscription,
      "rate_cap",
      `removed: it matched more than ${String(CAP_EVENTS)} events in ${String(CAP_MINUTES)} minutes; subscribe again with narrower filters such as name, conclusion or action`,
    );
  }
}

export class Relay extends DurableObject {
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
    forgetOthers(ctx.storage.kv, live);
  }

  // Removes the stored subscriptions that a check refuses and queues a
  // notice for each in the attachment, for `alarm` to send once the client's
  // gate is open. Returns the attachment as it stands afterwards.
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
    const next: Attachment = {
      ...attachment,
      subscriptions: attachment.subscriptions.filter(
        (subscription) => unboundedCheck(subscription) === null,
      ),
      pending: [
        ...(attachment.pending ?? []),
        ...refusals.map(({ subscription, message }) =>
          noticeFrame(subscription, "unfiltered", message),
        ),
      ].slice(0, MAX_PENDING),
    };
    socket.serializeAttachment(next);
    this.ctx.storage
      .setAlarm(Date.now() + NOTICE_DELAY_MS)
      .catch((error: unknown) => {
        console.error("notice alarm could not be set", error);
      });
    return next;
  }

  /** Sends the queued removal notices to the sockets that wait for them. */
  override alarm(): void {
    for (const socket of this.ctx.getWebSockets()) {
      const attachment = socket.deserializeAttachment() as Attachment | null;
      if (attachment?.pending === undefined) continue;
      try {
        for (const frame of attachment.pending) socket.send(frame);
      } catch {
        // The socket is closing; it has no subscriptions left to lose.
      }
      socket.serializeAttachment({ ...attachment, pending: undefined });
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
              delivered_last_hour: delivered(
                this.ctx.storage.kv,
                attachment.id,
                reply.subscriptions,
                thisMinute(),
              ),
            }
          : reply,
      ),
    );
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
      const id = screened.id ?? crypto.randomUUID();
      const { within, tripped, counted } = this.count(
        id,
        matched,
        minute,
        screened.subscriptions,
      );
      if (tripped.length > 0 || screened.id === undefined) {
        socket.serializeAttachment({
          ...screened,
          subscriptions: screened.subscriptions.filter(
            (s) => !tripped.includes(s),
          ),
          id,
        } satisfies Attachment);
      }
      announceCap(socket, tripped);
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
