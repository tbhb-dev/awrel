// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import { DurableObject } from "cloudflare:workers";
import { log } from "./log.ts";
import { handle, limit, type Subscription } from "./protocol.ts";

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
 * The first `count` code points of `text`. It stops reading after the last one,
 * so a very long string costs no more than a short one.
 */
function firstCharacters(text: string, count: number): string {
  let kept = "";
  let taken = 0;
  for (const character of text) {
    if (taken === count) {
      break;
    }
    kept += character;
    taken += 1;
  }
  return kept;
}

/**
 * Quotes client-supplied text for a log line: cuts it to 64 characters
 * without splitting a surrogate pair, and escapes every character that a log
 * reader could treat as a line break. JSON.stringify escapes the control
 * characters but leaves U+0085, U+2028 and U+2029 as they are.
 */
function quoted(text: string): string {
  return JSON.stringify(firstCharacters(text, 64)).replace(
    /[\u0085\u2028\u2029]/g,
    (char) => `\\u${char.charCodeAt(0).toString(16).padStart(4, "0")}`,
  );
}

/**
 * A connection's WebSocket attachment. It is stored with the socket, so the
 * subscriptions survive the Durable Object hibernating.
 */
export interface Attachment {
  readonly subscriptions: readonly Subscription[];
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

/**
 * The relay between webhook providers and MCP connections. One instance
 * holds every connection, as hibernatable WebSockets.
 */
export class Relay extends DurableObject {
  constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
    super(ctx, env);
    // The runtime answers a keepalive ping itself, so it never wakes the
    // object.
    ctx.setWebSocketAutoResponse(
      new WebSocketRequestResponsePair('{"type":"ping"}', '{"type":"pong"}'),
    );
  }

  /** Accepts a connection the Worker has already checked (src/connect.ts). */
  override fetch(): Response {
    const pair = new WebSocketPair();
    this.ctx.acceptWebSocket(pair[1]);
    pair[1].serializeAttachment({ subscriptions: [] } satisfies Attachment);
    log(`relay connect sockets=${String(this.ctx.getWebSockets().length)}`);
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
    const { subscriptions } = ws.deserializeAttachment() as Attachment;
    const outcome = handle(subscriptions, message);
    if (outcome.next !== null) {
      try {
        ws.serializeAttachment({
          subscriptions: outcome.next,
        } satisfies Attachment);
      } catch {
        // The attachment refused the new list: nothing was stored.
        ws.send(JSON.stringify(limit(outcome.reply.id)));
        return;
      }
      log(
        `relay subscriptions sockets=${String(this.ctx.getWebSockets().length)} subscriptions=${String(outcome.next.length)}`,
      );
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
    ws.send(JSON.stringify(outcome.reply));
  }

  /** Logs why a connection closed, then closes the server side to match. */
  override webSocketClose(
    ws: WebSocket,
    code: number,
    reason: string,
    wasClean: boolean,
  ): void {
    log(
      `relay close code=${String(code)} clean=${String(wasClean)} reason=${quoted(reason)} sockets=${String(this.others(ws))}`,
    );
    try {
      // Codes 1005 and 1006 are reserved: close() throws on them.
      ws.close(code === 1005 || code === 1006 ? 1000 : code, reason);
    } catch {
      // Closing an already closed socket throws and there is nothing left to do.
    }
  }

  /** Logs a connection error. */
  override webSocketError(ws: WebSocket, error: unknown): void {
    log(
      `relay error message=${quoted(String(error instanceof Error ? error.message : error))} sockets=${String(this.others(ws))}`,
    );
  }

  /** Counts the connections other than the one that is closing or failed. */
  private others(ws: WebSocket): number {
    return this.ctx.getWebSockets().filter((other) => other !== ws).length;
  }

  /**
   * Sends the event to every connection with a matching subscription, and
   * returns how many connections it reached.
   */
  publish(event: RelayEvent): number {
    const message = eventFrame(event);
    const fields = filterFields(event);
    const sockets = this.ctx.getWebSockets();
    let matched = 0;
    let reached = 0;
    for (const socket of sockets) {
      const attachment = socket.deserializeAttachment() as Attachment | null;
      if (
        attachment?.subscriptions.some((s) => matches(s, event, fields)) ===
        true
      ) {
        matched += 1;
        // One socket that fails mid-close mustn't stop the others from
        // receiving the event, or fail the webhook.
        try {
          socket.send(message);
          reached += 1;
        } catch {
          // Not reached, so not counted.
        }
      }
    }
    log(
      `relay publish provider=${event.provider} event=${event.event} sockets=${String(sockets.length)} matched=${String(matched)} reached=${String(reached)}`,
    );
    return reached;
  }
}
