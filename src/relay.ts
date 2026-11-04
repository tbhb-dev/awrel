// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import { DurableObject } from "cloudflare:workers";
import { handle, limit, type Subscription } from "./protocol.ts";

/** A webhook event, as the relay hands it to subscribed connections. */
export interface RelayEvent {
  readonly provider: string;
  readonly event: string;
  readonly delivery: string;
  readonly repository: string | null;
  readonly payload: unknown;
}

/**
 * A connection's WebSocket attachment. It is stored with the socket, so the
 * subscriptions survive the Durable Object hibernating.
 */
export interface Attachment {
  readonly subscriptions: readonly Subscription[];
}

function matches(subscription: Subscription, event: RelayEvent): boolean {
  return (
    subscription.provider === event.provider &&
    (subscription.event === undefined || subscription.event === event.event) &&
    (subscription.repository === undefined ||
      subscription.repository.toLowerCase() === event.repository?.toLowerCase())
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
    }
    ws.send(JSON.stringify(outcome.reply));
  }

  /**
   * Sends the event to every connection with a matching subscription, and
   * returns how many connections it reached.
   */
  publish(event: RelayEvent): number {
    const message = JSON.stringify({ type: "event", ...event });
    let reached = 0;
    for (const socket of this.ctx.getWebSockets()) {
      const attachment = socket.deserializeAttachment() as Attachment | null;
      if (attachment?.subscriptions.some((s) => matches(s, event)) === true) {
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
    return reached;
  }
}
