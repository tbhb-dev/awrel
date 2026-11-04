// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import { DurableObject } from "cloudflare:workers";

/** A webhook event, as the relay hands it to subscribed connections. */
export interface RelayEvent {
  readonly provider: string;
  readonly event: string;
  readonly delivery: string;
  readonly repository: string | null;
  readonly payload: unknown;
}

/**
 * What a connection asked to receive: one provider's events, optionally only
 * one event name, optionally only one repository (compared without case, as
 * GitHub compares names).
 */
export interface Subscription {
  readonly provider: string;
  readonly event?: string;
  readonly repository?: string;
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
        socket.send(message);
        reached += 1;
      }
    }
    return reached;
  }
}
