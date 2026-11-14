// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import type { Subscription } from "./protocol.ts";

/** A subscription that matches more than this many events within the window is removed. */
export const CAP_EVENTS = 100;

/** The window, in minutes, that the cap counts over. */
export const CAP_MINUTES = 5;

// Delivery counts reach back one hour, the span that list_subscriptions shows.
const HOUR_MINUTES = 60;

// Per connection: subscription key -> minute number -> deliveries that minute.
type Counters = Record<string, Record<string, number>>;

// A connection without an id has no counters; "c:undefined" is never written.
function slot(id: string | undefined): string {
  return `c:${String(id)}`;
}

function key(subscription: Subscription): string {
  return JSON.stringify([
    subscription.provider,
    subscription.event,
    subscription.repository,
    subscription.action,
    subscription.conclusion,
    subscription.name,
    subscription.workflow_path,
  ]);
}

// The stored counts without the minutes that left the hour.
function load(
  kv: SyncKvStorage,
  id: string | undefined,
  minute: number,
): Counters {
  const stored = kv.get<Counters>(slot(id)) ?? {};
  const live: Counters = {};
  for (const [name, buckets] of Object.entries(stored)) {
    const recent = Object.entries(buckets).filter(
      ([bucket]) => Number(bucket) > minute - HOUR_MINUTES,
    );
    if (recent.length > 0) live[name] = Object.fromEntries(recent);
  }
  return live;
}

function total(
  buckets: Record<string, number> | undefined,
  after: number,
): number {
  return Object.entries(buckets ?? {}).reduce(
    (sum, [bucket, count]) => (Number(bucket) > after ? sum + count : sum),
    0,
  );
}

/**
 * Counts one delivery for each of `subscriptions`, all matched by one event.
 * A subscription already at the cap is not counted: it is returned in
 * `tripped` and its counts are dropped, for the caller to remove it.
 */
export function charge(
  kv: SyncKvStorage,
  id: string,
  subscriptions: readonly Subscription[],
  minute: number,
): { within: Subscription[]; tripped: Subscription[] } {
  const within: Subscription[] = [];
  const tripped: Subscription[] = [];
  if (subscriptions.length === 0) return { within, tripped };
  const counters = load(kv, id, minute);
  const dropped = new Set<string>();
  for (const subscription of subscriptions) {
    const name = key(subscription);
    const buckets = counters[name] ?? {};
    if (total(buckets, minute - CAP_MINUTES) >= CAP_EVENTS) {
      dropped.add(name);
      tripped.push(subscription);
    } else {
      buckets[String(minute)] = (buckets[String(minute)] ?? 0) + 1;
      counters[name] = buckets;
      within.push(subscription);
    }
  }
  kv.put(
    slot(id),
    Object.fromEntries(
      Object.entries(counters).filter(([name]) => !dropped.has(name)),
    ),
  );
  return { within, tripped };
}

/** The deliveries in the last hour of each subscription, in order. */
export function delivered(
  kv: SyncKvStorage,
  id: string | undefined,
  subscriptions: readonly Subscription[],
  minute: number,
): number[] {
  const counters = load(kv, id, minute);
  return subscriptions.map((s) =>
    total(counters[key(s)], minute - HOUR_MINUTES),
  );
}

/** Drops the counts of a connection that closed. */
export function forget(kv: SyncKvStorage, id: string | undefined): void {
  kv.delete(slot(id));
}

/** Drops the counts of every connection that is not in `live`. */
export function forgetOthers(
  kv: SyncKvStorage,
  live: ReadonlySet<string>,
): void {
  for (const [name] of [...kv.list({ prefix: "c:" })]) {
    if (!live.has(name.slice(2))) kv.delete(name);
  }
}
