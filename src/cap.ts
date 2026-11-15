// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import type { Subscription } from "./protocol.ts";

/** A subscription that matches more than this many events within the window is removed. */
export const CAP_EVENTS = 100;

/** The window, in minutes, that the cap counts over. */
export const CAP_MINUTES = 5;

// Delivery counts reach back one hour, the span of `delivered_last_hour`.
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

// The counters of the subscriptions in `live` only, so the stored record
// stays within one entry per live subscription.
function keep(
  counters: Counters,
  live: readonly Subscription[],
  dropped: ReadonlySet<string> = new Set(),
): Counters {
  const names = new Set(live.map(key));
  return Object.fromEntries(
    Object.entries(counters).filter(
      ([name]) => names.has(name) && !dropped.has(name),
    ),
  );
}

/**
 * Counts one delivery for each of `subscriptions`, all matched by one event
 * and all among `live`, the connection's subscriptions. A subscription
 * already at the cap is not counted: it is returned in `tripped` and its
 * counts are dropped, for the caller to remove it. Counts of subscriptions
 * that are no longer in `live` go too.
 */
export function charge(
  kv: SyncKvStorage,
  id: string,
  subscriptions: readonly Subscription[],
  minute: number,
  live: readonly Subscription[],
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
  kv.put(slot(id), keep(counters, live, dropped));
  return { within, tripped };
}

/** Takes back one delivery that `charge` counted but the socket never got. */
export function refund(
  kv: SyncKvStorage,
  id: string,
  subscriptions: readonly Subscription[],
  minute: number,
): void {
  const counters = load(kv, id, minute);
  const bucket = String(minute);
  for (const subscription of subscriptions) {
    const buckets = counters[key(subscription)];
    if (buckets?.[bucket] !== undefined) buckets[bucket] -= 1;
  }
  kv.put(slot(id), counters);
}

/** Drops the counts of every subscription that is not in `live`. */
export function retain(
  kv: SyncKvStorage,
  id: string,
  live: readonly Subscription[],
  minute: number,
): void {
  kv.put(slot(id), keep(load(kv, id, minute), live));
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

/** The most removal notices kept for one connection; later ones are dropped. */
export const MAX_PENDING = 10;

// Removal notices (event frames) wait here, outside the socket attachment,
// until the client's next message shows that its event gate is open.
function queueSlot(id: string | undefined): string {
  return `n:${String(id)}`;
}

/** Adds removal notices to a connection's queue, dropping any past the limit. */
export function enqueue(
  kv: SyncKvStorage,
  id: string,
  frames: readonly string[],
): void {
  const queued = kv.get<string[]>(queueSlot(id)) ?? [];
  kv.put(queueSlot(id), [...queued, ...frames].slice(0, MAX_PENDING));
}

/** Takes a connection's queued removal notices, leaving its queue empty. */
export function dequeue(kv: SyncKvStorage, id: string | undefined): string[] {
  const queued = kv.get<string[]>(queueSlot(id)) ?? [];
  kv.delete(queueSlot(id));
  return queued;
}

/** Drops the counts and queued notices of a connection that closed. */
export function forget(kv: SyncKvStorage, id: string | undefined): void {
  kv.delete(slot(id));
  kv.delete(queueSlot(id));
}

/** Drops the counts and queued notices of every connection not in `live`. */
export function forgetOthers(
  kv: SyncKvStorage,
  live: ReadonlySet<string>,
): void {
  for (const prefix of ["c:", "n:"]) {
    for (const [name] of [...kv.list({ prefix })]) {
      if (!live.has(name.slice(2))) kv.delete(name);
    }
  }
}
