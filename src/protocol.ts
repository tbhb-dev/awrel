// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

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

interface ErrorReply {
  readonly type: "error";
  readonly id: string | null;
  readonly code:
    | "bad_message"
    | "bad_subscription"
    | "not_subscribed"
    | "limit";
  readonly message: string;
}

interface SubscriptionsReply {
  readonly type: "subscriptions";
  readonly id: string;
  readonly subscriptions: readonly Subscription[];
}

/** What a client message calls for: a reply, and the new list if it changed. */
export type Outcome =
  | { readonly reply: ErrorReply | SubscriptionsReply; readonly next: null }
  | {
      readonly reply: SubscriptionsReply;
      readonly next: readonly Subscription[];
    };

/** The `limit` error for a change the attachment refused. */
export function limit(id: string): ErrorReply {
  return {
    type: "error",
    id,
    code: "limit",
    message: "subscriptions would exceed 1792 bytes",
  };
}

function fail(
  id: string | null,
  code: ErrorReply["code"],
  message: string,
): Outcome {
  return { reply: { type: "error", id, code, message }, next: null };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

// Both sides are already normalized, so equality is field by field.
function same(a: Subscription, b: Subscription): boolean {
  return (
    a.provider === b.provider &&
    a.event === b.event &&
    a.repository === b.repository
  );
}

// The subscription in `value`, normalized, or the failure to report.
function parse(id: string, value: unknown): Subscription | Outcome {
  if (!isRecord(value)) {
    return fail(id, "bad_message", "subscription must be an object");
  }
  const { provider, event, repository } = value;
  if (typeof provider !== "string") {
    return fail(id, "bad_message", "provider must be a string");
  }
  if (!/^[a-z0-9-]{1,32}$/.test(provider)) {
    return fail(
      id,
      "bad_subscription",
      "provider must match ^[a-z0-9-]{1,32}$",
    );
  }
  if (event !== undefined) {
    if (typeof event !== "string") {
      return fail(id, "bad_message", "event must be a string");
    }
    if (!/^[a-z0-9_.-]{1,64}$/.test(event)) {
      return fail(
        id,
        "bad_subscription",
        "event must match ^[a-z0-9_.-]{1,64}$",
      );
    }
  }
  if (repository !== undefined && typeof repository !== "string") {
    return fail(id, "bad_message", "repository must be a string");
  }
  if (
    repository !== undefined &&
    !/^[A-Za-z0-9_.-]{1,39}\/[A-Za-z0-9_.-]{1,100}$/.test(repository)
  ) {
    return fail(id, "bad_subscription", "repository must be owner/name");
  }
  return {
    provider,
    ...(event === undefined ? {} : { event }),
    ...(repository === undefined
      ? {}
      : { repository: repository.toLowerCase() }),
  };
}

/** The reply to one client message, and the new subscription list when it changed. */
export function handle(
  current: readonly Subscription[],
  message: string | ArrayBuffer,
): Outcome {
  if (typeof message !== "string") {
    return fail(null, "bad_message", "messages must be text frames");
  }
  if (new TextEncoder().encode(message).byteLength > 4096) {
    return fail(null, "bad_message", "messages must be at most 4096 bytes");
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(message);
  } catch {
    return fail(null, "bad_message", "messages must be JSON");
  }
  if (!isRecord(parsed)) {
    return fail(null, "bad_message", "messages must be JSON objects");
  }
  const { type, id: rawId, subscription: rawSubscription } = parsed;
  const id =
    typeof rawId === "string" && rawId.length >= 1 && rawId.length <= 64
      ? rawId
      : null;
  if (type !== "subscribe" && type !== "unsubscribe" && type !== "list") {
    return fail(id, "bad_message", "unknown message type");
  }
  if (id === null) {
    return fail(
      null,
      "bad_message",
      "id must be a string of 1 to 64 characters",
    );
  }
  const unchanged = (): Outcome => ({
    reply: { type: "subscriptions", id, subscriptions: current },
    next: null,
  });
  if (type === "list") {
    return unchanged();
  }
  const subscription = parse(id, rawSubscription);
  if ("reply" in subscription) {
    return subscription;
  }
  const changed = (next: readonly Subscription[]): Outcome => ({
    reply: { type: "subscriptions", id, subscriptions: next },
    next,
  });
  if (type === "subscribe") {
    if (current.some((s) => same(s, subscription))) {
      return unchanged();
    }
    const next = [...current, subscription];
    if (JSON.stringify({ subscriptions: next }).length > 1792) {
      return { reply: limit(id), next: null };
    }
    return changed(next);
  }
  const next = current.filter((s) => !same(s, subscription));
  if (next.length === current.length) {
    return fail(id, "not_subscribed", "no such subscription");
  }
  return changed(next);
}
