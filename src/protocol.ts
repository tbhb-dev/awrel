// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

/**
 * What a connection asked to receive. New GitHub subscriptions require an
 * event name and repository; older stored subscriptions can omit either.
 * Repositories are compared without case, as GitHub compares names.
 */
export interface Subscription {
  readonly provider: string;
  readonly event?: string;
  readonly repository?: string;
  readonly action?: string[];
  readonly conclusion?: string[];
  readonly name?: string;
  readonly workflow_path?: string;
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
  readonly subscription_filters?: true;
}

/** What a client message calls for: a reply, and the new list if it changed. */
export type Outcome =
  | { readonly reply: ErrorReply | SubscriptionsReply; readonly next: null }
  | {
      readonly reply: SubscriptionsReply;
      readonly next: readonly Subscription[];
    };

const ATTACHMENT_BUDGET = 12288;

/** The UTF-8 byte count of a string. */
export function utf8Bytes(value: string): number {
  return new TextEncoder().encode(value).length;
}

/** The `limit` error for a change the attachment refused. */
export function limit(id: string): ErrorReply {
  return {
    type: "error",
    id,
    code: "limit",
    message: `subscriptions would exceed ${String(ATTACHMENT_BUDGET)} bytes`,
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
    a.repository === b.repository &&
    JSON.stringify(a.action) === JSON.stringify(b.action) &&
    JSON.stringify(a.conclusion) === JSON.stringify(b.conclusion) &&
    a.name === b.name &&
    a.workflow_path === b.workflow_path
  );
}

// Explicit support: new events need an entry, while new action values do not.
// https://docs.github.com/en/webhooks/webhook-events-and-payloads
const ACTION_EVENTS = new Set([
  "branch_protection_configuration",
  "branch_protection_rule",
  "check_run",
  "check_suite",
  "code_scanning_alert",
  "commit_comment",
  "custom_property",
  "custom_property_values",
  "dependabot_alert",
  "deploy_key",
  "deployment",
  "deployment_protection_rule",
  "deployment_review",
  "deployment_status",
  "discussion",
  "discussion_comment",
  "github_app_authorization",
  "installation",
  "installation_repositories",
  "installation_target",
  "issue_comment",
  "issue_dependencies",
  "issue_relates_to",
  "issues",
  "label",
  "marketplace_purchase",
  "member",
  "membership",
  "merge_group",
  "meta",
  "milestone",
  "org_block",
  "organization",
  "package",
  "personal_access_token_request",
  "project",
  "project_card",
  "project_column",
  "projects_v2",
  "projects_v2_item",
  "projects_v2_status_update",
  "pull_request",
  "pull_request_review",
  "pull_request_review_comment",
  "pull_request_review_thread",
  "registry_package",
  "release",
  "repository",
  "repository_advisory",
  "repository_dispatch",
  "repository_ruleset",
  "repository_vulnerability_alert",
  "secret_scanning_alert",
  "secret_scanning_alert_location",
  "secret_scanning_scan",
  "security_advisory",
  "sponsorship",
  "star",
  "sub_issues",
  "team",
  "watch",
  "workflow_job",
  "workflow_run",
]);

const FILTERS = {
  action: ACTION_EVENTS,
  conclusion: new Set([
    "check_run",
    "check_suite",
    "workflow_run",
    "workflow_job",
  ]),
  name: new Set(["check_run", "workflow_run", "workflow_job"]),
  workflow_path: new Set(["workflow_run"]),
};

type Filter = keyof typeof FILTERS;

function filterError(field: Filter, input: unknown): string | null {
  if (field === "action" || field === "conclusion") {
    if (
      !Array.isArray(input) ||
      input.length < 1 ||
      input.length > 8 ||
      !input.every(
        (item: unknown) =>
          typeof item === "string" && /^[a-z0-9_.-]{1,64}(?![\s\S])/.test(item),
      )
    ) {
      return `${field} must be a list of 1 to 8 strings matching ^[a-z0-9_.-]{1,64}$`;
    }
  } else {
    const pattern =
      field === "name" ? "^[\\x20-\\x7e]{1,128}$" : "^[A-Za-z0-9_./-]{1,256}$";
    // Require the absolute end: JavaScript's $ can also precede a final newline.
    if (
      typeof input !== "string" ||
      !new RegExp(`${pattern}(?![\\s\\S])`).test(input)
    ) {
      return `${field} must match ${pattern}`;
    }
  }
  return null;
}

function parseFilters(
  id: string,
  value: Record<string, unknown>,
  subscription: Subscription,
): Subscription | Outcome {
  const filters: {
    action?: string[];
    conclusion?: string[];
    name?: string;
    workflow_path?: string;
  } = {};
  for (const field of Object.keys(FILTERS) as Filter[]) {
    const input = value[field];
    if (input === undefined) continue;
    if (subscription.provider !== "github") {
      return fail(
        id,
        "bad_subscription",
        `${field} is supported only for github`,
      );
    }
    if (
      subscription.event === undefined ||
      subscription.repository === undefined
    ) {
      return fail(
        id,
        "bad_subscription",
        "filtered subscriptions require event and repository",
      );
    }
    if (!FILTERS[field].has(subscription.event)) {
      return fail(
        id,
        "bad_subscription",
        `${field} is not supported for github event ${subscription.event}`,
      );
    }
    const message = filterError(field, input);
    if (message !== null) return fail(id, "bad_subscription", message);
    if (field === "action" || field === "conclusion") {
      filters[field] = [...new Set(input as string[])].sort();
    } else {
      filters[field] = input as string;
    }
  }
  return { ...subscription, ...filters };
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
  return parseFilters(id, value, {
    provider,
    ...(event === undefined ? {} : { event }),
    ...(repository === undefined
      ? {}
      : { repository: repository.toLowerCase() }),
  });
}

interface Request {
  readonly type: "subscribe" | "unsubscribe" | "list";
  readonly id: string;
  readonly subscription: unknown;
  readonly subscription_filters: boolean;
}

/** The request a client message holds, or the error reply it earns. */
function read(message: string | ArrayBuffer): Outcome | Request {
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
  const { type, id: rawId, subscription, subscription_filters } = parsed;
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
  return {
    type,
    id,
    subscription,
    subscription_filters: subscription_filters === true,
  };
}

/** The reply to one client message, and the new subscription list when it changed. */
export function handle(
  current: readonly Subscription[],
  message: string | ArrayBuffer,
): Outcome {
  const request = read(message);
  if ("reply" in request) {
    return request;
  }
  const { type, id } = request;
  const capability = request.subscription_filters
    ? { subscription_filters: true as const }
    : {};
  const unchanged = (): Outcome => ({
    reply: { type: "subscriptions", id, subscriptions: current, ...capability },
    next: null,
  });
  if (type === "list") {
    return unchanged();
  }
  const subscription = parse(id, request.subscription);
  if ("reply" in subscription) {
    return subscription;
  }
  const changed = (next: readonly Subscription[]): Outcome => ({
    reply: { type: "subscriptions", id, subscriptions: next, ...capability },
    next,
  });
  if (type === "subscribe") {
    if (subscription.provider === "github") {
      const missing = (["event", "repository"] as const).filter(
        (field) => subscription[field] === undefined,
      );
      if (missing.length !== 0) {
        return fail(
          id,
          "bad_subscription",
          `github subscription is missing ${missing.join(" and ")}; subscribe once per event type and repository`,
        );
      }
    }
    if (current.some((s) => same(s, subscription))) {
      return unchanged();
    }
    const next = [...current, subscription];
    if (
      utf8Bytes(JSON.stringify({ subscriptions: next })) > ATTACHMENT_BUDGET
    ) {
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
