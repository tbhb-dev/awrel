// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import type { Env } from "./env.ts";

const MAX_BODY_BYTES = 1_048_576;
const HOST_NAME_PATTERN = /^[A-Za-z0-9_.-]{1,100}$/;

// Whether `header` is the 64-digit hex HMAC-SHA256 of `body` under `secret`.
// The check is Web Crypto's verify, which compares in constant time. The
// digest must be 64 hex digits, so a trailing odd digit or `sha256=` prefix
// can't ride along; non-hex digits decode to bytes that can't match.
async function signed(
  secret: string,
  body: ArrayBuffer,
  header: string | null,
): Promise<boolean> {
  if (header?.length !== 64) {
    return false;
  }
  const signature = Uint8Array.from({ length: header.length >> 1 }, (_, i) =>
    Number.parseInt(header.slice(2 * i, 2 * i + 2), 16),
  );
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    // Stryker disable next-line BooleanLiteral: a key's extractability isn't observable through verify, so false and true behave the same here
    false,
    ["verify"],
  );
  return crypto.subtle.verify("HMAC", key, signature, body);
}

// Deterministic delivery ID `gf-<32 hex>` from SHA-256(body || 0x3a || i), so
// Grafana retries with the same body yield the same delivery ID per index.
async function deliveryId(body: ArrayBuffer, index: number): Promise<string> {
  const suffix = new TextEncoder().encode(`:${String(index)}`);
  const input = new Uint8Array(body.byteLength + suffix.byteLength);
  input.set(new Uint8Array(body), 0);
  input.set(suffix, body.byteLength);
  const hash = await crypto.subtle.digest("SHA-256", input);
  const hex = Array.from(new Uint8Array(hash, 0, 16), (b) =>
    b.toString(16).padStart(2, "0"),
  ).join("");
  return `gf-${hex}`;
}

interface GrafanaAlert {
  readonly event: "alert.firing" | "alert.resolved";
  readonly repository: string;
  readonly payload: Record<string, string | undefined>;
}

// The string at `key` of a parsed JSON value, or undefined when the value
// isn't an object or holds anything else there. Indexing a primitive gives
// undefined, so only null and undefined need the optional chain.
function member(source: unknown, key: string): unknown {
  return (source as Record<string, unknown> | null | undefined)?.[key];
}

function text(source: unknown, key: string): string | undefined {
  const value = member(source, key);
  return typeof value === "string" ? value : undefined;
}

// Extracts the relay event fields for a single Grafana alert entry, or returns
// null when the entry is malformed, targets an invalid host_name, or has a
// status other than firing/resolved. Only the allowlisted string fields are
// copied so annotations and extra labels never leak into the relay payload.
function toAlert(entry: unknown): GrafanaAlert | null {
  const raw = entry;
  const labels = member(raw, "labels");
  const status = text(raw, "status");
  const hostName = text(labels, "host_name");
  if (
    (status !== "firing" && status !== "resolved") ||
    hostName === undefined ||
    !HOST_NAME_PATTERN.test(hostName)
  ) {
    return null;
  }
  const link = text(raw, "panelURL") || text(raw, "dashboardURL") || undefined;
  const fields = {
    alertname: text(labels, "alertname"),
    status,
    host: hostName,
    value: text(raw, "valueString"),
    starts_at: text(raw, "startsAt"),
    ends_at: text(raw, "endsAt"),
    fingerprint: text(raw, "fingerprint"),
    html_url: link,
  };
  const payload = Object.fromEntries(
    // Stryker disable next-line ConditionalExpression,MethodExpression: a kept undefined value is dropped when the relay serializes the event, so no test can tell
    Object.entries(fields).filter(([, value]) => value !== undefined),
  );
  return {
    event: `alert.${status}`,
    repository: `hosts/${hostName}`,
    payload,
  };
}

/**
 * Answers a Grafana Alerting webhook delivery: rejects oversized payloads,
 * verifies the HMAC-SHA256 signature before parsing, validates the `alerts`
 * envelope, and publishes each valid host alert sequentially to the relay.
 */
export async function receiveGrafana(
  request: Request,
  env: Env,
): Promise<Response> {
  if (Number(request.headers.get("Content-Length")) > MAX_BODY_BYTES) {
    return new Response("Payload Too Large", { status: 413 });
  }
  const body = await request.arrayBuffer();
  if (body.byteLength > MAX_BODY_BYTES) {
    return new Response("Payload Too Large", { status: 413 });
  }
  const header = request.headers.get("X-Grafana-Alerting-Signature");
  if (!(await signed(env.GRAFANA_WEBHOOK_SECRET, body, header))) {
    return new Response("Unauthorized", { status: 401 });
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(new TextDecoder().decode(body));
  } catch {
    console.log("grafana body is not JSON");
    return new Response("Bad Request", { status: 400 });
  }
  const alerts = (parsed as { alerts?: unknown } | null)?.alerts;
  if (!Array.isArray(alerts) || alerts.length === 0 || alerts.length > 100) {
    return new Response("Bad Request", { status: 400 });
  }
  let published = 0;
  let skipped = 0;
  for (let i = 0; i < alerts.length; i++) {
    const alert = toAlert(alerts[i]);
    if (alert === null) {
      skipped++;
      continue;
    }
    await env.RELAY.getByName("default").publish({
      provider: "grafana",
      event: alert.event,
      delivery: await deliveryId(body, i),
      repository: alert.repository,
      payload: alert.payload,
    });
    published++;
  }
  const total = alerts.length;
  console.log(
    `grafana alerts=${String(total)} published=${String(published)} skipped=${String(skipped)}`,
  );
  return new Response(null, { status: 202 });
}
