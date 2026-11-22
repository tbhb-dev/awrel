// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import type { Env } from "./env.ts";
import { log } from "./log.ts";

// Whether `header` is the HMAC-SHA256 of `body` under `secret`, in GitHub's
// `sha256=<hex>` form. The check is Web Crypto's verify, which compares in
// constant time. The digest must be 64 hex digits, so a trailing odd digit
// can't ride along; non-hex digits decode to bytes that can't match.
async function signed(
  secret: string,
  body: ArrayBuffer,
  header: string | null,
): Promise<boolean> {
  if (header?.startsWith("sha256=") !== true) {
    return false;
  }
  const hex = header.slice("sha256=".length);
  if (hex.length !== 64) {
    return false;
  }
  const signature = Uint8Array.from({ length: hex.length >> 1 }, (_, i) =>
    Number.parseInt(hex.slice(2 * i, 2 * i + 2), 16),
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

/** The largest body read: GitHub caps webhook payloads at 25 MB. */
export const MAX_BODY_BYTES = 25 * 1024 * 1024;

// The request's body, or null when it is over MAX_BODY_BYTES. A declared
// length over the cap is refused unread; a body without one is read only up
// to the cap, so the Worker never buffers more than the cap.
async function cappedBody(request: Request): Promise<ArrayBuffer | null> {
  const declared = Number(request.headers.get("Content-Length"));
  // A non-numeric length is NaN, which fails this comparison and gets 413.
  if (!(declared <= MAX_BODY_BYTES)) {
    return null;
  }
  const chunks: Uint8Array[] = [];
  let size = 0;
  if (request.body !== null) {
    const reader = (request.body as ReadableStream<Uint8Array>).getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > MAX_BODY_BYTES) {
        await reader.cancel();
        return null;
      }
      chunks.push(value);
    }
  }
  const body = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return body.buffer;
}

// The `owner/name` of the repository an event concerns, when it has one.
function repositoryOf(payload: unknown): string | null {
  const name = (payload as { repository?: { full_name?: unknown } } | null)
    ?.repository?.full_name;
  return typeof name === "string" ? name : null;
}

/**
 * Answers a GitHub App webhook delivery: checks its signature against the
 * App's webhook secret, then hands the event to the relay. The signature is
 * checked before anything else in the request is read. A body over
 * MAX_BODY_BYTES is refused with 413 before that check. Without a secret it
 * answers 503 and reads nothing.
 */
export async function receiveGitHub(
  request: Request,
  env: Env,
): Promise<Response> {
  if (!env.GITHUB_WEBHOOK_SECRET) {
    console.log("github webhook secret is not set");
    return new Response("Service Unavailable", { status: 503 });
  }
  const body = await cappedBody(request);
  if (body === null) {
    return new Response("Payload Too Large", { status: 413 });
  }
  const header = request.headers.get("X-Hub-Signature-256");
  if (!(await signed(env.GITHUB_WEBHOOK_SECRET, body, header))) {
    return new Response("Unauthorized", { status: 401 });
  }
  const event = request.headers.get("X-GitHub-Event");
  const delivery = request.headers.get("X-GitHub-Delivery");
  let payload: unknown;
  try {
    payload = JSON.parse(new TextDecoder().decode(body));
  } catch {
    return new Response("Bad Request", { status: 400 });
  }
  if (event === null || delivery === null) {
    return new Response("Bad Request", { status: 400 });
  }
  // GitHub sends a ping only to test a webhook's configuration, so agents get
  // nothing from it. Answering it here also keeps a signed ping, which the
  // deploy check sends, independent of the Durable Object's version.
  if (event === "ping") {
    return new Response(null, { status: 202 });
  }
  const repository = repositoryOf(payload);
  const reached = await env.RELAY.getByName("default").publish({
    provider: "github",
    event,
    delivery,
    repository,
    payload,
  });
  const action = (payload as { action?: unknown } | null)?.action;
  log(
    `github delivery=${delivery} event=${event} action=${typeof action === "string" ? action : "none"} repository=${repository ?? "none"} reached=${String(reached)}`,
  );
  return new Response(null, { status: 202 });
}
