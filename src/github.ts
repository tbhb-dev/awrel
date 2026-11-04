// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import type { Env } from "./env.ts";

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

// The `owner/name` of the repository an event concerns, when it has one.
function repositoryOf(payload: unknown): string | null {
  const name = (payload as { repository?: { full_name?: unknown } } | null)
    ?.repository?.full_name;
  return typeof name === "string" ? name : null;
}

/**
 * Answers a GitHub App webhook delivery: checks its signature against the
 * App's webhook secret, then hands the event to the relay. The signature is
 * checked before anything else in the request is read.
 */
export async function receiveGitHub(
  request: Request,
  env: Env,
): Promise<Response> {
  const body = await request.arrayBuffer();
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
  await env.RELAY.getByName("default").publish({
    provider: "github",
    event,
    delivery,
    repository: repositoryOf(payload),
    payload,
  });
  return new Response(null, { status: 202 });
}
