// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import type { Env } from "./env.ts";

// Whether `presented` is `expected`, in constant time. Both are hashed first,
// so the comparison always sees 32 bytes and leaks neither length.
async function matches(expected: string, presented: string): Promise<boolean> {
  const encoder = new TextEncoder();
  const [a, b] = await Promise.all([
    crypto.subtle.digest("SHA-256", encoder.encode(expected)),
    crypto.subtle.digest("SHA-256", encoder.encode(presented)),
  ]);
  return crypto.subtle.timingSafeEqual(a, b);
}

/**
 * Answers `GET /connect`: checks the WebSocket upgrade, the bearer token and
 * the awrel.v1 subprotocol, all before the relay is touched, then hands the
 * request to the relay, which accepts the socket.
 */
export async function connect(request: Request, env: Env): Promise<Response> {
  if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
    return new Response("Upgrade Required", {
      status: 426,
      headers: { Upgrade: "websocket" },
    });
  }
  const authorization = request.headers.get("Authorization");
  if (
    authorization?.startsWith("Bearer ") !== true ||
    !(await matches(env.CONNECT_TOKEN, authorization.slice("Bearer ".length)))
  ) {
    return new Response("Unauthorized", { status: 401 });
  }
  if (
    request.headers
      .get("Sec-WebSocket-Protocol")
      ?.split(",")
      .some((p) => p.trim() === "awrel.v1") !== true
  ) {
    return new Response("Bad Request", { status: 400 });
  }
  return env.RELAY.getByName("default").fetch(request);
}
