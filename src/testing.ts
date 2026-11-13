// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import { runInDurableObject } from "cloudflare:test";
import { env, exports } from "cloudflare:workers";
import type { Attachment, Relay } from "./relay.ts";

/** The webhook secret the tests run with; vitest.config.mjs binds the same. */
export const SECRET = "test-webhook-secret";

/** The Grafana secret the tests run with; vitest.config.mjs binds the same. */
export const GRAFANA_SECRET = "test-grafana-secret";

/** The connect token the tests run with; vitest.config.mjs binds the same. */
export const TOKEN = "test-connect-token";

/** The HMAC-SHA256 of `body` under `secret`, as lowercase hex. */
export async function digest(body: string, secret = SECRET): Promise<string> {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const mac = await crypto.subtle.sign(
    "HMAC",
    key,
    new TextEncoder().encode(body),
  );
  return Array.from(new Uint8Array(mac), (b) =>
    b.toString(16).padStart(2, "0"),
  ).join("");
}

/** The relay the Worker publishes to. */
export function relay(): DurableObjectStub<Relay> {
  return env.RELAY.getByName("default");
}

/**
 * Plants a connection in the relay with the given attachment (none for a
 * connection that never subscribed), and returns the messages it receives.
 * Read the array after `settle()`.
 */
export async function connect(
  stub: DurableObjectStub<Relay>,
  attachment: Attachment | null,
): Promise<string[]> {
  return runInDurableObject(stub, (_instance, state) => {
    const messages: string[] = [];
    const pair = new WebSocketPair();
    state.acceptWebSocket(pair[1]);
    if (attachment !== null) {
      pair[1].serializeAttachment(attachment);
    }
    pair[0].accept();
    pair[0].addEventListener("message", (event) => {
      messages.push(event.data as string);
    });
    return messages;
  });
}

/**
 * Closes every connection planted in the relay. The Durable Object outlives
 * a test, so a test file calls this after each test.
 */
export async function disconnectAll(
  stub: DurableObjectStub<Relay>,
): Promise<void> {
  await runInDurableObject(stub, (_instance, state) => {
    for (const socket of state.getWebSockets()) {
      socket.close(1000);
    }
  });
}

/** Waits for the messages sent so far to arrive at the client ends. */
export async function settle(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 20));
}

/** Signs `body` under `secret` as GitHub does: `sha256=<hex>`. */
export async function sign(body: string, secret = SECRET): Promise<string> {
  return `sha256=${await digest(body, secret)}`;
}

/** A relay connection opened through the Worker, and what it has received. */
export interface Client {
  readonly socket: WebSocket;
  readonly received: string[];
}

/** Opens a relay connection through the Worker with a valid handshake. */
export async function dial(): Promise<Client> {
  const response = await exports.default.fetch(
    "https://awrel.example/connect",
    {
      headers: {
        Upgrade: "websocket",
        Authorization: `Bearer ${TOKEN}`,
        "Sec-WebSocket-Protocol": "awrel.v1",
      },
    },
  );
  const socket = response.webSocket;
  if (socket === null) {
    throw new Error(`no WebSocket: status ${String(response.status)}`);
  }
  const received: string[] = [];
  socket.accept();
  socket.addEventListener("message", (event) => {
    received.push(event.data as string);
  });
  return { socket, received };
}

/** Sends one frame and returns the next message the client receives, parsed. */
export async function exchange(
  client: Client,
  frame: string | ArrayBuffer,
): Promise<unknown> {
  const before = client.received.length;
  client.socket.send(frame);
  for (let waited = 0; waited < 1000; waited += 5) {
    const reply = client.received[before];
    if (reply !== undefined) {
      return JSON.parse(reply) as unknown;
    }
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw new Error("no reply");
}
