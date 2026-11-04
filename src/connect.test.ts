// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import { env, exports } from "cloudflare:workers";
import { afterEach, expect, it } from "vitest";
import { disconnectAll, relay, TOKEN } from "./testing.ts";

afterEach(() => disconnectAll(relay()));

const VALID = {
  Upgrade: "websocket",
  Authorization: `Bearer ${TOKEN}`,
  "Sec-WebSocket-Protocol": "awrel.v1",
};

// Requests /connect with the valid headers, less those named in `omit`,
// plus `headers`.
async function connectWith(
  headers: Record<string, string> = {},
  omit: string[] = [],
): Promise<Response> {
  const merged = Object.entries({ ...VALID, ...headers }).filter(
    ([name]) => !omit.includes(name),
  );
  return exports.default.fetch("https://awrel.example/connect", {
    headers: merged,
  });
}

// The status and the Worker's version header, which every answer carries.
function expectAnswer(response: Response, status: number): void {
  expect(response.status).toBe(status);
  expect(response.headers.get("Awrel-Worker-Version")).toBe(
    env.CF_VERSION_METADATA.id,
  );
}

it("upgrades a valid request", async () => {
  const response = await connectWith();
  expectAnswer(response, 101);
  expect(response.headers.get("Sec-WebSocket-Protocol")).toBe("awrel.v1");
  expect(response.webSocket).not.toBeNull();
});

it.each([
  ["a capitalized Upgrade value", { Upgrade: "WebSocket" }],
  ["several subprotocols", { "Sec-WebSocket-Protocol": "chat, awrel.v1" }],
])("upgrades with %s", async (_name, headers) => {
  expectAnswer(await connectWith(headers), 101);
});

it.each([
  ["no Upgrade", {}, ["Upgrade"]],
  ["another Upgrade", { Upgrade: "h2c" }, []],
  ["no Upgrade and no Authorization", {}, ["Upgrade", "Authorization"]],
])("asks for an upgrade on %s", async (_name, headers, omit) => {
  const response = await connectWith(headers, omit);
  expectAnswer(response, 426);
  expect(await response.text()).toBe("Upgrade Required");
  expect(response.headers.get("Upgrade")).toBe("websocket");
});

it.each([
  ["no Authorization", {}, ["Authorization"]],
  ["a Basic scheme", { Authorization: `Basic ${TOKEN}` }, []],
  ["a lowercase scheme", { Authorization: `bearer ${TOKEN}` }, []],
  ["an empty token", { Authorization: "Bearer " }, []],
  ["a wrong token", { Authorization: "Bearer wrong" }, []],
  ["a longer token", { Authorization: `Bearer ${TOKEN}x` }, []],
  [
    "no Authorization and no subprotocol",
    {},
    ["Authorization", "Sec-WebSocket-Protocol"],
  ],
])("refuses %s", async (_name, headers, omit) => {
  const response = await connectWith(headers, omit);
  expectAnswer(response, 401);
  expect(await response.text()).toBe("Unauthorized");
});

it.each([
  ["no subprotocol", {}, ["Sec-WebSocket-Protocol"]],
  ["an older subprotocol", { "Sec-WebSocket-Protocol": "awrel.v0" }, []],
  ["a newer subprotocol", { "Sec-WebSocket-Protocol": "awrel.v10" }, []],
])("rejects %s", async (_name, headers, omit) => {
  const response = await connectWith(headers, omit);
  expectAnswer(response, 400);
  expect(await response.text()).toBe("Bad Request");
});

// Under exports.default.fetch, a request with an Upgrade header answers as a
// GET whatever its method (observed, not documented), so this one leaves
// the upgrade headers out.
it("answers another method with not found", async () => {
  const response = await exports.default.fetch(
    "https://awrel.example/connect",
    { method: "POST" },
  );
  expectAnswer(response, 404);
});
