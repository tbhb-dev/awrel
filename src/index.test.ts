// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import { env, exports } from "cloudflare:workers";
import { expect, it } from "vitest";
import { sign } from "./testing.ts";

// The header name is the deploy check's contract (e2e/check.mjs), so the
// test spells it out rather than importing it.
function versionOf(response: Response): string | null {
  return response.headers.get("Awrel-Worker-Version");
}

it("answers any other path with not found", async () => {
  const response = await exports.default.fetch("https://awrel.example/other", {
    method: "POST",
  });
  expect(response.status).toBe(404);
  expect(await response.text()).toBe("Not Found");
  expect(versionOf(response)).toBe(env.CF_VERSION_METADATA.id);
});

it("answers any other method on the webhook path with not found", async () => {
  const response = await exports.default.fetch(
    "https://awrel.example/webhooks/github",
  );
  expect(response.status).toBe(404);
  expect(await response.text()).toBe("Not Found");
  expect(versionOf(response)).toBe(env.CF_VERSION_METADATA.id);
});

it("routes a GitHub webhook to the receiver", async () => {
  const body = "{}";
  const response = await exports.default.fetch(
    "https://awrel.example/webhooks/github",
    {
      method: "POST",
      body,
      headers: {
        "X-Hub-Signature-256": await sign(body),
        "X-GitHub-Event": "push",
        "X-GitHub-Delivery": "d-1",
      },
    },
  );
  expect(response.status).toBe(202);
  expect(await response.text()).toBe("");
  expect(versionOf(response)).toBe(env.CF_VERSION_METADATA.id);
});

it("names the version on a rejected webhook", async () => {
  const response = await exports.default.fetch(
    "https://awrel.example/webhooks/github",
    { method: "POST", body: "{}" },
  );
  expect(response.status).toBe(401);
  expect(versionOf(response)).toBe(env.CF_VERSION_METADATA.id);
});

it("binds the relay Durable Object", () => {
  const id = env.RELAY.idFromName("default");
  expect(env.RELAY.get(id).id.equals(id)).toBe(true);
});
