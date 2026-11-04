// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import { env, exports } from "cloudflare:workers";
import { expect, it } from "vitest";

it("answers every request with not found", async () => {
  const response = await exports.default.fetch("https://awrel.example/x", {
    method: "POST",
  });
  expect(response.status).toBe(404);
  expect(await response.text()).toBe("Not Found");
});

// The header name is the deploy check's contract (e2e/check.mjs), so the
// test spells it out rather than importing it.
it("names the version that answered", async () => {
  const response = await exports.default.fetch("https://awrel.example/");
  expect(response.headers.get("Awrel-Worker-Version")).toBe(
    env.CF_VERSION_METADATA.id,
  );
});

it("binds the relay Durable Object", () => {
  const id = env.RELAY.idFromName("default");
  expect(env.RELAY.get(id).id.equals(id)).toBe(true);
});
