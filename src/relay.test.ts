// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import { afterEach, expect, it } from "vitest";
import type { RelayEvent, Subscription } from "./relay.ts";
import { connect, disconnectAll, relay, settle } from "./testing.ts";

afterEach(() => disconnectAll(relay()));

const EVENT: RelayEvent = {
  provider: "github",
  event: "push",
  delivery: "d-1",
  repository: "Owner/Repo",
  payload: { ref: "refs/heads/main" },
};

// Publishes `event` to a relay holding one connection with `subscription`,
// and returns the messages the connection received and the count reached.
async function publishTo(
  subscription: Subscription,
  event: RelayEvent = EVENT,
): Promise<{ messages: string[]; reached: number }> {
  const stub = relay();
  const messages = await connect(stub, { subscriptions: [subscription] });
  const reached = await stub.publish(event);
  await settle();
  return { messages, reached };
}

it("sends the event as a JSON message", async () => {
  const { messages, reached } = await publishTo({ provider: "github" });
  expect(reached).toBe(1);
  expect(messages.map((m) => JSON.parse(m) as unknown)).toEqual([
    { type: "event", ...EVENT },
  ]);
});

it("matches an event name and a repository without case", async () => {
  const { reached } = await publishTo({
    provider: "github",
    event: "push",
    repository: "owner/REPO",
  });
  expect(reached).toBe(1);
});

it("skips a subscription for another provider", async () => {
  const { messages, reached } = await publishTo({ provider: "gitlab" });
  expect(reached).toBe(0);
  expect(messages).toEqual([]);
});

it("skips a subscription for another event", async () => {
  const { messages, reached } = await publishTo({
    provider: "github",
    event: "issues",
  });
  expect(reached).toBe(0);
  expect(messages).toEqual([]);
});

it("skips a subscription for another repository", async () => {
  const { messages, reached } = await publishTo({
    provider: "github",
    repository: "owner/other",
  });
  expect(reached).toBe(0);
  expect(messages).toEqual([]);
});

it("skips a repository filter for an event with no repository", async () => {
  const { messages, reached } = await publishTo(
    { provider: "github", repository: "owner/repo" },
    { ...EVENT, repository: null },
  );
  expect(reached).toBe(0);
  expect(messages).toEqual([]);
});

it("delivers an event with no repository to an unfiltered subscription", async () => {
  const { reached } = await publishTo(
    { provider: "github" },
    { ...EVENT, repository: null },
  );
  expect(reached).toBe(1);
});

it("matches when any one subscription does", async () => {
  const stub = relay();
  const messages = await connect(stub, {
    subscriptions: [
      { provider: "gitlab" },
      { provider: "github", event: "push" },
    ],
  });
  expect(await stub.publish(EVENT)).toBe(1);
  await settle();
  expect(messages).toHaveLength(1);
});

it("skips a connection with no attachment", async () => {
  const stub = relay();
  const messages = await connect(stub, null);
  expect(await stub.publish(EVENT)).toBe(0);
  await settle();
  expect(messages).toEqual([]);
});

it("skips a connection with no subscriptions", async () => {
  const stub = relay();
  const messages = await connect(stub, { subscriptions: [] });
  expect(await stub.publish(EVENT)).toBe(0);
  await settle();
  expect(messages).toEqual([]);
});

it("counts every connection it reaches", async () => {
  const stub = relay();
  const first = await connect(stub, {
    subscriptions: [{ provider: "github" }],
  });
  const second = await connect(stub, {
    subscriptions: [{ provider: "github", event: "push" }],
  });
  await connect(stub, { subscriptions: [{ provider: "gitlab" }] });
  expect(await stub.publish(EVENT)).toBe(2);
  await settle();
  expect(first).toHaveLength(1);
  expect(second).toHaveLength(1);
});
