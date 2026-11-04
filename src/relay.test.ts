// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import { evictDurableObject, runInDurableObject } from "cloudflare:test";
import { afterEach, expect, it } from "vitest";
import { limit, type Subscription } from "./protocol.ts";
import type { RelayEvent } from "./relay.ts";
import {
  connect,
  dial,
  disconnectAll,
  exchange,
  relay,
  settle,
} from "./testing.ts";

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

// The messages of a client's `received` that are webhook events.
function events(received: string[]): unknown[] {
  return received
    .map((m): unknown => JSON.parse(m))
    .filter((m) => (m as { type: string }).type === "event");
}

const subscribe = (id: string, subscription: Subscription): string =>
  JSON.stringify({ type: "subscribe", id, subscription });

const list = '{"type":"list","id":"l"}';

const subscriptions = (id: string, entries: Subscription[]) => ({
  type: "subscriptions",
  id,
  subscriptions: entries,
});

it("starts a connection with no subscriptions", async () => {
  const client = await dial();
  expect(await exchange(client, list)).toStrictEqual(subscriptions("l", []));
});

it("delivers an event to a connection that subscribed, without case", async () => {
  const client = await dial();
  const stored = { provider: "github", repository: "owner/repo" };
  expect(
    await exchange(
      client,
      subscribe("s", { provider: "github", repository: "Owner/Repo" }),
    ),
  ).toStrictEqual(subscriptions("s", [stored]));
  expect(await relay().publish({ ...EVENT, repository: "OWNER/repo" })).toBe(1);
  await settle();
  expect(events(client.received)).toHaveLength(1);
});

it("keeps each connection's subscriptions apart", async () => {
  const first = await dial();
  const second = await dial();
  await exchange(first, subscribe("s", { provider: "github" }));
  expect(await exchange(second, list)).toStrictEqual(subscriptions("l", []));
  expect(await relay().publish(EVENT)).toBe(1);
  await settle();
  expect(events(first.received)).toHaveLength(1);
  expect(events(second.received)).toHaveLength(0);
});

it("changes nothing on a bad subscribe", async () => {
  const client = await dial();
  expect(
    await exchange(client, subscribe("s", { provider: "GitHub" })),
  ).toMatchObject({ type: "error", code: "bad_subscription" });
  expect(await exchange(client, list)).toStrictEqual(subscriptions("l", []));
});

it("answers a ping with a pong, even after eviction", async () => {
  const client = await dial();
  const ping = '{"type":"ping"}';
  expect(await exchange(client, ping)).toStrictEqual({ type: "pong" });
  await evictDurableObject(relay());
  expect(await exchange(client, ping)).toStrictEqual({ type: "pong" });
});

it("keeps subscriptions across an eviction", async () => {
  const client = await dial();
  const github = { provider: "github" };
  await exchange(client, subscribe("s", github));
  await evictDurableObject(relay());
  expect(await exchange(client, list)).toStrictEqual(
    subscriptions("l", [github]),
  );
  expect(await relay().publish(EVENT)).toBe(1);
});

it("answers a binary frame with an error and stays open", async () => {
  const client = await dial();
  expect(await exchange(client, new ArrayBuffer(3))).toMatchObject({
    type: "error",
    code: "bad_message",
  });
  expect(await exchange(client, list)).toStrictEqual(subscriptions("l", []));
});

it("refuses a change past the budget and keeps the list", async () => {
  const client = await dial();
  const names = Array.from({ length: 12 }, (_, i) => ({
    provider: "github",
    repository: `o/${String(i).padStart(100, "r")}`,
  }));
  for (const [i, name] of names.entries()) {
    await exchange(client, subscribe(`s${String(i)}`, name));
  }
  const last = { provider: "github", repository: `o/${"r".repeat(53)}` };
  await exchange(client, subscribe("last", last));
  expect(
    await exchange(
      client,
      subscribe("over", {
        provider: "github",
        repository: `o/${"s".repeat(53)}`,
      }),
    ),
  ).toStrictEqual(limit("over"));
  expect(await exchange(client, list)).toStrictEqual(
    subscriptions("l", [...names, last]),
  );
});

it("answers limit when the attachment refuses the list", async () => {
  const sent: string[] = [];
  const socket = {
    deserializeAttachment: () => ({ subscriptions: [] }),
    serializeAttachment: () => {
      throw new Error("too large");
    },
    send: (message: string) => sent.push(message),
  } as unknown as WebSocket;
  await runInDurableObject(relay(), (instance) => {
    instance.webSocketMessage(socket, subscribe("s", { provider: "github" }));
  });
  expect(sent).toStrictEqual([JSON.stringify(limit("s"))]);
});

it("reaches the other connections when one send fails", async () => {
  const client = await dial();
  await exchange(client, subscribe("s", { provider: "github" }));
  const broken = {
    deserializeAttachment: () => ({ subscriptions: [{ provider: "github" }] }),
    send: () => {
      throw new Error("closing");
    },
  } as unknown as WebSocket;
  const reached = await runInDurableObject(relay(), (instance, state) => {
    const original = state.getWebSockets.bind(state);
    state.getWebSockets = () => [broken, ...original()];
    try {
      return instance.publish(EVENT);
    } finally {
      state.getWebSockets = original;
    }
  });
  expect(reached).toBe(1);
  await settle();
  expect(events(client.received)).toHaveLength(1);
});

it("forgets a connection that closed", async () => {
  const client = await dial();
  await exchange(client, subscribe("s", { provider: "github" }));
  client.socket.close(1000);
  await settle();
  const open = await runInDurableObject(
    relay(),
    (_instance, state) => state.getWebSockets().length,
  );
  expect(open).toBe(0);
  expect(await relay().publish(EVENT)).toBe(0);
});
