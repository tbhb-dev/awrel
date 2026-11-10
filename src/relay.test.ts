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

// Seeds stored subscriptions directly, including older subscriptions with omitted fields.
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

it("removes an older event-only subscription from a live socket", async () => {
  const client = await dial();
  const stored = { provider: "github", event: "push" };
  await runInDurableObject(relay(), (_instance, state) => {
    const socket = state.getWebSockets()[0];
    expect(socket).toBeDefined();
    socket?.serializeAttachment({ subscriptions: [stored] });
  });
  expect(await exchange(client, list)).toStrictEqual(
    subscriptions("l", [stored]),
  );
  expect(await relay().publish(EVENT)).toBe(1);
  await settle();
  expect(events(client.received)).toHaveLength(1);
  expect(
    await exchange(
      client,
      JSON.stringify({
        type: "unsubscribe",
        id: "u",
        subscription: stored,
      }),
    ),
  ).toStrictEqual(subscriptions("u", []));
  expect(await relay().publish(EVENT)).toBe(0);
});

it("starts a connection with no subscriptions", async () => {
  const client = await dial();
  expect(await exchange(client, list)).toStrictEqual(subscriptions("l", []));
});

it("delivers an event to a connection that subscribed, without case", async () => {
  const client = await dial();
  const stored = {
    provider: "github",
    event: "push",
    repository: "owner/repo",
  };
  expect(
    await exchange(
      client,
      subscribe("s", {
        provider: "github",
        event: "push",
        repository: "Owner/Repo",
      }),
    ),
  ).toStrictEqual(subscriptions("s", [stored]));
  expect(await relay().publish({ ...EVENT, repository: "OWNER/repo" })).toBe(1);
  await settle();
  expect(events(client.received)).toHaveLength(1);
});

it("keeps each connection's subscriptions apart", async () => {
  const first = await dial();
  const second = await dial();
  await exchange(
    first,
    subscribe("s", {
      provider: "github",
      event: "push",
      repository: "owner/repo",
    }),
  );
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
  const github = {
    provider: "github",
    event: "push",
    repository: "owner/repo",
  };
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
  const names = Array.from({ length: 100 }, (_, i) => ({
    provider: "github",
    event: "push",
    repository: `o/${String(i).padStart(67, "r")}`,
  }));
  for (const [i, name] of names.entries()) {
    await exchange(client, subscribe(`s${String(i)}`, name));
  }
  const last = {
    provider: "github",
    event: "push",
    repository: `o/${"r".repeat(14)}`,
  };
  expect(await exchange(client, subscribe("last", last))).toStrictEqual(
    subscriptions("last", [...names, last]),
  );
  expect(
    await exchange(
      client,
      subscribe("over", {
        provider: "github",
        event: "push",
        repository: `o/${"s".repeat(13)}`,
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
    instance.webSocketMessage(
      socket,
      subscribe("s", {
        provider: "github",
        event: "push",
        repository: "owner/repo",
      }),
    );
  });
  expect(sent).toStrictEqual([JSON.stringify(limit("s"))]);
});

it("reaches the other connections when one send fails", async () => {
  const client = await dial();
  await exchange(
    client,
    subscribe("s", {
      provider: "github",
      event: "push",
      repository: "owner/repo",
    }),
  );
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
  await exchange(
    client,
    subscribe("s", {
      provider: "github",
      event: "push",
      repository: "owner/repo",
    }),
  );
  client.socket.close(1000);
  await settle();
  const open = await runInDurableObject(
    relay(),
    (_instance, state) => state.getWebSockets().length,
  );
  expect(open).toBe(0);
  expect(await relay().publish(EVENT)).toBe(0);
});

// Exercise the matching matrix synchronously; socket delivery is covered above
// and by the overlapping-subscription test below.
async function publishFilteredTo(
  subscription: Subscription,
  event: RelayEvent,
) {
  return runInDurableObject(relay(), (instance, state) => {
    const messages: string[] = [];
    const socket = {
      deserializeAttachment: () => ({ subscriptions: [subscription] }),
      send: (message: string) => messages.push(message),
    } as unknown as WebSocket;
    const original = state.getWebSockets.bind(state);
    state.getWebSockets = () => [socket];
    try {
      return { reached: instance.publish(event), messages };
    } finally {
      state.getWebSockets = original;
    }
  });
}

const FILTERED = {
  provider: "github",
  event: "workflow_run",
  repository: "owner/repo",
  action: ["completed", "requested"],
  conclusion: ["failure", "timed_out"],
  name: " CI / test ",
  workflow_path: ".github/workflows/ci.yml",
};
const WORKFLOW: RelayEvent = {
  ...EVENT,
  event: "workflow_run",
  payload: {
    action: "completed",
    workflow_run: {
      conclusion: "failure",
      name: " CI / test ",
      path: ".github/workflows/ci.yml",
    },
  },
};

it.each(["check_run", "check_suite", "workflow_run", "workflow_job"])(
  "matches %s conclusion from its own object and action from the payload",
  async (event) => {
    const subscription = {
      provider: "github",
      event,
      repository: "owner/repo",
      action: ["created", "completed"],
      conclusion: ["success", "future_value"],
    };
    const { reached, messages } = await publishFilteredTo(subscription, {
      ...EVENT,
      event,
      payload: { action: "completed", [event]: { conclusion: "future_value" } },
    });
    expect(reached).toBe(1);
    expect(messages).toHaveLength(1);
  },
);

it.each(["check_run", "workflow_run", "workflow_job"])(
  "matches exact names on %s",
  async (event) => {
    const { reached } = await publishFilteredTo(
      {
        provider: "github",
        event,
        repository: "owner/repo",
        action: FILTERED.action,
        conclusion: FILTERED.conclusion,
        name: FILTERED.name,
      },
      {
        ...EVENT,
        event,
        payload: {
          action: "requested",
          [event]: { conclusion: "timed_out", name: FILTERED.name },
        },
      },
    );
    expect(reached).toBe(1);
  },
);

it("matches all supplied filters together", async () => {
  const { reached } = await publishFilteredTo(FILTERED, WORKFLOW);
  expect(reached).toBe(1);
});

for (const field of [
  "action",
  "conclusion",
  "name",
  "workflow_path",
] as const) {
  it.each([undefined, null, 1, false, [], {}, "other"])(
    `rejects a missing, non-string or different ${field}: %j`,
    async (value) => {
      const object = {
        conclusion: "failure",
        name: FILTERED.name,
        path: FILTERED.workflow_path,
      };
      const payload =
        field === "action"
          ? { action: value, workflow_run: object }
          : {
              action: "completed",
              workflow_run: {
                ...object,
                [field === "workflow_path" ? "path" : field]: value,
              },
            };
      const { reached, messages } = await publishFilteredTo(FILTERED, {
        ...WORKFLOW,
        payload,
      });
      expect(reached).toBe(0);
      expect(messages).toEqual([]);
    },
  );
}

it.each([undefined, null, 1, false, "text"])(
  "ignores absent or non-object payload %j without filters",
  async (payload) => {
    const { reached } = await publishFilteredTo(
      { provider: "github" },
      { ...EVENT, payload },
    );
    expect(reached).toBe(1);
  },
);
it.each([undefined, null, 1, false, "text"])(
  "cannot match filters on an absent or non-object event object %j",
  async (object) => {
    const { reached } = await publishFilteredTo(FILTERED, {
      ...WORKFLOW,
      payload: { action: "completed", workflow_run: object },
    });
    expect(reached).toBe(0);
  },
);

it("does not use a nested check suite conclusion in a check run", async () => {
  const { reached } = await publishFilteredTo(
    {
      provider: "github",
      event: "check_run",
      repository: "owner/repo",
      conclusion: ["failure"],
    },
    {
      ...EVENT,
      event: "check_run",
      payload: {
        check_run: { conclusion: null, check_suite: { conclusion: "failure" } },
        check_suite: { conclusion: "failure" },
        conclusion: "failure",
      },
    },
  );
  expect(reached).toBe(0);
});

it.each([
  { name: "ci / test" },
  { name: "CI / test" },
  { name: " CI / test" },
  { name: "*" },
  { name: "test" },
  { workflow_path: ".github/workflows/CI.yml" },
  { workflow_path: "ci.yml" },
  { workflow_path: "./.github/workflows/ci.yml" },
])("keeps names and paths exact: %j", async (changed) => {
  const { reached } = await publishFilteredTo(
    { ...FILTERED, ...changed },
    WORKFLOW,
  );
  expect(reached).toBe(0);
});

it("delivers overlapping filtered subscriptions once to each matching socket", async () => {
  const stub = relay();
  const first = await connect(stub, {
    subscriptions: [
      FILTERED,
      { ...FILTERED, conclusion: ["failure"] },
      { provider: "github" },
    ],
  });
  const second = await connect(stub, { subscriptions: [FILTERED] });
  await connect(stub, { subscriptions: [{ ...FILTERED, name: "other" }] });
  expect(await stub.publish(WORKFLOW)).toBe(2);
  await settle();
  expect(first).toHaveLength(1);
  expect(second).toHaveLength(1);
});

it("stores canonical filters, echoes capability, and preserves the attachment on invalid filters", async () => {
  const client = await dial();
  const canonical = {
    ...FILTERED,
    action: ["completed", "requested"],
    conclusion: ["failure", "timed_out"],
  };
  const request = (type: string, subscription: unknown, flag = true) =>
    JSON.stringify({ type, id: "f", subscription, subscription_filters: flag });
  const reply = {
    ...subscriptions("f", [canonical]),
    subscription_filters: true,
  };
  expect(
    await exchange(
      client,
      request("subscribe", {
        ...FILTERED,
        repository: "OWNER/Repo",
        action: ["requested", "completed", "requested"],
      }),
    ),
  ).toStrictEqual(reply);
  for (const invalid of [
    { ...FILTERED, action: null },
    { ...FILTERED, event: "push" },
    { ...FILTERED, provider: "gitlab" },
    { ...FILTERED, repository: undefined },
  ]) {
    expect(await exchange(client, request("subscribe", invalid))).toMatchObject(
      { type: "error", code: "bad_subscription" },
    );
    expect(await exchange(client, request("list", undefined))).toStrictEqual(
      reply,
    );
  }
  expect(await exchange(client, list)).toStrictEqual(
    subscriptions("l", [canonical]),
  );
  expect(
    await exchange(client, request("unsubscribe", canonical)),
  ).toStrictEqual({ ...subscriptions("f", []), subscription_filters: true });
});

it.each(["check_run", "check_suite", "workflow_run", "workflow_job"])(
  "rejects absent and non-string fields on %s without consulting other objects",
  async (event) => {
    await runInDurableObject(relay(), (instance, state) => {
      const subscription: Subscription = {
        provider: "github",
        event,
        repository: "owner/repo",
        conclusion: ["failure"],
      };
      const sent: string[] = [];
      const socket = {
        deserializeAttachment: () => ({ subscriptions: [subscription] }),
        send: (message: string) => sent.push(message),
      } as unknown as WebSocket;
      const original = state.getWebSockets.bind(state);
      state.getWebSockets = () => [socket];
      try {
        for (const conclusion of [
          undefined,
          null,
          0,
          false,
          {},
          [],
          "success",
        ]) {
          expect(
            instance.publish({
              ...EVENT,
              event,
              payload: { [event]: { conclusion }, conclusion: "failure" },
            }),
          ).toBe(0);
        }
        expect(sent).toEqual([]);
        expect(
          instance.publish({
            ...EVENT,
            event,
            payload: { [event]: { conclusion: "failure" } },
          }),
        ).toBe(1);
        expect(sent).toHaveLength(1);
      } finally {
        state.getWebSockets = original;
      }
    });
  },
);

it("extracts payload filters once per publication and reuses them across sockets", async () => {
  await runInDurableObject(relay(), (instance, state) => {
    const reads = { action: 0, object: 0, conclusion: 0, name: 0, path: 0 };
    const payload = {
      get action() {
        reads.action += 1;
        return "completed";
      },
      get workflow_run() {
        reads.object += 1;
        return {
          get conclusion() {
            reads.conclusion += 1;
            return "failure";
          },
          get name() {
            reads.name += 1;
            return FILTERED.name;
          },
          get path() {
            reads.path += 1;
            return FILTERED.workflow_path;
          },
        };
      },
    };
    const sent: string[] = [];
    const socket = {
      deserializeAttachment: () => ({ subscriptions: [FILTERED] }),
      send: (message: string) => sent.push(message),
    } as unknown as WebSocket;
    const original = state.getWebSockets.bind(state);
    state.getWebSockets = () => [socket, socket, socket];
    try {
      expect(instance.publish({ ...WORKFLOW, payload })).toBe(3);
      expect(sent).toHaveLength(3);
      // Each property is read once to serialize the event, and once to match it.
      expect(reads).toStrictEqual({
        action: 2,
        object: 2,
        conclusion: 2,
        name: 2,
        path: 2,
      });
    } finally {
      state.getWebSockets = original;
    }
  });
});

it("does not test omitted filters even when the payload has those fields", async () => {
  const { reached } = await publishFilteredTo(
    { provider: "github", event: "workflow_run", repository: "owner/repo" },
    WORKFLOW,
  );
  expect(reached).toBe(1);
});

it("echoes only to the requesting socket, before acknowledging, without matching or changing subscriptions", async () => {
  const other = await dial();
  await exchange(other, subscribe("s", { provider: "awrel" }));
  other.received.length = 0;
  const client = await dial();
  const before = Date.now();
  const event = await exchange(
    client,
    '{"type":"echo","id":"e","nonce":"nonce_1-ABC"}',
  );
  await settle();
  const frames: unknown[] = client.received.map((message): unknown =>
    JSON.parse(message),
  );
  expect(frames).toStrictEqual([event, { type: "echoed", id: "e" }]);
  const timestamp = (event as { payload: { relay_received_at: string } })
    .payload.relay_received_at;
  expect(event).toStrictEqual({
    type: "event",
    provider: "awrel",
    event: "echo",
    delivery: "nonce_1-ABC",
    payload: { echo: "nonce_1-ABC", relay_received_at: timestamp },
  });
  expect(new Date(timestamp).toISOString()).toBe(timestamp);
  expect(timestamp.endsWith("Z")).toBe(true);
  expect(Date.parse(timestamp)).toBeGreaterThanOrEqual(before);
  expect(Date.parse(timestamp)).toBeLessThanOrEqual(Date.now());
  expect(other.received).toStrictEqual([]);
  expect(await exchange(client, list)).toStrictEqual(subscriptions("l", []));
  expect(await exchange(other, list)).toStrictEqual(
    subscriptions("l", [{ provider: "awrel" }]),
  );
});
