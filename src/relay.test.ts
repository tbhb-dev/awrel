// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import { evictDurableObject, runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it } from "vitest";
import { charge, delivered } from "./cap.ts";
import { limit, type Subscription, unboundedCheck } from "./protocol.ts";
import {
  type Attachment,
  connectionId,
  GATE_GRACE_MS,
  type RelayEvent,
} from "./relay.ts";
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

const unsubscribe = (id: string, subscription: Subscription): string =>
  JSON.stringify({ type: "unsubscribe", id, subscription });

const list = '{"type":"list","id":"l"}';

const subscriptions = (id: string, entries: Subscription[]) => ({
  type: "subscriptions",
  id,
  subscriptions: entries,
  delivered_last_hour: entries.map(() => 0),
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
    serializeAttachment: () => undefined,
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
      serializeAttachment: () => undefined,
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

it.each(["check_run", "workflow_run", "workflow_job"])(
  "matches %s conclusion from its own object and action from the payload",
  async (event) => {
    const subscription = {
      provider: "github",
      event,
      repository: "owner/repo",
      name: "build",
      action: ["created", "completed"],
      conclusion: ["success", "future_value"],
    };
    const { reached, messages } = await publishFilteredTo(subscription, {
      ...EVENT,
      event,
      payload: {
        action: "completed",
        [event]: { conclusion: "future_value", name: "build" },
      },
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
      name: "build",
      conclusion: ["failure"],
    },
    {
      ...EVENT,
      event: "check_run",
      payload: {
        check_run: {
          name: "build",
          conclusion: null,
          check_suite: { conclusion: "failure" },
        },
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

it.each(["check_run", "workflow_run", "workflow_job"])(
  "rejects absent and non-string fields on %s without consulting other objects",
  async (event) => {
    await runInDurableObject(relay(), (instance, state) => {
      const subscription: Subscription = {
        provider: "github",
        event,
        repository: "owner/repo",
        name: "build",
        conclusion: ["failure"],
      };
      const sent: string[] = [];
      const socket = {
        deserializeAttachment: () => ({ subscriptions: [subscription] }),
        serializeAttachment: () => undefined,
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
              payload: {
                [event]: { conclusion, name: "build" },
                conclusion: "failure",
              },
            }),
          ).toBe(0);
        }
        expect(sent).toEqual([]);
        expect(
          instance.publish({
            ...EVENT,
            event,
            payload: { [event]: { conclusion: "failure", name: "build" } },
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
      serializeAttachment: () => undefined,
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

describe("check subscriptions and the delivery cap", () => {
  const PUSH = {
    provider: "github",
    event: "push",
    repository: "owner/repo",
  };
  const UNNAMED = {
    provider: "github",
    event: "check_run",
    repository: "owner/repo",
  };
  const SUITE = { ...UNNAMED, event: "check_suite" };

  interface Frame {
    type: string;
    event?: string;
    delivery?: string;
  }

  // The frames a client received, parsed.
  const frames = (client: { received: string[] }) =>
    client.received.map((m) => JSON.parse(m) as Frame);

  // Replaces what the relay stored for the only live socket.
  const plant = (attachment: Attachment) =>
    runInDurableObject(relay(), (_instance, state) => {
      state.getWebSockets()[0]?.serializeAttachment(attachment);
    });

  const attachments = () =>
    runInDurableObject(relay(), (_instance, state) =>
      state
        .getWebSockets()
        .map((socket) => socket.deserializeAttachment() as Attachment),
    );

  it("refuses to subscribe to check_run without a name", async () => {
    const client = await dial();
    expect(await exchange(client, subscribe("s", UNNAMED))).toStrictEqual({
      type: "error",
      id: "s",
      code: "bad_subscription",
      message: unboundedCheck(UNNAMED),
    });
    expect(await exchange(client, list)).toStrictEqual(subscriptions("l", []));
  });

  const queued = (id: string) =>
    runInDurableObject(
      relay(),
      (_instance, state) => state.storage.kv.get<string[]>(`n:${id}`) ?? [],
    );

  const connection = async () =>
    String(((await attachments())[0] as Attachment).id);

  const noticeOf = (subscription: Subscription): unknown =>
    expect.objectContaining({
      type: "event",
      provider: "awrel",
      event: "notice",
      payload: {
        code: "subscription_removed",
        reason: "unfiltered",
        message: unboundedCheck(subscription),
        subscription,
        limit: { events: 100, minutes: 5 },
      },
    });

  const afterGrace = () =>
    runInDurableObject(relay(), (instance) => {
      instance.gatedAt = 0;
    });

  const PROBE = '{"type":"list","id":"p","subscription_filters":true}';
  const NAMED = {
    provider: "github",
    event: "pull_request",
    repository: "o/n",
    action: ["opened"],
  };

  it("holds the notices through gated requests, and sends them before the reply to an unfiltered one", async () => {
    const client = await dial();
    await plant({ subscriptions: [UNNAMED, PUSH, SUITE] });
    client.socket.send(PROBE);
    await settle();
    client.socket.send(subscribe("s", NAMED));
    await settle();
    expect(frames(client).map((f) => f.type)).toStrictEqual([
      "subscriptions",
      "subscriptions",
    ]);
    expect(await queued(await connection())).toHaveLength(2);
    // Two lists in the grace after a filter request keep the queue intact.
    client.socket.send(list);
    client.socket.send(list);
    await settle();
    expect(client.received).toHaveLength(4);
    expect(await queued(await connection())).toHaveLength(2);
    await afterGrace();
    client.socket.send(list);
    await settle();
    const [, , , , second, third, last] = frames(client);
    expect(second).toStrictEqual(noticeOf(UNNAMED));
    expect(third).toStrictEqual(noticeOf(SUITE));
    expect(last?.type).toBe("subscriptions");
    expect(second?.delivery).toMatch(/^[0-9a-f-]{36}$/);
    expect(client.received).toHaveLength(7);
    expect(await queued(await connection())).toStrictEqual([]);
  });

  it("sends the notices once, before the reply to an unfiltered subscribe", async () => {
    const client = await dial();
    await plant({ subscriptions: [SUITE] });
    client.socket.send(subscribe("s", PUSH));
    await settle();
    expect(frames(client).map((f) => f.event ?? f.type)).toStrictEqual([
      "notice",
      "subscriptions",
    ]);
    // A second send covers a client that dropped the first.
    client.socket.send(list);
    await settle();
    expect(client.received).toHaveLength(4);
    client.socket.send(list);
    await settle();
    expect(client.received).toHaveLength(5);
  });

  it("holds the notice for a filtered unsubscribe too", async () => {
    const client = await dial();
    await plant({ subscriptions: [SUITE, NAMED] });
    client.socket.send(unsubscribe("u", NAMED));
    await settle();
    expect(frames(client).map((f) => f.type)).toStrictEqual(["subscriptions"]);
  });

  it("keeps at most ten notices for a connection", async () => {
    const client = await dial();
    const suites = Array.from({ length: 12 }, (_, i) => ({
      ...SUITE,
      repository: `o/r${String(i)}`,
    }));
    await plant({ subscriptions: suites });
    client.socket.send(PROBE);
    await settle();
    expect(await queued(await connection())).toHaveLength(10);
    await afterGrace();
    client.socket.send(list);
    await settle();
    expect(frames(client).filter((f) => f.event === "notice")).toHaveLength(10);
  });

  it("queues the notice for a stored unnamed check at the next event", async () => {
    const client = await dial();
    await plant({ subscriptions: [UNNAMED, PUSH] });
    expect(await relay().publish(EVENT)).toBe(1);
    await settle();
    expect(frames(client).map((f) => f.event)).toStrictEqual(["push"]);
    expect(await attachments()).toMatchObject([{ subscriptions: [PUSH] }]);
    expect(await queued(await connection())).toHaveLength(1);
  });

  it("queues the notice for a relay that wakes with a stored unnamed check", async () => {
    const client = await dial();
    await plant({ subscriptions: [SUITE] });
    await evictDurableObject(relay());
    await runInDurableObject(relay(), () => undefined);
    await settle();
    expect(frames(client)).toStrictEqual([]);
    expect(await attachments()).toMatchObject([{ subscriptions: [] }]);
    client.socket.send(list);
    await settle();
    expect(frames(client).map((f) => f.event ?? f.type)).toStrictEqual([
      "notice",
      "subscriptions",
    ]);
  });

  it("sends nothing extra to a socket without notices", async () => {
    const client = await dial();
    await exchange(client, subscribe("s", PUSH));
    client.socket.send(list);
    await settle();
    expect(frames(client).map((f) => f.type)).toStrictEqual([
      "subscriptions",
      "subscriptions",
    ]);
  });

  it("drops the notices of a connection that closed", async () => {
    const client = await dial();
    await plant({ subscriptions: [SUITE] });
    client.socket.send(PROBE);
    await settle();
    const id = await connection();
    expect(await queued(id)).toHaveLength(1);
    client.socket.close();
    await settle();
    expect(await queued(id)).toStrictEqual([]);
  });

  describe("when storage fails", () => {
    interface Mutable {
      subscriptions: unknown[];
      id?: string;
    }

    const socketOf = (stored: Mutable, send: (m: string) => void) =>
      ({
        deserializeAttachment: () => stored,
        serializeAttachment: (value: Mutable) => {
          Object.assign(stored, value);
        },
        send,
      }) as unknown as WebSocket;

    it("keeps the attachment when the removal cannot be stored, and goes on", async () => {
      const logged: unknown[][] = [];
      const stored: Mutable = { subscriptions: [SUITE, PUSH] };
      const socket = {
        deserializeAttachment: () => stored,
        serializeAttachment: () => {
          throw new Error("too large");
        },
        send: () => undefined,
      } as unknown as WebSocket;
      await runInDurableObject(relay(), (instance, state) => {
        const original = state.getWebSockets.bind(state);
        state.getWebSockets = () => [socket];
        const log = console.error;
        console.error = (...args: unknown[]) => logged.push(args);
        try {
          expect(instance.publish(EVENT)).toBe(1);
        } finally {
          state.getWebSockets = original;
          console.error = log;
        }
      });
      expect(logged.map((l) => l[0])).toStrictEqual([
        "removed subscriptions could not be stored",
        "connection state could not be stored",
      ]);
    });

    it("removes the subscription and logs when its notice cannot be queued", async () => {
      const logged: unknown[][] = [];
      const stored: Mutable = { subscriptions: [SUITE, PUSH] };
      const sent: string[] = [];
      await runInDurableObject(relay(), (instance, state) => {
        const { kv } = state.storage;
        const put = kv.put.bind(kv);
        kv.put = () => {
          throw new Error("value too large");
        };
        const log = console.error;
        console.error = (...args: unknown[]) => logged.push(args);
        const original = state.getWebSockets.bind(state);
        state.getWebSockets = () => [socketOf(stored, (m) => sent.push(m))];
        try {
          expect(instance.publish(EVENT)).toBe(1);
        } finally {
          state.getWebSockets = original;
          kv.put = put;
          console.error = log;
        }
      });
      expect(stored.subscriptions).toStrictEqual([PUSH]);
      expect(logged.map((l) => l[0])).toContain(
        "removal notices could not be queued",
      );
    });

    it("still replies when the queued notices cannot be read", async () => {
      const logged: unknown[][] = [];
      const sent: string[] = [];
      await runInDurableObject(relay(), (instance, state) => {
        const { kv } = state.storage;
        instance.gatedAt = 0;
        const get = kv.get.bind(kv);
        kv.get = (key: string) => {
          if (key.startsWith("n:")) throw new Error("storage unavailable");
          return get(key);
        };
        const log = console.error;
        console.error = (...args: unknown[]) => logged.push(args);
        try {
          instance.webSocketMessage(
            socketOf({ subscriptions: [], id: "m" }, (m) => sent.push(m)),
            list,
          );
          instance.webSocketMessage(
            socketOf({ subscriptions: [] }, (m) => sent.push(m)),
            list,
          );
        } finally {
          kv.get = get;
          console.error = log;
        }
      });
      // The connection without an id has no queue to read.
      expect(logged.map((l) => l[0])).toStrictEqual([
        "removal notices could not be sent",
      ]);
      expect(sent.map((m) => (JSON.parse(m) as Frame).type)).toStrictEqual([
        "subscriptions",
        "subscriptions",
      ]);
    });

    it("sends notices once the grace has just run out", async () => {
      const sent: string[] = [];
      await runInDurableObject(relay(), (instance, state) => {
        state.storage.kv.put("n:g", ["frame"]);
        const now = Date.now;
        Date.now = () => 1_000_000;
        try {
          instance.gatedAt = 1_000_000 - GATE_GRACE_MS;
          instance.webSocketMessage(
            socketOf({ subscriptions: [], id: "g" }, (m) => sent.push(m)),
            list,
          );
        } finally {
          Date.now = now;
        }
      });
      expect(sent[0]).toBe("frame");
    });

    it("replies without counts when they cannot be read", async () => {
      const logged: unknown[][] = [];
      const sent: string[] = [];
      await runInDurableObject(relay(), (instance, state) => {
        const { kv } = state.storage;
        const get = kv.get.bind(kv);
        kv.get = (key: string) => {
          if (key.startsWith("c:")) throw new Error("storage unavailable");
          return get(key);
        };
        const log = console.error;
        console.error = (...args: unknown[]) => logged.push(args);
        try {
          instance.webSocketMessage(
            socketOf({ subscriptions: [], id: "q" }, (m) => sent.push(m)),
            list,
          );
        } finally {
          kv.get = get;
          console.error = log;
        }
      });
      expect(logged.map((l) => l[0])).toStrictEqual([
        "delivery counts could not be read",
      ]);
      expect(sent.map((m) => JSON.parse(m) as unknown)).toStrictEqual([
        { type: "subscriptions", id: "l", subscriptions: [] },
      ]);
    });

    it("starts when stale rows cannot be dropped", async () => {
      const logged: unknown[][] = [];
      await runInDurableObject(relay(), (instance, state) => {
        const { kv } = state.storage;
        const list = kv.list.bind(kv);
        kv.list = () => {
          throw new Error("storage unavailable");
        };
        const log = console.error;
        console.error = (...args: unknown[]) => logged.push(args);
        try {
          const Made = instance.constructor as new (
            ctx: DurableObjectState,
            env: unknown,
          ) => unknown;
          expect(new Made(state, {})).toBeDefined();
        } finally {
          kv.list = list;
          console.error = log;
        }
      });
      expect(logged.map((l) => l[0])).toStrictEqual([
        "stale connection rows could not be dropped",
      ]);
    });

    it("sends the event, and writes nothing, when a new connection's id cannot be stored", async () => {
      const writes: string[] = [];
      const logged: unknown[][] = [];
      let delivered = 0;
      const socket = {
        deserializeAttachment: () => ({ subscriptions: [PUSH] }),
        serializeAttachment: () => {
          throw new Error("too large");
        },
        send: () => {
          delivered += 1;
          throw new Error("closing");
        },
      } as unknown as WebSocket;
      const reached = await runInDurableObject(relay(), (instance, state) => {
        const { kv } = state.storage;
        const put = kv.put.bind(kv);
        kv.put = (key: string, value: unknown) => {
          writes.push(key);
          put(key, value);
        };
        const original = state.getWebSockets.bind(state);
        state.getWebSockets = () => [socket];
        const log = console.error;
        console.error = (...args: unknown[]) => logged.push(args);
        try {
          return instance.publish(EVENT);
        } finally {
          state.getWebSockets = original;
          kv.put = put;
          console.error = log;
        }
      });
      expect(reached).toBe(0);
      expect(delivered).toBe(1);
      expect(writes).toStrictEqual([]);
      expect(logged.map((l) => l[0])).toStrictEqual([
        "connection state could not be stored",
      ]);
    });

    it("keeps the other subscriptions when one trips the cap", async () => {
      const client = await dial();
      const PULL = { provider: "github", event: "pull_request" };
      await exchange(client, subscribe("s", PUSH));
      await plant({ subscriptions: [PUSH, PULL] });
      for (let i = 0; i < 100; i += 1) await relay().publish(EVENT);
      expect(await relay().publish(EVENT)).toBe(0);
      expect(await attachments()).toMatchObject([{ subscriptions: [PULL] }]);
    });

    it("sends no cap notice while the removal cannot be stored", async () => {
      const sent: string[] = [];
      const stored: Mutable = { subscriptions: [PUSH], id: "t" };
      let writes = 0;
      const socket = {
        deserializeAttachment: () => stored,
        serializeAttachment: () => {
          throw new Error("too large");
        },
        send: (m: string) => sent.push(m),
      } as unknown as WebSocket;
      await runInDurableObject(relay(), (instance, state) => {
        const { kv } = state.storage;
        const put = kv.put.bind(kv);
        kv.put = (key: string, value: unknown) => {
          if (key.startsWith("n:")) writes += 1;
          put(key, value);
        };
        const original = state.getWebSockets.bind(state);
        state.getWebSockets = () => [socket];
        const log = console.error;
        console.error = () => undefined;
        try {
          for (let i = 0; i < 101; i += 1) instance.publish(EVENT);
        } finally {
          state.getWebSockets = original;
          kv.put = put;
          console.error = log;
        }
      });
      // 100 events within the cap, then 1 skipped; no notice reached the socket.
      expect(sent.filter((m) => m.includes('"notice"'))).toStrictEqual([]);
      expect(writes).toBe(0);
    });

    it("sends the event when a cap notice cannot be queued", async () => {
      const logged: unknown[][] = [];
      const client = await dial();
      await exchange(client, subscribe("s", PUSH));
      for (let i = 0; i < 100; i += 1) await relay().publish(EVENT);
      const reached = await runInDurableObject(relay(), (instance, state) => {
        const { kv } = state.storage;
        const put = kv.put.bind(kv);
        kv.put = (key: string, value: unknown) => {
          if (key.startsWith("n:")) throw new Error("value too large");
          put(key, value);
        };
        const log = console.error;
        console.error = (...args: unknown[]) => logged.push(args);
        try {
          return instance.publish(EVENT);
        } finally {
          kv.put = put;
          console.error = log;
        }
      });
      expect(reached).toBe(0);
      expect(logged.map((l) => l[0])).toStrictEqual([
        "cap notices could not be queued",
      ]);
    });
  });

  it("drops the counts of connections that are gone when it wakes", async () => {
    const client = await dial();
    await exchange(client, subscribe("s", PUSH));
    await relay().publish(EVENT);
    await runInDurableObject(relay(), (_instance, state) => {
      state.storage.kv.put("c:ghost", {});
    });
    await evictDurableObject(relay());
    const keys = await runInDurableObject(relay(), (_instance, state) =>
      [...state.storage.kv.list({ prefix: "c:" })].map(([key]) => key),
    );
    const [{ id }] = (await attachments()) as [Attachment];
    expect(keys).toStrictEqual([`c:${String(id)}`]);
  });

  it("counts deliveries and shows the last hour beside each subscription", async () => {
    const client = await dial();
    await exchange(client, subscribe("s", PUSH));
    for (let i = 0; i < 3; i += 1) await relay().publish(EVENT);
    expect(await exchange(client, list)).toStrictEqual({
      type: "subscriptions",
      id: "l",
      subscriptions: [PUSH],
      delivered_last_hour: [3],
    });
  });

  it("keeps the counts out of the stored subscriptions", async () => {
    const client = await dial();
    await exchange(client, subscribe("s", PUSH));
    await relay().publish(EVENT);
    const [stored] = await attachments();
    expect(stored?.subscriptions).toStrictEqual([PUSH]);
    expect(Object.keys(stored ?? {}).sort()).toStrictEqual([
      "id",
      "subscriptions",
    ]);
  });

  it("keeps the connection's id when a subscription changes", async () => {
    const client = await dial();
    await exchange(client, subscribe("s", PUSH));
    await relay().publish(EVENT);
    const [before] = await attachments();
    await exchange(client, subscribe("t", { ...PUSH, event: "issues" }));
    const [after] = await attachments();
    expect(after?.id).toBe(before?.id);
    expect(after?.subscriptions).toHaveLength(2);
  });

  it("removes a subscription on the event past the cap, and tells the session once", async () => {
    const client = await dial();
    await exchange(client, subscribe("s", PUSH));
    for (let i = 0; i < 100; i += 1) {
      expect(await relay().publish(EVENT)).toBe(1);
    }
    expect(await relay().publish(EVENT)).toBe(0);
    expect(await relay().publish(EVENT)).toBe(0);
    await settle();
    // A listen-only client gets the notice at once.
    expect(frames(client).filter((f) => f.event === "notice")).toHaveLength(1);
    client.socket.send(list);
    await settle();
    const received = frames(client);
    expect(received.filter((f) => f.event === "push")).toHaveLength(100);
    const notices = received.filter((f) => f.event === "notice");
    expect(notices).toHaveLength(2);
    expect(notices[1]).toStrictEqual(notices[0]);
    expect(notices[0]).toMatchObject({
      provider: "awrel",
      payload: {
        code: "subscription_removed",
        reason: "rate_cap",
        message:
          "removed: it matched more than 100 events in 5 minutes; subscribe again with narrower filters such as name, conclusion or action",
        subscription: PUSH,
        limit: { events: 100, minutes: 5 },
      },
    });
    expect(received.at(-1)).toStrictEqual(subscriptions("l", []));
  });

  it("keeps delivering to a subscription that shares the event with a capped one", async () => {
    const client = await dial();
    const other = { ...PUSH, repository: "OWNER/repo" };
    await exchange(client, subscribe("s", PUSH));
    await plant({
      subscriptions: [PUSH, { provider: "github", event: "push" }],
    });
    for (let i = 0; i < 100; i += 1) await relay().publish(EVENT);
    expect(other.repository.toLowerCase()).toBe(PUSH.repository);
    expect(await relay().publish(EVENT)).toBe(0);
    await settle();
    client.socket.send(list);
    await settle();
    expect(frames(client).at(-1)).toMatchObject({ subscriptions: [] });
  });

  it("counts across an eviction", async () => {
    const client = await dial();
    await exchange(client, subscribe("s", PUSH));
    for (let i = 0; i < 60; i += 1) await relay().publish(EVENT);
    await evictDurableObject(relay());
    for (let i = 0; i < 40; i += 1) {
      expect(await relay().publish(EVENT)).toBe(1);
    }
    await evictDurableObject(relay());
    expect(await relay().publish(EVENT)).toBe(0);
    client.socket.send(list);
    await settle();
    expect(frames(client).at(-1)).toMatchObject({ subscriptions: [] });
  });

  it("forgets a connection's counts when it closes", async () => {
    const client = await dial();
    await exchange(client, subscribe("s", PUSH));
    await relay().publish(EVENT);
    const [{ id }] = (await attachments()) as [Attachment];
    const stored = () =>
      runInDurableObject(relay(), (_instance, state) =>
        state.storage.kv.get(`c:${String(id)}`),
      );
    expect(await stored()).toBeDefined();
    client.socket.close(1000);
    await settle();
    expect(await stored()).toBeUndefined();
  });

  it("forgets a connection's counts when it fails", async () => {
    const calls: Array<string | undefined> = [];
    await runInDurableObject(relay(), (instance, state) => {
      state.storage.kv.put("c:x", {});
      instance.webSocketError({
        deserializeAttachment: () => ({ subscriptions: [], id: "x" }),
      } as unknown as WebSocket);
      calls.push(String(state.storage.kv.get("c:x")));
    });
    expect(calls).toStrictEqual(["undefined"]);
  });

  it("does not fail when a notice cannot be sent", async () => {
    const reached = await runInDurableObject(relay(), (instance, state) => {
      const socket = {
        deserializeAttachment: () => ({ subscriptions: [SUITE] }),
        serializeAttachment: () => undefined,
        send: () => {
          throw new Error("closing");
        },
      } as unknown as WebSocket;
      const original = state.getWebSockets.bind(state);
      state.getWebSockets = () => [socket];
      try {
        return instance.publish(EVENT);
      } finally {
        state.getWebSockets = original;
      }
    });
    expect(reached).toBe(0);
  });

  it("wakes with a connection that never subscribed", async () => {
    await dial();
    await evictDurableObject(relay());
    expect(await relay().publish(EVENT)).toBe(0);
  });

  it("closes a connection that never subscribed", async () => {
    await runInDurableObject(relay(), (instance) => {
      expect(() => {
        instance.webSocketClose({
          deserializeAttachment: () => null,
        } as unknown as WebSocket);
      }).not.toThrow();
    });
  });

  it("gives a stored connection an id once an event matches it", async () => {
    await dial();
    await plant({ subscriptions: [PUSH] });
    expect(await relay().publish(EVENT)).toBe(1);
    const [stored] = await attachments();
    expect(stored?.id).toMatch(/^[0-9a-f-]{36}$/);
  });

  it("leaves a connection without a matching subscription as it was", async () => {
    await dial();
    await plant({ subscriptions: [PUSH] });
    expect(await relay().publish({ ...EVENT, event: "issues" })).toBe(0);
    const [stored] = await attachments();
    expect(stored).toStrictEqual({ subscriptions: [PUSH] });
  });

  it("writes the attachment only when the id or the list changes", async () => {
    const writes: unknown[] = [];
    await runInDurableObject(relay(), (instance, state) => {
      const socket = {
        deserializeAttachment: () => ({ subscriptions: [PUSH], id: "fixed" }),
        serializeAttachment: (value: unknown) => writes.push(value),
        send: () => undefined,
      } as unknown as WebSocket;
      const original = state.getWebSockets.bind(state);
      state.getWebSockets = () => [socket];
      try {
        instance.publish(EVENT);
      } finally {
        state.getWebSockets = original;
      }
    });
    expect(writes).toStrictEqual([]);
  });

  it("names a connection by its id, and one that never subscribed by none", () => {
    expect(connectionId({ subscriptions: [], id: "x" })).toBe("x");
    expect(connectionId(null)).toBe("undefined");
  });

  it("drops the counts of an unsubscribed subscription", async () => {
    const client = await dial();
    const record = () =>
      runInDurableObject(relay(), async (_instance, state) => {
        const [{ id }] = (await attachments()) as [Attachment];
        return state.storage.kv.get(`c:${String(id)}`) as Record<
          string,
          unknown
        >;
      });
    await exchange(client, subscribe("s", PUSH));
    await relay().publish(EVENT);
    expect(Object.keys(await record())).toHaveLength(1);
    await exchange(client, subscribe("t", { ...PUSH, event: "issues" }));
    await exchange(client, unsubscribe("u", PUSH));
    expect(await record()).toStrictEqual({});
  });

  it("keeps the record to the live subscriptions as the list changes", async () => {
    const client = await dial();
    const filters = Array.from({ length: 6 }, (_, i) => ({
      ...PUSH,
      action: [`a${String(i)}`],
    }));
    for (const filter of filters) {
      await exchange(client, subscribe(filter.action[0] ?? "", filter));
      await relay().publish({
        ...EVENT,
        payload: { action: filter.action[0] },
      });
      await runInDurableObject(relay(), (_instance, state) => {
        const sockets = state.getWebSockets();
        sockets[0]?.serializeAttachment({
          subscriptions: [filter],
          id: (sockets[0].deserializeAttachment() as Attachment).id ?? "",
        });
      });
    }
    const sizes = await runInDurableObject(relay(), (_instance, state) =>
      [...state.storage.kv.list({ prefix: "c:" })].map(
        ([, value]) => Object.keys(value as object).length,
      ),
    );
    expect(sizes).toStrictEqual([1]);
  });

  it("delivers the event when the counts cannot be stored, and logs it", async () => {
    const client = await dial();
    await exchange(client, subscribe("s", PUSH));
    const logged: unknown[][] = [];
    const reached = await runInDurableObject(relay(), (instance, state) => {
      const { kv } = state.storage;
      const put = kv.put.bind(kv);
      const log = console.error;
      console.error = (...args: unknown[]) => logged.push(args);
      kv.put = () => {
        throw new Error("value too large");
      };
      try {
        return instance.publish(EVENT);
      } finally {
        kv.put = put;
        console.error = log;
      }
    });
    await settle();
    expect(reached).toBe(1);
    expect(
      frames(client)
        .filter((f) => f.type === "event")
        .map((f) => f.event),
    ).toStrictEqual(["push"]);
    expect(await attachments()).toMatchObject([{ subscriptions: [PUSH] }]);
    expect(logged.map((l) => l[0])).toStrictEqual([
      "delivery counts could not be stored",
    ]);
  });

  it("takes back the count of an event a socket never got", async () => {
    const minute = Math.floor(Date.now() / 60_000);
    const counts = await runInDurableObject(relay(), (instance, state) => {
      const socket = {
        deserializeAttachment: () => ({ subscriptions: [PUSH], id: "m" }),
        serializeAttachment: () => undefined,
        send: () => {
          throw new Error("closing");
        },
      } as unknown as WebSocket;
      const original = state.getWebSockets.bind(state);
      state.getWebSockets = () => [socket];
      try {
        expect(instance.publish(EVENT)).toBe(0);
      } finally {
        state.getWebSockets = original;
      }
      return delivered(state.storage.kv, "m", [PUSH], minute);
    });
    expect(counts).toStrictEqual([0]);
  });

  it("logs a failed refund and goes on", async () => {
    const logged: unknown[][] = [];
    await runInDurableObject(relay(), (instance, state) => {
      const { kv } = state.storage;
      const put = kv.put.bind(kv);
      let calls = 0;
      kv.put = (key: string, value: unknown) => {
        calls += 1;
        if (calls > 1) throw new Error("value too large");
        put(key, value);
      };
      const log = console.error;
      console.error = (...args: unknown[]) => logged.push(args);
      const socket = {
        deserializeAttachment: () => ({ subscriptions: [PUSH], id: "m" }),
        serializeAttachment: () => undefined,
        send: () => {
          throw new Error("closing");
        },
      } as unknown as WebSocket;
      const original = state.getWebSockets.bind(state);
      state.getWebSockets = () => [socket];
      try {
        expect(instance.publish(EVENT)).toBe(0);
      } finally {
        state.getWebSockets = original;
        kv.put = put;
        console.error = log;
      }
    });
    expect(logged[0]?.[0]).toBe("delivery counts could not be corrected");
  });

  it("stores no counts for a connection that never matched an event", async () => {
    const client = await dial();
    await exchange(client, subscribe("s", PUSH));
    await exchange(client, unsubscribe("u", PUSH));
    const keys = await runInDurableObject(relay(), (_instance, state) =>
      [...state.storage.kv.list({ prefix: "c:" })].map(([key]) => key),
    );
    expect(keys).not.toContain("c:undefined");
  });

  it("replies when the counts cannot be trimmed", async () => {
    const sent: string[] = [];
    const logged: unknown[][] = [];
    const socket = {
      deserializeAttachment: () => ({ subscriptions: [SUITE], id: "m" }),
      serializeAttachment: () => undefined,
      send: (message: string) => sent.push(message),
    } as unknown as WebSocket;
    await runInDurableObject(relay(), (instance, state) => {
      const { kv } = state.storage;
      const put = kv.put.bind(kv);
      const log = console.error;
      console.error = (...args: unknown[]) => logged.push(args);
      kv.put = () => {
        throw new Error("value too large");
      };
      try {
        instance.webSocketMessage(socket, subscribe("s", PUSH));
      } finally {
        kv.put = put;
        console.error = log;
      }
    });
    expect(sent.map((m) => (JSON.parse(m) as Frame).type)).toStrictEqual([
      "subscriptions",
    ]);
    expect(logged.map((l) => l[0])).toContain(
      "delivery counts could not be trimmed",
    );
  });

  it("does not refund an event that was never charged", async () => {
    const minute = Math.floor(Date.now() / 60_000);
    const counts = await runInDurableObject(relay(), (instance, state) => {
      const { kv } = state.storage;
      charge(kv, "n", [PUSH], minute, [PUSH]);
      const get = kv.get.bind(kv);
      let failing = true;
      kv.get = (key: string) => {
        if (failing) throw new Error("storage unavailable");
        return get(key);
      };
      const socket = {
        deserializeAttachment: () => ({ subscriptions: [PUSH], id: "n" }),
        serializeAttachment: () => undefined,
        send: () => {
          failing = false;
          throw new Error("closing");
        },
      } as unknown as WebSocket;
      const original = state.getWebSockets.bind(state);
      state.getWebSockets = () => [socket];
      const log = console.error;
      const logs: unknown[] = [];
      console.error = (...a: unknown[]) => logs.push(a[0]);
      try {
        expect(instance.publish(EVENT)).toBe(0);
      } finally {
        state.getWebSockets = original;
        kv.get = get;
        console.error = log;
      }
      return delivered(kv, "n", [PUSH], minute);
    });
    expect(counts).toStrictEqual([1]);
  });
});
