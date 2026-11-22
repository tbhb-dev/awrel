// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import { env, exports } from "cloudflare:workers";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { Env } from "./env.ts";
import { MAX_BODY_BYTES, receiveGitHub } from "./github.ts";
import {
  connect,
  digest,
  disconnectAll,
  relay,
  SECRET,
  settle,
  sign,
} from "./testing.ts";

afterEach(() => disconnectAll(relay()));

interface Delivery {
  readonly body?: string;
  readonly signature?: string | null;
  readonly event?: string | null;
  readonly delivery?: string | null;
}

// Posts a delivery to the Worker; every part defaults to a valid one.
async function post(delivery: Delivery = {}): Promise<Response> {
  const body = delivery.body ?? '{"repository":{"full_name":"Owner/Repo"}}';
  const signature =
    delivery.signature === undefined ? await sign(body) : delivery.signature;
  const event = delivery.event === undefined ? "push" : delivery.event;
  const id = delivery.delivery === undefined ? "d-1" : delivery.delivery;
  const headers = new Headers();
  if (signature !== null) {
    headers.set("X-Hub-Signature-256", signature);
  }
  if (event !== null) {
    headers.set("X-GitHub-Event", event);
  }
  if (id !== null) {
    headers.set("X-GitHub-Delivery", id);
  }
  return exports.default.fetch("https://awrel.example/webhooks/github", {
    method: "POST",
    body,
    headers,
  });
}

// Seeds an older catch-all connection directly, posts a delivery, and returns
// the parsed messages the connection received.
async function received(delivery: Delivery): Promise<unknown[]> {
  const messages = await connect(relay(), {
    subscriptions: [{ provider: "github" }],
  });
  expect((await post(delivery)).status).toBe(202);
  await settle();
  return messages.map((m): unknown => JSON.parse(m));
}

describe("signature", () => {
  it("accepts a valid signature and relays the event", async () => {
    const response = await post();
    expect(response.status).toBe(202);
    expect(await response.text()).toBe("");
  });

  async function expectUnauthorized(delivery: Delivery): Promise<void> {
    const response = await post(delivery);
    expect(response.status).toBe(401);
    expect(await response.text()).toBe("Unauthorized");
  }

  it("rejects a missing signature", async () => {
    await expectUnauthorized({ signature: null });
  });

  it("rejects a signature under another secret", async () => {
    await expectUnauthorized({
      signature: await sign("{}", "other"),
      body: "{}",
    });
  });

  it("rejects a tampered body", async () => {
    await expectUnauthorized({
      body: '{"a":2}',
      signature: await sign('{"a":1}'),
    });
  });

  it("rejects another algorithm's prefix on a valid digest", async () => {
    await expectUnauthorized({
      body: "{}",
      signature: `sha512=${await digest("{}", SECRET)}`,
    });
  });

  it("rejects a bare digest", async () => {
    await expectUnauthorized({
      body: "{}",
      signature: await digest("{}", SECRET),
    });
  });

  it("rejects a prefix with no digest", async () => {
    await expectUnauthorized({ signature: "sha256=" });
  });

  it("rejects an empty header", async () => {
    await expectUnauthorized({ signature: "" });
  });

  it("rejects a digest that is not hex", async () => {
    await expectUnauthorized({ signature: `sha256=${"zz".repeat(32)}` });
  });

  it("rejects a digest with an extra byte", async () => {
    await expectUnauthorized({
      body: "{}",
      signature: `${await sign("{}")}00`,
    });
  });

  it("rejects a truncated digest", async () => {
    await expectUnauthorized({
      body: "{}",
      signature: (await sign("{}")).slice(0, -2),
    });
  });

  it("rejects a digest with a trailing odd nibble", async () => {
    await expectUnauthorized({
      body: "{}",
      signature: `${await sign("{}")}0`,
    });
  });
});

describe("request", () => {
  it("rejects invalid JSON", async () => {
    const response = await post({ body: "{" });
    expect(response.status).toBe(400);
    expect(await response.text()).toBe("Bad Request");
  });

  it("rejects a missing event header", async () => {
    const response = await post({ event: null });
    expect(response.status).toBe(400);
    expect(await response.text()).toBe("Bad Request");
  });

  it("rejects a missing delivery header", async () => {
    const response = await post({ delivery: null });
    expect(response.status).toBe(400);
    expect(await response.text()).toBe("Bad Request");
  });

  it("checks the signature before the rest of the request", async () => {
    const response = await post({ signature: null, event: null, body: "{" });
    expect(response.status).toBe(401);
  });
});

describe("body size", () => {
  it("refuses a body over the cap before checking its signature", async () => {
    const response = await post({
      body: "x".repeat(MAX_BODY_BYTES + 1),
      signature: null,
    });
    expect(response.status).toBe(413);
  });

  it("refuses a declared length over the cap without reading", async () => {
    const response = await receiveGitHub(
      new Request("https://awrel.example/webhooks/github", {
        method: "POST",
        headers: { "Content-Length": String(MAX_BODY_BYTES + 1) },
      }),
      env as unknown as Env,
    );
    expect(response.status).toBe(413);
  });

  it("checks the signature with a declared length at the cap and no body", async () => {
    const response = await receiveGitHub(
      new Request("https://awrel.example/webhooks/github", {
        method: "POST",
        headers: { "Content-Length": String(MAX_BODY_BYTES) },
      }),
      env as unknown as Env,
    );
    expect(response.status).toBe(401);
  });

  it("checks the signature of a streamed body at the cap", async () => {
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new Uint8Array(MAX_BODY_BYTES));
        controller.close();
      },
    });
    const response = await receiveGitHub(
      new Request("https://awrel.example/webhooks/github", {
        method: "POST",
        body,
      }),
      env as unknown as Env,
    );
    expect(response.status).toBe(401);
  });

  it.each([
    ["absent", null],
    ["understated", "1"],
  ])("cancels an oversize stream with an %s length", async (_name, length) => {
    let reads = 0;
    const cancel = vi.fn(() => undefined);
    const body = new ReadableStream<Uint8Array>(
      {
        pull(controller) {
          controller.enqueue(
            new Uint8Array(reads++ === 0 ? MAX_BODY_BYTES : 1),
          );
        },
        cancel,
      },
      { highWaterMark: 0 },
    );
    const headers = new Headers();
    if (length !== null) headers.set("Content-Length", length);
    const response = await receiveGitHub(
      new Request("https://awrel.example/webhooks/github", {
        method: "POST",
        body,
        headers,
      }),
      env as unknown as Env,
    );
    expect(response.status).toBe(413);
    expect(await response.text()).toBe("Payload Too Large");
    expect(cancel).toHaveBeenCalledOnce();
    expect(reads).toBe(2);
  });

  it("verifies a signature over multiple body chunks", async () => {
    const chunks = ['{"repository":', '{"full_name":"Owner/Repo"}}'];
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        for (const chunk of chunks) {
          controller.enqueue(new TextEncoder().encode(chunk));
        }
        controller.close();
      },
    });
    const response = await receiveGitHub(
      new Request("https://awrel.example/webhooks/github", {
        method: "POST",
        body,
        headers: {
          "X-Hub-Signature-256": await sign(chunks.join("")),
          "X-GitHub-Event": "push",
          "X-GitHub-Delivery": "d-1",
        },
      }),
      env as unknown as Env,
    );
    expect(response.status).toBe(202);
  });

  it("accepts a body under the cap", async () => {
    expect((await post()).status).toBe(202);
  });
});

describe("relayed event", () => {
  it("carries the headers, repository and payload", async () => {
    expect(
      await received({
        body: '{"repository":{"full_name":"Owner/Repo"},"n":1}',
        event: "issues",
        delivery: "d-9",
      }),
    ).toEqual([
      {
        type: "event",
        provider: "github",
        event: "issues",
        delivery: "d-9",
        repository: "Owner/Repo",
        payload: { repository: { full_name: "Owner/Repo" }, n: 1 },
      },
    ]);
  });

  it.each([
    ["null", "null"],
    ["a number", "7"],
    ["a string", '"x"'],
    ["no repository", "{}"],
    ["a null repository", '{"repository":null}'],
    ["a repository with no name", '{"repository":{}}'],
    ["a non-string name", '{"repository":{"full_name":5}}'],
  ])("has a null repository for %s", async (_name, body) => {
    const [message] = (await received({ body })) as {
      repository: unknown;
      payload: unknown;
    }[];
    expect(message?.repository).toBeNull();
    expect(message?.payload).toEqual(JSON.parse(body) as unknown);
  });

  it("answers a ping without relaying it", async () => {
    const messages = await connect(relay(), {
      subscriptions: [
        { provider: "github", event: "ping", repository: "owner/repo" },
      ],
    });
    const response = await post({ event: "ping" });
    expect(response.status).toBe(202);
    expect(await response.text()).toBe("");
    await settle();
    expect(messages).toEqual([]);
  });

  it("still validates a ping", async () => {
    expect((await post({ event: "ping", signature: null })).status).toBe(401);
    expect((await post({ event: "ping", body: "{" })).status).toBe(400);
    expect((await post({ event: "ping", delivery: null })).status).toBe(400);
  });

  it("reaches only the matching subscribers", async () => {
    const stub = relay();
    const hit = await connect(stub, {
      subscriptions: [
        { provider: "github", event: "push", repository: "owner/repo" },
      ],
    });
    const miss = await connect(stub, {
      subscriptions: [
        { provider: "github", event: "push", repository: "owner/other" },
      ],
    });
    expect((await post()).status).toBe(202);
    await settle();
    expect(hit).toHaveLength(1);
    expect(miss).toEqual([]);
  });
});

describe("secret", () => {
  it.each([
    ["empty", ""],
    ["absent", undefined],
  ])(
    "answers 503 and publishes nothing with an %s secret",
    async (_, secret) => {
      const messages = await connect(relay(), {
        subscriptions: [{ provider: "github" }],
      });
      const logSpy = vi
        .spyOn(console, "log")
        .mockImplementation(() => undefined);
      const body = "{}";
      const response = await receiveGitHub(
        new Request("https://awrel.example/webhooks/github", {
          method: "POST",
          body,
          headers: {
            "X-Hub-Signature-256": await sign(body, "unused"),
            "X-GitHub-Event": "push",
            "X-GitHub-Delivery": "d-1",
          },
        }),
        { ...env, GITHUB_WEBHOOK_SECRET: secret } as unknown as Env,
      );
      await settle();
      expect(response.status).toBe(503);
      expect(await response.text()).toBe("Service Unavailable");
      expect(logSpy).toHaveBeenCalledWith("github webhook secret is not set");
      expect(messages).toStrictEqual([]);
      logSpy.mockRestore();
    },
  );
});

describe("delivery log", () => {
  async function logged(delivery: Delivery): Promise<string[]> {
    const log = vi.spyOn(console, "log").mockImplementation(() => undefined);
    try {
      expect((await post(delivery)).status).toBe(202);
      return log.mock.calls.map((call) => String(call[0]));
    } finally {
      log.mockRestore();
    }
  }

  it("logs the delivery, action, repository and reach", async () => {
    await connect(relay(), { subscriptions: [{ provider: "github" }] });
    const lines = await logged({
      body: '{"action":"opened","repository":{"full_name":"Owner/Repo"}}',
      event: "pull_request",
    });
    expect(lines).toContain(
      "github delivery=d-1 event=pull_request action=opened repository=Owner/Repo reached=1",
    );
  });

  it("logs none for a missing repository and a non-string action", async () => {
    const lines = await logged({ body: '{"action":1}' });
    expect(lines).toContain(
      "github delivery=d-1 event=push action=none repository=none reached=0",
    );
  });

  it("logs none for a null payload", async () => {
    const lines = await logged({ body: "null" });
    expect(lines).toContain(
      "github delivery=d-1 event=push action=none repository=none reached=0",
    );
  });
});
