// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import { exports } from "cloudflare:workers";
import { env } from "cloudflare:workers";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Env } from "./env.ts";
import { receiveGrafana } from "./grafana.ts";
import {
  connect,
  digest,
  disconnectAll,
  GRAFANA_SECRET,
  relay,
  settle,
} from "./testing.ts";

// A fixed clock, so a timestamp signed in a test is the one the Worker sees.
const NOW_MS = Date.UTC(2026, 10, 13, 12, 0, 0);
const NOW_SECONDS = String(NOW_MS / 1000);

beforeEach(() => {
  vi.useFakeTimers({ toFake: ["Date"] });
  vi.setSystemTime(NOW_MS);
});

// The signature a valid delivery carries: the digest of `<timestamp>:<body>`.
function sign(
  body = DEFAULT_BODY,
  timestamp = NOW_SECONDS,
  secret = GRAFANA_SECRET,
): Promise<string> {
  return digest(`${timestamp}:${body}`, secret);
}

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
  return disconnectAll(relay());
});

interface Delivery {
  readonly body?: string;
  readonly signature?: string | null;
  readonly contentLength?: string | null;
  readonly timestamp?: string | null;
  readonly chunked?: boolean;
}

const DEFAULT_BODY = JSON.stringify({
  alerts: [
    {
      status: "firing",
      labels: { alertname: "HighCPU", host_name: "host-a" },
    },
  ],
});

// Posts a Grafana webhook delivery to the Worker; every part defaults to a
// valid one.
async function post(delivery: Delivery = {}): Promise<Response> {
  const body = delivery.body ?? DEFAULT_BODY;
  const timestamp =
    delivery.timestamp === undefined ? NOW_SECONDS : delivery.timestamp;
  const signature =
    delivery.signature === undefined
      ? await sign(body, timestamp ?? NOW_SECONDS)
      : delivery.signature;
  const headers = new Headers();
  if (timestamp !== null) {
    headers.set("X-Grafana-Alerting-Timestamp", timestamp);
  }
  if (signature !== null) {
    headers.set("X-Grafana-Alerting-Signature", signature);
  }
  if (delivery.contentLength !== undefined && delivery.contentLength !== null) {
    headers.set("Content-Length", delivery.contentLength);
  }
  return exports.default.fetch("https://awrel.example/webhooks/grafana", {
    method: "POST",
    body: delivery.chunked === true ? chunked(body) : body,
    headers,
    // @ts-expect-error: workerd takes a streamed body without duplex, which the DOM types require
    duplex: delivery.chunked === true ? "half" : undefined,
  });
}

// The body as a stream of 64 KiB chunks, which workerd sends without a
// Content-Length.
function chunked(body: string): ReadableStream<Uint8Array> {
  const bytes = new TextEncoder().encode(body);
  let offset = 0;
  return new ReadableStream({
    pull(controller) {
      if (offset >= bytes.byteLength) {
        controller.close();
        return;
      }
      controller.enqueue(bytes.slice(offset, offset + 65_536));
      offset += 65_536;
    },
  });
}

// Seeds a catch-all Grafana subscription, posts a delivery, and returns the
// parsed messages the connection received.
async function received(delivery: Delivery = {}): Promise<unknown[]> {
  const messages = await connect(relay(), {
    subscriptions: [{ provider: "grafana" }],
  });
  expect((await post(delivery)).status).toBe(202);
  await settle();
  return messages.map((m): unknown => JSON.parse(m));
}

// Computes the expected deterministic delivery ID for alert `index` in `body`.
async function expectedDelivery(body: string, index: number): Promise<string> {
  const bytes = new TextEncoder().encode(`${body}:${String(index)}`);
  const hash = await crypto.subtle.digest("SHA-256", bytes);
  const hex = Array.from(new Uint8Array(hash, 0, 16), (b) =>
    b.toString(16).padStart(2, "0"),
  ).join("");
  return `gf-${hex}`;
}

describe("routing", () => {
  it("returns 404 for GET", async () => {
    const response = await exports.default.fetch(
      "https://awrel.example/webhooks/grafana",
      { method: "GET" },
    );
    expect(response.status).toBe(404);
    expect(await response.text()).toBe("Not Found");
  });
});

describe("payload size", () => {
  it("rejects a body over 1 MiB", async () => {
    const body = "x".repeat(1_048_577);
    const response = await post({ body, contentLength: "0", signature: null });
    expect(response.status).toBe(413);
    expect(await response.text()).toBe("Payload Too Large");
  });

  it("rejects a numeric Content-Length over 1 MiB", async () => {
    const response = await post({
      contentLength: "1048577",
      signature: null,
    });
    expect(response.status).toBe(413);
    expect(await response.text()).toBe("Payload Too Large");
  });

  it("allows a body of exactly 1 MiB past the size check", async () => {
    const body = "x".repeat(1_048_576);
    const response = await post({
      body,
      contentLength: "1048576",
      signature: null,
    });
    expect(response.status).toBe(401);
  });

  it("rejects a chunked body over 1 MiB without a Content-Length", async () => {
    const response = await post({
      body: "x".repeat(1_048_577),
      chunked: true,
      signature: null,
    });
    expect(response.status).toBe(413);
    expect(await response.text()).toBe("Payload Too Large");
  });

  it("allows a chunked body of exactly 1 MiB past the size check", async () => {
    const response = await post({
      body: "x".repeat(1_048_576),
      chunked: true,
      signature: null,
    });
    expect(response.status).toBe(401);
  });

  it("relays a chunked body in several chunks", async () => {
    const body = JSON.stringify({
      alerts: [
        {
          status: "firing",
          labels: { host_name: "host-a", pad: "y".repeat(150_000) },
        },
      ],
    });
    expect(await received({ body, chunked: true })).toHaveLength(1);
  });

  it("uses actual body length when Content-Length is not numeric", async () => {
    const response = await post({ contentLength: "not-a-number" });
    expect(response.status).toBe(202);
  });
});

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

  it("rejects an empty header", async () => {
    await expectUnauthorized({ signature: "" });
  });

  it("rejects a short signature", async () => {
    await expectUnauthorized({
      signature: (await sign()).slice(0, -2),
    });
  });

  it("rejects a signature with a trailing odd nibble", async () => {
    await expectUnauthorized({
      signature: `${await sign()}0`,
    });
  });

  it("rejects a signature with an extra byte", async () => {
    await expectUnauthorized({
      signature: `${await sign()}00`,
    });
  });

  it("rejects non-hex digits that do not match", async () => {
    await expectUnauthorized({ signature: "zz".repeat(32) });
  });

  it("rejects uppercase hex digits that do not match", async () => {
    await expectUnauthorized({ signature: "FF".repeat(32) });
  });

  it("rejects a signature under another secret", async () => {
    await expectUnauthorized({
      signature: await sign(DEFAULT_BODY, NOW_SECONDS, "other"),
    });
  });

  it("rejects a tampered body", async () => {
    await expectUnauthorized({
      body: JSON.stringify({
        alerts: [{ status: "resolved", labels: { host_name: "host-a" } }],
      }),
      signature: await sign(),
    });
  });

  it("rejects a sha256= prefixed signature", async () => {
    await expectUnauthorized({
      signature: `sha256=${await sign()}`,
    });
  });

  it("checks the signature before parsing JSON", async () => {
    await expectUnauthorized({ signature: null, body: "{" });
    await expectUnauthorized({ signature: "00".repeat(32), body: "{" });
  });
});

describe("timestamp", () => {
  async function expectUnauthorized(delivery: Delivery): Promise<void> {
    const response = await post(delivery);
    expect(response.status).toBe(401);
    expect(await response.text()).toBe("Unauthorized");
  }

  it("rejects a missing timestamp", async () => {
    await expectUnauthorized({ timestamp: null, signature: await sign() });
  });

  it("rejects a signature over the body alone", async () => {
    await expectUnauthorized({
      signature: await digest(DEFAULT_BODY, GRAFANA_SECRET),
    });
  });

  it("rejects a timestamp the signature doesn't cover", async () => {
    await expectUnauthorized({
      timestamp: String(Number(NOW_SECONDS) + 1),
      signature: await sign(),
    });
  });

  it.each(["abc", "", "-1", "1.5", "1e3", "1234567890123456", `+${NOW_SECONDS}`, `${NOW_SECONDS}e0`])(
    "rejects the malformed timestamp %j",
    async (timestamp) => {
      await expectUnauthorized({
        timestamp,
        signature: await sign(DEFAULT_BODY, timestamp),
      });
    },
  );

  it("accepts a timestamp exactly 5 minutes old or ahead", async () => {
    for (const offset of [-300, 300]) {
      const timestamp = String(Number(NOW_SECONDS) + offset);
      expect((await post({ timestamp })).status).toBe(202);
    }
  });

  it("rejects a timestamp more than 5 minutes old or ahead", async () => {
    for (const offset of [-301, 301]) {
      const timestamp = String(Number(NOW_SECONDS) + offset);
      await expectUnauthorized({ timestamp });
    }
  });
});

describe("secret", () => {
  it.each([
    ["empty", ""],
    ["absent", undefined],
  ])("answers 503 and publishes nothing with an %s secret", async (_, secret) => {
    const messages = await connect(relay(), {
      subscriptions: [{ provider: "grafana" }],
    });
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
    const response = await receiveGrafana(
      new Request("https://awrel.example/webhooks/grafana", {
        method: "POST",
        body: DEFAULT_BODY,
        headers: {
          "X-Grafana-Alerting-Timestamp": NOW_SECONDS,
          "X-Grafana-Alerting-Signature": await sign(DEFAULT_BODY, NOW_SECONDS, "unused"),
        },
      }),
      { ...env, GRAFANA_WEBHOOK_SECRET: secret } as unknown as Env,
    );
    await settle();
    expect(response.status).toBe(503);
    expect(await response.text()).toBe("Service Unavailable");
    expect(logSpy).toHaveBeenCalledWith("grafana webhook secret is not set");
    expect(messages).toStrictEqual([]);
  });
});

describe("request", () => {
  async function expectBadRequest(body: string): Promise<void> {
    const response = await post({ body });
    expect(response.status).toBe(400);
    expect(await response.text()).toBe("Bad Request");
  }

  it("rejects a signed empty body", async () => {
    await expectBadRequest("");
  });

  it("rejects a signed request with no body at all", async () => {
    const response = await receiveGrafana(
      new Request("https://awrel.example/webhooks/grafana", {
        method: "POST",
        headers: {
          "X-Grafana-Alerting-Timestamp": NOW_SECONDS,
          "X-Grafana-Alerting-Signature": await sign(""),
        },
      }),
      env as unknown as Env,
    );
    expect(response.status).toBe(400);
  });

  it("rejects invalid JSON and logs it", async () => {
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
    await expectBadRequest("{");
    expect(logSpy).toHaveBeenCalledWith("grafana body is not JSON");
  });

  it.each([
    ["null", "null"],
    ["a number", "7"],
    ["a string", '"x"'],
    ["a top-level array", "[]"],
    ["missing alerts", "{}"],
    ["null alerts", '{"alerts":null}'],
    ["object alerts", '{"alerts":{}}'],
    ["string alerts", '{"alerts":"firing"}'],
    ["an empty alerts array", '{"alerts":[]}'],
  ])("rejects %s", async (_name, body) => {
    await expectBadRequest(body);
  });

  it("accepts 100 alerts", async () => {
    const alerts = Array.from({ length: 100 }, () => ({
      status: "firing",
      labels: { host_name: "host-a" },
    }));
    const response = await post({ body: JSON.stringify({ alerts }) });
    expect(response.status).toBe(202);
  });

  it("rejects 101 alerts", async () => {
    const alerts = Array.from({ length: 101 }, () => ({
      status: "firing",
      labels: { host_name: "host-a" },
    }));
    await expectBadRequest(JSON.stringify({ alerts }));
  });
});

describe("relayed event", () => {
  it("delivers 3 events in order from a post with 3 alerts", async () => {
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
    const body = JSON.stringify({
      alerts: [
        {
          status: "firing",
          labels: { alertname: "HighCPU", host_name: "host-a" },
        },
        {
          status: "resolved",
          labels: { alertname: "HighCPU", host_name: "host-a" },
        },
        {
          status: "firing",
          labels: { alertname: "DiskFull", host_name: "host-b" },
        },
      ],
    });
    expect(await received({ body })).toStrictEqual([
      {
        type: "event",
        provider: "grafana",
        event: "alert.firing",
        delivery: await expectedDelivery(body, 0),
        repository: "hosts/host-a",
        payload: {
          alertname: "HighCPU",
          status: "firing",
          host: "host-a",
        },
      },
      {
        type: "event",
        provider: "grafana",
        event: "alert.resolved",
        delivery: await expectedDelivery(body, 1),
        repository: "hosts/host-a",
        payload: {
          alertname: "HighCPU",
          status: "resolved",
          host: "host-a",
        },
      },
      {
        type: "event",
        provider: "grafana",
        event: "alert.firing",
        delivery: await expectedDelivery(body, 2),
        repository: "hosts/host-b",
        payload: {
          alertname: "DiskFull",
          status: "firing",
          host: "host-b",
        },
      },
    ]);
    expect(logSpy).toHaveBeenCalledWith(
      "grafana alerts=3 published=3 skipped=0",
    );
  });

  it("skips invalid entries while publishing valid ones", async () => {
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
    const maxHost = "a".repeat(100);
    const body = JSON.stringify({
      alerts: [
        null,
        7,
        "not-an-object",
        { status: "firing" },
        { status: "firing", labels: null },
        { status: "firing", labels: { host: "host-a" } },
        { status: "firing", labels: { host_name: 123 } },
        { status: "firing", labels: { host_name: "" } },
        { status: "firing", labels: { host_name: "/host-a" } },
        { status: "firing", labels: { host_name: "host-a/" } },
        { status: "firing", labels: { host_name: "host a" } },
        { status: "firing", labels: { host_name: "a".repeat(101) } },
        { status: "pending", labels: { host_name: "host-a" } },
        { status: "firing", labels: { alertname: "Up", host_name: "host-a" } },
        { status: "resolved", labels: { host_name: maxHost } },
      ],
    });
    expect(await received({ body })).toStrictEqual([
      {
        type: "event",
        provider: "grafana",
        event: "alert.firing",
        delivery: await expectedDelivery(body, 13),
        repository: "hosts/host-a",
        payload: {
          alertname: "Up",
          status: "firing",
          host: "host-a",
        },
      },
      {
        type: "event",
        provider: "grafana",
        event: "alert.resolved",
        delivery: await expectedDelivery(body, 14),
        repository: `hosts/${maxHost}`,
        payload: {
          status: "resolved",
          host: maxHost,
        },
      },
    ]);
    expect(logSpy).toHaveBeenCalledWith(
      "grafana alerts=15 published=2 skipped=13",
    );
  });

  it("extracts exact payload keys and falls back for html_url", async () => {
    const body = JSON.stringify({
      alerts: [
        {
          status: "firing",
          labels: {
            alertname: "HighCPU",
            host_name: "host-a",
            host: "host-b",
            severity: "critical",
          },
          annotations: { summary: "CPU is high" },
          valueString: "[ var='A' labels={} value=99 ]",
          startsAt: "2026-11-13T14:00:00Z",
          endsAt: "0001-01-01T00:00:00Z",
          fingerprint: "fp-1",
          panelURL: "https://grafana.example/d/1?viewPanel=2",
          dashboardURL: "https://grafana.example/d/1",
          silenceURL: "https://grafana.example/alerting/silence/new",
        },
        {
          status: "resolved",
          labels: { host_name: "host-a" },
          panelURL: "",
          dashboardURL: "https://grafana.example/d/1",
        },
        {
          status: "firing",
          labels: { host_name: "host-b" },
          panelURL: 123,
          dashboardURL: "https://grafana.example/d/2",
        },
        {
          status: "firing",
          labels: { alertname: 42, host_name: "host-b" },
          valueString: null,
          startsAt: 100,
          endsAt: false,
          fingerprint: {},
          panelURL: "",
          dashboardURL: 456,
        },
        {
          status: "resolved",
          labels: { host_name: "host-b" },
          panelURL: "",
          dashboardURL: "",
        },
      ],
    });
    const events = (await received({ body })) as {
      payload: unknown;
    }[];
    expect(events.map((e) => e.payload)).toStrictEqual([
      {
        alertname: "HighCPU",
        status: "firing",
        host: "host-a",
        value: "[ var='A' labels={} value=99 ]",
        starts_at: "2026-11-13T14:00:00Z",
        ends_at: "0001-01-01T00:00:00Z",
        fingerprint: "fp-1",
        html_url: "https://grafana.example/d/1?viewPanel=2",
      },
      {
        status: "resolved",
        host: "host-a",
        html_url: "https://grafana.example/d/1",
      },
      {
        status: "firing",
        host: "host-b",
        html_url: "https://grafana.example/d/2",
      },
      {
        status: "firing",
        host: "host-b",
      },
      {
        status: "resolved",
        host: "host-b",
      },
    ]);
  });

  it("keeps delivery IDs stable on retry and distinct per index", async () => {
    const body = JSON.stringify({
      alerts: [
        { status: "firing", labels: { host_name: "host-a" } },
        { status: "firing", labels: { host_name: "host-a" } },
      ],
    });
    const first = (await received({ body })) as { delivery: string }[];
    await disconnectAll(relay());
    const second = (await received({ body })) as { delivery: string }[];

    expect(first).toHaveLength(2);
    expect(first[0]?.delivery).toMatch(/^gf-[0-9a-f]{32}$/);
    expect(first[1]?.delivery).toMatch(/^gf-[0-9a-f]{32}$/);
    expect(first[0]?.delivery).not.toBe(first[1]?.delivery);
    expect(first[0]?.delivery).toBe(second[0]?.delivery);
    expect(first[1]?.delivery).toBe(second[1]?.delivery);
  });

  it("ignores a provider field in the body", async () => {
    const body = JSON.stringify({
      provider: "github",
      alerts: [
        {
          provider: "github",
          status: "firing",
          labels: { host_name: "host-a" },
        },
      ],
    });
    const [message] = (await received({ body })) as {
      provider: string;
      payload: Record<string, unknown>;
    }[];
    expect(message?.provider).toBe("grafana");
    expect(message?.payload).not.toHaveProperty("provider");
  });

  it("reaches only subscribers for the matching host repository", async () => {
    const stub = relay();
    const hit = await connect(stub, {
      subscriptions: [
        {
          provider: "grafana",
          event: "alert.firing",
          repository: "hosts/host-a",
        },
      ],
    });
    const miss = await connect(stub, {
      subscriptions: [
        {
          provider: "grafana",
          event: "alert.firing",
          repository: "hosts/host-b",
        },
      ],
    });
    expect((await post()).status).toBe(202);
    await settle();
    expect(hit).toHaveLength(1);
    expect(miss).toEqual([]);
  });

  it("takes only http(s) links for html_url", async () => {
    const alert = (panelURL: string, dashboardURL: string): unknown => ({
      status: "firing",
      labels: { host_name: "host-a" },
      panelURL,
      dashboardURL,
    });
    const body = JSON.stringify({
      alerts: [
        alert("javascript:alert(1)", "https://grafana.example/d/1"),
        alert("javascript:alert(1)", "data:text/html,x"),
        alert("xhttp://grafana.example", "HTTPS://grafana.example/d/3"),
        alert("http://grafana.example/d/4", "https://grafana.example/d/1"),
      ],
    });
    const events = (await received({ body })) as { payload: unknown }[];
    expect(events.map((e) => e.payload)).toStrictEqual([
      { status: "firing", host: "host-a", html_url: "https://grafana.example/d/1" },
      { status: "firing", host: "host-a" },
      { status: "firing", host: "host-a", html_url: "HTTPS://grafana.example/d/3" },
      { status: "firing", host: "host-a", html_url: "http://grafana.example/d/4" },
    ]);
  });
});
