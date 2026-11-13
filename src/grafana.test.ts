// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import { exports } from "cloudflare:workers";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  connect,
  digest,
  disconnectAll,
  GRAFANA_SECRET,
  relay,
  settle,
} from "./testing.ts";

afterEach(() => {
  vi.restoreAllMocks();
  return disconnectAll(relay());
});

interface Delivery {
  readonly body?: string;
  readonly signature?: string | null;
  readonly contentLength?: string | null;
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
  const signature =
    delivery.signature === undefined
      ? await digest(body, GRAFANA_SECRET)
      : delivery.signature;
  const headers = new Headers();
  if (signature !== null) {
    headers.set("X-Grafana-Alerting-Signature", signature);
  }
  if (delivery.contentLength !== undefined && delivery.contentLength !== null) {
    headers.set("Content-Length", delivery.contentLength);
  }
  return exports.default.fetch("https://awrel.example/webhooks/grafana", {
    method: "POST",
    body,
    headers,
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
      signature: (await digest(DEFAULT_BODY, GRAFANA_SECRET)).slice(0, -2),
    });
  });

  it("rejects a signature with a trailing odd nibble", async () => {
    await expectUnauthorized({
      signature: `${await digest(DEFAULT_BODY, GRAFANA_SECRET)}0`,
    });
  });

  it("rejects a signature with an extra byte", async () => {
    await expectUnauthorized({
      signature: `${await digest(DEFAULT_BODY, GRAFANA_SECRET)}00`,
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
      signature: await digest(DEFAULT_BODY, "other"),
    });
  });

  it("rejects a tampered body", async () => {
    await expectUnauthorized({
      body: JSON.stringify({
        alerts: [{ status: "resolved", labels: { host_name: "host-a" } }],
      }),
      signature: await digest(DEFAULT_BODY, GRAFANA_SECRET),
    });
  });

  it("rejects a sha256= prefixed signature", async () => {
    await expectUnauthorized({
      signature: `sha256=${await digest(DEFAULT_BODY, GRAFANA_SECRET)}`,
    });
  });

  it("checks the signature before parsing JSON", async () => {
    await expectUnauthorized({ signature: null, body: "{" });
    await expectUnauthorized({ signature: "00".repeat(32), body: "{" });
  });
});

describe("request", () => {
  async function expectBadRequest(body: string): Promise<void> {
    const response = await post({ body });
    expect(response.status).toBe(400);
    expect(await response.text()).toBe("Bad Request");
  }

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
});
