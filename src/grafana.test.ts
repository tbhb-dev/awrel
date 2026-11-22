// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import { env, exports } from "cloudflare:workers";
import { afterEach, expect, it, vi } from "vitest";
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

const firing = { status: "firing", labels: { host_name: "host-a" } };
const envelope = (alerts: unknown[]) => JSON.stringify({ alerts });
const defaultBody = envelope([firing]);
afterEach(() => {
  vi.restoreAllMocks();
  return disconnectAll(relay());
});

async function post(
  body = defaultBody,
  signature?: string | null,
  length?: string,
  streamed = false,
) {
  const headers = new Headers();
  if (signature !== null)
    headers.set(
      "X-Grafana-Alerting-Signature",
      signature ?? (await digest(body, GRAFANA_SECRET)),
    );
  if (length !== undefined) headers.set("Content-Length", length);
  const bytes = new TextEncoder().encode(body);
  const stream = new ReadableStream({
    start(controller) {
      for (let i = 0; i < bytes.length; i += 65_536)
        controller.enqueue(bytes.slice(i, i + 65_536));
      controller.close();
    },
  });
  return exports.default.fetch("https://awrel.example/webhooks/grafana", {
    method: "POST",
    headers,
    body: streamed ? stream : body,
    // @ts-expect-error: workerd accepts streams; DOM types require duplex
    duplex: streamed ? "half" : undefined,
  });
}

async function received(alerts: unknown[]) {
  const messages = await connect(relay(), {
    subscriptions: [{ provider: "grafana" }],
  });
  expect((await post(envelope(alerts))).status).toBe(202);
  await settle();
  return messages.map(
    (message) =>
      JSON.parse(message) as {
        provider: string;
        event: string;
        repository: string;
        delivery: string;
        payload: Record<string, string>;
      },
  );
}

it.each([
  ["declared", defaultBody, "1048577", false],
  ["streamed", "x".repeat(1_048_577), undefined, true],
])(
  "answers 413 for an oversized %s body",
  async (_, body, length, streamed) => {
    expect((await post(body, null, length, streamed)).status).toBe(413);
  },
);
it("accepts exactly 1 MiB in multiple chunks", async () => {
  const body = defaultBody.padEnd(1_048_576, " ");
  expect((await post(body, undefined, "1048576", true)).status).toBe(202);
});
it.each([null, "FF".repeat(32), "sha256=invalid"])(
  "answers 401 for missing or bad signature %j before parsing",
  async (signature) => {
    expect((await post("{", signature)).status).toBe(401);
  },
);
it.each(["", "{"])("answers 400 for signed bad JSON %j", async (body) => {
  expect((await post(body)).status).toBe(400);
});
it("answers 400 for a signed request without a body", async () => {
  const request = new Request("https://awrel.example/webhooks/grafana", {
    method: "POST",
    headers: {
      "X-Grafana-Alerting-Signature": await digest("", GRAFANA_SECRET),
    },
  });
  expect((await receiveGrafana(request, env as unknown as Env)).status).toBe(
    400,
  );
});
it.each([
  "null",
  "{}",
  '{"alerts":{}}',
  envelope([]),
  envelope(Array.from({ length: 101 }, () => firing)),
])("answers 400 for an invalid alerts envelope %j", async (body) => {
  expect((await post(body)).status).toBe(400);
});
it("answers 503 without a secret and publishes nothing", async () => {
  const messages = await connect(relay(), {
    subscriptions: [{ provider: "grafana" }],
  });
  const request = new Request("https://awrel.example/webhooks/grafana", {
    method: "POST",
  });
  expect(
    (
      await receiveGrafana(request, {
        ...env,
        GRAFANA_WEBHOOK_SECRET: "",
      } as unknown as Env)
    ).status,
  ).toBe(503);
  await settle();
  expect(messages).toEqual([]);
});
it("skips alerts without valid status or scope and counts them", async () => {
  const log = vi.spyOn(console, "log").mockImplementation(() => undefined);
  expect(
    await received([
      null,
      { status: "pending" },
      { status: "firing" },
      {
        status: "firing",
        labels: { host_name: "bad/name", agent_account: "" },
      },
    ]),
  ).toEqual([]);
  expect(log).toHaveBeenCalledWith("grafana alerts=4 published=0 skipped=4");
});
it.each([
  [
    { host_name: "Host_1.a-b", agent_account: "account-a" },
    "hosts",
    "Host_1.a-b",
    "firing",
  ],
  [{ agent_account: "account-a" }, "capacity", "account-a", "resolved"],
  [
    { host_name: "/invalid", agent_account: "account-a" },
    "capacity",
    "account-a",
    "firing",
  ],
])(
  "uses the first valid scope label %j",
  async (labels, kind, scope, status) => {
    const [event] = await received([{ status, labels }]);
    expect(event?.provider).toBe("grafana");
    expect(event?.event).toBe(`alert.${status}`);
    expect(event?.repository).toBe(`${kind}/${scope}`);
    expect(event?.payload).toEqual({ status, scope, kind });
  },
);
it("copies only allowlisted string payload fields", async () => {
  const [event] = await received([
    {
      ...firing,
      provider: "github",
      labels: { ...firing.labels, alertname: "HighCPU", extra: "private" },
      annotations: { summary: "extra" },
      valueString: "99",
      startsAt: "start",
      endsAt: "end",
      fingerprint: "fp",
      panelURL: "https://example.com/p",
      dashboardURL: "https://example.com/d",
      silenceURL: "https://example.com/s",
    },
  ]);
  expect(event?.payload).toEqual({
    status: "firing",
    scope: "host-a",
    kind: "hosts",
    alertname: "HighCPU",
    value: "99",
    starts_at: "start",
    ends_at: "end",
    fingerprint: "fp",
    html_url: "https://example.com/p",
  });
});
it.each([
  ["", "https://example.com/d", "https://example.com/d"],
  ["javascript:x", "data:text/html,x", undefined],
  [undefined, undefined, undefined],
])(
  "uses an allowed panel or dashboard link %j",
  async (panelURL, dashboardURL, expected) => {
    const [event] = await received([{ ...firing, panelURL, dashboardURL }]);
    expect(event?.payload["html_url"]).toBe(expected);
  },
);
it("keeps delivery IDs stable across a retry and distinct per alert", async () => {
  const alerts = [firing, firing];
  const first = await received(alerts);
  await disconnectAll(relay());
  const second = await received(alerts);
  expect(first.map((event) => event.delivery)).toEqual(
    second.map((event) => event.delivery),
  );
  expect(first[0]?.delivery).toMatch(/^gf-[0-9a-f]{32}$/);
  expect(first[0]?.delivery).not.toBe(first[1]?.delivery);
});
