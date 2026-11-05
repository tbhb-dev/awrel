// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

// The deploy's pre- and post-deploy check against a deployed Worker.
// worker_deploy sets WORKER_URL, WORKER_VERSION_ID (the new version) and
// WORKER_REQUEST_HEADERS (the version override before the traffic move).
// Every response must name WORKER_VERSION_ID in Awrel-Worker-Version, so a
// check that reached the old version fails. Every request is a POST under
// /webhooks/ or a GET /connect upgrade, the only things the edge lets
// through. GITHUB_WEBHOOK_SECRET signs the test delivery and CONNECT_TOKEN
// is the relay token; neither is ever printed. The Worker answers a signed
// ping without calling the relay, and answers every /connect case here
// before it calls the Durable Object, so the cases pass against a Durable
// Object still running the previous version. The signed ping covers the
// signature path only, not the relay, and no case opens a connection.
// fetch can't send an Upgrade header, so /connect goes through node:http.
//
// main.mjs runs it; check.test.mjs tests it in-process.

import { createHmac, randomBytes } from "node:crypto";
import http from "node:http";
import https from "node:https";

// Runs the check against env's Worker and returns the exit status, writing
// progress to out.log and failures to out.error.
export async function run(env, out = console) {
  const url = env.WORKER_URL;
  if (!url) {
    out.error("e2e: WORKER_URL must be set");
    return 1;
  }
  const secret = env.GITHUB_WEBHOOK_SECRET;
  if (!secret) {
    out.error("e2e: GITHUB_WEBHOOK_SECRET must be set");
    return 1;
  }
  const token = env.CONNECT_TOKEN;
  if (!token) {
    out.error("e2e: CONNECT_TOKEN must be set");
    return 1;
  }
  const extra = JSON.parse(env.WORKER_REQUEST_HEADERS ?? "{}");
  const version = env.WORKER_VERSION_ID;
  const failures = [];

  // A response, or null after recording why the request failed: a timeout or
  // network error is a failed check, not a crash.
  async function call(name, path, headers, body) {
    try {
      return await fetch(new URL(path, url), {
        method: "POST",
        headers: { ...extra, ...headers },
        body,
        redirect: "manual",
        signal: AbortSignal.timeout(15_000),
      });
    } catch (error) {
      failures.push(`${name}: POST failed: ${error.message}`);
      return null;
    }
  }

  // Records whether an answer has the expected status and names the expected
  // version.
  function judge(name, status, expected, answered) {
    if (status !== expected) {
      failures.push(`${name}: status ${status}, expected ${expected}`);
    } else if (version && answered !== version) {
      failures.push(`${name}: version ${answered} answered, expected ${version}`);
    } else {
      out.log(`e2e: ok: ${name}`);
    }
  }

  async function expect(name, path, headers, status) {
    const body = "{}";
    const response = await call(name, path, headers, body);
    if (response === null) {
      return;
    }
    await response.arrayBuffer();
    judge(
      name,
      response.status,
      status,
      response.headers.get("Awrel-Worker-Version"),
    );
  }

  // A GET /connect upgrade request. Both an answer and an upgrade count as the
  // Worker answering; the socket is dropped either way.
  function handshake(name, headers, status) {
    const target = new URL("/connect", url);
    const request = (target.protocol === "https:" ? https : http).request(
      target,
      {
        method: "GET",
        headers: {
          ...extra,
          Connection: "Upgrade",
          Upgrade: "websocket",
          "Sec-WebSocket-Version": "13",
          "Sec-WebSocket-Key": randomBytes(16).toString("base64"),
          ...headers,
        },
        signal: AbortSignal.timeout(15_000),
      },
    );
    return new Promise((resolve) => {
      const answer = (response, socket) => {
        judge(
          name,
          response.statusCode,
          status,
          response.headers["awrel-worker-version"],
        );
        socket?.destroy();
        response.resume();
        resolve();
      };
      request.on("response", (response) => answer(response));
      request.on("upgrade", answer);
      request.on("error", (error) => {
        failures.push(`${name}: GET failed: ${error.message}`);
        resolve();
      });
      request.end();
    });
  }

  // The headers of a GitHub ping delivery of the body "{}", signed under `key`.
  function ping(key) {
    const digest = createHmac("sha256", key).update("{}").digest("hex");
    return {
      "X-Hub-Signature-256": `sha256=${digest}`,
      "X-GitHub-Event": "ping",
      "X-GitHub-Delivery": `e2e-${crypto.randomUUID()}`,
    };
  }

  const github = "/webhooks/github";
  await expect("an unsigned delivery is refused", github, {}, 401);
  await expect("a signed ping is accepted", github, ping(secret), 202);
  await expect(
    "a ping under another secret is refused",
    github,
    ping(`not-${secret}`),
    401,
  );
  await expect("an unknown provider is not found", "/webhooks/unknown", {}, 404);

  const subprotocol = { "Sec-WebSocket-Protocol": "awrel.v1" };
  await handshake("a connection without a token is refused", subprotocol, 401);
  await handshake(
    "a connection under another token is refused",
    { ...subprotocol, Authorization: `Bearer not-${token}` },
    401,
  );
  await handshake(
    "a connection without awrel.v1 is refused",
    { Authorization: `Bearer ${token}`, "Sec-WebSocket-Protocol": "awrel.v0" },
    400,
  );

  if (failures.length > 0) {
    for (const failure of failures) {
      out.error(`e2e: FAIL: ${failure}`);
    }
    return 1;
  }
  out.log(`e2e: ${url} passed`);
  return 0;
}
