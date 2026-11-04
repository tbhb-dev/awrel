// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

// The deploy's pre- and post-deploy check against a deployed Worker.
// worker_deploy sets WORKER_URL, WORKER_VERSION_ID (the new version) and
// WORKER_REQUEST_HEADERS (the version override before the traffic move).
// Every response must name WORKER_VERSION_ID in Awrel-Worker-Version, so a
// check that reached the old version fails. Every request is a POST under
// /webhooks/, the only paths the edge lets through. GITHUB_WEBHOOK_SECRET
// signs the test delivery and is never printed. The Worker answers a signed
// ping without calling the relay, so that case covers the signature path
// only, not the relay.

import { createHmac } from "node:crypto";

const url = process.env.WORKER_URL;
if (!url) {
  console.error("e2e: WORKER_URL must be set");
  process.exit(1);
}
const secret = process.env.GITHUB_WEBHOOK_SECRET;
if (!secret) {
  console.error("e2e: GITHUB_WEBHOOK_SECRET must be set");
  process.exit(1);
}
const extra = JSON.parse(process.env.WORKER_REQUEST_HEADERS ?? "{}");
const version = process.env.WORKER_VERSION_ID;

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

async function expect(name, path, headers, status) {
  const body = "{}";
  const response = await call(name, path, headers, body);
  if (response === null) {
    return;
  }
  await response.arrayBuffer();
  const answered = response.headers.get("Awrel-Worker-Version");
  if (response.status !== status) {
    failures.push(`${name}: status ${response.status}, expected ${status}`);
  } else if (version && answered !== version) {
    failures.push(`${name}: version ${answered} answered, expected ${version}`);
  } else {
    console.log(`e2e: ok: ${name}`);
  }
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

if (failures.length > 0) {
  for (const failure of failures) {
    console.error(`e2e: FAIL: ${failure}`);
  }
  process.exit(1);
}
console.log(`e2e: ${url} passed`);
