// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

// Runs check.mjs against a stand-in for a deployed Worker, served over HTTP,
// the way a deploy runs it. The stand-in runs the Worker's real webhook
// receiver (src/github.ts) with a relay that drops events, answers anything
// else with 404, and names its version, but only when the request carries
// the version override header the deploy passes.

import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { join } from "node:path";
import { test } from "node:test";

const runfiles = process.env.RUNFILES_DIR ?? process.env.TEST_SRCDIR;
const root = join(runfiles, "_main");
const { receiveGitHub } = await import(join(root, "src", "github.ts"));

const VERSION = "7d2e4f60-1b3c-4a5d-8e9f-0a1b2c3d4e5f";
const OVERRIDE = "Cloudflare-Workers-Version-Overrides";
const HEADERS = { [OVERRIDE]: `awrel-stg="${VERSION}"` };
const SECRET = "stand-in-webhook-secret";

const env = {
  GITHUB_WEBHOOK_SECRET: SECRET,
  RELAY: { getByName: () => ({ publish: async () => 0 }) },
};

// `status` replaces every answer's status, to stand in for a broken Worker.
async function serve({ status, version = VERSION } = {}) {
  const server = createServer(async (req, res) => {
    if (req.headers[OVERRIDE.toLowerCase()] !== HEADERS[OVERRIDE]) {
      res.writeHead(412);
      res.end();
      return;
    }
    const chunks = [];
    for await (const chunk of req) {
      chunks.push(chunk);
    }
    const response =
      req.method === "POST" && req.url === "/webhooks/github"
        ? await receiveGitHub(
            new Request(`http://localhost${req.url}`, {
              method: "POST",
              headers: req.headers,
              body: Buffer.concat(chunks),
            }),
            env,
          )
        : new Response("Not Found", { status: 404 });
    res.writeHead(status ?? response.status, { "Awrel-Worker-Version": version });
    res.end(Buffer.from(await response.arrayBuffer()));
  });
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  return server;
}

// A null `secret` leaves GITHUB_WEBHOOK_SECRET unset.
function check(
  url,
  { headers = HEADERS, version = VERSION, secret = SECRET } = {},
) {
  const childEnv = {
    ...process.env,
    WORKER_REQUEST_HEADERS: JSON.stringify(headers),
    WORKER_VERSION_ID: version,
  };
  delete childEnv.WORKER_URL;
  delete childEnv.GITHUB_WEBHOOK_SECRET;
  if (url !== undefined) {
    childEnv.WORKER_URL = url;
  }
  if (secret !== null) {
    childEnv.GITHUB_WEBHOOK_SECRET = secret;
  }
  const child = spawn(process.execPath, [join(root, "e2e", "check.mjs")], {
    env: childEnv,
  });
  let output = "";
  child.stdout.on("data", (d) => (output += d));
  child.stderr.on("data", (d) => (output += d));
  return new Promise((r) =>
    child.on("close", (status) => r({ status, output })),
  );
}

async function against(options, run) {
  const server = await serve(options);
  try {
    return await run(`http://127.0.0.1:${server.address().port}`);
  } finally {
    server.close();
  }
}

test("passes against the version it expects", async () => {
  const { status, output } = await against({}, (url) => check(url));
  assert.equal(status, 0, output);
  assert.match(output, /passed/);
  assert.ok(!output.includes(SECRET));
});

test("fails when the Worker holds another secret", async () => {
  const { status, output } = await against({}, (url) =>
    check(url, { secret: "other-secret" }),
  );
  assert.equal(status, 1, output);
  assert.match(output, /a signed ping is accepted: status 401, expected 202/);
  assert.ok(!output.includes("other-secret"));
});

test("fails when another version answers", async () => {
  const { status, output } = await against({ version: "old" }, (url) =>
    check(url),
  );
  assert.equal(status, 1, output);
  assert.match(output, /version old answered/);
});

test("fails on an unexpected status", async () => {
  const { status, output } = await against({ status: 200 }, (url) =>
    check(url),
  );
  assert.equal(status, 1, output);
  assert.match(output, /status 200, expected 401/);
});

test("fails without the version override", async () => {
  const { status, output } = await against({}, (url) =>
    check(url, { headers: {} }),
  );
  assert.equal(status, 1, output);
  assert.match(output, /status 412/);
});

test("fails when the Worker can't be reached", async () => {
  const { status, output } = await check("http://127.0.0.1:1");
  assert.equal(status, 1, output);
  assert.match(output, /POST failed/);
});

test("refuses to run without WORKER_URL", async () => {
  const { status, output } = await check(undefined);
  assert.equal(status, 1, output);
  assert.match(output, /WORKER_URL must be set/);
});

test("refuses to run without GITHUB_WEBHOOK_SECRET", async () => {
  const { status, output } = await check("http://127.0.0.1:1", {
    secret: null,
  });
  assert.equal(status, 1, output);
  assert.match(output, /GITHUB_WEBHOOK_SECRET must be set/);
});
