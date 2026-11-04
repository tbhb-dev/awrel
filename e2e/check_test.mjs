// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

// Runs check.mjs against a stand-in for a deployed Worker, served over HTTP,
// the way a deploy runs it. The stand-in answers like the Worker (404 with
// its version header) only when the request carries the version override
// header the deploy passes.

import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { join } from "node:path";
import { test } from "node:test";

const runfiles = process.env.RUNFILES_DIR ?? process.env.TEST_SRCDIR;
const root = join(runfiles, "_main");

const VERSION = "7d2e4f60-1b3c-4a5d-8e9f-0a1b2c3d4e5f";
const OVERRIDE = "Cloudflare-Workers-Version-Overrides";
const HEADERS = { [OVERRIDE]: `awrel-stg="${VERSION}"` };

async function serve({ status = 404, version = VERSION } = {}) {
  const server = createServer((req, res) => {
    if (req.headers[OVERRIDE.toLowerCase()] !== HEADERS[OVERRIDE]) {
      res.writeHead(412);
      res.end();
      return;
    }
    res.writeHead(status, { "Awrel-Worker-Version": version });
    res.end("Not Found");
  });
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  return server;
}

function check(url, { headers = HEADERS, version = VERSION } = {}) {
  const env = { ...process.env, WORKER_REQUEST_HEADERS: JSON.stringify(headers) };
  if (url !== undefined) {
    env.WORKER_URL = url;
  }
  env.WORKER_VERSION_ID = version;
  const child = spawn(process.execPath, [join(root, "e2e", "check.mjs")], {
    env,
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
  assert.match(output, /status 200, expected 404/);
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
  assert.match(output, /GET failed/);
});

test("refuses to run without WORKER_URL", async () => {
  const { status, output } = await check(undefined);
  assert.equal(status, 1, output);
  assert.match(output, /WORKER_URL must be set/);
});
