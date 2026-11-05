// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

// Runs check.mjs against a stand-in for a deployed Worker, served over HTTP,
// the way a deploy runs it. The stand-in runs the Worker's real webhook
// receiver (src/github.ts) and connection handshake (src/connect.ts) with a
// relay that drops events and refuses connections, answers anything else
// with 404, and names its version, but only when the request carries the
// version override header the deploy passes.

import { timingSafeEqual } from "node:crypto";
import { createServer } from "node:http";
import { afterEach, describe, expect, test, vi } from "vitest";
import { connect } from "../src/connect.ts";
import { receiveGitHub } from "../src/github.ts";
import { run } from "./check.mjs";

// workerd has crypto.subtle.timingSafeEqual and Node doesn't, so the stand-in
// borrows node:crypto's for src/connect.ts. connect.ts reads it only when a
// request arrives, so the static import above is safe.
Object.defineProperty(crypto.subtle, "timingSafeEqual", {
  value: (a, b) => timingSafeEqual(new Uint8Array(a), new Uint8Array(b)),
});

const VERSION = "7d2e4f60-1b3c-4a5d-8e9f-0a1b2c3d4e5f";
const OVERRIDE = "Cloudflare-Workers-Version-Overrides";
const HEADERS = { [OVERRIDE]: `awrel-stg="${VERSION}"` };
const SECRET = "stand-in-webhook-secret";
const TOKEN = "stand-in-connect-token";

const env = {
  GITHUB_WEBHOOK_SECRET: SECRET,
  CONNECT_TOKEN: TOKEN,
  RELAY: {
    getByName: () => ({
      publish: async () => 0,
      fetch: async () => new Response("stand-in relay", { status: 500 }),
    }),
  },
};

const servers = [];
afterEach(async () => {
  await Promise.all(
    servers.splice(0).map((s) => new Promise((r) => s.close(r))),
  );
  vi.unstubAllEnvs();
});

// The stand-in at the returned URL. `status` replaces every webhook answer's
// status and `connectStatus` every /connect answer's, to stand in for a
// broken Worker.
async function serve({ status, connectStatus, version = VERSION } = {}) {
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
    res.writeHead(status ?? response.status, {
      "Awrel-Worker-Version": version,
    });
    res.end(Buffer.from(await response.arrayBuffer()));
  });
  server.on("upgrade", async (req, socket) => {
    const refuse = (code, body = "") =>
      socket.end(
        `HTTP/1.1 ${code} X\r\nContent-Length: ${body.length}\r\nConnection: close\r\n\r\n${body}`,
      );
    if (req.headers[OVERRIDE.toLowerCase()] !== HEADERS[OVERRIDE]) {
      refuse(412);
      return;
    }
    const response =
      req.method === "GET" && req.url === "/connect"
        ? await connect(
            new Request(`http://localhost${req.url}`, {
              headers: req.headers,
            }),
            env,
          )
        : new Response("Not Found", { status: 404 });
    const code = connectStatus ?? response.status;
    const body = await response.text();
    const lines = [
      `HTTP/1.1 ${code} X`,
      `Awrel-Worker-Version: ${version}`,
      `Content-Length: ${body.length}`,
      "Connection: close",
    ];
    socket.end(`${lines.join("\r\n")}\r\n\r\n${body}`);
  });
  servers.push(server);
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  return `http://127.0.0.1:${server.address().port}`;
}

// The deploy's environment for a check against url. An override of undefined
// leaves the variable unset.
function deploy(url, overrides = {}) {
  return {
    WORKER_URL: url,
    GITHUB_WEBHOOK_SECRET: SECRET,
    CONNECT_TOKEN: TOKEN,
    WORKER_REQUEST_HEADERS: JSON.stringify(HEADERS),
    WORKER_VERSION_ID: VERSION,
    ...overrides,
  };
}

// Runs the check, returning its status and everything it wrote.
async function check(environment) {
  let output = "";
  const write = (line) => {
    output += `${line}\n`;
  };
  const status = await run(environment, { log: write, error: write });
  return { status, output };
}

describe("run", () => {
  test("passes against the version it expects", async () => {
    const { status, output } = await check(deploy(await serve()));
    expect(status, output).toBe(0);
    expect(output).toMatch(/passed/);
    expect(output).not.toContain(SECRET);
    expect(output).not.toContain(TOKEN);
  });

  test("passes without a version to require", async () => {
    const { status, output } = await check(
      deploy(await serve({ version: "any" }), { WORKER_VERSION_ID: undefined }),
    );
    expect(status, output).toBe(0);
  });

  test("fails when the Worker holds another connect token", async () => {
    const { status, output } = await check(
      deploy(await serve(), { CONNECT_TOKEN: "other-token" }),
    );
    expect(status, output).toBe(1);
    expect(output).toMatch(
      /a connection without awrel.v1 is refused: status 401, expected 400/,
    );
    expect(output).not.toContain("other-token");
  });

  test("fails when /connect accepts the upgrade", async () => {
    const { status, output } = await check(
      deploy(await serve({ connectStatus: 101 })),
    );
    expect(status, output).toBe(1);
    expect(output).toMatch(/status 101, expected 401/);
  });

  test("fails when the Worker holds another secret", async () => {
    const { status, output } = await check(
      deploy(await serve(), { GITHUB_WEBHOOK_SECRET: "other-secret" }),
    );
    expect(status, output).toBe(1);
    expect(output).toMatch(/a signed ping is accepted: status 401, expected 202/);
    expect(output).not.toContain("other-secret");
  });

  test("fails when another version answers", async () => {
    const { status, output } = await check(
      deploy(await serve({ version: "old" })),
    );
    expect(status, output).toBe(1);
    expect(output).toMatch(/version old answered/);
  });

  test("fails on an unexpected status", async () => {
    const { status, output } = await check(
      deploy(await serve({ status: 200 })),
    );
    expect(status, output).toBe(1);
    expect(output).toMatch(/status 200, expected 401/);
  });

  test("fails without the version override", async () => {
    const { status, output } = await check(
      deploy(await serve(), { WORKER_REQUEST_HEADERS: "{}" }),
    );
    expect(status, output).toBe(1);
    expect(output).toMatch(/status 412/);
  });

  test("sends no extra headers when WORKER_REQUEST_HEADERS is unset", async () => {
    const { status, output } = await check(
      deploy(await serve(), { WORKER_REQUEST_HEADERS: undefined }),
    );
    expect(status, output).toBe(1);
    expect(output).toMatch(/status 412/);
  });

  test("fails when the Worker can't be reached", async () => {
    const { status, output } = await check(deploy("http://127.0.0.1:1"));
    expect(status, output).toBe(1);
    expect(output).toMatch(/POST failed/);
    expect(output).toMatch(/GET failed/);
  });

  test("reaches an https Worker through the https module", async () => {
    const { status, output } = await check(deploy("https://127.0.0.1:1"));
    expect(status, output).toBe(1);
    expect(output).toMatch(/GET failed/);
  });

  test.each([
    ["WORKER_URL", { WORKER_URL: undefined }],
    ["GITHUB_WEBHOOK_SECRET", { GITHUB_WEBHOOK_SECRET: undefined }],
    ["CONNECT_TOKEN", { CONNECT_TOKEN: undefined }],
  ])("refuses to run without %s", async (name, overrides) => {
    const { status, output } = await check(
      deploy("http://127.0.0.1:1", overrides),
    );
    expect(status, output).toBe(1);
    expect(output).toContain(`${name} must be set`);
  });

  test("keeps no failures from one call to the next", async () => {
    const url = await serve();
    const failing = await check(deploy(url, { WORKER_VERSION_ID: "other" }));
    expect(failing.status).toBe(1);
    const passing = await check(deploy(url));
    expect(passing.status, passing.output).toBe(0);
  });
});

test("main.mjs runs the check from the process environment", async () => {
  for (const [name, value] of Object.entries(deploy(await serve()))) {
    vi.stubEnv(name, value);
  }
  const log = vi.spyOn(console, "log").mockImplementation(() => {});
  try {
    await import("./main.mjs");
    expect(process.exitCode).toBe(0);
    expect(log).toHaveBeenCalledWith(expect.stringMatching(/passed$/));
  } finally {
    process.exitCode = undefined;
    log.mockRestore();
  }
});
