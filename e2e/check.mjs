// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

// The deploy's pre- and post-deploy check against a deployed Worker.
// worker_deploy sets WORKER_URL, WORKER_VERSION_ID (the new version) and
// WORKER_REQUEST_HEADERS (the version override before the traffic move).
// Every response must name WORKER_VERSION_ID in Awrel-Worker-Version, so a
// check that reached the old version fails.

const url = process.env.WORKER_URL;
if (!url) {
  console.error("e2e: WORKER_URL must be set");
  process.exit(1);
}
const extra = JSON.parse(process.env.WORKER_REQUEST_HEADERS ?? "{}");
const version = process.env.WORKER_VERSION_ID;

const failures = [];

// A response, or null after recording why the request failed: a timeout or
// network error is a failed check, not a crash.
async function call(name, method, path) {
  try {
    return await fetch(new URL(path, url), {
      method,
      headers: extra,
      redirect: "manual",
      signal: AbortSignal.timeout(15_000),
    });
  } catch (error) {
    failures.push(`${name}: ${method} failed: ${error.message}`);
    return null;
  }
}

async function expect(name, method, path, status) {
  const response = await call(name, method, path);
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

await expect("the root is not found", "GET", "/", 404);
await expect("an unknown path is not found", "POST", "/nope", 404);

if (failures.length > 0) {
  for (const failure of failures) {
    console.error(`e2e: FAIL: ${failure}`);
  }
  process.exit(1);
}
console.log(`e2e: ${url} passed`);
