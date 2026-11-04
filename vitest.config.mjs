// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

// The tests run inside workerd against the top level of wrangler.jsonc, with
// a local Durable Object namespace. The webhook secret is a Worker secret, so
// the config binds one for the tests; it must match SECRET in src/testing.ts.
export default defineConfig({
  plugins: [
    cloudflareTest({
      wrangler: { configPath: "./wrangler.jsonc" },
      miniflare: { bindings: { GITHUB_WEBHOOK_SECRET: "test-webhook-secret" } },
    }),
  ],
  test: {
    include: ["src/**/*.test.ts"],
    coverage: {
      provider: "istanbul",
      include: ["src/**/*.ts"],
      exclude: ["src/**/*.test.ts", "src/testing.ts"],
      thresholds: { 100: true },
    },
  },
});
