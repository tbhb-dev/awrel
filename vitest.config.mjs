// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

// The tests run inside workerd against the top level of wrangler.jsonc, with
// a local Durable Object namespace. The webhook secrets are Worker secrets, so
// the config binds them for the tests; they must match SECRET, GRAFANA_SECRET
// and TOKEN in src/testing.ts.
export default defineConfig({
  plugins: [
    cloudflareTest({
      wrangler: { configPath: "./wrangler.jsonc" },
      miniflare: {
        bindings: {
          CONNECT_TOKEN: "test-connect-token",
          GITHUB_WEBHOOK_SECRET: "test-webhook-secret",
          GRAFANA_WEBHOOK_SECRET: "test-grafana-secret",
        },
      },
    }),
  ],
  test: {
    include: ["src/**/*.test.ts"],
    coverage: {
      provider: "istanbul",
      include: ["src/**/*.ts"],
      exclude: ["src/**/*.test.ts", "src/testing.ts"],
    },
  },
});
