// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

/** @type {import("@stryker-mutator/api/core").PartialStrykerOptions} */
export default {
  testRunner: "vitest",
  plugins: ["@stryker-mutator/vitest-runner"],
  // The tests reach the Worker through SELF, not an import, so Vitest
  // can't relate them to the mutated file.
  vitest: { configFile: "vitest.config.mjs", related: false },
  // Per-test coverage doesn't reach code running inside workerd, so every
  // mutant runs every test.
  coverageAnalysis: "all",
  mutate: ["src/**/*.ts", "!src/**/*.test.ts"],
  concurrency: 1,
  thresholds: { high: 100, low: 100, break: 100 },
  reporters: ["clear-text"],
};
