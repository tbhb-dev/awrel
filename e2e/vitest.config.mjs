// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

// The deploy check's tests. They import the Worker's handlers from src/, so
// the root is the repository's; //:vitest_test covers the Worker itself.
// //e2e:check_test enforces the 100% coverage floor.
export default defineConfig({
  test: {
    root: fileURLToPath(new URL("..", import.meta.url)),
    include: ["e2e/**/*.test.mjs"],
    coverage: {
      provider: "v8",
      include: ["e2e/**/*.mjs"],
      exclude: ["e2e/**/*.test.mjs", "e2e/vitest.config.mjs"],
    },
  },
});
