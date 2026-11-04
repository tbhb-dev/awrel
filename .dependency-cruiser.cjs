// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

/** @type {import("dependency-cruiser").IConfiguration} */
module.exports = {
  forbidden: [
    {
      name: "no-circular",
      severity: "error",
      from: {},
      to: { circular: true },
    },
    {
      name: "no-test-imports",
      comment: "Only tests import test code.",
      severity: "error",
      from: { pathNot: "\\.test\\.ts$" },
      to: { path: "\\.test\\.ts$|testing\\.ts$" },
    },
  ],
  options: {
    doNotFollow: { path: "node_modules" },
    tsConfig: { fileName: "tsconfig.json" },
  },
};
