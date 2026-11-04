// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import tseslint from "typescript-eslint";

export default tseslint.config(
  // wrangler's generated types (worker_types_test checks them).
  { ignores: ["worker-configuration.d.ts"] },
  ...tseslint.configs.strictTypeChecked,
  {
    languageOptions: {
      parserOptions: {
        projectService: true,
        tsconfigRootDir: import.meta.dirname,
      },
    },
  },
  {
    files: ["**/*.{js,cjs,mjs}"],
    ...tseslint.configs.disableTypeChecked,
  },
);
