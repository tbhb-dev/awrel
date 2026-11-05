// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

// The deploys' pre- and post-deploy check, run from the environment that
// worker_deploy sets; see check.mjs.

import { run } from "./check.mjs";

process.exitCode = await run(process.env);
