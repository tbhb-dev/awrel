// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import type { Relay } from "./relay.ts";

/**
 * The Worker's bindings: the relay Durable Object, the GitHub App's webhook
 * secret (a Worker secret), and the Worker's own version.
 */
export interface Env {
  readonly RELAY: DurableObjectNamespace<Relay>;
  readonly GITHUB_WEBHOOK_SECRET: string;
  readonly CF_VERSION_METADATA: { readonly id: string };
}
