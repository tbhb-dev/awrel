// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import type { Relay } from "./relay.ts";

/**
 * The Worker's bindings: the relay Durable Object, the GitHub App's and
 * Grafana's webhook secrets and the relay connection token (all Worker
 * secrets), and the Worker's own version.
 */
export interface Env {
  readonly RELAY: DurableObjectNamespace<Relay>;
  readonly GITHUB_WEBHOOK_SECRET: string;
  readonly GRAFANA_WEBHOOK_SECRET: string;
  readonly CONNECT_TOKEN: string;
  readonly CF_VERSION_METADATA: { readonly id: string };
}
