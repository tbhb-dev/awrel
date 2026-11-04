// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import { DurableObject } from "cloudflare:workers";

/** The Worker's bindings: the relay Durable Object and its own version. */
export interface Env {
  readonly RELAY: DurableObjectNamespace<Relay>;
  readonly CF_VERSION_METADATA: { readonly id: string };
}

/**
 * The relay between webhook providers and MCP connections. It has no
 * behavior yet; the class exists so its binding and first migration deploy
 * with the script.
 */
export class Relay extends DurableObject<Env> {}

export default {
  fetch(_request, env): Response {
    // Awrel-Worker-Version names the version that answered, so a deploy's
    // checks can confirm they reached the version they meant to test. The
    // name is spelled out here, not in a module constant, so a mutation test
    // inside workerd can change it.
    return new Response("Not Found", {
      status: 404,
      headers: { "Awrel-Worker-Version": env.CF_VERSION_METADATA.id },
    });
  },
} satisfies ExportedHandler<Env>;
