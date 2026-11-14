// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

import { connect } from "./connect.ts";
import type { Env } from "./env.ts";
import { receiveGitHub } from "./github.ts";
import { receiveGrafana } from "./grafana.ts";

export { Relay } from "./relay.ts";

export default {
  async fetch(request, env): Promise<Response> {
    const { pathname } = new URL(request.url);
    const response =
      request.method === "POST" && pathname === "/webhooks/github"
        ? await receiveGitHub(request, env)
        : request.method === "POST" && pathname === "/webhooks/grafana"
          ? await receiveGrafana(request, env)
          : request.method === "GET" && pathname === "/connect"
            ? await connect(request, env)
            : new Response("Not Found", { status: 404 });
    // Awrel-Worker-Version names the version that answered, so a deploy's
    // checks can confirm they reached the version they meant to test. The
    // name is spelled out here, not in a module constant, so a mutation test
    // inside workerd can change it. A 101 keeps its WebSocket, or workerd
    // refuses the response.
    const headers = new Headers(response.headers);
    headers.set("Awrel-Worker-Version", env.CF_VERSION_METADATA.id);
    return new Response(response.body, {
      status: response.status,
      headers,
      webSocket: response.webSocket,
    });
  },
} satisfies ExportedHandler<Env>;
