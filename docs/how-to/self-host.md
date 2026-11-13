---
description: Deploy the Worker and Durable Object, create the GitHub App, and point the awrel server at your own relay.
generated:
  - role: generated
    harness: claude-code:2.1.322
    model: anthropic:claude-sonnet-5-5
    effort: medium
    surface: cli
    session: sha-256-64:dc0c7bb9a6fc4839
    at: "2026-11-13T16:46:24Z"
status: stable
tags: [docs, self-host, deploy]
title: Self-host a relay
type: How-to
---

# Self-host a relay

The relay is a Cloudflare Worker and a Durable Object. The Worker receives GitHub App webhooks and serves the `/connect` WebSocket, and the Durable Object keeps the connections and subscriptions. `awrel mcp` has no default relay, so you need one to use awrel.

## Before you start

You need a Cloudflare account with Workers and Durable Objects, a domain on Cloudflare for the relay, [Node.js](https://nodejs.org) with `npx`, and a GitHub account or organization that can own a GitHub App.

## Deploy the worker

Clone the repository and install its dependencies. The Worker is `src/index.ts`, and `wrangler.jsonc` configures it, with a Durable Object binding `RELAY` and a SQLite-backed migration.

`wrangler.jsonc` holds the maintainers' Worker names, so change them. Set `name` to your Worker name, and add a custom domain for the relay in `routes`:

```jsonc
{
  "name": "my-awrel",
  "routes": [{ "pattern": "relay.example.com", "custom_domain": true }]
}
```

Keep the `RELAY` binding, `version_metadata` and `migrations`. Delete the `env` block, which holds the maintainers' two environments, and use the top level. The maintainers' environments turn on Workers Logs. To get logs for yours, add the same `observability` block at the top level: `"observability": { "enabled": true, "head_sampling_rate": 1 }`.

Deploy with `npx wrangler deploy`.

## Set the secrets

The Worker reads three secrets and nothing else:

| Secret | Use |
| --- | --- |
| `GITHUB_WEBHOOK_SECRET` | The secret that signs each webhook delivery. The Worker answers 401 to a delivery whose `X-Hub-Signature-256` header doesn't match. |
| `GRAFANA_WEBHOOK_SECRET` | The secret that signs each Grafana alert delivery. The Worker answers 503 when it's empty and 401 to a delivery whose signature or timestamp doesn't check out. |
| `CONNECT_TOKEN` | The bearer token that `awrel mcp` presents on `/connect`. |

Pick a long random value for each, and keep them out of version control:

```sh
npx wrangler secret put GITHUB_WEBHOOK_SECRET
npx wrangler secret put GRAFANA_WEBHOOK_SECRET
npx wrangler secret put CONNECT_TOKEN
```

Wrangler prompts for each value. Give `GITHUB_WEBHOOK_SECRET` the same value you enter in the GitHub App below, and `GRAFANA_WEBHOOK_SECRET` the same value you enter in the Grafana contact point.

## Create the GitHub app

In your GitHub account or organization settings, create a GitHub App with these settings:

- Webhook URL: `https://relay.example.com/webhooks/github`, with your relay host.
- Webhook secret: the value of `GITHUB_WEBHOOK_SECRET`.
- Repository permissions: read-only access to what the events need, such as Issues, Pull requests, and so on. GitHub sends an event only when the App can read its subject.
- Subscribe to events: whichever events agents should see, such as Pull request.

The Worker relays whatever events the App sends. It answers a `ping` event without relaying it. Agents choose an event type and repository with each `subscribe` call: `{"provider":"github","event":"pull_request","repository":"owner/name"}`.

Deploy a relay with subscription filter support before using filtered tools. The client probes support on every connection and refuses incompatible relays. The [subscription filter examples](../../README.md#subscription-filters) cover completed check failures, exact check names, workflow paths, and removal. Filters narrow the events the App already sends.

## Create the Grafana contact point

Skip this section if you don't relay Grafana alerts. In Grafana, add a webhook contact point with these settings:

- URL: `https://relay.example.com/webhooks/grafana`, with your relay host.
- HTTP method: `POST`.
- Optional settings, HMAC signature secret: the value of `GRAFANA_WEBHOOK_SECRET`.
- Optional settings, Timestamp Header: exactly `X-Grafana-Alerting-Timestamp`.

Grafana names no default for the timestamp header, and the field is optional there, but the Worker requires it. With the field set, Grafana signs `<unix-seconds>:<body>` and sends the signature in `X-Grafana-Alerting-Signature`, a 64-digit hexadecimal Hash-based Message Authentication Code (HMAC) of that input. The Worker answers 401 when the timestamp is missing or more than five minutes from its clock. The window bounds a replayed delivery and the Worker keeps no record of earlier deliveries, so a captured request can repeat inside it.

An alert becomes an event when its `host_name` label is a host name of up to 100 letters, digits, dots, hyphens, and underscores, and its status is `firing` or `resolved`. Agents subscribe with `{"provider":"grafana","event":"alert.firing","repository":"hosts/<host_name>"}`.

## Install the app

Install the App on the repositories to watch. GitHub then delivers their events to the relay. In the App's advanced settings, check that recent deliveries show a 202 response.

## Connect the server

Point the server at your relay with the same token. Put the endpoint in `AWREL_ENDPOINT`, or pass `--endpoint`:

```json
{
  "mcpServers": {
    "awrel": {
      "command": "awrel",
      "args": ["mcp"],
      "env": {
        "AWREL_ENDPOINT": "wss://relay.example.com/connect",
        "AWREL_CONNECT_TOKEN": "${AWREL_CONNECT_TOKEN}"
      }
    }
  }
}
```

Set `AWREL_CONNECT_TOKEN` to the value of `CONNECT_TOKEN`, or point `AWREL_CONNECT_TOKEN_FILE` at a file that holds it. The [relay protocol](../reference/relay-protocol.md) describes the `/connect` handshake and its error statuses.
