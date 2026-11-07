# awrel

awrel relays webhooks from providers such as GitHub to coding agents. A Cloudflare Worker and Durable Object receive the events, and a stdio Model Context Protocol (MCP) server written in Go delivers them to the agent. The repository is at the bootstrap stage: the binary prints only its usage and build metadata.

## Development

Run `tbhbdev alpha install` once per machine to install the user-level harness hooks. Each session start then installs the clone's git hooks.

Bazel pins and runs every gate. [mise](https://mise.jdx.dev) only installs `bazelisk` and `uv`, pinned in `mise.toml` and locked in `mise.lock`. Run every gate with:

```sh
bazelisk test //...
```

`bazelisk run //:format` formats the tree in place, and `bazelisk run //:bazel_env` builds the shims for the pinned tools in `.bazel_env/bin`. Docs are in [docs/](docs/index.md).

## Running the server

The `awrel mcp` command speaks MCP over stdio, and it holds the connection to the relay. Matching events go to the agent. The agent manages its subscriptions with `subscribe`, `unsubscribe` and `list_subscriptions`. Add the server to the MCP configuration of the agent:

```json
{
  "mcpServers": {
    "awrel": {
      "command": "awrel",
      "args": ["mcp"],
      "env": { "AWREL_CONNECT_TOKEN": "${AWREL_CONNECT_TOKEN}" }
    }
  }
}
```

Set `AWREL_CONNECT_TOKEN` to the connect token. If that variable is empty, `AWREL_CONNECT_TOKEN_FILE` can point to a file with the token in it, and the server trims the whitespace around the token. Without a token the server still starts, and every tool fails with the reason. `AWREL_ENDPOINT`, or the `--endpoint` flag, sets the relay URL, which defaults to `wss://awrel.tbhb.dev/connect`.

Start Claude Code with the development channels flag, or it drops the events. A payload over 64 KiB reaches the agent as a short note, with the attribute `truncated` set to `true`.

Sessions in this repository use the staging relay through a pinned release in `.mcp.json`. They need Go and the staging token in `~/.config/awrel/stg-connect-token`:

```console
claude --dangerously-load-development-channels server:awrel
```

## Worker

The Worker and its Durable Object are in `src/`, configured by `wrangler.jsonc`, and their tests run inside `workerd`, the Workers runtime. After a change to `wrangler.jsonc`, `bazelisk run //:worker_types_test.update` regenerates `worker-configuration.d.ts`. The Worker accepts GitHub App webhooks at `POST /webhooks/github` and checks each delivery's signature against `GITHUB_WEBHOOK_SECRET`. It answers a `ping` event without relaying it. Every push to main deploys to `awrel-stg`, then `awrel-prd`, after the first manual deploy of each environment.

## License

Apache-2.0. See `LICENSE`.
