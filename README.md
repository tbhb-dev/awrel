# awrel

awrel relays webhooks from providers such as GitHub to coding agents. A Cloudflare Worker and Durable Object receive the events, and a stdio Model Context Protocol (MCP) server written in Go delivers them to the agent. The agent subscribes to the events it wants, and awrel delivers them as they happen. You run the relay yourself.

## Install

You need Go. Install the binary with:

```sh
go install go.tbhb.dev/awrel/cmd/awrel@latest
```

Or skip the install and run a pinned release from the MCP configuration, with `go run go.tbhb.dev/awrel/cmd/awrel@<version> mcp` as the command.

## Running the server

The `awrel mcp` command speaks MCP over stdio, and it holds the connection to the relay. Matching events go to the agent. The agent manages its subscriptions with `subscribe`, `unsubscribe` and `list_subscriptions`. `awrel mcp` needs a relay to connect to, and the endpoint has no default. [Self-host one](docs/how-to/self-host.md) first. Add the server to the MCP configuration of the agent:

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

Set `AWREL_ENDPOINT`, or the `--endpoint` flag, to the relay URL, and `AWREL_CONNECT_TOKEN` to the connect token. If that variable is empty, `AWREL_CONNECT_TOKEN_FILE` can point to a file with the token in it, and the server trims the whitespace around the token. If the endpoint or the token is missing, the server still starts, and every tool fails with the reason.

Start Claude Code with the development channels flag, or it drops the events:

```console
claude --dangerously-load-development-channels server:awrel
```

A payload over 64 KiB reaches the agent as a short note, with the attribute `truncated` set to `true`.

By default the server delivers compact payloads, which reduce each payload to a selection of its fields. A compact body has no API URLs, only the login and type for each user, and can still include third-party text such as issue and comment bodies and commit messages. Events arrive with the attribute `compact` set to `true`, and the agent fetches details with `gh`. Compaction runs before the 64 KiB check, so most large pushes arrive. Pass `--format full` or set `AWREL_FORMAT=full` to get the whole payload. Either overrides the default of `compact`, and the flag takes precedence over the variable. See the documentation of the `internal/compact` package for the rules.

## Worker

The Worker and its Durable Object are in `src/`, configured by `wrangler.jsonc`, and their tests run inside `workerd`, the Workers runtime. After a change to `wrangler.jsonc`, `bazelisk run //:worker_types_test.update` regenerates `worker-configuration.d.ts`. The Worker accepts GitHub App webhooks at `POST /webhooks/github` and checks each delivery's signature against `GITHUB_WEBHOOK_SECRET`. It answers a `ping` event without relaying it. To deploy your own, see [Self-host a relay](docs/how-to/self-host.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). The documentation is in [docs/](docs/index.md).

## License

Apache-2.0. See `LICENSE`.
