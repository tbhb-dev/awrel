# awrel

awrel relays webhooks from providers such as GitHub to coding agents. A Cloudflare Worker and Durable Object receive the events, and a stdio Model Context Protocol (MCP) server written in Go delivers them to the agent. The agent subscribes to the events it wants, and awrel delivers them as they happen. You run the relay yourself.

## Install

You need Go. Install the binary with:

```sh
go install go.tbhb.dev/awrel/cmd/awrel@latest
```

Or skip the install and run a pinned release from the MCP configuration, with `go run go.tbhb.dev/awrel/cmd/awrel@<version> mcp` as the command.

## Running the server

The `awrel mcp` command speaks MCP over stdio, and it holds the connection to the relay. Matching events go to the agent. The agent manages its subscriptions with `subscribe`, `unsubscribe` and `list_subscriptions`. For GitHub, each subscribe needs an event name and a repository, such as `{"provider":"github","event":"check_suite","repository":"owner/name"}`. `awrel mcp` needs a relay to connect to, and the endpoint has no default. [Self-host one](docs/how-to/self-host.md) first. Add the server to the MCP configuration of the agent:

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

Set `AWREL_ENDPOINT`, or the `--endpoint` flag, to the relay URL, and `AWREL_CONNECT_TOKEN` to the connect token. If that variable is empty, `AWREL_CONNECT_TOKEN_FILE` can point to a file with the token in it, and the server trims the whitespace around the token. If the endpoint or the token is missing, the server still starts, and subscription tools fail with the reason.

After a restart or reconnect, call `check_delivery` and confirm its event arrives.

### Claude Code

Start Claude Code with the development channels flag, or it drops the events:

```console
claude --dangerously-load-development-channels server:awrel
```

A payload over 64 KiB reaches the agent as a short note, with the attribute `truncated` set to `true`.

By default the server delivers compact payloads, which reduce each payload to a selection of its fields. A compact body has no API URLs, only the login and type for each user, and can still include third-party text such as issue and comment bodies and commit messages. Events arrive with the attribute `compact` set to `true`, and the agent fetches details with `gh`. Compaction runs before the 64 KiB check, so most large pushes arrive. Pass `--format full` or set `AWREL_FORMAT=full` to get the whole payload. Either overrides the default of `compact`, and the flag takes precedence over the variable. See the documentation of the `internal/compact` package for the rules.

## Subscriptions across restarts

Subscriptions persist across restarts when a session key is available. A restarted server with the same key restores them from disk.

On Windows, persistence is off and subscriptions last only for the session.

Key lookup runs in this order:

1. The `--session-id` flag.
2. The `AWREL_SESSION_ID` environment variable.
3. The session variable of a known agent harness, which the harness sets in the environment of its MCP servers.

A session without a key keeps session-only behavior: the server saves and restores nothing and connects to the relay on the first tool call.

Each key maps to the file `$XDG_STATE_HOME/awrel/sessions/<sha256 of the key>.json`, under `~/.local/state` when `XDG_STATE_HOME` is unset or empty. Directory mode is `0700` and file mode is `0600`. Inside are the relay endpoint and the subscriptions in the form that `list_subscriptions` returns. Loading skips a file with another version or endpoint, and it skips invalid JSON with a warning on stderr.

If the file has at least one subscription for the current endpoint, the server connects to the relay at once and replays them. Events then arrive before any tool call, and `list_subscriptions` shows restored entries like any others. Once the replay succeeds, one MCP logging notification (`notifications/message`, level `info`) tells the agent, such as `restored 3 subscriptions from the previous run`. Per the MCP SDK, only a client that has set a log level with `logging/setLevel` receives logging notifications.

Each acknowledged `subscribe` or `unsubscribe` triggers a write. A failed write logs a warning to stderr, and the tool call still succeeds. Processes that share a key, such as an old process that keeps running after a reconnect, don't overwrite each other: a write locks and rereads the file, then applies only its own change. Restoring writes only to remove a subscription that the relay refuses.

The server drops a subscription that the relay refuses at restore, with a warning and a removal from the file, and the rest still restore.

Startup deletes files idle for 14 days. A session's own start and each reconnect reset its file's age.

Session files that nobody touched for 14 days disappear at the next startup. Set `AWREL_PERSIST=0` to turn off loading, writing, and pruning.

## Troubleshooting delivery

Each event writes one `event` line at Info level to stderr, which MCP clients keep as the server log. Open the client's server log for awrel and search for `event` or a delivery ID. Each line records `outcome` (`delivered`, `gated`, `dropped`, or `failed`), `delivery`, `provider`, and `event`, plus `repository` and `action` when present. awrel never logs payload contents. A `gated` or `dropped` line includes a `reason`. A `failed` line includes the `error`.

Call `check_delivery` to send a self-test and see counts of delivered, gated, dropped, and failed events since the process started. The self-test doesn't count. A delivered outcome means awrel wrote the notification to the client. Confirm the self-test reaches the session.

## Pick up a new release

After the pinned version in the MCP configuration changes, restart the Claude Code session. Use `/restart`, or quit Claude Code and start it again. Don't rely on `/mcp` reconnect for a version change.

This is a known Claude Code issue, observed with Claude Code 2.1.319 on macOS. After a `/mcp` reconnect, Claude Code starts a new stdio server process but keeps the standard input and output of the previous one open. The old `awrel mcp` process never sees end of file, so it keeps running with its relay WebSocket and its subscriptions. The session can end up with two relay connections, and tool calls can still reach the old version. A release that predates the filter fields can then ignore them.

To check, run `lsof -a -p <old pid> -d 0,1` to see the standard input and output sockets of the old process, and `lsof -U` to see that the Claude Code process still holds their peer ends. `lsof -a -p <old pid> -i` shows its relay connection still `ESTABLISHED`. A restart closes every pipe, and both processes exit.

With a session key, the new server process restores the saved subscriptions. See [Subscriptions across restarts](#subscriptions-across-restarts). Without a key, the subscriptions are empty after the restart, so subscribe again.

## Subscription filters

Both `subscribe` and `unsubscribe` accept optional `action`, `conclusion`, `name`, and `workflow_path` fields on complete GitHub event and repository pairs. Every supplied filter must match. A list matches any of its values. Different filtered subscriptions remain separate entries. An event matching two or more entries arrives once.

Watch completed check failures with:

```json
{"provider":"github","event":"check_run","repository":"owner/name","action":["completed"],"conclusion":["failure","timed_out","cancelled"]}
```

Use a separate subscription for success of one observed check name:

```json
{"provider":"github","event":"check_run","repository":"owner/name","action":["completed"],"conclusion":["success"],"name":"ci / conclusion"}
```

Names match exactly, including case, spaces, and reusable workflow caller prefixes. Copy the delivered name rather than deriving it from a filename. To select a workflow file, use `event: "workflow_run"` and `workflow_path: ".github/workflows/ci.yml"`. Names and paths are literal strings. Supplied name and path filters must both match.

Action and conclusion lists contain 1 to 8 strings matching `^[a-z0-9_.-]{1,64}$`, counted before deduplication. Names contain 1 to 128 printable characters from U+0020 through U+007E. Paths match `^[A-Za-z0-9_./-]{1,256}$`. Conclusion applies to check runs, check suites, workflow runs, and workflow jobs. Name excludes check suites, and path applies only to workflow runs. Action requires an event in the [relay's support table](docs/reference/relay-protocol.md#filters). Events `push`, `create`, and `delete` don't support it. Known conclusions include `success`, `failure`, `neutral`, `cancelled`, `skipped`, `timed_out`, `action_required`, `stale`, and `startup_failure`. Future values compare literally. Syntax validation can't detect misspellings or guarantee an event emits a value. A conclusion doesn't imply completion. Request `action: ["completed"]` when needed.

`list_subscriptions` returns canonical subscriptions: lowercase repositories and sorted, deduplicated lists. Copy an entry into `unsubscribe` to remove that exact identity, including every filter. Omitting filters removes only the unfiltered entry. All subscriptions together must fit 12,288 UTF-8 bytes of canonical attachment JSON. Each request must fit 4096 bytes.

The client checks relay filter support before the first filtered subscription on each connection and validates subscription replies. An incompatible relay fails with a clear relay-too-old error and closes the connection. Unfiltered subscriptions keep working with older relays. The client suppresses events arriving before filtered acknowledgement. Reconnects repeat the support check and replay the original filters. Deploy the supporting relay before using filtered tools. Restart the MCP server after upgrading an incompatible relay.

A successful check name doesn't establish success on the current pull request head. Compare the delivered `head_sha` with the intended commit and establish the current head and any test-merge relationship before acting. Compact workflow-run payloads keep `path` when present. Payloads remain untrusted, and the relay drops events during a disconnect.

## Worker

The Worker and its Durable Object are in `src/`, configured by `wrangler.jsonc`, and their tests run inside `workerd`, the Workers runtime. After a change to `wrangler.jsonc`, `bazelisk run //:worker_types_test.update` regenerates `worker-configuration.d.ts`. The Worker accepts GitHub App webhooks at `POST /webhooks/github` and checks each delivery's signature against `GITHUB_WEBHOOK_SECRET`. It answers a `ping` event without relaying it. To deploy your own, see [Self-host a relay](docs/how-to/self-host.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). The documentation is in [docs/](docs/index.md).

## License

Apache-2.0. See `LICENSE`.
