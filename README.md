# awrel

awrel relays webhooks from providers such as GitHub to coding agents. A Cloudflare Worker and Durable Object receive the events, and a stdio Model Context Protocol (MCP) server written in Go delivers them to the agent. The repository is at the bootstrap stage: the binary prints only its usage and build metadata.

## Development

Bazel pins and runs every gate. [mise](https://mise.jdx.dev) only installs `bazelisk` and `uv`, pinned in `mise.toml` and locked in `mise.lock`. Run every gate with:

```sh
bazelisk test //...
```

`bazelisk run //:format` formats the tree in place, and `bazelisk run //:bazel_env` builds the shims for the pinned tools in `.bazel_env/bin`. Docs are in [docs/](docs/index.md).

## Worker

The Worker and its Durable Object are in `src/`, configured by `wrangler.jsonc`, and their tests run inside `workerd`, the Workers runtime. After a change to `wrangler.jsonc`, `bazelisk run //:worker_types_test.update` regenerates `worker-configuration.d.ts`. The Worker accepts GitHub App webhooks at `POST /webhooks/github` and checks each delivery's signature against `GITHUB_WEBHOOK_SECRET`. It answers a `ping` event without relaying it. Every push to main deploys to `awrel-stg`, then `awrel-prd`, after the first manual deploy of each environment.

## License

Apache-2.0. See `LICENSE`.
