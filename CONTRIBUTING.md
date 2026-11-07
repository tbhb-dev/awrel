# Contributing to awrel

## Build and check

Bazel pins and runs every gate. [mise](https://mise.jdx.dev) only installs `bazelisk` and `uv`, pinned in `mise.toml` and locked in `mise.lock`. Run every gate with:

```sh
bazelisk test //...
```

`bazelisk run //:format` formats the tree in place, and `bazelisk run //:bazel_env` builds the shims for the pinned tools in `.bazel_env/bin`. Docs are in [docs/](docs/index.md).

## Maintainer workflow

This section describes the maintainers' own tooling. You don't need it to contribute: open a pull request the usual way.

- `tbhbdev alpha install` installs the user-level harness hooks once per machine. Each session start then installs the clone's git hooks.
- `mise run pr` runs `tbhbdev alpha pr`, which checks `.github/PULL_REQUEST_DRAFT.md` and opens or edits the PR from it, and `mise run merge` runs `tbhbdev alpha merge`, which squash-merges an approved PR. `tbhbdev` comes from the public tbhb-dev/tools repository.
- The deny rules in `.claude/settings.json` and `.codex/rules/` block `gh pr create`, `gh pr edit` and `gh pr merge`.

## Maintainer configuration

`.mcp.json` is the maintainers' own project configuration. It points at their staging relay with a token file in `~/.config/awrel/`. Replace the endpoint and the token with those of your own relay.

`wrangler.jsonc`, `.github/workflows/deploy.yml` and the deploy-check targets in `BUILD.bazel` hold the maintainers' deployment values, such as Worker names and domains. To run your own relay, see [Self-host a relay](docs/how-to/self-host.md).
