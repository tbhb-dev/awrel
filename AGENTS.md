# Agent instructions

Before choosing a model for a subagent or peer run, read [models/README.md](https://github.com/tbhb-dev/wiki/blob/main/models/README.md) in tbhb-dev/wiki.

Never tell a builder to unset `GIT_CONFIG*` variables in a brief. Those variables hold the agent signing key, and without them git signs with the user's own key. After the first commit, check the signing key against the agent key's fingerprint before any push.
