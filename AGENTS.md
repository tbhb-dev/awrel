# Agent instructions

Before choosing a model for a subagent or peer run, read [models/README.md](https://github.com/tbhb-dev/wiki/blob/main/models/README.md) in tbhb-dev/wiki.

Never tell a builder to unset `GIT_CONFIG*` variables in a brief. Those variables select the agent key path and the signing program. Without them git falls back to the remaining configuration, which can name the user's own key. After the first commit, compare the fingerprint of the key that signed it with the agent key's expected fingerprint before any push.
