---
okf_version: "0.2"
---

# awrel docs

Hand-written documentation for awrel, as an OKF v0.2 bundle. The rules for adding to it are in `AGENTS.md` in this directory.

## How-to

* [Self-host a relay](how-to/self-host.md) - Deploy the Worker and Durable Object, create the GitHub App, and point the awrel server at your own relay.

## Reference

* [Frontmatter reference](reference/frontmatter.md) - The frontmatter fields, their values, and how they map to commit trailers.
* [Relay WebSocket protocol](reference/relay-protocol.md) - The messages that the awrel server and the relay Durable Object exchange over the /connect WebSocket.
