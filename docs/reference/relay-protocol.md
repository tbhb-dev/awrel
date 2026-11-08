---
description: The messages that the awrel server and the relay Durable Object exchange over the /connect WebSocket.
generated:
  - role: generated
    harness: claude-code:2.1.319
    model: anthropic:claude-sonnet-5-5
    effort: medium
    surface: cli
    session: sha-256-64:dc0c7bb9a6fc4839
    subagent: sha-256-64:95a8c6deb0220742
    at: "2026-11-08T03:43:57Z"
status: stable
tags: [protocol, websocket, relay]
title: Relay WebSocket protocol
type: Reference
---

# Relay protocol

The awrel Model Context Protocol (MCP) server and the relay Durable Object exchange JSON messages over one WebSocket. This page covers the handshake, the subscription format, and each message in both directions.

## Handshake

The client opens `GET /connect` (`wss://<relay-host>/connect`) with `Authorization: Bearer <CONNECT_TOKEN>` (the word Bearer is case-sensitive) and `Sec-WebSocket-Protocol: awrel.v1`. A 401 means the token doesn't match, a 426 means `Upgrade: websocket` is missing, and a 400 means `awrel.v1` isn't offered. On success the Durable Object answers 101 with `Sec-WebSocket-Protocol: awrel.v1`, after accepting the socket with the hibernatable API and setting the attachment to `{"subscriptions":[]}`. Because the subprotocol name is the protocol version, a breaking change adds `awrel.v2` while `awrel.v1` stays served until no client uses it. Every HTTP answer from the Worker, the 101 included, carries an `Awrel-Worker-Version` header, and an answer from the edge, such as an edge 404, doesn't. A client uses the header to tell the edge from the Worker.

Every message is one JSON text frame. A client message is at most 4096 bytes, and a larger one gets a `bad_message` reply. The server ignores unknown fields in a client message of a known type. It answers a binary frame with `bad_message` and a null `id`, and keeps the socket open.

## Subscriptions

A subscription has `provider`, optional `event` and `repository`, and optional filters `action`, `conclusion`, `name`, and `workflow_path`. `provider` matches `^[a-z0-9-]{1,32}$`. `event` matches `^[a-z0-9_.-]{1,64}$`. `repository` is `owner/name`, with 1 to 39 characters in the owner and 1 to 100 in the name, all from `[A-Za-z0-9_.-]`. The server stores `repository` in lowercase and compares event repositories without case. The relay deduplicates and sorts each filter list, and preserves names and paths exactly. Subscriptions with equal canonical fields count as the same subscription. Each filter participates in equality. An omitted filter differs from any supplied list. A socket's subscriptions live in its attachment and end with the socket.

For `github`, `subscribe` requires both `event` and `repository`. A complete subscription is `{"provider":"github","event":"check_suite","repository":"owner/name"}`. Subscribe once per event type and repository. `unsubscribe` removes only an exact canonical match, including every filter. Omitting filters removes only the unfiltered subscription. For legacy incomplete subscriptions without filters, `unsubscribe` accepts an absent `event` or `repository`. Stored subscriptions keep matching until the socket closes, without further validation. Other providers follow the preceding field rules.

### Filters

For subscribe and unsubscribe, filters require `provider: "github"` with both an `event` and a `repository`. Each supplied filter must match. A list matches when it contains the payload value. Missing, null, or non-string payload fields never match a supplied filter. Without a filter, the relay doesn't test that payload field. Names and paths compare exactly with case and spaces preserved. Wildcard characters in names are literal, and the relay keeps paths without resolution or normalization.

| Filter | Validation | Payload field and supported events |
| --- | --- | --- |
| `action` | Nonempty list of at most 8 strings, each matching `^[a-z0-9_.-]{1,64}$` | `payload.action`, for the action events listed below |
| `conclusion` | Nonempty list of at most 8 strings, each matching `^[a-z0-9_.-]{1,64}$` | `payload.<event>.conclusion`, for `check_run`, `check_suite`, `workflow_run`, `workflow_job` |
| `name` | String of 1 to 128 printable characters, U+0020 through U+007E | `payload.<event>.name`, for `check_run`, `workflow_run`, `workflow_job` |
| `workflow_path` | String matching `^[A-Za-z0-9_./-]{1,256}$` | `payload.workflow_run.path`, for `workflow_run` only |

List limits apply before deduplication. Scalars, nulls, empty lists, non-string list entries, and nested lists are invalid. Names preserve leading and trailing spaces. Quotes and backslashes use JSON escaping. The path syntax excludes some possible filenames. The conclusion is the field on the event's own object. A nested check suite's conclusion can't match a filter on a check run's conclusion. `name` is unsupported on `check_suite`.

Action filtering uses an explicit support table based on [GitHub's webhook events](https://docs.github.com/en/webhooks/webhook-events-and-payloads):

- `branch_protection_configuration`, `branch_protection_rule`, `check_run`, `check_suite`, `code_scanning_alert`, `commit_comment`, `custom_property`, `custom_property_values`.
- `dependabot_alert`, `deploy_key`, `deployment`, `deployment_protection_rule`, `deployment_review`, `deployment_status`, `discussion`, `discussion_comment`.
- `github_app_authorization`, `installation`, `installation_repositories`, `installation_target`, `issue_comment`, `issue_dependencies`, `issue_relates_to`, `issues`.
- `label`, `marketplace_purchase`, `member`, `membership`, `merge_group`, `meta`, `milestone`, `org_block`.
- `organization`, `package`, `personal_access_token_request`, `project`, `project_card`, `project_column`, `projects_v2`, `projects_v2_item`.
- `projects_v2_status_update`, `pull_request`, `pull_request_review`, `pull_request_review_comment`, `pull_request_review_thread`, `registry_package`, `release`, `repository`.
- `repository_advisory`, `repository_dispatch`, `repository_ruleset`, `repository_vulnerability_alert`, `secret_scanning_alert`, `secret_scanning_alert_location`, `secret_scanning_scan`, `security_advisory`.
- `sponsorship`, `star`, `sub_issues`, `team`, `watch`, `workflow_job`, `workflow_run`.

Actionless events such as `push`, `create`, and `delete` reject `action`. Unknown events remain usable without filters. Action filtering for a new event requires a table update. Other filters on unsupported events and any filter on another provider return `bad_subscription`.

Actions and conclusions use syntax validation rather than a fixed vocabulary. Common actions include `created` and `completed`. Known conclusions include `success`, `failure`, `neutral`, `cancelled`, `skipped`, `timed_out`, `action_required`, `stale`, and `startup_failure`. Individual events may emit only some of these. Future values matching the pattern compare literally. Check spelling to avoid requesting values that no event emits. A conclusion filter doesn't imply a completion action: request `action: ["completed"]` when needed.

This subscription selects completed workflow failures for one exact workflow file:

```json
{"type":"subscribe","id":"workflow-failures","subscription_filters":true,"subscription":{"provider":"github","event":"workflow_run","repository":"owner/name","action":["completed"],"conclusion":["failure","timed_out"],"workflow_path":".github/workflows/ci.yml"}}
```

Filters, including JSON keys, list separators, and escapes, count toward the canonical attachment budget of 12,288 UTF-8 bytes. The separate client message limit remains 4096 bytes. The relay keeps capability flags outside the attachment budget and stored subscriptions.

## Client to server

| Message | Meaning |
| --- | --- |
| `{"type":"subscribe","id":ID,"subscription":{...}}` | Add the subscription. Adding one already present succeeds and leaves the list unchanged. |
| `{"type":"unsubscribe","id":ID,"subscription":{...}}` | Remove the subscription. An absent subscription gets a `not_subscribed` reply. |
| `{"type":"list","id":ID}` | Report the current subscriptions. |
| `{"type":"ping"}` | Keepalive, exactly these 15 bytes. |

Clients choose each `id`, a string of 1 to 64 characters, and the reply echoes it.

A list, subscribe, or unsubscribe request with top-level `"subscription_filters":true` receives `"subscription_filters":true` on its successful `subscriptions` reply. Other values or an omitted flag don't add the echo. Subscriptions replies include canonical filter fields. Copy an entry into an unsubscribe request to remove that exact subscription. The flag is outside subscription identity. A client can probe support with `{"type":"list","id":"probe","subscription_filters":true}` before changing subscriptions, and check both the capability echo and the exact canonical subscription in a subscribe reply.

## Server to client

| Message | Meaning |
| --- | --- |
| `{"type":"subscriptions","id":ID,"subscriptions":[{...}]}` | Success reply to subscribe, unsubscribe or list, with the full list after the change. |
| `{"type":"error","id":ID or null,"code":string,"message":string}` | Failure reply. `id` is null when the message had no readable id. |
| `{"type":"pong"}` | Reply to ping, exactly these 15 bytes. |
| `{"type":"event","provider":string,"event":string,"delivery":string,"repository":string or null,"payload":any}` | A webhook event that matched at least one subscription. A socket gets it once, whatever the number of matches. |

Error codes are `bad_message` (not JSON, unknown `type`, missing or ill-typed envelope or base subscription field), `bad_subscription` (a field fails the subscription patterns, a GitHub subscribe lacks `event` or `repository`, or a filter has an invalid type, value, provider, event, or incomplete pair), `not_subscribed`, and `limit` (the change would push the attachment past its budget). An error leaves the subscriptions unchanged.

A ping frame must be exactly the 15 bytes `{"type":"ping"}` to get the automatic response. Any other frame that resembles a ping, such as one with spaces or extra fields, goes to the Durable Object as a normal message and gets `bad_message`. Ping never wakes the Durable Object, because `setWebSocketAutoResponse(new WebSocketRequestResponsePair('{"type":"ping"}', '{"type":"pong"}'))` answers it. The Durable Object rejects a change whose attachment JSON exceeds 12,288 UTF-8 bytes, under the 16,384-byte `serializeAttachment` limit, and treats a throw from `serializeAttachment` as `limit`.

Clients ignore message types they don't know. For a client message type the server doesn't know, the reply is `bad_message`.

## Reconnecting

A reconnect begins with an empty list. Before replaying its first filtered subscription, the Go client probes with a list request with `subscription_filters: true` and requires the capability echo. It then sends one `subscribe` per subscription it held, in order, retaining all filters. It checks capability echoes on every filtered subscribe and unsubscribe reply, and checks that the exact canonical subscription is present after subscribe and absent after unsubscribe. A missing or changed echo closes the socket and stops retries with a relay-too-old error. While validating filtered requests and reconnect replay, the client suppresses events. Older relays still work with unfiltered clients, which don't require a capability echo. The server drops events from a period without a socket permanently.
