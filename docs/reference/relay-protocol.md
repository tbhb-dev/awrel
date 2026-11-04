---
description: The messages that the awrel server and the relay Durable Object exchange over the /connect WebSocket.
generated:
  - role: generated
    harness: claude-code:2.1.318
    model: anthropic:claude-sonnet-5-5
    effort: medium
    surface: cli
    session: sha-256-64:dc0c7bb9a6fc4839
    subagent: sha-256-64:c6786fca249ea845
    at: "2026-11-04T03:24:55Z"
  - role: assisted
    harness: claude-code:2.1.318
    model: anthropic:claude-opus-5-5
    effort: medium
    surface: cli
    session: sha-256-64:dc0c7bb9a6fc4839
    at: "2026-11-04T03:24:55Z"
status: stable
tags: [protocol, websocket, relay]
title: Relay WebSocket protocol
type: Reference
---

# Relay protocol

The awrel Model Context Protocol (MCP) server and the relay Durable Object exchange JSON messages over one WebSocket. This page covers the handshake, the subscription format, and each message in both directions.

## Handshake

The client opens `GET /connect` (`wss://awrel.tbhb.dev/connect` in prd, `wss://awrel-stg.tbhb.dev/connect` in stg) with `Authorization: Bearer <CONNECT_TOKEN>` (the word Bearer is case-sensitive) and `Sec-WebSocket-Protocol: awrel.v1`. A 401 means the token doesn't match, a 426 means `Upgrade: websocket` is missing, and a 400 means `awrel.v1` isn't offered. On success the Durable Object answers 101 with `Sec-WebSocket-Protocol: awrel.v1`, after accepting the socket with the hibernatable API and setting the attachment to `{"subscriptions":[]}`. Because the subprotocol name is the protocol version, a breaking change adds `awrel.v2` while `awrel.v1` stays served until no client uses it. Every HTTP answer from the Worker, the 101 included, carries an `Awrel-Worker-Version` header, and an answer from the edge, such as an edge 404, doesn't. A client uses the header to tell the edge from the Worker.

Every message is one JSON text frame. A client message is at most 4096 bytes, and a larger one gets a `bad_message` reply. The server ignores unknown fields in a client message of a known type. It answers a binary frame with `bad_message` and a null `id`, and keeps the socket open.

## Subscriptions

A subscription is `{"provider": string, "event"?: string, "repository"?: string}`. `provider` matches `^[a-z0-9-]{1,32}$`. `event` matches `^[a-z0-9_.-]{1,64}$`. `repository` is `owner/name`, with 1 to 39 characters in the owner and 1 to 100 in the name, all from `[A-Za-z0-9_.-]`. The server stores `repository` in lowercase and compares event repositories without case. Subscriptions with equal fields, repository case aside, count as the same subscription. A socket's subscriptions live in its attachment and end with the socket.

## Client to server

| Message | Meaning |
| --- | --- |
| `{"type":"subscribe","id":ID,"subscription":{...}}` | Add the subscription. Adding one already present succeeds and leaves the list unchanged. |
| `{"type":"unsubscribe","id":ID,"subscription":{...}}` | Remove the subscription. An absent subscription gets a `not_subscribed` reply. |
| `{"type":"list","id":ID}` | Report the current subscriptions. |
| `{"type":"ping"}` | Keepalive, exactly these 15 bytes. |

Clients choose each `id`, a string of 1 to 64 characters, and the reply echoes it.

## Server to client

| Message | Meaning |
| --- | --- |
| `{"type":"subscriptions","id":ID,"subscriptions":[{...}]}` | Success reply to subscribe, unsubscribe or list, with the full list after the change. |
| `{"type":"error","id":ID or null,"code":string,"message":string}` | Failure reply. `id` is null when the message had no readable id. |
| `{"type":"pong"}` | Reply to ping, exactly these 15 bytes. |
| `{"type":"event","provider":string,"event":string,"delivery":string,"repository":string or null,"payload":any}` | A webhook event that matched at least one subscription. A socket gets it once, whatever the number of matches. |

Error codes are `bad_message` (not JSON, unknown `type`, missing or ill-typed field), `bad_subscription` (a field fails the patterns in the Subscriptions section), `not_subscribed`, and `limit` (the change would push the attachment past its budget). An error leaves the subscriptions unchanged.

A ping frame must be exactly the 15 bytes `{"type":"ping"}` to get the automatic response. Any other frame that resembles a ping, such as one with spaces or extra fields, goes to the Durable Object as a normal message and gets `bad_message`. Ping never wakes the Durable Object, because `setWebSocketAutoResponse(new WebSocketRequestResponsePair('{"type":"ping"}', '{"type":"pong"}'))` answers it. The Durable Object rejects a change whose `JSON.stringify` of the attachment would exceed 1792 bytes, well under the 16,384-byte `serializeAttachment` limit, and treats a throw from `serializeAttachment` as `limit`.

Clients ignore message types they don't know. For a client message type the server doesn't know, the reply is `bad_message`.

## Reconnecting

A reconnect begins with an empty list. Before anything else, the client sends one `subscribe` per subscription it held, in order. The server drops events from a period without a socket permanently.
