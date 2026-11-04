// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

// Package protocol defines the messages of the relay WebSocket protocol, as
// docs/reference/relay-protocol.md describes them.
package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
)

var errMissingField = errors.New("missing field")

const (
	// Subprotocol names the protocol version in the WebSocket handshake.
	Subprotocol = "awrel.v1"
	// PingFrame is the keepalive message. The relay answers it without waking.
	PingFrame = `{"type":"ping"}`
)

// Subscription selects the events a client wants. Empty Event and Repository
// match everything.
type Subscription struct {
	Provider   string `json:"provider"`
	Event      string `json:"event,omitempty"`
	Repository string `json:"repository,omitempty"`
}

// request is the wire form of every client message with an id.
type request struct {
	Subscription *Subscription `json:"subscription,omitempty"`
	Type         string        `json:"type"`
	ID           string        `json:"id"`
}

func encode(kind, id string, sub *Subscription) []byte {
	return mustMarshal(request{Type: kind, ID: id, Subscription: sub})
}

// EncodeSubscribe returns the subscribe message for sub.
func EncodeSubscribe(id string, sub Subscription) []byte {
	return encode("subscribe", id, &sub)
}

// EncodeUnsubscribe returns the unsubscribe message for sub.
func EncodeUnsubscribe(id string, sub Subscription) []byte {
	return encode("unsubscribe", id, &sub)
}

// EncodeList returns the list message.
func EncodeList(id string) []byte {
	return encode("list", id, nil)
}

func mustMarshal(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}

	return data
}

// Message is a decoded server message: Subscriptions, Error, Pong, Event or
// Unknown.
type Message any

// Subscriptions is the success reply to a subscribe, unsubscribe or list.
type Subscriptions struct {
	ID            string
	Subscriptions []Subscription
}

// Error is the failure reply. ID is empty when the relay sent a null id.
type Error struct {
	ID      string
	Code    string
	Message string
}

// Error returns the code and the message.
func (e Error) Error() string {
	return e.Code + ": " + e.Message
}

// Pong is the reply to a ping.
type Pong struct{}

// Event is a webhook event that matched a subscription. Repository is nil
// when the event has none. Payload is the webhook body as received.
type Event struct {
	Repository *string
	Provider   string
	Event      string
	Delivery   string
	Payload    json.RawMessage
}

// Unknown is a message of a type this client doesn't know.
type Unknown struct {
	Type string
}

// Decode parses one server message. It fails on invalid JSON and on a known
// type with a missing or ill-typed field. On failure the Message is
// meaningless.
//
//nolint:ireturn // Message is a closed set of value types with no shared method
func Decode(data []byte) (Message, error) {
	var head struct {
		Type *string `json:"type"`
	}

	err := json.Unmarshal(data, &head)
	if err != nil {
		return nil, fmt.Errorf("decode message: %w", err)
	}

	if head.Type == nil {
		return nil, missing("message", "type")
	}

	switch *head.Type {
	case "subscriptions":
		return decodeSubscriptions(data)
	case "error":
		return decodeError(data)
	case "pong":
		return Pong{}, nil
	case "event":
		return decodeEvent(data)
	default:
		return Unknown{Type: *head.Type}, nil
	}
}

func missing(kind, field string) error {
	return fmt.Errorf("decode %s: %w %q", kind, errMissingField, field)
}

func decodeSubscriptions(data []byte) (Subscriptions, error) {
	var raw struct {
		ID            *string         `json:"id"`
		Subscriptions *[]Subscription `json:"subscriptions"`
	}

	err := json.Unmarshal(data, &raw)
	if err != nil {
		return Subscriptions{}, fmt.Errorf("decode subscriptions: %w", err)
	}

	if raw.ID == nil {
		return Subscriptions{}, missing("subscriptions", "id")
	}

	if raw.Subscriptions == nil {
		return Subscriptions{}, missing("subscriptions", "subscriptions")
	}

	return Subscriptions{ID: *raw.ID, Subscriptions: *raw.Subscriptions}, nil
}

func decodeError(data []byte) (Error, error) {
	var raw struct {
		ID      *string `json:"id"`
		Code    *string `json:"code"`
		Message *string `json:"message"`
	}

	err := json.Unmarshal(data, &raw)
	if err != nil {
		return Error{}, fmt.Errorf("decode error: %w", err)
	}

	if raw.Code == nil {
		return Error{}, missing("error", "code")
	}

	if raw.Message == nil {
		return Error{}, missing("error", "message")
	}

	result := Error{ID: "", Code: *raw.Code, Message: *raw.Message}
	if raw.ID != nil {
		result.ID = *raw.ID
	}

	return result, nil
}

func decodeEvent(data []byte) (Event, error) {
	var raw struct {
		Provider   *string         `json:"provider"`
		Event      *string         `json:"event"`
		Delivery   *string         `json:"delivery"`
		Repository *string         `json:"repository"`
		Payload    json.RawMessage `json:"payload"`
	}

	err := json.Unmarshal(data, &raw)
	if err != nil {
		return Event{}, fmt.Errorf("decode event: %w", err)
	}

	if raw.Provider == nil {
		return Event{}, missing("event", "provider")
	}

	if raw.Event == nil {
		return Event{}, missing("event", "event")
	}

	if raw.Delivery == nil {
		return Event{}, missing("event", "delivery")
	}

	if raw.Payload == nil {
		return Event{}, missing("event", "payload")
	}

	return Event{
		Repository: raw.Repository,
		Provider:   *raw.Provider,
		Event:      *raw.Event,
		Delivery:   *raw.Delivery,
		Payload:    raw.Payload,
	}, nil
}
