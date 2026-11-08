// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

// Package protocol defines the messages of the relay WebSocket protocol, as
// docs/reference/relay-protocol.md describes them.
package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
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
//
//nolint:govet // field order matches the relay canonical JSON
type Subscription struct {
	Provider   string `json:"provider"`
	Event      string `json:"event,omitempty"`
	Repository string `json:"repository,omitempty"`
	//exhaustruct:optional
	Action []string `json:"action,omitempty"`
	//exhaustruct:optional
	Conclusion []string `json:"conclusion,omitempty"`
	//exhaustruct:optional
	Name string `json:"name,omitempty"`
	//exhaustruct:optional
	WorkflowPath string `json:"workflow_path,omitempty"` //nolint:tagliatelle // relay wire field
}

// HasFilters reports whether any narrowing predicate is supplied. Nil lists
// mean omission. An empty supplied list still counts as a filter and fails
// validation before normalization.
func (s Subscription) HasFilters() bool {
	return s.Action != nil || s.Conclusion != nil || s.Name != "" || s.WorkflowPath != ""
}

// Canonical returns an independent copy with normalized repository and lists.
func (s Subscription) Canonical() Subscription {
	s.Repository = strings.ToLower(s.Repository)
	s.Action = slices.Compact(slices.Sorted(slices.Values(s.Action)))
	s.Conclusion = slices.Compact(slices.Sorted(slices.Values(s.Conclusion)))

	return s
}

// Equal compares canonical subscription identity, including all filters.
func (s Subscription) Equal(other Subscription) bool {
	a, b := s.Canonical(), other.Canonical()

	return a.Provider == b.Provider && a.Event == b.Event && a.Repository == b.Repository &&
		slices.Equal(a.Action, b.Action) && slices.Equal(a.Conclusion, b.Conclusion) &&
		a.Name == b.Name && a.WorkflowPath == b.WorkflowPath
}

// AttachmentBytes measures the relay's canonical JSON wrapper in UTF-8 bytes.
// It includes keys, commas, lists and JSON escapes. A nil input encodes as an
// empty array, since the relay always stores an array. SetEscapeHTML(false)
// below preserves printable names containing <, > and &, as JSON.stringify
// does. The relay's validated subscription fields all use the basic character range U+0020 through U+007E.
func AttachmentBytes(subs []Subscription) int {
	canonical := make([]Subscription, len(subs))
	for i, sub := range subs {
		canonical[i] = sub.Canonical()
	}

	return len(mustMarshal(struct {
		Subscriptions []Subscription `json:"subscriptions"`
	}{canonical}))
}

// request is the wire form of every client message with an id.
type request struct {
	Subscription        *Subscription `json:"subscription,omitempty"`
	Type                string        `json:"type"`
	ID                  string        `json:"id"`
	SubscriptionFilters bool          `json:"subscription_filters,omitempty"` //nolint:tagliatelle // relay wire field
}

func encode(kind, id string, sub *Subscription) []byte {
	return mustMarshal(
		request{Type: kind, ID: id, Subscription: sub, SubscriptionFilters: sub != nil && sub.HasFilters()},
	)
}

// EncodeSubscribe returns the subscribe message for sub.
func EncodeSubscribe(id string, sub Subscription) []byte {
	sub = sub.Canonical()

	return encode("subscribe", id, &sub)
}

// EncodeUnsubscribe returns the unsubscribe message for sub.
func EncodeUnsubscribe(id string, sub Subscription) []byte {
	sub = sub.Canonical()

	return encode("unsubscribe", id, &sub)
}

// EncodeList returns the list message.
func EncodeList(id string) []byte {
	return encode("list", id, nil)
}

// EncodeFilterList probes subscription filter support without changing subscriptions.
func EncodeFilterList(id string) []byte {
	return mustMarshal(request{Type: "list", ID: id, Subscription: nil, SubscriptionFilters: true})
}

func mustMarshal(value any) []byte {
	var buffer bytes.Buffer

	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)

	err := encoder.Encode(value)
	if err != nil {
		panic(err)
	}

	return bytes.TrimSuffix(buffer.Bytes(), []byte("\n"))
}

// Message is a decoded server message: Subscriptions, Error, Pong, Event or
// Unknown.
type Message any

// Subscriptions is the success reply to a subscribe, unsubscribe or list.
type Subscriptions struct {
	ID            string
	Subscriptions []Subscription
	//exhaustruct:optional
	SubscriptionFilters bool
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
		ID                  *string         `json:"id"`
		Subscriptions       *[]Subscription `json:"subscriptions"`
		SubscriptionFilters json.RawMessage `json:"subscription_filters"` //nolint:tagliatelle // relay wire field
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

	return Subscriptions{
		ID:                  *raw.ID,
		Subscriptions:       *raw.Subscriptions,
		SubscriptionFilters: bytes.Equal(raw.SubscriptionFilters, []byte("true")),
	}, nil
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
