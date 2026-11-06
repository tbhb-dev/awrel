// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

// Package delivery turns relay events into whatever form an agent client
// accepts. Today, this means the Claude Code channel notification.
package delivery

import (
	"context"
	"encoding/json"
	"fmt"

	"go.tbhb.dev/awrel/internal/compact"
	"go.tbhb.dev/awrel/internal/protocol"
)

const (
	// ClaudeCodeClient is the client name that accepts channel notifications.
	ClaudeCodeClient = "claude-code"
	// ChannelMethod is the method name of a channel notification.
	ChannelMethod = "notifications/claude/channel"

	// maxContentBytes is the largest payload delivered whole. A larger one is
	// replaced by a short note.
	maxContentBytes = 64 * 1024
)

// Notifier sends a notification to the connected client.
type Notifier interface {
	Notify(ctx context.Context, method string, params json.RawMessage) error
}

// Deliverer hands one relay event to the agent.
type Deliverer interface {
	Deliver(ctx context.Context, event protocol.Event) error
}

// Channel delivers events as Claude Code channel notifications. Each event
// becomes one notification whose body is the webhook payload and whose
// attributes give the provider, the event, the delivery and, when known, the
// repository and the action. A payload over the size cap is replaced by a
// short note, because the agent reads the whole body.
type Channel struct {
	notifier Notifier
	compact  bool
}

// Options configure a Channel.
type Options struct {
	// Compact delivers the compact form of each payload, a reduced selection
	// of its fields, and marks the event with the attribute
	// compact. Compaction runs before the size cap applies.
	Compact bool
}

// NewChannel returns a Channel that sends through n.
func NewChannel(n Notifier, opts Options) *Channel {
	return &Channel{notifier: n, compact: opts.Compact}
}

// Deliver sends event as one channel notification.
func (c *Channel) Deliver(ctx context.Context, event protocol.Event) error {
	err := c.notifier.Notify(ctx, ChannelMethod, channelParams(event, c.compact))
	if err != nil {
		return fmt.Errorf("send channel notification: %w", err)
	}

	return nil
}

// channelParameters is the params object of a channel notification.
type channelParameters struct {
	Meta    map[string]string `json:"meta"`
	Content string            `json:"content"`
}

// channelParams builds the params of the notification for event. The content is
// the payload's JSON text, or a note when the payload is over maxContentBytes.
// With `compacted` set, the payload is compacted first, and the note covers
// whatever is still too large. A payload that can't be compacted goes whole, without the
// attribute compact.
func channelParams(event protocol.Event, compacted bool) json.RawMessage {
	action := payloadAction(event.Payload)

	meta := map[string]string{
		"provider": event.Provider,
		"event":    event.Event,
		"delivery": event.Delivery,
	}

	if event.Repository != nil {
		meta["repository"] = *event.Repository
	}

	if action != "" {
		meta["action"] = action
	}

	payload := event.Payload

	if compacted {
		small, err := compact.Payload(event.Provider, event.Event, payload)
		if err == nil {
			payload = small
			meta["compact"] = "true"
		}
	}

	content := string(payload)

	if len(payload) > maxContentBytes {
		meta["truncated"] = "true"
		content = tooLargeNote(event, action, len(payload))
	}

	return mustMarshal(channelParameters{Content: content, Meta: meta})
}

// payloadAction returns the payload's action field. It returns "" unless the
// payload is an object with a string action.
func payloadAction(payload json.RawMessage) string {
	var parsed struct {
		Action string `json:"action"`
	}

	// A payload that isn't an object, or whose action isn't a string, leaves
	// Action empty, which is the answer.
	// gomutants:disable-next-line BRANCH_IF reason="a failed parse leaves Action empty anyway"
	if json.Unmarshal(payload, &parsed) != nil {
		return ""
	}

	return parsed.Action
}

// tooLargeNote describes an event whose payload, of size bytes as delivered, is
// too large to deliver.
func tooLargeNote(event protocol.Event, action string, size int) string {
	note := fmt.Sprintf("%s event %q", event.Provider, event.Event)

	if action != "" {
		note += fmt.Sprintf(", action %q", action)
	}

	if event.Repository != nil {
		note += ", repository " + *event.Repository
	}

	return note + fmt.Sprintf(
		", delivery %s. The payload is %d bytes, too large to deliver. "+
			"Fetch the details from the provider with its API or CLI.",
		event.Delivery, size,
	)
}

func mustMarshal(v any) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}

	return data
}
