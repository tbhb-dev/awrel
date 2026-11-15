// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

// Package delivery turns relay events into whatever form an agent client
// accepts. Today, this means the Claude Code channel notification.
package delivery

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

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
	maxFieldBytes   = 256
)

// Event holds the metadata extracted once for logging and delivery.
type Event struct {
	Action  string
	Message protocol.Event
}

// Describe extracts the action for both the outcome log and notification.
func Describe(event protocol.Event) Event {
	return Event{Message: event, Action: payloadAction(event.Payload)}
}

// Counted reports whether the event belongs in the webhook traffic counts.
// Events from awrel itself are logged but never counted.
func (e Event) Counted() bool {
	return e.Message.Provider != "awrel"
}

// Log records an outcome without including payload contents.
func (e Event) Log(ctx context.Context, logger *slog.Logger, outcome string, extra ...any) {
	attrs := []any{
		"outcome", outcome, "delivery", e.Message.Delivery,
		"provider", e.Message.Provider, "event", e.Message.Event,
	}
	if e.Message.Repository != nil {
		attrs = append(attrs, "repository", *e.Message.Repository)
	}

	if e.Action != "" {
		attrs = append(attrs, "action", e.Action)
	}

	logger.InfoContext(ctx, "event", append(attrs, extra...)...)
}

// Notifier sends a notification to the connected client.
type Notifier interface {
	Notify(ctx context.Context, method string, params json.RawMessage) error
}

// Deliverer hands one relay event to the agent.
type Deliverer interface {
	Deliver(ctx context.Context, event Event) error
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
func (c *Channel) Deliver(ctx context.Context, event Event) error {
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
func channelParams(event Event, compacted bool) json.RawMessage {
	action := clip(event.Action)
	message := event.Message
	message.Provider = clip(message.Provider)
	message.Event = clip(message.Event)
	message.Delivery = clip(message.Delivery)

	meta := map[string]string{
		"provider": neutralize(message.Provider),
		"event":    neutralize(message.Event),
		"delivery": neutralize(message.Delivery),
	}

	if message.Repository != nil {
		repository := clip(*message.Repository)
		message.Repository = &repository
		meta["repository"] = neutralize(repository)
	}

	if action != "" {
		meta["action"] = neutralize(action)
	}

	payload := event.Message.Payload

	if compacted {
		small, err := compact.Payload(event.Message.Provider, event.Message.Event, payload)
		if err == nil {
			payload = small
			meta["compact"] = "true"
		}
	}

	// Escaping comes first. The size cap bounds the delivered content.
	content := neutralize(string(payload))

	if len(content) > maxContentBytes {
		meta["truncated"] = "true"
		// The note replaces the body, so no compact payload is delivered.
		delete(meta, "compact")

		content = neutralize(tooLargeNote(message, action, len(payload)))
	}

	return mustMarshal(channelParameters{Content: content, Meta: meta})
}

// neutralize escapes the framing characters in delivered content.
func neutralize(content string) string {
	// framing maps the characters that mark up channel content to their JSON
	// escapes. In JSON text they occur only inside strings, so the content is
	// the same JSON value after parsing.
	framing := strings.NewReplacer("<", `\u003c`, ">", `\u003e`, "&", `\u0026`)

	return framing.Replace(content)
}

func clip(s string) string {
	if len(s) > maxFieldBytes {
		return strings.ToValidUTF8(s[:maxFieldBytes], "")
	}

	return s
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

// tooLargeNote describes an event whose payload, of size bytes before escaping,
// is too large to deliver.
func tooLargeNote(event protocol.Event, action string, size int) string {
	note := fmt.Sprintf("%s event %q", event.Provider, event.Event)

	if action != "" {
		note += fmt.Sprintf(", action %q", action)
	}

	if event.Repository != nil {
		note += ", repository " + *event.Repository
	}

	return note + fmt.Sprintf(
		", delivery %s. The payload is %d bytes before escaping, too large to deliver. "+
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
