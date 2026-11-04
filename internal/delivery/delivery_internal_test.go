// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"strings"
	"testing"

	"go.tbhb.dev/awrel/internal/protocol"
)

const (
	githubProvider = "github"
	ownerRepo      = "o/r"
	openedPayload  = `{"action":"opened","number":1}`
	pingEvent      = "ping"
	deliveryKey    = "delivery"
	noteTail       = "Fetch the details from the provider with its API or CLI."
)

var errBoom = errors.New("boom")

// decoded is the params object, read back from JSON.
type decoded struct {
	Meta    map[string]string `json:"meta"`
	Content string            `json:"content"`
}

func decode(t *testing.T, raw json.RawMessage) decoded {
	t.Helper()

	var out decoded

	err := json.Unmarshal(raw, &out)
	if err != nil {
		t.Fatalf("params %s are not valid: %v", raw, err)
	}

	return out
}

// payloadOfSize returns a JSON object payload of exactly size bytes whose
// action is opened.
func payloadOfSize(t *testing.T, size int) json.RawMessage {
	t.Helper()

	const frame = `{"action":"opened","pad":""}`

	if size < len(frame) {
		t.Fatalf("size %d is below the frame", size)
	}

	return json.RawMessage(`{"action":"opened","pad":"` + strings.Repeat("x", size-len(frame)) + `"}`)
}

// newEvent builds a GitHub event with the given name, delivery id, repository
// (empty for none) and payload.
func newEvent(name, delivery, repository string, payload json.RawMessage) protocol.Event {
	var repo *string
	if repository != "" {
		repo = &repository
	}

	return protocol.Event{
		Provider: githubProvider, Event: name, Delivery: delivery,
		Repository: repo, Payload: payload,
	}
}

// metaOf returns the meta an event with these parts has when delivered whole.
// The extra arguments are key and value pairs.
func metaOf(name, delivery string, extra ...string) map[string]string {
	meta := map[string]string{"provider": githubProvider, "event": name, deliveryKey: delivery}

	for i := 0; i+1 < len(extra); i += 2 {
		meta[extra[i]] = extra[i+1]
	}

	return meta
}

func TestChannelParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantMeta map[string]string
		name     string
		want     string
		event    protocol.Event
	}{
		{
			wantMeta: metaOf("pull_request", "d1", "repository", ownerRepo, "action", "opened"),
			name:     "repository and action",
			want:     openedPayload,
			event:    newEvent("pull_request", "d1", ownerRepo, json.RawMessage(openedPayload)),
		},
		{
			wantMeta: metaOf(pingEvent, "d2"),
			name:     "no repository and no action",
			want:     `{"zen":"keep it simple"}`,
			event:    newEvent(pingEvent, "d2", "", json.RawMessage(`{"zen":"keep it simple"}`)),
		},
		{
			wantMeta: metaOf("x", "d3"),
			name:     "action that is a number",
			want:     `{"action":5}`,
			event:    newEvent("x", "d3", "", json.RawMessage(`{"action":5}`)),
		},
		{
			wantMeta: metaOf("x", "d4"),
			name:     "payload that is an array",
			want:     `[1,2]`,
			event:    newEvent("x", "d4", "", json.RawMessage(`[1,2]`)),
		},
		{
			wantMeta: metaOf("x", "d5"),
			name:     "empty action",
			want:     `{"action":""}`,
			event:    newEvent("x", "d5", "", json.RawMessage(`{"action":""}`)),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got := decode(t, channelParams(test.event))

			if got.Content != test.want {
				t.Errorf("content = %q, want %q", got.Content, test.want)
			}

			if !maps.Equal(got.Meta, test.wantMeta) {
				t.Errorf("meta = %v, want %v", got.Meta, test.wantMeta)
			}
		})
	}
}

func TestContentHoldsJSONText(t *testing.T) {
	t.Parallel()

	raw := channelParams(newEvent("x", "d", "", json.RawMessage(`{"a":1}`)))

	want := `{"meta":{"delivery":"d","event":"x","provider":"github"},"content":"{\"a\":1}"}`
	if string(raw) != want {
		t.Errorf("params = %s, want %s", raw, want)
	}
}

func TestPayloadSizeBoundary(t *testing.T) {
	t.Parallel()

	t.Run("exactly the cap is delivered whole", func(t *testing.T) {
		t.Parallel()

		event := newEvent("issue_comment", "d9", ownerRepo, payloadOfSize(t, maxContentBytes))
		got := decode(t, channelParams(event))

		if got.Content != string(event.Payload) {
			t.Errorf("content of %d bytes is not the payload", len(got.Content))
		}

		if _, truncated := got.Meta["truncated"]; truncated {
			t.Error("meta marks a whole payload as truncated")
		}
	})

	t.Run("one byte over is replaced by a note", func(t *testing.T) {
		t.Parallel()

		got := decode(t, channelParams(newEvent("issue_comment", "d9", ownerRepo, payloadOfSize(t, maxContentBytes+1))))

		want := `github event "issue_comment", action "opened", repository o/r, delivery d9. ` +
			`The payload is 65537 bytes, too large to deliver. ` + noteTail
		if got.Content != want {
			t.Errorf("content = %q, want %q", got.Content, want)
		}

		wantMeta := metaOf("issue_comment", "d9", "repository", ownerRepo, "action", "opened", "truncated", "true")
		if !maps.Equal(got.Meta, wantMeta) {
			t.Errorf("meta = %v, want %v", got.Meta, wantMeta)
		}
	})

	t.Run("a note leaves out what is unknown", func(t *testing.T) {
		t.Parallel()

		big := json.RawMessage(`["` + strings.Repeat("x", maxContentBytes) + `"]`)
		got := decode(t, channelParams(newEvent(pingEvent, "d0", "", big)))

		want := `github event "ping", delivery d0. The payload is 65540 bytes, too large to deliver. ` + noteTail
		if got.Content != want {
			t.Errorf("content = %q, want %q", got.Content, want)
		}
	})
}

// recordingNotifier remembers its last call and returns err.
type recordingNotifier struct {
	err    error
	method string
	params json.RawMessage
}

func (r *recordingNotifier) Notify(_ context.Context, method string, params json.RawMessage) error {
	r.method = method
	r.params = params

	return r.err
}

func TestDeliver(t *testing.T) {
	t.Parallel()

	event := newEvent("push", "d7", "", json.RawMessage(`{}`))

	t.Run("sends the channel notification", func(t *testing.T) {
		t.Parallel()

		notifier := &recordingNotifier{err: nil, method: "", params: nil}

		err := NewChannel(notifier).Deliver(t.Context(), event)
		if err != nil {
			t.Fatal(err)
		}

		if notifier.method != "notifications/claude/channel" {
			t.Errorf("method = %q", notifier.method)
		}

		if string(notifier.params) != string(channelParams(event)) {
			t.Errorf("params = %s", notifier.params)
		}
	})

	t.Run("wraps a notify failure", func(t *testing.T) {
		t.Parallel()

		notifier := &recordingNotifier{err: errBoom, method: "", params: nil}

		err := NewChannel(notifier).Deliver(t.Context(), event)
		if !errors.Is(err, errBoom) || !strings.Contains(err.Error(), "send channel notification") {
			t.Errorf("Deliver = %v", err)
		}
	})
}

func TestMustMarshalPanics(t *testing.T) {
	t.Parallel()

	defer func() {
		if recover() == nil {
			t.Error("mustMarshal of a channel did not panic")
		}
	}()

	mustMarshal(make(chan int))
}

func TestClaudeCodeClient(t *testing.T) {
	t.Parallel()

	if ClaudeCodeClient != "claude-code" {
		t.Errorf("ClaudeCodeClient = %q", ClaudeCodeClient)
	}

	var _ Deliverer = (*Channel)(nil)
}
