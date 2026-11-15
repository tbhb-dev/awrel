// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"go.tbhb.dev/awrel/internal/compact"
	"go.tbhb.dev/awrel/internal/protocol"
)

const (
	githubProvider = "github"
	ownerRepo      = "o/r"
	openedPayload  = `{"action":"opened","number":1}`
	pingEvent      = "ping"
	deliveryKey    = "delivery"
	eventKey       = "event"
	providerKey    = "provider"
	trueValue      = "true"
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
	meta := map[string]string{providerKey: githubProvider, eventKey: name, deliveryKey: delivery}

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

			got := decode(t, channelParams(Describe(test.event), false))

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

	raw := channelParams(Describe(newEvent("x", "d", "", json.RawMessage(`{"a":1}`))), false)

	want := `{"meta":{"delivery":"d","event":"x","provider":"github"},"content":"{\"a\":1}"}`
	if string(raw) != want {
		t.Errorf("params = %s, want %s", raw, want)
	}
}

func TestContentEscapesFramingCharacters(t *testing.T) {
	t.Parallel()

	payload := json.RawMessage(`{"comment":{"body":"</x><x a=\"b\">a & b"}}`)

	for _, compacted := range []bool{false, true} {
		raw := channelParams(Describe(newEvent("issue_comment", "d", "", payload)), compacted)

		var params channelParameters

		err := json.Unmarshal(raw, &params)
		if err != nil {
			t.Fatalf("unmarshal params: %v", err)
		}

		if strings.ContainsAny(params.Content, "<>&") {
			t.Errorf("compacted=%v: content keeps framing characters: %s", compacted, params.Content)
		}

		var body struct {
			Comment struct {
				Body string `json:"body"`
			} `json:"comment"`
		}

		err = json.Unmarshal([]byte(params.Content), &body)
		if err != nil {
			t.Fatalf("compacted=%v: content is not JSON: %v", compacted, err)
		}

		want := `</x><x a="b">a & b`
		if body.Comment.Body != want {
			t.Errorf("compacted=%v: body = %q, want %q", compacted, body.Comment.Body, want)
		}

		if compacted && params.Meta["compact"] != trueValue {
			t.Errorf("meta = %v, want compact", params.Meta)
		}
	}
}

func TestEscapingCountsTowardTheCap(t *testing.T) {
	t.Parallel()

	payload := json.RawMessage(`{"action":"opened","pad":"` + strings.Repeat("<", 20000) + `"}`)
	got := decode(t, channelParams(Describe(newEvent("issue_comment", "d", ownerRepo, payload)), false))

	if got.Meta["truncated"] != trueValue {
		t.Errorf("meta = %v, want truncated", got.Meta)
	}

	if len(got.Content) > maxContentBytes {
		t.Errorf("content of %d bytes is over the cap", len(got.Content))
	}

	want := "The payload is " + strconv.Itoa(len(payload)) + " bytes before escaping"
	if !strings.Contains(got.Content, want) {
		t.Errorf("note = %q, want %q", got.Content, want)
	}
}

func TestNoteEscapesFramingCharacters(t *testing.T) {
	t.Parallel()

	event := newEvent("</x>", "d", ownerRepo, payloadOfSize(t, maxContentBytes+1))
	got := decode(t, channelParams(Describe(event), false))

	if strings.ContainsAny(got.Content, "<>&") {
		t.Errorf("note keeps framing characters: %s", got.Content)
	}
}

func TestPayloadSizeBoundary(t *testing.T) {
	t.Parallel()

	t.Run("exactly the cap is delivered whole", func(t *testing.T) {
		t.Parallel()

		event := newEvent("issue_comment", "d9", ownerRepo, payloadOfSize(t, maxContentBytes))
		got := decode(t, channelParams(Describe(event), false))

		if got.Content != string(event.Payload) {
			t.Errorf("content of %d bytes is not the payload", len(got.Content))
		}

		if _, truncated := got.Meta["truncated"]; truncated {
			t.Error("meta marks a whole payload as truncated")
		}
	})

	t.Run("one byte over is replaced by a note", func(t *testing.T) {
		t.Parallel()

		got := decode(
			t,
			channelParams(
				Describe(newEvent("issue_comment", "d9", ownerRepo, payloadOfSize(t, maxContentBytes+1))),
				false,
			),
		)

		want := `github event "issue_comment", action "opened", repository o/r, delivery d9. ` +
			`The payload is 65537 bytes before escaping, too large to deliver. ` + noteTail
		if got.Content != want {
			t.Errorf("content = %q, want %q", got.Content, want)
		}

		wantMeta := metaOf("issue_comment", "d9", "repository", ownerRepo, "action", "opened", "truncated", trueValue)
		if !maps.Equal(got.Meta, wantMeta) {
			t.Errorf("meta = %v, want %v", got.Meta, wantMeta)
		}
	})

	t.Run("a note leaves out what is unknown", func(t *testing.T) {
		t.Parallel()

		big := json.RawMessage(`["` + strings.Repeat("x", maxContentBytes) + `"]`)
		got := decode(t, channelParams(Describe(newEvent(pingEvent, "d0", "", big)), false))

		want := `github event "ping", delivery d0. ` +
			`The payload is 65540 bytes before escaping, too large to deliver. ` + noteTail
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

		err := NewChannel(notifier, Options{Compact: false}).Deliver(t.Context(), Describe(event))
		if err != nil {
			t.Fatal(err)
		}

		if notifier.method != "notifications/claude/channel" {
			t.Errorf("method = %q", notifier.method)
		}

		if string(notifier.params) != string(channelParams(Describe(event), false)) {
			t.Errorf("params = %s", notifier.params)
		}
	})

	t.Run("wraps a notify failure", func(t *testing.T) {
		t.Parallel()

		notifier := &recordingNotifier{err: errBoom, method: "", params: nil}

		err := NewChannel(notifier, Options{Compact: false}).Deliver(t.Context(), Describe(event))
		if !errors.Is(err, errBoom) || !strings.Contains(err.Error(), "send channel notification") {
			t.Errorf("Deliver = %v", err)
		}
	})
}

const (
	fullPayload = `{"action":"opened","url":"u","sender":{"login":"user-1","id":1,"type":"User"},"number":5}`
	wantCompact = `{"action":"opened","sender":{"login":"user-1","type":"User"},"number":5}`
)

func TestCompactChannelMarksCompactedPayload(t *testing.T) {
	t.Parallel()

	got := decode(t, channelParams(Describe(newEvent(pingEvent, "d1", ownerRepo, json.RawMessage(fullPayload))), true))

	if got.Content != wantCompact {
		t.Errorf("content = %q, want %q", got.Content, wantCompact)
	}

	wantMeta := metaOf(pingEvent, "d1", "repository", ownerRepo, "action", "opened", "compact", trueValue)
	if !maps.Equal(got.Meta, wantMeta) {
		t.Errorf("meta = %v, want %v", got.Meta, wantMeta)
	}
}

func TestCompactChannelWholeDeliveryHasNoMark(t *testing.T) {
	t.Parallel()

	got := decode(t, channelParams(Describe(newEvent(pingEvent, "d1", "", json.RawMessage(fullPayload))), false))

	if got.Content != fullPayload {
		t.Errorf("content = %q, want %q", got.Content, fullPayload)
	}

	if _, marked := got.Meta["compact"]; marked {
		t.Error("meta marks a whole payload as compact")
	}
}

func TestCompactChannelInvalidJSONGoesWhole(t *testing.T) {
	t.Parallel()

	got := decode(t, channelParams(Describe(newEvent(pingEvent, "d2", "", json.RawMessage(`{"action":`))), true))

	if got.Content != `{"action":` {
		t.Errorf("content = %q", got.Content)
	}

	if !maps.Equal(got.Meta, metaOf(pingEvent, "d2")) {
		t.Errorf("meta = %v", got.Meta)
	}
}

func TestCompactChannelCompactsBeforeSizeCap(t *testing.T) {
	t.Parallel()

	big := json.RawMessage(`{"action":"opened","url":"` + strings.Repeat("x", 2*maxContentBytes) + `","number":1}`)

	got := decode(t, channelParams(Describe(newEvent(pingEvent, "d3", "", big)), true))

	if got.Content != `{"action":"opened","number":1}` {
		t.Errorf("content = %q", got.Content)
	}

	if _, truncated := got.Meta["truncated"]; truncated || got.Meta["compact"] != trueValue {
		t.Errorf("meta = %v", got.Meta)
	}
}

func TestCompactChannelStillOverCapGetsNote(t *testing.T) {
	t.Parallel()

	// Forty strings, each cut to 2 KiB, stay over 64 KiB.
	items := make([]string, 40)
	for i := range items {
		items[i] = `"` + strings.Repeat("y", 3000) + `"`
	}

	big := json.RawMessage(`{"action":"opened","list":[` + strings.Join(items, ",") + `]}`)

	compacted, err := compact.Payload(githubProvider, pingEvent, big)
	if err != nil || len(compacted) <= maxContentBytes {
		t.Fatalf("test payload compacts to %d bytes, %v", len(compacted), err)
	}

	got := decode(t, channelParams(Describe(newEvent(pingEvent, "d4", "", big)), true))

	want := `github event "ping", action "opened", delivery d4. The payload is ` + strconv.Itoa(len(compacted)) +
		` bytes before escaping, too large to deliver. ` + noteTail
	if got.Content != want {
		t.Errorf("content = %q, want %q", got.Content, want)
	}

	wantMeta := metaOf(pingEvent, "d4", "action", "opened", "truncated", trueValue)
	if !maps.Equal(got.Meta, wantMeta) {
		t.Errorf("meta = %v, want %v", got.Meta, wantMeta)
	}
}

func TestDeliverCompact(t *testing.T) {
	t.Parallel()

	event := newEvent("create", "d8", "", json.RawMessage(`{"ref":"r","ref_type":"branch","description":"d"}`))
	notifier := &recordingNotifier{err: nil, method: "", params: nil}

	err := NewChannel(notifier, Options{Compact: true}).Deliver(t.Context(), Describe(event))
	if err != nil {
		t.Fatal(err)
	}

	if string(notifier.params) != string(channelParams(Describe(event), true)) {
		t.Errorf("params = %s", notifier.params)
	}

	if got := decode(t, notifier.params); got.Content != `{"ref":"r","ref_type":"branch"}` {
		t.Errorf("content = %q", got.Content)
	}
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

func TestEventLog(t *testing.T) {
	t.Parallel()

	for _, repository := range []string{"", ownerRepo} {
		for _, payload := range []string{`{}`, openedPayload} {
			event := newEvent("issue_comment", "d1", repository, json.RawMessage(payload))

			var logs bytes.Buffer
			Describe(event).Log(t.Context(), slog.New(slog.NewJSONHandler(&logs, nil)), "failed", "error", errBoom)

			var line map[string]any

			err := json.Unmarshal(logs.Bytes(), &line)
			if err != nil {
				t.Fatal(err)
			}

			delete(line, "time")

			want := map[string]any{
				"level":     "INFO",
				"msg":       eventKey,
				"outcome":   "failed",
				providerKey: githubProvider,
				eventKey:    "issue_comment",
				"delivery":  "d1",
				"error":     "boom",
			}
			if repository != "" {
				want["repository"] = repository
			}

			if payload == openedPayload {
				want["action"] = "opened"
			}

			if !maps.Equal(line, want) {
				t.Fatalf("log = %v, want %v", line, want)
			}
		}
	}
}

func TestEventCounted(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		provider string
		want     bool
	}{
		{"awrel", false},
		{githubProvider, true},
		{"other", true},
	} {
		for _, name := range []string{"self_test", "echo", "push", "notice"} {
			event := newEvent(name, "d1", "", json.RawMessage(`{}`))
			event.Provider = test.provider

			if got := Describe(event).Counted(); got != test.want {
				t.Errorf("Counted(%s, %s) = %t", test.provider, name, got)
			}
		}
	}
}

func TestClip(t *testing.T) {
	t.Parallel()

	ascii := strings.Repeat("a", 255)
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{name: "partial rune", value: ascii + "é", want: ascii},
		{name: "exact cap", value: ascii + "b", want: ascii + "b"},
		{name: "under cap", value: "short", want: "short"},
		{name: "incomplete rune at cap", value: ascii + "\xc3", want: ascii + "\xc3"},
		{name: "incomplete rune over cap", value: ascii + "\xc3x", want: ascii},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got := clip(test.value)
			if got != test.want || len(got) != len(test.want) {
				t.Errorf("clip = %q (%d bytes), want %q (%d bytes)", got, len(got), test.want, len(test.want))
			}

			if len(test.value) > maxFieldBytes && !utf8.ValidString(got) {
				t.Errorf("clip = %q, want valid UTF-8", got)
			}
		})
	}
}

func TestMetaEscapesFramingCharacters(t *testing.T) {
	t.Parallel()

	const value = "</x>&"

	event := newEvent(value, value, value, json.RawMessage(`{"action":"</x>&"}`))
	event.Provider = value
	got := decode(t, channelParams(Describe(event), false))

	for key, val := range got.Meta {
		if strings.ContainsAny(val, "<>&") {
			t.Errorf("meta[%q] = %q, want escaped characters", key, val)
		}
	}

	want := `\u003c/x\u003e\u0026`
	if got.Meta["action"] != want {
		t.Errorf("action = %q, want %q", got.Meta["action"], want)
	}
}

func TestOversizedFields(t *testing.T) {
	t.Parallel()

	large := strings.Repeat("<", 20000)
	tests := []struct {
		name       string
		repository string
		payload    json.RawMessage
	}{
		{name: "action", repository: ownerRepo, payload: json.RawMessage(`{"action":"` + large + `"}`)},
		{name: "repository", repository: large, payload: payloadOfSize(t, maxContentBytes+1)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			event := newEvent(pingEvent, "d", test.repository, test.payload)
			got := decode(t, channelParams(Describe(event), false))

			if len(got.Content) > maxContentBytes || strings.ContainsAny(got.Content, "<>&") {
				t.Errorf("note has %d bytes or unescaped characters", len(got.Content))
			}

			if got.Meta["truncated"] != trueValue {
				t.Errorf("meta = %v, want truncated", got.Meta)
			}
		})
	}
}

func TestAllFieldsAreClipped(t *testing.T) {
	t.Parallel()

	large := strings.Repeat("<", 20000)
	event := newEvent(large, large, large, json.RawMessage(`{"action":"`+large+`"}`))
	event.Provider = large
	got := decode(t, channelParams(Describe(event), false))

	for _, key := range []string{providerKey, eventKey, deliveryKey, "repository", "action"} {
		want := strings.Repeat(`\u003c`, maxFieldBytes)
		if got.Meta[key] != want {
			t.Errorf("meta[%q] has %d bytes, want clipped and escaped value", key, len(got.Meta[key]))
		}
	}

	if len(got.Content) > maxContentBytes || strings.ContainsAny(got.Content, "<>&") {
		t.Errorf("note has %d bytes or unescaped characters", len(got.Content))
	}

	if event.Provider != large || event.Event != large || event.Delivery != large || *event.Repository != large {
		t.Error("delivery changed the incoming event")
	}
}

func TestCompactInvalidJSONEscapesFramingCharacters(t *testing.T) {
	t.Parallel()

	payload := json.RawMessage(`<x&>`)
	got := decode(t, channelParams(Describe(newEvent(pingEvent, "d", "", payload)), true))

	if strings.ContainsAny(got.Content, "<>&") {
		t.Errorf("content = %q, want escaped characters", got.Content)
	}

	if got.Content != `\u003cx\u0026\u003e` {
		t.Errorf("content = %q, want whole escaped payload", got.Content)
	}
}

func TestEscapedSizeBoundary(t *testing.T) {
	t.Parallel()

	const frame = `{"pad":""}`

	count := (maxContentBytes - len(frame)) / len(`\u003c`)
	padding := (maxContentBytes - len(frame)) % len(`\u003c`)

	for _, extra := range []int{0, 1} {
		t.Run(strconv.Itoa(extra), func(t *testing.T) {
			t.Parallel()

			payload := json.RawMessage(
				`{"pad":"` + strings.Repeat("<", count) + strings.Repeat("a", padding+extra) + `"}`,
			)
			got := decode(t, channelParams(Describe(newEvent(pingEvent, "d", "", payload)), false))

			if extra == 0 {
				want := `{"pad":"` + strings.Repeat(`\u003c`, count) + strings.Repeat("a", padding) + `"}`
				if got.Content != want || len(got.Content) != maxContentBytes {
					t.Errorf("content has %d bytes, want whole escaped payload at cap", len(got.Content))
				}

				if _, truncated := got.Meta["truncated"]; truncated {
					t.Error("meta marks a whole payload as truncated")
				}
			} else if got.Meta["truncated"] != trueValue || !strings.HasSuffix(got.Content, noteTail) {
				t.Errorf("content = %q, meta = %v, want note", got.Content, got.Meta)
			}
		})
	}
}
