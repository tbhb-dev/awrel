// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package protocol

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

const (
	githubProvider = "github"
	decodeMessage  = "decode message"
	ownerRepo      = "o/r"
)

func TestEncode(t *testing.T) {
	t.Parallel()

	full := Subscription{Provider: githubProvider, Event: "pull_request", Repository: ownerRepo}
	bare := Subscription{Provider: githubProvider, Event: "", Repository: ""}

	tests := []struct {
		name string
		want string
		got  []byte
	}{
		{
			"subscribe full",
			`{"subscription":{"provider":"github","event":"pull_request","repository":"o/r"},"type":"subscribe","id":"1"}`,
			EncodeSubscribe("1", full),
		},
		{
			"subscribe omits empty event and repository",
			`{"subscription":{"provider":"github"},"type":"subscribe","id":"2"}`,
			EncodeSubscribe("2", bare),
		},
		{
			"unsubscribe",
			`{"subscription":{"provider":"github","event":"pull_request","repository":"o/r"},"type":"unsubscribe","id":"3"}`,
			EncodeUnsubscribe("3", full),
		},
		{"list", `{"type":"list","id":"4"}`, EncodeList("4")},
		{"escaping", `{"type":"list","id":"a\"b\n"}`, EncodeList("a\"b\n")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if string(test.got) != test.want {
				t.Errorf("got %s, want %s", test.got, test.want)
			}
		})
	}
}

func TestConstants(t *testing.T) {
	t.Parallel()

	if Subprotocol != "awrel.v1" {
		t.Errorf("Subprotocol = %q", Subprotocol)
	}

	if PingFrame != `{"type":"ping"}` || len(PingFrame) != 15 {
		t.Errorf("PingFrame = %q", PingFrame)
	}
}

func TestDecode(t *testing.T) {
	t.Parallel()

	repo := ownerRepo

	tests := []struct {
		want Message
		name string
		in   string
	}{
		{
			Subscriptions{
				ID:            "1",
				Subscriptions: []Subscription{{Provider: githubProvider, Event: "push", Repository: ownerRepo}},
			},
			"subscriptions",
			`{"type":"subscriptions","id":"1","subscriptions":[{"provider":"github","event":"push","repository":"o/r"}]}`,
		},
		{
			Subscriptions{ID: "1", Subscriptions: []Subscription{}},
			"empty subscriptions",
			`{"type":"subscriptions","id":"1","subscriptions":[]}`,
		},
		{
			Error{ID: "7", Code: "limit", Message: "full"},
			"error",
			`{"type":"error","id":"7","code":"limit","message":"full"}`,
		},
		{
			Error{ID: "", Code: "bad_message", Message: "no id"},
			"error with null id",
			`{"type":"error","id":null,"code":"bad_message","message":"no id"}`,
		},
		{
			Error{ID: "", Code: "bad_message", Message: "no id"},
			"error without id",
			`{"type":"error","code":"bad_message","message":"no id"}`,
		},
		{Pong{}, "pong", `{"type":"pong"}`},
		{
			Event{
				Repository: &repo,
				Provider:   "github",
				Event:      "push",
				Delivery:   "d1",
				Payload:    []byte(`{ "a" : [1, 2] }`),
			},
			"event keeps the payload bytes",
			`{"type":"event","provider":"github","event":"push","delivery":"d1","repository":"o/r","payload":{ "a" : [1, 2] }}`,
		},
		{
			Event{Repository: nil, Provider: githubProvider, Event: "ping", Delivery: "d2", Payload: []byte(`null`)},
			"event with null repository and payload",
			`{"type":"event","provider":"github","event":"ping","delivery":"d2","repository":null,"payload":null}`,
		},
		{
			Event{Repository: nil, Provider: githubProvider, Event: "ping", Delivery: "d2", Payload: []byte(`"text"`)},
			"event without repository",
			`{"type":"event","provider":"github","event":"ping","delivery":"d2","payload":"text"}`,
		},
		{Unknown{Type: "later"}, "unknown type", `{"type":"later","x":1}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := Decode([]byte(test.in))
			if err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("got %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestDecodeErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		wantErr string
		missing bool
	}{
		{"invalid json", `{`, decodeMessage, false},
		{"not an object", `[]`, decodeMessage, false},
		{"type ill-typed", `{"type":1}`, decodeMessage, false},
		{"type missing", `{"id":"1"}`, `message: missing field "type"`, true},
		{"type null", `{"type":null}`, `message: missing field "type"`, true},
		{
			"subscriptions ill-typed",
			`{"type":"subscriptions","id":"1","subscriptions":{}}`,
			"decode subscriptions", false,
		},
		{
			"subscriptions missing id",
			`{"type":"subscriptions","subscriptions":[]}`,
			`subscriptions: missing field "id"`, true,
		},
		{
			"subscriptions missing list",
			`{"type":"subscriptions","id":"1"}`,
			`subscriptions: missing field "subscriptions"`, true,
		},
		{
			"subscriptions null list",
			`{"type":"subscriptions","id":"1","subscriptions":null}`,
			`subscriptions: missing field "subscriptions"`, true,
		},
		{"error ill-typed", `{"type":"error","id":5,"code":"x","message":"y"}`, "decode error", false},
		{"error missing code", `{"type":"error","id":"1","message":"y"}`, `error: missing field "code"`, true},
		{"error missing message", `{"type":"error","id":"1","code":"x"}`, `error: missing field "message"`, true},
		{
			"event ill-typed",
			`{"type":"event","provider":"p","event":1,"delivery":"d","payload":1}`,
			"decode event", false,
		},
		{
			"event missing provider",
			`{"type":"event","event":"e","delivery":"d","payload":1}`,
			`event: missing field "provider"`, true,
		},
		{
			"event missing event",
			`{"type":"event","provider":"p","delivery":"d","payload":1}`,
			`event: missing field "event"`, true,
		},
		{
			"event missing delivery",
			`{"type":"event","provider":"p","event":"e","payload":1}`,
			`event: missing field "delivery"`, true,
		},
		{
			"event missing payload",
			`{"type":"event","provider":"p","event":"e","delivery":"d"}`,
			`event: missing field "payload"`, true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := Decode([]byte(test.in))
			if err == nil {
				t.Fatal("got no error")
			}

			if !strings.Contains(err.Error(), test.wantErr) {
				t.Errorf("error %q does not contain %q", err, test.wantErr)
			}

			if errors.Is(err, errMissingField) != test.missing {
				t.Errorf("errors.Is(missing) = %v, want %v", !test.missing, test.missing)
			}

			if !test.missing && errors.Unwrap(err) == nil {
				t.Errorf("error %q wraps nothing", err)
			}
		})
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

func TestErrorText(t *testing.T) {
	t.Parallel()

	var err error = Error{ID: "1", Code: "limit", Message: "attachment full"}
	if got, want := err.Error(), "limit: attachment full"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}
