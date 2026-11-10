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
			"unsubscribe omits empty event and repository",
			`{"subscription":{"provider":"github"},"type":"unsubscribe","id":"2"}`,
			EncodeUnsubscribe("2", bare),
		},
		{
			"unsubscribe",
			`{"subscription":{"provider":"github","event":"pull_request","repository":"o/r"},"type":"unsubscribe","id":"3"}`,
			EncodeUnsubscribe("3", full),
		},
		{"echo", `{"type":"echo","id":"e","nonce":"n_1-ABC"}`, EncodeEcho("e", "n_1-ABC")},
		{"echo escaping", `{"type":"echo","id":"a\"b","nonce":"x\ny"}`, EncodeEcho("a\"b", "x\ny")},
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
				ID: "1",
				Subscriptions: []Subscription{
					{Provider: githubProvider, Event: eventPush, Repository: ownerRepo},
				},
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
		{Echoed{ID: "e"}, "echoed", `{"type":"echoed","id":"e"}`},
		{Pong{}, "pong", `{"type":"pong"}`},
		{
			Event{
				Repository: &repo,
				Provider:   githubProvider,
				Event:      eventPush,
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
		{"echoed ill-typed", `{"type":"echoed","id":3}`, "decode echoed", false},
		{"echoed missing id", `{"type":"echoed"}`, `echoed: missing field "id"`, true},
		{"echoed null id", `{"type":"echoed","id":null}`, `echoed: missing field "id"`, true},
		{"invalid json", `{`, decodeMessage, false},
		{"not an object", `[]`, decodeMessage, false},
		{"type ill-typed", `{"type":1}`, decodeMessage, false},
		{
			"type missing", `{"id":"1"}`,
			`message: missing field "type"`, true,
		},
		{
			"type null", `{"type":null}`,
			`message: missing field "type"`, true,
		},
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
		{
			"error missing code", `{"type":"error","id":"1","message":"y"}`,
			`error: missing field "code"`, true,
		},
		{
			"error missing message", `{"type":"error","id":"1","code":"x"}`,
			`error: missing field "message"`, true,
		},
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

const (
	eventPush               = "push"
	suiteEvent              = "check_suite"
	otherProvider           = "other"
	incompleteFilterMessage = "filters require github, event and repository"
	conclusionSuccess       = "success"
	conclusionFailure       = "failure"
	workflowFile            = ".github/workflows/CI.yml"
	exactName               = " CI "
)

func TestFilterValidation(t *testing.T) {
	t.Parallel()

	valid := Subscription{
		Provider:     githubProvider,
		Event:        eventWorkflowRun,
		Repository:   "Owner/Repo",
		Action:       []string{"completed", "future.action-1_"},
		Conclusion:   []string{conclusionFailure},
		Name:         " CI / result ",
		WorkflowPath: workflowFile,
	}

	validErr := valid.ValidateFilters()
	if validErr != nil {
		t.Fatal(validErr)
	}

	cases := []struct {
		change func(*Subscription)
		name   string
	}{
		{name: "provider", change: func(s *Subscription) { s.Provider = otherProvider }},
		{name: "event absent", change: func(s *Subscription) { s.Event = "" }},
		{name: "repository absent", change: func(s *Subscription) { s.Repository = "" }},
		{name: "actionless", change: func(s *Subscription) { s.Event = eventPush }},
		{name: "empty action", change: func(s *Subscription) { s.Action = []string{} }},
		{
			name:   "too many duplicates",
			change: func(s *Subscription) { s.Action = strings.Split(strings.Repeat("x,", 8)+"x", ",") },
		},
		{name: "invalid action", change: func(s *Subscription) { s.Action = []string{"Completed"} }},
		{name: "newline action", change: func(s *Subscription) { s.Action = []string{"completed\n"} }},
		{name: "empty action value", change: func(s *Subscription) { s.Action = []string{""} }},
		{name: "long action", change: func(s *Subscription) { s.Action = []string{strings.Repeat("x", 65)} }},
		{name: "conclusion event", change: func(s *Subscription) { s.Event = "issues" }},
		{name: "empty conclusion", change: func(s *Subscription) { s.Conclusion = []string{} }},
		{name: "invalid conclusion", change: func(s *Subscription) { s.Conclusion = []string{"success!"} }},
		{name: "suite name", change: func(s *Subscription) { s.Event = suiteEvent }},
		{name: "control name", change: func(s *Subscription) { s.Name = "CI\n" }},
		{name: "unicode name", change: func(s *Subscription) { s.Name = "café" }},
		{name: "long name", change: func(s *Subscription) { s.Name = strings.Repeat("x", 129) }},
		{name: "job path", change: func(s *Subscription) { s.Event = "workflow_job" }},
		{name: "invalid path", change: func(s *Subscription) { s.WorkflowPath = "work flow.yml" }},
		{name: "long path", change: func(s *Subscription) { s.WorkflowPath = strings.Repeat("x", 257) }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			sub := valid
			test.change(&sub)

			err := sub.ValidateFilters()
			if err == nil {
				t.Fatal("accepted invalid filter")
			}
		})
	}

	for _, sub := range []Subscription{
		{Provider: "", Event: "", Repository: ""},
		{Provider: githubProvider, Event: "", Repository: ""},
		{Provider: githubProvider, Event: eventCheckRun, Repository: ownerRepo, Name: "x"},
		{Provider: githubProvider, Event: suiteEvent, Repository: ownerRepo, Conclusion: []string{conclusionSuccess}},
		{
			Provider: githubProvider, Event: eventWorkflowRun, Repository: ownerRepo,
			Name: strings.Repeat("~", 128), WorkflowPath: strings.Repeat("/", 256),
			Action: []string{strings.Repeat("a", 64), "b", "c", "d", "e", "f", "g", "h"},
		},
	} {
		err := sub.ValidateFilters()
		if err != nil {
			t.Fatalf("%+v: %v", sub, err)
		}
	}
}

func TestCanonicalIdentity(t *testing.T) {
	t.Parallel()

	sub := Subscription{
		Provider:     githubProvider,
		Event:        eventWorkflowRun,
		Repository:   "Owner/Repo",
		Action:       []string{"z", "a", "z"},
		Conclusion:   []string{conclusionSuccess, conclusionFailure, conclusionFailure},
		Name:         exactName,
		WorkflowPath: workflowFile,
	}
	want := Subscription{
		Provider:     githubProvider,
		Event:        eventWorkflowRun,
		Repository:   "owner/repo",
		Action:       []string{"a", "z"},
		Conclusion:   []string{conclusionFailure, conclusionSuccess},
		Name:         exactName,
		WorkflowPath: workflowFile,
	}

	got := sub.Canonical()
	if !reflect.DeepEqual(got, want) || !sub.Equal(want) {
		t.Fatalf("canonical = %+v", got)
	}

	got.Action[0] = "changed"

	got.Conclusion[0] = "changed"
	if sub.Action[0] != "z" || sub.Conclusion[0] != conclusionSuccess {
		t.Fatal("aliased lists")
	}

	for _, change := range []func(*Subscription){
		func(s *Subscription) { s.Provider = otherProvider },
		func(s *Subscription) { s.Event = eventCheckRun },
		func(s *Subscription) { s.Repository = ownerRepo },
		func(s *Subscription) { s.Action = nil },
		func(s *Subscription) { s.Conclusion = nil },
		func(s *Subscription) { s.Name = "CI" },
		func(s *Subscription) { s.WorkflowPath = ".github/workflows/ci.yml" },
	} {
		other := want
		change(&other)

		if sub.Equal(other) {
			t.Fatalf("equal to %+v", other)
		}
	}

	for _, filtered := range []Subscription{
		{Provider: "", Event: "", Repository: "", Action: []string{"a"}},
		{Provider: "", Event: "", Repository: "", Conclusion: []string{"a"}},
		{Provider: "", Event: "", Repository: "", Name: "n"},
		{Provider: "", Event: "", Repository: "", WorkflowPath: "p"},
	} {
		if !filtered.HasFilters() {
			t.Fatal("missing filter")
		}
	}
}

func TestFilterWireAndAttachment(t *testing.T) {
	t.Parallel()

	sub := Subscription{
		Provider:     githubProvider,
		Event:        eventWorkflowRun,
		Repository:   "O/R",
		Action:       []string{"z", "a", "z"},
		Conclusion:   []string{conclusionSuccess},
		Name:         ` <CI> & "x"\ `,
		WorkflowPath: ".github/workflows/ci.yml",
	}
	for _, data := range [][]byte{EncodeSubscribe("1", sub), EncodeUnsubscribe("1", sub)} {
		text := string(data)
		for _, part := range []string{
			`"subscription_filters":true`,
			`"action":["a","z"]`,
			`"conclusion":["success"]`,
			`"repository":"o/r"`,
			`"workflow_path":".github/workflows/ci.yml"`,
			`<CI> &`,
		} {
			if !strings.Contains(text, part) {
				t.Fatalf("%s lacks %s", text, part)
			}
		}
	}

	if got := string(EncodeFilterList("probe")); got != `{"type":"list","id":"probe","subscription_filters":true}` {
		t.Fatal(got)
	}

	data := `{"type":"subscriptions","id":"1","subscription_filters":true,` +
		`"subscriptions":[{"provider":"github","event":"workflow_run","repository":"o/r","action":["a",` +
		`"z"],"conclusion":["success"],"name":" CI ","workflow_path":"ci.yml"}]}`

	msg, err := Decode([]byte(data))
	if err != nil {
		t.Fatal(err)
	}

	reply, ok := msg.(Subscriptions)
	if !ok {
		t.Fatalf("reply = %T", msg)
	}

	want := sub.Canonical()
	want.Name = exactName

	want.WorkflowPath = "ci.yml"
	if !reply.SubscriptionFilters || !reply.Subscriptions[0].Equal(want) {
		t.Fatalf("%+v", reply)
	}

	if AttachmentBytes(nil) != 20 {
		t.Fatal("empty wrapper size")
	}
	// JSON.stringify leaves HTML characters literal and escapes quotes and backslashes.
	expected := `{"subscriptions":[{"provider":"github","event":"workflow_run","repository":"o/r","action":["a",` +
		`"z"],"conclusion":["success"],"name":" <CI> & \"x\"\\ ",` +
		`"workflow_path":".github/workflows/ci.yml"}]}`
	if got := AttachmentBytes([]Subscription{sub}); got != len(expected) {
		t.Fatalf("size %d, want %d: %s", got, len(expected), mustMarshal(sub.Canonical()))
	}
}

func TestFilterErrorsAndEventSupport(t *testing.T) {
	t.Parallel()

	if AttachmentBudget != 12288 {
		t.Fatal("attachment budget changed")
	}

	cases := []struct {
		sub     Subscription
		message string
	}{
		{
			Subscription{Provider: otherProvider, Event: eventCheckRun, Repository: ownerRepo, Name: "CI"},
			incompleteFilterMessage,
		},
		{
			Subscription{Provider: githubProvider, Event: "", Repository: ownerRepo, Name: "CI"},
			incompleteFilterMessage,
		},
		{
			Subscription{Provider: githubProvider, Event: eventCheckRun, Repository: "", Name: "CI"},
			incompleteFilterMessage,
		},
		{
			Subscription{Provider: githubProvider, Event: suiteEvent, Repository: ownerRepo, Name: "CI"},
			"name needs a supported event and 1 to 128 printable ASCII characters",
		},
		{
			Subscription{Provider: githubProvider, Event: eventCheckRun, Repository: ownerRepo, Name: "CI\n"},
			"name needs a supported event and 1 to 128 printable ASCII characters",
		},
		{
			Subscription{
				Provider:   githubProvider,
				Event:      eventPush,
				Repository: ownerRepo,
				Action:     []string{"completed"},
			},
			"action is unsupported for push",
		},
		{
			Subscription{
				Provider:   githubProvider,
				Event:      "issues",
				Repository: ownerRepo,
				Conclusion: []string{conclusionSuccess},
			},
			"conclusion is unsupported for issues",
		},
		{
			Subscription{Provider: githubProvider, Event: eventCheckRun, Repository: ownerRepo, Conclusion: []string{}},
			"conclusion needs 1 to 8 values",
		},
		{
			Subscription{
				Provider:   githubProvider,
				Event:      eventCheckRun,
				Repository: ownerRepo,
				Conclusion: []string{"success!"},
			},
			"invalid conclusion value",
		},
		{
			Subscription{Provider: githubProvider, Event: eventCheckRun, Repository: ownerRepo, WorkflowPath: "ci.yml"},
			"workflow_path needs workflow_run and 1 to 256 path characters",
		},
	}
	for _, test := range cases {
		err := test.sub.ValidateFilters()
		if !errors.Is(err, errFilter) || err.Error() != "bad subscription filter: "+test.message {
			t.Fatalf("%+v: %v", test.sub, err)
		}
	}

	for _, event := range actionEvents {
		sub := Subscription{
			Provider:   githubProvider,
			Event:      event,
			Repository: ownerRepo,
			Action:     []string{"future.action-1"},
		}

		err := sub.ValidateFilters()
		if err != nil {
			t.Fatalf("%s: %v", event, err)
		}
	}
}
