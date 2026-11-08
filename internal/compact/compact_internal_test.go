// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package compact

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

//nolint:gochecknoglobals // a test flag can only be a package-level variable
var update = flag.Bool("update", false, "rewrite the golden files from the output")

const (
	goldenSuffix = ".golden.json"
	testdataDir  = "testdata"

	eventPing        = "ping"
	eventPullRequest = "pull_request"
	eventIssues      = "issues"
	nullJSON         = "null"
	keepOne          = `{"keep":1}`
	refOnly          = `{"ref":"r"}`
)

// A payload and the compact form it should have.
type testCase struct{ name, payload, want string }

// eventCase adds the event name to a case.
type eventCase struct{ name, event, payload, want string }

// compactOf is Payload for a GitHub event, as text.
func compactOf(t *testing.T, event, payload string) string {
	t.Helper()

	got, err := Payload(githubProvider, event, json.RawMessage(payload))
	if err != nil {
		t.Fatal(err)
	}

	return string(got)
}

// Compares the compact form of each payload, as the event, to the wanted one.
func checkCases(t *testing.T, event string, tests []testCase) {
	t.Helper()

	for _, test := range tests {
		got := compactOf(t, event, test.payload)
		if got != test.want {
			t.Errorf("%s:\ngot  %s\nwant %s", test.name, got, test.want)
		}
	}
}

// checkEvents runs cases that each name an event.
func checkEvents(t *testing.T, tests []eventCase) {
	t.Helper()

	for _, test := range tests {
		checkCases(t, test.event, []testCase{{name: test.name, payload: test.payload, want: test.want}})
	}
}

// fixture is one input file and where its expected output is.
type fixture struct {
	name     string
	provider string
	event    string
	input    string
	golden   string
}

// fixtures finds the inputs under `testdata`. The path of an input is
// <event>/<name>.json for GitHub, or <event>/<provider>/<name>.json.
func fixtures(t *testing.T) []fixture {
	t.Helper()

	var found []fixture

	err := filepath.WalkDir(testdataDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || filepath.Ext(path) != ".json" || strings.HasSuffix(path, goldenSuffix) {
			return err
		}

		parts := strings.Split(filepath.ToSlash(strings.TrimPrefix(path, testdataDir+string(filepath.Separator))), "/")
		provider := githubProvider

		if len(parts) == 3 {
			provider = parts[1]
		}

		found = append(found, fixture{
			name:     strings.Join(parts, "/"),
			provider: provider,
			event:    parts[0],
			input:    path,
			golden:   strings.TrimSuffix(path, ".json") + goldenSuffix,
		})

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(found) == 0 {
		t.Fatal("no fixtures")
	}

	return found
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	return data
}

// squeeze removes the whitespace between the tokens of JSON text.
func squeeze(t *testing.T, data []byte) string {
	t.Helper()

	var out bytes.Buffer

	err := json.Compact(&out, data)
	if err != nil {
		t.Fatalf("%v in %s", err, data)
	}

	return out.String()
}

// writeGolden stores `got` as indented JSON in the golden file at `path`.
func writeGolden(t *testing.T, path string, got []byte) {
	t.Helper()

	var pretty bytes.Buffer

	err := json.Indent(&pretty, got, "", "  ")
	if err != nil {
		t.Fatal(err)
	}

	pretty.WriteByte('\n')

	err = os.WriteFile(path, pretty.Bytes(), 0o600)
	if err != nil {
		t.Fatal(err)
	}
}

func TestGolden(t *testing.T) {
	t.Parallel()

	for _, fix := range fixtures(t) {
		t.Run(fix.name, func(t *testing.T) {
			t.Parallel()

			got, err := Payload(fix.provider, fix.event, readFile(t, fix.input))
			if err != nil {
				t.Fatal(err)
			}

			if *update {
				writeGolden(t, fix.golden, got)

				return
			}

			want := squeeze(t, readFile(t, fix.golden))
			if string(got) != want {
				t.Errorf("output differs from %s; run the test with -update to see the change\ngot:  %s\nwant: %s",
					fix.golden, got, want)
			}
		})
	}
}

// TestSize logs how much each fixture shrinks. The input is measured without
// its whitespace, as a webhook delivers it.
func TestSize(t *testing.T) {
	t.Parallel()

	for _, fix := range fixtures(t) {
		t.Run(fix.name, func(t *testing.T) {
			t.Parallel()

			input := squeeze(t, readFile(t, fix.input))

			got, err := Payload(fix.provider, fix.event, json.RawMessage(input))
			if err != nil {
				t.Fatal(err)
			}

			t.Logf("%s: %d -> %d bytes", fix.name, len(input), len(got))

			if fix.provider == githubProvider && len(got) >= len(input) {
				t.Errorf("output of %d bytes is not smaller than the input of %d", len(got), len(input))
			}
		})
	}
}

func TestInvalidJSON(t *testing.T) {
	t.Parallel()

	for _, payload := range []string{"", "{", `{"a":}`, `{"a":1} {"b":2}`, "nope", `{"a":1,}`} {
		got, err := Payload(githubProvider, eventPush, json.RawMessage(payload))
		if !errors.Is(err, ErrInvalidPayload) || got != nil {
			t.Errorf("Payload(%q) = %s, %v", payload, got, err)
		}
	}
}

// TestParserEndsOnBadInput feeds the parser what Payload never lets through. A
// broken token must end the loops instead of repeating them forever.
func TestParserEndsOnBadInput(t *testing.T) {
	t.Parallel()

	for _, input := range []string{"", "{", "[", `{"a"`, `{"a":1`, `[1,`, `{"a":1,}`, `{"a":}`, "nope"} {
		dec := json.NewDecoder(strings.NewReader(input))
		dec.UseNumber()

		got := render((&parser{dec: dec, limit: len(input), budget: maxNodes, nodes: 0, over: false}).parse())
		if len(got) > 10*len(input)+16 {
			t.Errorf("%q parsed to %d bytes", input, len(got))
		}
	}
}

// bigArray is a body whose "unused" array has `count` zeros. The reproducer in
// the review of #22 is one of 524,288, sent as a push.
func bigArray(count int) json.RawMessage {
	return json.RawMessage(`{"unused":[` + strings.Repeat("0,", count-1) + `0]}`)
}

// TestHostilePayloadFailsFast feeds the 1 MiB reproducer: half a million
// values of two bytes each, which take about 250 MB to parse. Payload must
// return ErrTooComplex after `maxNodes` values and take no more memory. The
// test runs alone, so that no other test allocates while it counts.
//
//nolint:paralleltest // AllocsPerRun and MemStats count the whole process
func TestHostilePayloadFailsFast(t *testing.T) {
	payload := bigArray(524_288)
	if len(payload) != 1_048_588 {
		t.Fatalf("the reproducer has %d bytes", len(payload))
	}

	got, err := Payload(githubProvider, eventPush, payload)
	if !errors.Is(err, ErrTooComplex) || got != nil {
		t.Fatalf("Payload = %.40s, %v; want ErrTooComplex", got, err)
	}

	// Reading every value would take over 524,288 allocations.
	allocs := testing.AllocsPerRun(1, func() {
		_, _ = Payload(githubProvider, eventPush, payload) //nolint:errcheck // the error is checked above
	})
	if allocs > 2*maxNodes {
		t.Errorf("%.0f allocations, want at most %d", allocs, 2*maxNodes)
	}

	// The bytes allocated are bounded by `maxNodes`, not by the input: a
	// payload four times as large costs the same.
	const ceiling = 128 << 20

	for _, count := range []int{524_288, 4 * 524_288} {
		var before, after runtime.MemStats

		runtime.ReadMemStats(&before)

		_, _ = Payload(githubProvider, eventPush, bigArray(count)) //nolint:errcheck // an error is the point

		runtime.ReadMemStats(&after)

		if used := after.TotalAlloc - before.TotalAlloc; used > ceiling {
			t.Errorf("%d values allocated %d bytes, want at most %d", count, used, ceiling)
		}
	}
}

// TestPayloadAtTheLimit checks the count at the boundary: the array and its
// items are values too.
func TestPayloadAtTheLimit(t *testing.T) {
	t.Parallel()

	// The limit is written out so that a change to the constant fails the test.
	const limit = 200_000

	atLimit := bigArray(limit - 2) // the object, the array and the items

	got, err := Payload(githubProvider, eventPing, atLimit)
	if err != nil || len(got) != len(atLimit) {
		t.Errorf("at the limit: %d bytes, %v", len(got), err)
	}

	_, err = Payload(githubProvider, eventPing, bigArray(limit-1))
	if !errors.Is(err, ErrTooComplex) {
		t.Errorf("one over the limit: %v", err)
	}
}

func TestParserBudget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input  string
		budget int
		over   bool
	}{
		{`1`, 1, false},
		{`1`, 0, true},
		{`[1,2]`, 3, false},
		{`[1,2]`, 2, true},
		{`{"a":[true,null],"b":"s"}`, 5, false},
		{`{"a":[true,null],"b":"s"}`, 4, true},
	}
	for _, test := range tests {
		dec := json.NewDecoder(strings.NewReader(test.input))
		dec.UseNumber()

		read := &parser{dec: dec, limit: len(test.input), budget: test.budget, nodes: 0, over: false}
		read.parse()

		if read.over != test.over {
			t.Errorf("%s with a budget of %d: over = %v, want %v", test.input, test.budget, read.over, test.over)
		}
	}
}

// TestGitIdentitiesOfAPush calls simplify to see the five paths that skip the
// user reduction, since the keep-list of a push drops three of them after.
func TestGitIdentitiesOfAPush(t *testing.T) {
	t.Parallel()

	identity := `{"name":"n","email":"e","username":"u"}`
	payload := `{"pusher":` + identity + `,"sender":` + identity + `,` +
		`"head_commit":{"author":` + identity + `,"committer":` + identity + `,"owner":` + identity + `},` +
		`"commits":[{"author":` + identity + `,"committer":` + identity + `,"user":` + identity + `}]}`

	dec := json.NewDecoder(strings.NewReader(payload))
	dec.UseNumber()

	tree := (&parser{dec: dec, limit: len(payload), budget: maxNodes, nodes: 0, over: false}).parse()
	got := string(render(simplify(tree, "", "", pushIdentities)))
	want := `{"pusher":` + identity + `,"sender":{},` +
		`"head_commit":{"author":` + identity + `,"committer":` + identity + `,"owner":{}},` +
		`"commits":[{"author":` + identity + `,"committer":` + identity + `,"user":{}}]}`

	if got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

func TestPayloadIsNotAnObject(t *testing.T) {
	t.Parallel()

	checkEvents(t, []eventCase{
		{"array with a keep-list", eventPush, `[1,"a",null,true]`, `[1,"a",null,true]`},
		{"string with a keep-list", eventPush, `"text"`, `"text"`},
		{"number", eventPing, `-1.5e3`, `-1.5e3`},
		{"null value", eventPing, nullJSON, nullJSON},
		{"array of objects", eventPing, `[{"url":"u","a":1}]`, `[{"a":1}]`},
	})
}

func TestKeepsKeyOrderAndNumbers(t *testing.T) {
	t.Parallel()

	payload := `{"z":1,"a":[3,2,1],"m":{"y":1.0,"b":12345678901234567890,"a":1e2},"action":"x"}`
	if got := compactOf(t, eventPing, payload); got != payload {
		t.Errorf("got %s, want %s", got, payload)
	}

	// The keep-list's own order never reorders the input.
	got := compactOf(t, "create", `{"ref_type":"branch","other":1,"ref":"r","action":"a"}`)
	if want := `{"ref_type":"branch","ref":"r","action":"a"}`; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestOutputIsDeterministic(t *testing.T) {
	t.Parallel()

	for _, fix := range fixtures(t) {
		first, err := Payload(fix.provider, fix.event, readFile(t, fix.input))
		if err != nil {
			t.Fatal(err)
		}

		for range 3 {
			again, againErr := Payload(fix.provider, fix.event, readFile(t, fix.input))
			if againErr != nil || !bytes.Equal(first, again) {
				t.Fatalf("%s: %s then %s, %v", fix.name, first, again, againErr)
			}
		}
	}
}

func TestInputIsNotChanged(t *testing.T) {
	t.Parallel()

	payload := []byte(`{"url":"u","sender":{"login":"l","id":1}}`)
	before := string(payload)

	_ = compactOf(t, eventPing, string(payload))

	if string(payload) != before {
		t.Errorf("payload became %s", payload)
	}
}

func TestStringsAreWrittenAsTheyAre(t *testing.T) {
	t.Parallel()

	payload := `{"body":"<b>R&D</b> \"quoted\" \\ \n é € \u0000 \u2028"}`

	if got := compactOf(t, eventPing, payload); got != payload {
		t.Errorf("got %s, want %s", got, payload)
	}

	// A key is written the same way.
	if got := compactOf(t, eventPing, `{"a<b":1}`); got != `{"a<b":1}` {
		t.Errorf("got %s", got)
	}
}

func TestDroppedKeys(t *testing.T) {
	t.Parallel()

	checkCases(t, eventPing, []testCase{
		{"bare link key", `{"url":"u","keep":1}`, keepOne},
		{"ends in _url", `{"avatar_url":"u","x_url":"u","_url":"u","keep":1}`, keepOne},
		{"html_url stays", `{"html_url":"h","comments_url":"c"}`, `{"html_url":"h"}`},
		{"compare stays", `{"compare":"c","compare_url":"c"}`, `{"compare":"c"}`},
		{
			"a key that merely contains the letters",
			`{"urls":1,"url_x":2,"curl":3,"https_url_x":4}`,
			`{"urls":1,"url_x":2,"curl":3,"https_url_x":4}`,
		},
		{
			"the named keys",
			`{"node_id":1,"gravatar_id":1,"_links":1,"performed_via_github_app":1,` +
				`"reactions":1,"author_association":1,"keep":2}`,
			`{"keep":2}`,
		},
		{"deep", `{"a":[{"b":{"url":1,"node_id":2,"c":3}}]}`, `{"a":[{"b":{"c":3}}]}`},
		{"not recursive into dropped", `{"_links":{"self":{"href":"h"}},"keep":1}`, keepOne},
	})
}

func TestReductions(t *testing.T) {
	t.Parallel()

	user := `{"login":"user-1","id":1,"type":"User","site_admin":false,"html_url":"h"}`
	want := `{"login":"user-1","type":"User"}`

	checkCases(t, eventPing, []testCase{
		{"sender", `{"sender":` + user + `}`, `{"sender":` + want + `}`},
		{"user", `{"user":` + user + `}`, `{"user":` + want + `}`},
		{"pusher with a login", `{"pusher":` + user + `}`, `{"pusher":` + want + `}`},
		{"owner", `{"owner":` + user + `}`, `{"owner":` + want + `}`},
		{"author", `{"author":` + user + `}`, `{"author":` + want + `}`},
		{"committer", `{"committer":` + user + `}`, `{"committer":` + want + `}`},
		{
			"requested reviewers",
			`{"requested_reviewers":[` + user + `,` + user + `]}`,
			`{"requested_reviewers":[` + want + `,` + want + `]}`,
		},
		{"deep", `{"a":{"user":` + user + `}}`, `{"a":{"user":` + want + `}}`},
		{"login without a type", `{"user":{"login":"l","id":1}}`, `{"user":{"login":"l"}}`},
		{
			"a user without a login keeps its type",
			`{"sender":{"type":"User","email":"example@example.com","id":1}}`,
			`{"sender":{"type":"User"}}`,
		},
		{
			"user, owner and reviewers without a login",
			`{"user":{"type":"Bot","id":1},"owner":{"id":2,"type":"Organization"},` +
				`"requested_reviewers":[{"type":"User","id":3},{"id":4}]}`,
			`{"user":{"type":"Bot"},"owner":{"type":"Organization"},"requested_reviewers":[{"type":"User"},{}]}`,
		},
		{
			"a name and an email are no user outside a push",
			`{"pusher":{"name":"n","email":"e"},"author":{"name":"n","username":"u"},"committer":{"email":"e"}}`,
			`{"pusher":{},"author":{},"committer":{}}`,
		},
		{
			"user key with other values",
			`{"user":null,"owner":"o","author":[1]}`,
			`{"user":null,"owner":"o","author":[1]}`,
		},
		{"other keys keep a login", `{"assignee":` + user + `}`, `{"assignee":` + user + `}`},
		{
			"repository",
			`{"repository":{"id":1,"full_name":"o/r","private":true,"owner":` + user +
				`,"default_branch":"main","name":"r"}}`,
			`{"repository":{"full_name":"o/r","private":true,"default_branch":"main"}}`,
		},
		{"repository that is not an object", `{"repository":"o/r"}`, `{"repository":"o/r"}`},
		{
			"organization installation enterprise",
			`{"organization":{"id":1,"login":"o"},"installation":{"id":2,"node_id":"n"},` +
				`"enterprise":{"id":3,"slug":"s"}}`,
			`{"organization":{"id":1},"installation":{"id":2},"enterprise":{"id":3}}`,
		},
		{"reduced then dropped", `{"sender":{"login":"l","url":"u"}}`, `{"sender":{"login":"l"}}`},
	})
}

func TestStringCut(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"just under", strings.Repeat("a", maxString-1), strings.Repeat("a", maxString-1)},
		{"exactly the limit", strings.Repeat("a", maxString), strings.Repeat("a", maxString)},
		{"one over", strings.Repeat("a", maxString+1), strings.Repeat("a", maxString) + "…[1 bytes cut]"},
		{"far over", strings.Repeat("a", 10000), strings.Repeat("a", maxString) + "…[7952 bytes cut]"},
		// "é" takes two bytes. After 2047 one-byte characters it covers bytes
		// 2047 and 2048, which puts the limit inside it and moves the cut back.
		{
			"inside a character",
			strings.Repeat("a", maxString-1) + "é" + "bb",
			strings.Repeat("a", maxString-1) + "…[4 bytes cut]",
		},
		// "€" takes three bytes: the limit falls on its second byte.
		{
			"inside a three-byte character",
			strings.Repeat("a", maxString-1) + "€" + "b",
			strings.Repeat("a", maxString-1) + "…[4 bytes cut]",
		},
		// The limit falls on its third byte.
		{
			"inside at the third byte",
			strings.Repeat("a", maxString-2) + "€" + "b",
			strings.Repeat("a", maxString-2) + "…[4 bytes cut]",
		},
		{
			"on a character boundary",
			strings.Repeat("a", maxString-2) + "é" + "b",
			strings.Repeat("a", maxString-2) + "é…[1 bytes cut]",
		},
	}
	for _, test := range tests {
		if got := cut(test.in); got != test.want {
			t.Errorf("%s: got %q, want %q", test.name, got, test.want)
		}
	}
}

func TestStringCutInPayload(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("x", maxString+5)
	payload := `{"body":"` + long + `","list":["` + long + `"],"nested":{"k":"` + long + `"},"short":"s"}`
	cutText := strings.Repeat("x", maxString) + "…[5 bytes cut]"
	want := `{"body":"` + cutText + `","list":["` + cutText + `"],"nested":{"k":"` + cutText + `"},"short":"s"}`

	if got := compactOf(t, eventPing, payload); got != want {
		t.Errorf("got %.200s", got)
	}

	// A key and a number are left whole.
	if got := compactOf(t, eventPing, `{"`+long+`":12345678}`); got != `{"`+long+`":12345678}` {
		t.Errorf("a key or a number changed: %.80s", got)
	}
}

func TestUnknownProviderGetsTheGenericRules(t *testing.T) {
	t.Parallel()

	payload := `{"url":"u","ref":"r","other":1,"sender":{"login":"l","id":2},` +
		`"pusher":{"name":"n","email":"e"},"head_commit":{"author":{"name":"n","username":"u"}}}`
	want := `{"ref":"r","other":1,"sender":{"login":"l"},"pusher":{},"head_commit":{"author":{}}}`

	for _, provider := range []string{"gitlab", "", "GitHub"} {
		got, err := Payload(provider, eventPush, json.RawMessage(payload))
		if err != nil || string(got) != want {
			t.Errorf("provider %q: got %s, %v", provider, got, err)
		}
	}
}

func TestEventWithoutAKeepListGetsTheGenericRules(t *testing.T) {
	t.Parallel()

	for _, event := range []string{eventPing, "star", "", "Push", "pushed"} {
		got := compactOf(t, event, `{"url":"u","ref":"r","other":1}`)
		if want := `{"ref":"r","other":1}`; got != want {
			t.Errorf("event %q: got %s, want %s", event, got, want)
		}
	}
}

func TestEveryListKeepsActionAndSender(t *testing.T) {
	t.Parallel()

	payload := `{"action":"a","sender":{"login":"l","type":"User"},"other":1}`
	want := `{"action":"a","sender":{"login":"l"}}`

	if len(keepLists) != 12 {
		t.Errorf("%d keep-lists, want 12", len(keepLists))
	}

	for event := range keepLists {
		if got := compactOf(t, event, payload); got != want {
			t.Errorf("%s: got %s, want %s", event, got, want)
		}
	}
}

func TestKeepListShapes(t *testing.T) {
	t.Parallel()

	checkEvents(t, []eventCase{
		{
			"create",
			"create",
			`{"ref":"r","ref_type":"branch","master_branch":"main","description":"d","repository":{"full_name":"o/r"}}`,
			`{"ref":"r","ref_type":"branch"}`,
		},
		{
			"delete",
			"delete",
			`{"ref":"r","ref_type":"tag","pusher_type":"user"}`,
			`{"ref":"r","ref_type":"tag"}`,
		},
		{
			"a missing path is skipped",
			eventIssues,
			`{"action":"opened","issue":{"number":1}}`,
			`{"action":"opened","issue":{"number":1}}`,
		},
		{
			"a path into a scalar keeps the scalar",
			eventIssues,
			`{"issue":"gone","label":null}`,
			`{"issue":"gone","label":null}`,
		},
		{
			"a path into an array of scalars keeps them",
			eventIssues,
			`{"issue":{"labels":["a","b"],"number":2}}`,
			`{"issue":{"labels":["a","b"],"number":2}}`,
		},
		{
			"a null kept whole",
			eventPush,
			`{"head_commit":null,"base_ref":null,"ref":"r"}`,
			`{"head_commit":null,"base_ref":null,"ref":"r"}`,
		},
		{
			"changes stay whole",
			eventPullRequest,
			`{"changes":{"title":{"from":"t"},"body":{"from":"b"}},"number":3,"unlisted":1}`,
			`{"changes":{"title":{"from":"t"},"body":{"from":"b"}},"number":3}`,
		},
	})
}

func TestKeepListPullRequest(t *testing.T) {
	t.Parallel()

	checkCases(t, eventPullRequest, []testCase{{
		"labels and reviewers keep one field",
		`{"pull_request":{"labels":[{"id":1,"name":"bug"},{"id":2,"name":"ui"}],` +
			`"requested_reviewers":[{"login":"user-2","id":9,"type":"User"}],` +
			`"head":{"ref":"h","sha":"s","label":"l"},"base":{"ref":"b","sha":"t","repo":{"id":1}},` +
			`"user":{"login":"user-1","id":1},"draft":false,"merged":true,"merged_at":null,` +
			`"merge_commit_sha":"m","state":"open","title":"t","number":4,"body":null,"id":99}}`,
		`{"pull_request":{"labels":[{"name":"bug"},{"name":"ui"}],"requested_reviewers":[{"login":"user-2"}],` +
			`"head":{"ref":"h","sha":"s"},"base":{"ref":"b","sha":"t"},"user":{"login":"user-1"},` +
			`"draft":false,"merged":true,"merged_at":null,"merge_commit_sha":"m","state":"open",` +
			`"title":"t","number":4,"body":null}}`,
	}})
}

func TestKeepListChecks(t *testing.T) {
	t.Parallel()

	checkEvents(t, []eventCase{
		{
			"workflow_run",
			"workflow_run",
			`{"workflow_run":{"id":1,"name":"CI","status":"completed","conclusion":"success",` +
				`"head_branch":"b","head_sha":"s","event":"push","run_attempt":2,"html_url":"h","path":"p",` +
				`"pull_requests":[{"number":5,"id":7}],"actor":{"login":"a"}}}`,
			`{"workflow_run":{"id":1,"name":"CI","status":"completed","conclusion":"success",` +
				`"head_branch":"b","head_sha":"s","event":"push","run_attempt":2,"html_url":"h","path":"p",` +
				`"pull_requests":[{"number":5}]}}`,
		},
		{
			"workflow_job",
			"workflow_job",
			`{"workflow_job":{"id":1,"name":"test","path":"p","steps":[1],"run_id":4,"html_url":"h","conclusion":null}}`,
			`{"workflow_job":{"id":1,"name":"test","html_url":"h","conclusion":null}}`,
		},
		{
			"check_run",
			"check_run",
			`{"check_run":{"id":1,"name":"n","status":"s","conclusion":"c","head_sha":"h","html_url":"u",` +
				`"app":{"slug":"a","id":1},"check_suite":{"id":2,"head_sha":"x"},` +
				`"pull_requests":[{"number":3,"url":"u","head":{}}],"output":{}}}`,
			`{"check_run":{"id":1,"name":"n","status":"s","conclusion":"c","head_sha":"h","html_url":"u",` +
				`"app":{"slug":"a"},"check_suite":{"id":2},"pull_requests":[{"number":3}]}}`,
		},
		{
			"check_suite",
			"check_suite",
			`{"check_suite":{"id":1,"status":"s","conclusion":"c","head_branch":"b","head_sha":"h",` +
				`"app":{"slug":"a","id":1},"pull_requests":[{"number":3,"id":1}],"before":"x"}}`,
			`{"check_suite":{"id":1,"status":"s","conclusion":"c","head_branch":"b","head_sha":"h",` +
				`"app":{"slug":"a"},"pull_requests":[{"number":3}]}}`,
		},
	})
}

func TestKeepListReviewsAndIssues(t *testing.T) {
	t.Parallel()

	checkEvents(t, []eventCase{
		{
			"pull_request_review",
			"pull_request_review",
			`{"review":{"id":1,"state":"approved","body":"b","commit_id":"c","html_url":"h",` +
				`"submitted_at":"t","user":{"login":"u","id":1},"x":1},` +
				`"pull_request":{"number":2,"title":"t","html_url":"h","head":{"sha":"s","ref":"r"},"body":"b"}}`,
			`{"review":{"id":1,"state":"approved","body":"b","commit_id":"c","html_url":"h",` +
				`"submitted_at":"t","user":{"login":"u"}},` +
				`"pull_request":{"number":2,"title":"t","html_url":"h","head":{"sha":"s"}}}`,
		},
		{
			"pull_request_review_comment",
			"pull_request_review_comment",
			`{"comment":{"id":1,"body":"b","path":"p","line":3,"commit_id":"c","in_reply_to_id":4,` +
				`"html_url":"h","user":{"login":"u","id":1},"diff_hunk":"d"},` +
				`"pull_request":{"number":2,"title":"t","html_url":"h"}}`,
			`{"comment":{"id":1,"body":"b","path":"p","line":3,"commit_id":"c","in_reply_to_id":4,` +
				`"html_url":"h","user":{"login":"u"}},"pull_request":{"number":2,"title":"t"}}`,
		},
		{
			"issues",
			eventIssues,
			`{"issue":{"number":1,"title":"t","state":"open","html_url":"h","body":"b",` +
				`"user":{"login":"u","id":1},"labels":[{"name":"l","id":1}],"id":9},` +
				`"label":{"name":"l","color":"c"}}`,
			`{"issue":{"number":1,"title":"t","state":"open","html_url":"h","body":"b",` +
				`"user":{"login":"u"},"labels":[{"name":"l"}]},"label":{"name":"l"}}`,
		},
	})
}

func TestPushCommits(t *testing.T) {
	t.Parallel()

	commit := func(i int, message string) string {
		return `{"id":"c` + strconv.Itoa(i) + `","message":` + strconv.Quote(message) +
			`,"author":{"name":"n","username":"u"},"added":["f"]}`
	}
	commits := func(count int) string {
		items := make([]string, count)
		for i := range items {
			items[i] = commit(i, "subject "+strconv.Itoa(i)+"\n\nbody")
		}

		return `{"commits":[` + strings.Join(items, ",") + `]}`
	}
	want := func(shown, total int) string {
		items := make([]string, shown)
		for i := range items {
			items[i] = `{"id":"c` + strconv.Itoa(i) + `","message":"subject ` + strconv.Itoa(i) + `"}`
		}

		return `{"commits":[` + strings.Join(items, ",") + `],"commits_total":` + strconv.Itoa(total) + `}`
	}

	checkCases(t, eventPush, []testCase{
		{"none", `{"commits":[]}`, `{"commits":[],"commits_total":0}`},
		{"one", commits(1), want(1, 1)},
		{"19", commits(19), want(19, 19)},
		{"20", commits(20), want(20, 20)},
		{"21", commits(21), want(20, 21)},
		{"100", commits(100), want(20, 100)},
		{
			"total goes right after commits",
			`{"a":1,"commits":[],"ref":"r"}`,
			`{"commits":[],"commits_total":0,"ref":"r"}`,
		},
		{"no commits member", refOnly, refOnly},
		{
			"a commits_total before commits is replaced",
			`{"commits_total":999,"commits":[{"id":"c","message":"m"}]}`,
			`{"commits":[{"id":"c","message":"m"}],"commits_total":1}`,
		},
		{
			"a commits_total after commits is replaced",
			`{"commits":[{"id":"c","message":"m"}],"commits_total":999}`,
			`{"commits":[{"id":"c","message":"m"}],"commits_total":1}`,
		},
		{"a commits_total without commits goes", `{"commits_total":999,"ref":"r"}`, `{"ref":"r"}`},
		{
			"another array is left alone",
			`{"parents":[{"id":"p","message":"a\nb"}],"commits":[]}`,
			`{"commits":[],"commits_total":0}`,
		},
		{"commits that is not an array", `{"commits":null}`, `{"commits":null}`},
		{"commits that is an object", `{"commits":{"id":"x"}}`, `{"commits":{"id":"x"}}`},
		{
			"message without a newline",
			`{"commits":[{"id":"i","message":"one line"}]}`,
			`{"commits":[{"id":"i","message":"one line"}],"commits_total":1}`,
		},
		{
			"message with a carriage return",
			`{"commits":[{"id":"i","message":"first\r\nsecond"}]}`,
			`{"commits":[{"id":"i","message":"first"}],"commits_total":1}`,
		},
		{
			"empty first line",
			`{"commits":[{"id":"i","message":"\nsecond"}]}`,
			`{"commits":[{"id":"i","message":""}],"commits_total":1}`,
		},
		{
			"message that is not a string",
			`{"commits":[{"id":"i","message":null},{"id":"j","message":5}]}`,
			`{"commits":[{"id":"i","message":null},{"id":"j","message":5}],"commits_total":2}`,
		},
		{
			"only the message is cut to a line",
			`{"commits":[{"id":"x\ny","message":"m\nn"}]}`,
			`{"commits":[{"id":"x\ny","message":"m"}],"commits_total":1}`,
		},
		{
			"commit that is not an object",
			`{"commits":["x",null]}`,
			`{"commits":["x",null],"commits_total":2}`,
		},
		{
			"a commit without an id or message",
			`{"commits":[{"added":["f"]}]}`,
			`{"commits":[{}],"commits_total":1}`,
		},
		{
			"head commit and pusher",
			`{"pusher":{"name":"p","email":"e"},"head_commit":{"id":"h","message":"m\nn","timestamp":"t",` +
				`"url":"u","author":{"name":"n","email":"e","username":"u"},"committer":{"username":"c"}},` +
				`"ref":"r","before":"b","after":"a","created":true,"deleted":false,"forced":false,` +
				`"base_ref":null,"compare":"c","repository":{"full_name":"o/r"}}`,
			`{"pusher":{"name":"p"},"head_commit":{"id":"h","message":"m\nn","timestamp":"t",` +
				`"author":{"username":"u"}},"ref":"r","before":"b","after":"a","created":true,` +
				`"deleted":false,"forced":false,"base_ref":null,"compare":"c"}`,
		},
	})
}

func TestIssueCommentPullRequestFlag(t *testing.T) {
	t.Parallel()

	checkCases(t, "issue_comment", []testCase{
		{
			"pull request",
			`{"issue":{"number":1,"pull_request":{"url":"u","html_url":"h","merged_at":null}}}`,
			`{"issue":{"number":1,"pull_request":true}}`,
		},
		{"plain issue", `{"issue":{"number":1}}`, `{"issue":{"number":1}}`},
		{
			"null pull request",
			`{"issue":{"number":1,"pull_request":null}}`,
			`{"issue":{"number":1,"pull_request":null}}`,
		},
		{"no issue", `{"comment":{"id":1}}`, `{"comment":{"id":1}}`},
		{"issue that is not an object", `{"issue":null}`, `{"issue":null}`},
		{
			"comment fields",
			`{"comment":{"id":1,"body":"b","html_url":"h","user":{"login":"u","id":2},"updated_at":"t"},` +
				`"issue":{"number":2,"title":"t","state":"open","html_url":"h","body":"x"}}`,
			`{"comment":{"id":1,"body":"b","html_url":"h","user":{"login":"u"}},` +
				`"issue":{"number":2,"title":"t","state":"open","html_url":"h"}}`,
		},
	})
}
