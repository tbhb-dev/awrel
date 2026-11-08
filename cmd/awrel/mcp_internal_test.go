// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"testing/iotest"
	"time"

	"github.com/coder/websocket"
	"github.com/spf13/cobra"

	"go.tbhb.dev/awrel/internal/protocol"
)

const (
	waitLimit     = time.Second
	sentinelValue = "tok-tok-tok-tok"
	unreachable   = "ws://127.0.0.1:1/connect"
	envURL        = "ws://env.example/connect"
	envToken      = "tok-env"
	mcpCommand    = "mcp"
	badFormat     = "brief"
	mcpHandshake  = `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25",` +
		`"capabilities":{},"clientInfo":{"name":"claude-code","version":"1"}}}` + "\n" +
		`{"jsonrpc":"2.0","method":"notifications/initialized"}` + "\n"
	subscribeCall = `{"jsonrpc":"2.0","id":2,"method":"tools/call",` +
		`"params":{"name":"subscribe","arguments":{"provider":"github","event":"issue_comment","repository":"o/r"}}}` + "\n"
	toolCalls = subscribeCall +
		`{"jsonrpc":"2.0","id":3,"method":"tools/call",` +
		`"params":{"name":"unsubscribe","arguments":{"provider":"github"}}}` + "\n" +
		`{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"list_subscriptions","arguments":{}}}` + "\n"
)

// errStdin is the failure of a stdin that can't be read.
var errStdin = errors.New("stdin broke")

// ignore drops the results of a call that has no failure to report here.
func ignore(...any) {}

// env makes a getenv that reads vars.
func env(vars map[string]string) func(string) string {
	return func(key string) string { return vars[key] }
}

// lockedBuffer is an output sink that the command writes while a test reads.
type lockedBuffer struct {
	buf bytes.Buffer
	mu  sync.Mutex
}

func newLockedBuffer() *lockedBuffer {
	return &lockedBuffer{buf: bytes.Buffer{}, mu: sync.Mutex{}}
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p) //nolint:wrapcheck // a bytes.Buffer never fails
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

// mcpRun is the server command running in the background.
type mcpRun struct {
	in     *io.PipeWriter
	out    *lockedBuffer
	errOut *lockedBuffer
	done   chan error
	cancel context.CancelFunc
}

// startMCP runs the server command on stdin with the given getenv and relay
// request budget.
func startMCP(
	t *testing.T, stdin io.Reader, getenv func(string) string, timeout time.Duration, args ...string,
) *mcpRun {
	t.Helper()

	root := &cobra.Command{Use: "awrel", SilenceUsage: true}
	root.AddCommand(newMCPCmdWithTimeout(stdin, getenv, timeout))

	ctx, cancel := context.WithCancel(t.Context())
	running := &mcpRun{
		in:     nil,
		out:    newLockedBuffer(),
		errOut: newLockedBuffer(),
		done:   make(chan error, 1),
		cancel: cancel,
	}

	root.SetArgs(append([]string{mcpCommand}, args...))
	root.SetOut(running.out)
	root.SetErr(running.errOut)

	go func() {
		err := root.ExecuteContext(ctx)

		// A command that has stopped reading would block the test's writes to
		// a pipe forever. Closing the reader makes them fail.
		closer, ok := stdin.(io.Closer)
		if ok {
			ignore(closer.Close())
		}

		running.done <- err
	}()

	return running
}

// startSession runs the server command on a stdin that finish closes.
func startSession(t *testing.T, getenv func(string) string, timeout time.Duration, args ...string) *mcpRun {
	t.Helper()

	reader, writer := io.Pipe()
	running := startMCP(t, reader, getenv, timeout, args...)
	running.in = writer

	t.Cleanup(func() { _ = writer.Close() })

	return running
}

// send writes text to the command's stdin.
func (r *mcpRun) send(t *testing.T, text string) {
	t.Helper()

	_, err := io.WriteString(r.in, text)
	if err != nil {
		t.Fatal(err)
	}
}

// await waits until stdout (or stderr, when onStderr is set) holds text, and
// fails after the wait limit.
func (r *mcpRun) await(t *testing.T, onStderr bool, text string) {
	t.Helper()

	buf := r.out
	if onStderr {
		buf = r.errOut
	}

	deadline := time.Now().Add(waitLimit)
	for !strings.Contains(buf.String(), text) {
		if time.Now().After(deadline) {
			t.Fatalf("no %q\nstdout:\n%s\nstderr:\n%s", text, r.out.String(), r.errOut.String())
		}

		time.Sleep(time.Millisecond)
	}
}

// handshake plays the Claude Code side of the MCP handshake. The server picks
// its event delivery when it has handled the initialized notification, which
// happens after this returns, so it waits for the server's log of that.
func (r *mcpRun) handshake(t *testing.T) {
	t.Helper()

	r.send(t, mcpHandshake)
	r.await(t, true, "client initialized")
}

// askAll makes the three tool calls and returns stdout once every reply is in.
func (r *mcpRun) askAll(t *testing.T) string {
	t.Helper()

	r.handshake(t)
	r.send(t, toolCalls)
	r.await(t, false, `"id":2`)
	r.await(t, false, `"id":3`)
	r.await(t, false, `"id":4`)

	return r.out.String()
}

// finish ends the input and returns the command's error.
func (r *mcpRun) finish(t *testing.T) error {
	t.Helper()

	if r.in != nil {
		_ = r.in.Close()
	}

	select {
	case err := <-r.done:
		return err
	case <-time.After(waitLimit):
		t.Fatal("the command did not stop")

		return nil
	}
}

//nolint:dupl // two table tests of the same shape
func TestResolveEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		flag    string
		env     map[string]string
		want    string
		wantErr string
	}{
		{"none", "", nil, "", "no relay endpoint set: pass --endpoint or set AWREL_ENDPOINT"},
		{
			"env",
			"",
			map[string]string{endpointEnv: envURL},
			envURL,
			"",
		},
		{
			"flag over env", "wss://flag.example/connect",
			map[string]string{endpointEnv: envURL},
			"wss://flag.example/connect", "",
		},
		{"unparsable", "://nope", nil, "", `invalid endpoint "://nope": `},
		{
			"wrong scheme",
			"https://x.example/connect",
			nil,
			"",
			`invalid endpoint "https://x.example/connect": the scheme must be ws or wss`,
		},
		{"no host", "ws:///connect", nil, "", `invalid endpoint "ws:///connect": the host is missing`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := resolveEndpoint(test.flag, env(test.env))
			if test.wantErr == "" {
				if err != nil || got != test.want {
					t.Errorf("resolveEndpoint = %q, %v; want %q", got, err, test.want)
				}

				return
			}

			if err == nil || !strings.HasPrefix(err.Error(), test.wantErr) || got != "" {
				t.Errorf("resolveEndpoint = %q, %v; want error %q", got, err, test.wantErr)
			}
		})
	}
}

//nolint:dupl // two table tests of the same shape
func TestResolveToken(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	write := func(name, contents string) string {
		path := filepath.Join(dir, name)

		err := os.WriteFile(path, []byte(contents), 0o600)
		if err != nil {
			t.Fatal(err)
		}

		return path
	}
	filled := write("filled", "  tok-from-file\n")
	empty := write("empty", " \n\t\n")
	missing := filepath.Join(dir, "missing")

	tests := []struct {
		name    string
		env     map[string]string
		want    string
		wantErr string
	}{
		{"env", map[string]string{tokenEnv: envToken}, envToken, ""},
		{"env wins over file", map[string]string{tokenEnv: envToken, tokenFileEnv: filled}, envToken, ""},
		{"file is trimmed", map[string]string{tokenFileEnv: filled}, "tok-from-file", ""},
		{"empty env falls back to file", map[string]string{tokenEnv: "", tokenFileEnv: filled}, "tok-from-file", ""},
		{"neither", nil, "", "neither AWREL_CONNECT_TOKEN nor AWREL_CONNECT_TOKEN_FILE is set"},
		{
			"missing file",
			map[string]string{tokenFileEnv: missing},
			"",
			"AWREL_CONNECT_TOKEN_FILE " + missing + ": " + osReason(t, missing),
		},
		{"empty file", map[string]string{tokenFileEnv: empty}, "", "AWREL_CONNECT_TOKEN_FILE " + empty + " is empty"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := resolveToken(env(test.env))
			if test.wantErr == "" {
				if err != nil || got != test.want {
					t.Errorf("resolveToken = %q, %v; want %q", got, err, test.want)
				}

				return
			}

			if err == nil || err.Error() != test.wantErr || got != "" {
				t.Errorf("resolveToken = %q, %v; want error %q", got, err, test.wantErr)
			}
		})
	}
}

// quoted returns text as it appears inside a JSON string.
func quoted(t *testing.T, text string) string {
	t.Helper()

	encoded, err := json.Marshal(text)
	if err != nil {
		t.Fatal(err)
	}

	return strings.Trim(string(encoded), `"`)
}

// osReason is the operating system's own wording for why path can't be read.
// Each system words it differently.
func osReason(t *testing.T, path string) string {
	t.Helper()

	_, err := os.ReadFile(path)

	pathErr, ok := errors.AsType[*fs.PathError](err)
	if !ok {
		t.Fatalf("reading %s: %v; want a path error", path, err)
	}

	return pathErr.Err.Error()
}

func TestResolveErrorsKeepTheirCause(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	empty := filepath.Join(dir, "empty")

	err := os.WriteFile(empty, nil, 0o600)
	if err != nil {
		t.Fatal(err)
	}

	_, parseErr := resolveEndpoint("://nope", env(nil))
	_, schemeErr := resolveEndpoint("https://x.example", env(nil))
	_, hostErr := resolveEndpoint("ws:///connect", env(nil))
	_, missingErr := resolveToken(env(map[string]string{tokenFileEnv: filepath.Join(dir, "missing")}))
	_, emptyErr := resolveToken(env(map[string]string{tokenFileEnv: empty}))

	checks := []struct {
		err  error
		want error
		name string
	}{
		{parseErr, errBadEndpoint, "unparsable endpoint"},
		{schemeErr, errEndpointScheme, "wrong scheme"},
		{schemeErr, errBadEndpoint, "wrong scheme, outer error"},
		{hostErr, errEndpointNoHost, "no host"},
		{hostErr, errBadEndpoint, "no host, outer error"},
		{missingErr, fs.ErrNotExist, "missing file"},
		{emptyErr, errEmptyTokenFile, "empty file"},
	}
	for _, check := range checks {
		if !errors.Is(check.err, check.want) {
			t.Errorf("%s: %v does not wrap %v", check.name, check.err, check.want)
		}
	}

	_, isURLErr := errors.AsType[*url.Error](parseErr)
	if !isURLErr {
		t.Errorf("unparsable endpoint: %v does not wrap the parse error", parseErr)
	}
}

func TestInvalidEndpointExitsWithError(t *testing.T) {
	t.Parallel()

	var out, errOut bytes.Buffer

	code := run([]string{mcpCommand, "--endpoint", "http://x.example"}, strings.NewReader(""), &out, &errOut, env(nil))
	if code != 1 || !strings.HasPrefix(errOut.String(), "Error: invalid endpoint ") || out.Len() != 0 {
		t.Errorf("code = %d, stdout %q, stderr %q", code, out.String(), errOut.String())
	}
}

func TestMissingSettingFailsEveryTool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		vars   map[string]string
		want   string
		stderr string
	}{
		// Each case sets the other setting, so only the missing one can fail the tools.
		{
			name:   "endpoint",
			vars:   map[string]string{tokenEnv: sentinelValue},
			want:   "not connected to the relay: no relay endpoint set",
			stderr: "no relay endpoint",
		},
		{
			name:   "token",
			vars:   map[string]string{endpointEnv: unreachable},
			want:   "not connected to the relay: neither AWREL_CONNECT_TOKEN nor AWREL_CONNECT_TOKEN_FILE is set",
			stderr: "AWREL_CONNECT_TOKEN",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			running := startSession(t, env(test.vars), 0)
			out := running.askAll(t)

			if strings.Count(out, test.want) != 3 || !strings.Contains(running.errOut.String(), test.stderr) {
				t.Errorf("stdout:\n%s\nstderr:\n%s", out, running.errOut.String())
			}

			err := running.finish(t)
			if err != nil {
				t.Errorf("exit: %v", err)
			}
		})
	}
}

func TestUnreadableTokenFileFailsEveryTool(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "missing")
	running := startSession(t, env(map[string]string{tokenFileEnv: path, endpointEnv: unreachable}), 0)
	out := running.askAll(t)

	// Stdout is JSON and stderr is a quoted log value. Both double the
	// backslashes of a Windows path.
	want := quoted(t, "not connected to the relay: AWREL_CONNECT_TOKEN_FILE "+path+": "+osReason(t, path))
	logged := strings.Trim(strconv.Quote(path), `"`)

	if strings.Count(out, want) != 3 || !strings.Contains(running.errOut.String(), logged) {
		t.Errorf("stdout:\n%s\nstderr:\n%s", out, running.errOut.String())
	}

	err := running.finish(t)
	if err != nil {
		t.Errorf("exit: %v", err)
	}
}

func TestTokenIsNeverLogged(t *testing.T) {
	t.Parallel()

	file := filepath.Join(t.TempDir(), "token")

	err := os.WriteFile(file, []byte(sentinelValue+"\n"), 0o600)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		vars map[string]string
		name string
	}{
		{map[string]string{tokenEnv: sentinelValue, endpointEnv: unreachable}, "from the environment"},
		{map[string]string{tokenFileEnv: file, endpointEnv: unreachable}, "from a file"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			running := startSession(t, env(test.vars), 100*time.Millisecond)
			out := running.askAll(t)

			finishErr := running.finish(t)
			if finishErr != nil {
				t.Errorf("exit: %v", finishErr)
			}

			if !strings.Contains(out, "not connected to the relay: ") {
				t.Errorf("stdout:\n%s", out)
			}

			if strings.Contains(out+running.errOut.String(), sentinelValue) {
				t.Errorf("the token leaked:\nstdout:\n%s\nstderr:\n%s", out, running.errOut.String())
			}
		})
	}
}

func TestEndpointFlagBeatsEnvironment(t *testing.T) {
	t.Parallel()

	// The environment's endpoint is invalid, so only the flag can let the
	// command start.
	vars := map[string]string{endpointEnv: "http://bad.example", tokenEnv: sentinelValue}
	running := startSession(t, env(vars), 100*time.Millisecond, "--endpoint", unreachable)
	running.askAll(t)

	err := running.finish(t)
	if err != nil {
		t.Errorf("exit: %v", err)
	}
}

func TestReadFailureExitsWithError(t *testing.T) {
	t.Parallel()

	running := startMCP(t, iotest.ErrReader(errStdin), env(nil), 0)

	err := running.finish(t)
	if !errors.Is(err, errStdin) || !strings.Contains(err.Error(), "serve MCP: ") {
		t.Errorf("exit: %v", err)
	}
}

// TestCancelStopsTheServerCleanly cancels the command's context while the
// input is still open. Serve can then stop only by returning context.Canceled,
// which isn't an error. The signal test covers this too, but it skips Windows.
func TestCancelStopsTheServerCleanly(t *testing.T) {
	t.Parallel()

	running := startSession(t, env(nil), 0)
	running.handshake(t)
	running.cancel()

	err := running.finish(t)
	if err != nil {
		t.Errorf("exit: %v", err)
	}
}

// fakeRelay accepts one connection, answers its first message as a successful
// subscribe, then pushes an event.
func fakeRelay(t *testing.T) string {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conn, err := websocket.Accept(w, req, &websocket.AcceptOptions{Subprotocols: []string{protocol.Subprotocol}})
		if err != nil {
			return
		}

		defer conn.CloseNow() //nolint:errcheck // the test ends the connection

		_, data, err := conn.Read(req.Context())
		if err != nil {
			return
		}

		var msg struct {
			ID string `json:"id"`
		}

		if json.Unmarshal(data, &msg) != nil {
			return
		}

		reply := `{"type":"subscriptions","id":"` + msg.ID +
			`","subscriptions":[{"provider":"github","event":"issue_comment","repository":"o/r"}]}`
		event := `{"type":"event","provider":"github","event":"issue_comment","delivery":"d1",` +
			`"repository":"o/r","payload":{"action":"created"}}`

		for _, text := range []string{reply, event} {
			if conn.Write(req.Context(), websocket.MessageText, []byte(text)) != nil {
				return
			}
		}

		// Hold the connection until the client goes away.
		ignore(conn.Read(req.Context()))
	}))
	t.Cleanup(server.Close)

	return "ws" + strings.TrimPrefix(server.URL, "http") + "/connect"
}

func TestEventsReachTheAgent(t *testing.T) {
	t.Parallel()

	vars := map[string]string{tokenEnv: sentinelValue, endpointEnv: fakeRelay(t)}
	running := startSession(t, env(vars), 0)
	running.handshake(t)
	running.send(t, subscribeCall)
	running.await(t, false, `"id":2`)
	running.await(t, false, `"method":"notifications/claude/channel"`)

	out := running.out.String()
	for _, want := range []string{`"event":"issue_comment"`, `"repository":"o/r"`, `"delivery":"d1"`} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout lacks %s:\n%s", want, out)
		}
	}

	err := running.finish(t)
	if err != nil {
		t.Errorf("exit: %v", err)
	}
}

// eventMarkedCompact runs a session against the fake relay and reports whether
// the event it delivers has the attribute compact.
func eventMarkedCompact(t *testing.T, vars map[string]string, args ...string) bool {
	t.Helper()

	vars[tokenEnv] = sentinelValue
	vars[endpointEnv] = fakeRelay(t)

	running := startSession(t, env(vars), 0, args...)
	running.handshake(t)
	running.send(t, subscribeCall)
	running.await(t, false, `"method":"notifications/claude/channel"`)

	marked := strings.Contains(running.out.String(), `"compact":"true"`)

	err := running.finish(t)
	if err != nil {
		t.Errorf("exit: %v", err)
	}

	return marked
}

func TestEventsAreCompactByDefault(t *testing.T) {
	t.Parallel()

	if !eventMarkedCompact(t, map[string]string{}) {
		t.Error("the delivered event is not marked compact")
	}
}

func TestFormatFullDeliversWholeEvents(t *testing.T) {
	t.Parallel()

	if eventMarkedCompact(t, map[string]string{}, "--format", "full") {
		t.Error("the event is marked compact with --format full")
	}
}

func TestFormatEnvironmentSetsTheFormat(t *testing.T) {
	t.Parallel()

	if eventMarkedCompact(t, map[string]string{formatEnv: formatFull}) {
		t.Error("the event is marked compact with AWREL_FORMAT=full")
	}
}

func TestFormatFlagBeatsEnvironment(t *testing.T) {
	t.Parallel()

	if !eventMarkedCompact(t, map[string]string{formatEnv: formatFull}, "--format", "compact") {
		t.Error("the event is not marked compact with --format compact")
	}
}

//nolint:dupl // two table tests of the same shape
func TestResolveFormat(t *testing.T) {
	t.Parallel()

	const emptyFormatErr = `invalid format "": the value must be compact or full`

	tests := []struct {
		env         map[string]string
		name        string
		flag        string
		wantErr     string
		flagGiven   bool
		wantCompact bool
	}{
		{nil, "default is compact", "", "", false, true},
		{map[string]string{formatEnv: formatFull}, "env full over default", "", "", false, false},
		{map[string]string{formatEnv: formatCompact}, "env compact", "", "", false, true},
		{map[string]string{formatEnv: formatFull}, "flag compact over env", formatCompact, "", true, true},
		{map[string]string{formatEnv: formatCompact}, "flag full over env", formatFull, "", true, false},
		{nil, "bad flag", badFormat, `invalid format "brief": the value must be compact or full`, true, false},
		{
			map[string]string{formatEnv: "Full"},
			"bad env",
			"",
			`invalid format "Full": the value must be compact or full`,
			false,
			false,
		},
		{map[string]string{formatEnv: "bad"}, "a good flag ignores a bad env", formatFull, "", true, false},
		{nil, "empty flag without env", "", emptyFormatErr, true, false},
		{map[string]string{formatEnv: formatFull}, "empty flag beats a good env", "", emptyFormatErr, true, false},
		{map[string]string{formatEnv: badFormat}, "empty flag beats a bad env", "", emptyFormatErr, true, false},
		{map[string]string{formatEnv: ""}, "empty env is the default", "", "", false, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := resolveFormat(test.flag, test.flagGiven, env(test.env))
			if test.wantErr == "" {
				if err != nil || got != test.wantCompact {
					t.Errorf("resolveFormat = %v, %v; want %v", got, err, test.wantCompact)
				}

				return
			}

			if err == nil || err.Error() != test.wantErr || got || !errors.Is(err, errBadFormat) {
				t.Errorf("resolveFormat = %v, %v; want error %q", got, err, test.wantErr)
			}
		})
	}
}

func TestInvalidFormatExitsWithError(t *testing.T) {
	t.Parallel()

	var out, errOut bytes.Buffer

	code := run([]string{mcpCommand, "--format", badFormat}, strings.NewReader(""), &out, &errOut, env(nil))
	if code != 1 || out.Len() != 0 {
		t.Errorf("with --format: code = %d, stdout %q", code, out.String())
	}

	if !strings.HasPrefix(errOut.String(), "Error: invalid format ") {
		t.Errorf("stderr = %q", errOut.String())
	}

	code = run([]string{mcpCommand}, strings.NewReader(""), &out, &errOut, env(map[string]string{formatEnv: badFormat}))
	if code != 1 || out.Len() != 0 {
		t.Errorf("with AWREL_FORMAT: code = %d, stdout %q", code, out.String())
	}
}

func TestEmptyFormatFlagExitsWithError(t *testing.T) {
	t.Parallel()

	for _, environ := range []map[string]string{nil, {formatEnv: badFormat}, {formatEnv: formatFull}} {
		var out, errOut bytes.Buffer

		code := run([]string{mcpCommand, "--format="}, strings.NewReader(""), &out, &errOut, env(environ))
		if code != 1 || out.Len() != 0 {
			t.Errorf("env %v: code = %d, stdout %q", environ, code, out.String())
		}

		if want := `Error: invalid format "": the value must be compact or full`; !strings.HasPrefix(
			errOut.String(),
			want,
		) {
			t.Errorf("env %v: stderr = %q, want prefix %q", environ, errOut.String(), want)
		}
	}
}

func TestSigtermStopsTheServer(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("Windows has no SIGTERM")
	}

	// The test binary itself, which runs main in the environment below.
	cmd := exec.CommandContext(t.Context(), os.Args[0], mcpCommand) //nolint:gosec // see above
	// GORACE keeps a race-enabled binary from sleeping a second before it exits.
	cmd.Env = append(os.Environ(), mainEnv+"=1", tokenEnv+"=", tokenFileEnv+"=", "GORACE=atexit_sleep_ms=0")

	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}

	defer stdin.Close()

	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}

	err = cmd.Start()
	if err != nil {
		t.Fatal(err)
	}

	// The startup error comes after the signal handler is in place.
	started := make(chan struct{})
	logged := make(chan string, 1)

	go func() {
		buf := make([]byte, 1)
		ignore(io.ReadFull(stderr, buf))

		close(started)

		rest, readErr := io.ReadAll(stderr)
		ignore(readErr)

		logged <- string(buf) + string(rest)
	}()

	select {
	case <-started:
	case <-time.After(waitLimit):
		t.Fatal("the server did not start")
	}

	err = cmd.Process.Signal(syscall.SIGTERM)
	if err != nil {
		t.Fatal(err)
	}

	exited := make(chan error, 1)

	go func() { exited <- cmd.Wait() }()

	select {
	case exitErr := <-exited:
		if exitErr != nil {
			t.Errorf("exit: %v", exitErr)
		}
	case <-time.After(waitLimit):
		ignore(cmd.Process.Kill())
		ignore(<-exited)
		t.Fatalf("the server ignored SIGTERM; stderr:\n%s", <-logged)
	}
}
