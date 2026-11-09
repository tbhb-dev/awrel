// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/spf13/cobra"

	"go.tbhb.dev/awrel/internal/persist"
	"go.tbhb.dev/awrel/internal/protocol"
)

const (
	sessionKey     = "key-one"
	savedEvent     = `{"provider":"github","event":"issue_comment","repository":"o/r"}`
	setLevelCall   = `{"jsonrpc":"2.0","id":9,"method":"logging/setLevel","params":{"level":"info"}}` + "\n"
	harnessSession = "CLAUDE_CODE_SESSION_ID"
	stateHome      = "XDG_STATE_HOME"
	listCall       = `{"jsonrpc":"2.0","id":5,"method":"tools/call",` +
		`"params":{"name":"list_subscriptions","arguments":{}}}` + "\n"
)

// sessionRelay is a relay that can delay its answer to each subscription until
// the test lets it answer. The answer lists the subscriptions of the
// connection. After the first answer the relay pushes an event once the test
// closes push.
type sessionRelay struct {
	release  chan struct{}
	push     chan struct{}
	received chan protocol.Subscription
	url      string
	dials    atomic.Int32
	hold     bool
}

func newSessionRelay(t *testing.T, hold bool) *sessionRelay {
	t.Helper()

	relay := &sessionRelay{
		release:  make(chan struct{}),
		push:     make(chan struct{}),
		received: make(chan protocol.Subscription, 16),
		url:      "",
		dials:    atomic.Int32{},
		hold:     hold,
	}

	server := httptest.NewServer(http.HandlerFunc(relay.serve))
	t.Cleanup(server.Close)

	relay.url = "ws" + strings.TrimPrefix(server.URL, "http") + "/connect"

	return relay
}

func (r *sessionRelay) serve(w http.ResponseWriter, req *http.Request) {
	conn, err := websocket.Accept(w, req, &websocket.AcceptOptions{Subprotocols: []string{protocol.Subprotocol}})
	if err != nil {
		return
	}

	defer conn.CloseNow() //nolint:errcheck // the test ends the connection

	r.dials.Add(1)
	r.converse(req.Context(), conn)
}

// relayMessage is the part of a client frame that the relay reads.
type relayMessage struct {
	Subscription *protocol.Subscription `json:"subscription"`
	ID           string                 `json:"id"`
}

// converse answers the client's frames until the connection ends.
func (r *sessionRelay) converse(ctx context.Context, conn *websocket.Conn) {
	var list []protocol.Subscription

	pushed := false

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return
		}

		var msg relayMessage

		if json.Unmarshal(data, &msg) != nil {
			return
		}

		if msg.Subscription != nil {
			list = append(list, *msg.Subscription)
			r.received <- *msg.Subscription

			if r.hold {
				<-r.release
			}
		}

		if !answer(ctx, conn, msg.ID, list) {
			return
		}

		if !pushed && msg.Subscription != nil {
			pushed = true

			go r.pushEvent(ctx, conn)
		}
	}
}

// answer sends the list as the reply to the request. It reports whether the
// reply went out.
func answer(ctx context.Context, conn *websocket.Conn, requestID string, list []protocol.Subscription) bool {
	listJSON, err := json.Marshal(list)
	if err != nil {
		return false
	}

	reply := `{"type":"subscriptions","id":"` + requestID + `","subscriptions":` + string(listJSON) + `}`

	return conn.Write(ctx, websocket.MessageText, []byte(reply)) == nil
}

// pushEvent sends an event once the test closes push.
func (r *sessionRelay) pushEvent(ctx context.Context, conn *websocket.Conn) {
	const event = `{"type":"event","provider":"github","event":"issue_comment","delivery":"d1",` +
		`"repository":"o/r","payload":{"action":"created"}}`

	<-r.push

	ignore(conn.Write(ctx, websocket.MessageText, []byte(event)))
}

// sessionFile is where the command keeps the file of a session key.
func sessionFile(state, key string) string {
	sum := sha256.Sum256([]byte(key))

	return filepath.Join(state, "awrel", "sessions", hex.EncodeToString(sum[:])+".json")
}

func seedFile(t *testing.T, state, key, endpoint, subscriptions string) string {
	t.Helper()

	path := sessionFile(state, key)

	err := os.MkdirAll(filepath.Dir(path), 0o700)
	if err != nil {
		t.Fatal(err)
	}

	content := `{"version":1,"endpoint":"` + endpoint + `","subscriptions":[` + subscriptions + `]}`

	err = os.WriteFile(path, []byte(content), 0o600)
	if err != nil {
		t.Fatal(err)
	}

	return path
}

func awaitSubscription(t *testing.T, relay *sessionRelay) protocol.Subscription {
	t.Helper()

	select {
	case sub := <-relay.received:
		return sub
	case <-time.After(waitLimit):
		t.Fatal("the relay received no subscribe")

		return protocol.Subscription{Provider: "", Event: "", Repository: ""}
	}
}

func TestSeededSessionDialsAtOnce(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vars map[string]string
		name string
		args []string
	}{
		{map[string]string{}, "flag", []string{"--session-id", sessionKey}},
		{map[string]string{persist.SessionEnv: sessionKey}, "environment", nil},
		{map[string]string{harnessSession: sessionKey}, "harness variable", nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			state := t.TempDir()
			relay := newSessionRelay(t, true)
			path := seedFile(t, state, sessionKey, relay.url, savedEvent)

			seed, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}

			test.vars[tokenEnv] = sentinelValue
			test.vars[endpointEnv] = relay.url
			test.vars[stateHome] = state

			running := startSession(t, env(test.vars), 0, test.args...)

			driveRestore(t, running, relay)

			err = running.finish(t)
			if err != nil {
				t.Errorf("exit: %v", err)
			}

			checkUntouched(t, path, seed)
		})
	}
}

// driveRestore walks a seeded session through its restore and checks what the
// agent sees.
func driveRestore(t *testing.T, running *mcpRun, relay *sessionRelay) {
	t.Helper()

	// The relay sees the replay without any tool call. It delays the answer
	// until the agent has finished its handshake and asked for logs.
	if got := awaitSubscription(t, relay); got.Repository != "o/r" || got.Event != "issue_comment" {
		t.Errorf("replayed %+v, want the saved subscription", got)
	}

	running.handshake(t)
	running.send(t, setLevelCall)
	running.await(t, false, `"id":9`)
	close(relay.release)

	running.await(t, false, `restored 1 subscription from the previous run`)
	close(relay.push)
	running.await(t, false, `"method":"notifications/claude/channel"`)

	running.send(t, listCall)
	running.await(t, false, `"id":5`)

	out := running.out.String()
	if !strings.Contains(out, `\"repository\":\"o/r\"`) && !strings.Contains(out, `"repository":"o/r"`) {
		t.Errorf("list_subscriptions lacks the restored entry:\n%s", out)
	}

	if relay.dials.Load() != 1 {
		t.Errorf("dials = %d, want 1", relay.dials.Load())
	}
}

// checkUntouched fails the test if the file differs from seed or has a lock
// beside it, since a process that only restores never writes or locks.
func checkUntouched(t *testing.T, path string, seed []byte) {
	t.Helper()

	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(after, seed) {
		t.Errorf("the restoring process changed the file: %s, %v", after, err)
	}

	if exists(path + ".lock") {
		t.Error("the restoring process took the lock")
	}
}

// exists reports whether path is there.
func exists(path string) bool {
	_, err := os.Stat(path)

	return err == nil
}

func TestSubscribeSavesTheSubscription(t *testing.T) {
	t.Parallel()

	state := t.TempDir()
	relay := newSessionRelay(t, false)
	vars := map[string]string{
		tokenEnv: sentinelValue, endpointEnv: relay.url, stateHome: state, persist.SessionEnv: sessionKey,
	}

	running := startSession(t, env(vars), 0)

	running.handshake(t)
	running.send(t, subscribeCall)
	running.await(t, false, `"id":2`)

	path := sessionFile(state, sessionKey)

	deadline := time.Now().Add(waitLimit)

	var data []byte

	for {
		var err error

		data, err = os.ReadFile(path)
		if err == nil {
			break
		}

		if time.Now().After(deadline) {
			t.Fatalf("no session file: %v\nstderr:\n%s", err, running.errOut.String())
		}

		time.Sleep(time.Millisecond)
	}

	want := `{"version":1,"endpoint":"` + relay.url + `","subscriptions":[` + savedEvent + `]}`
	if string(data) != want {
		t.Errorf("file = %s\nwant   %s", data, want)
	}

	err := running.finish(t)
	if err != nil {
		t.Errorf("exit: %v", err)
	}
}

func TestNoKeyKeepsDialOnFirstCall(t *testing.T) {
	t.Parallel()

	state := t.TempDir()
	relay := newSessionRelay(t, false)
	vars := map[string]string{tokenEnv: sentinelValue, endpointEnv: relay.url, stateHome: state}

	running := startSession(t, env(vars), 0)

	running.handshake(t)
	time.Sleep(20 * time.Millisecond)

	if relay.dials.Load() != 0 {
		t.Error("dialed before any tool call")
	}

	running.send(t, subscribeCall)
	running.await(t, false, `"id":2`)

	err := running.finish(t)
	if err != nil {
		t.Errorf("exit: %v", err)
	}

	if exists(filepath.Join(state, "awrel")) {
		t.Error("a session without a key touched the state directory")
	}
}

func TestOptOutKeepsDialOnFirstCallAndWritesNothing(t *testing.T) {
	t.Parallel()

	state := t.TempDir()
	relay := newSessionRelay(t, false)
	path := seedFile(t, state, sessionKey, relay.url, savedEvent)
	vars := map[string]string{
		tokenEnv: sentinelValue, endpointEnv: relay.url, stateHome: state,
		persist.SessionEnv: sessionKey, persist.PersistEnv: "0",
	}

	running := startSession(t, env(vars), 0)

	running.handshake(t)
	time.Sleep(20 * time.Millisecond)

	if relay.dials.Load() != 0 {
		t.Error("dialed before any tool call although AWREL_PERSIST=0")
	}

	running.send(t, subscribeCall)
	running.await(t, false, `"id":2`)

	err := running.finish(t)
	if err != nil {
		t.Errorf("exit: %v", err)
	}

	data, readErr := os.ReadFile(path)
	if readErr != nil || !strings.Contains(string(data), savedEvent) ||
		strings.Count(string(data), "repository") != 1 {
		t.Errorf("the opted-out session changed the file: %s, %v", data, readErr)
	}
}

func TestSessionFlagIsAcceptedBeforeTheSubcommand(t *testing.T) {
	t.Parallel()

	var out, errOut strings.Builder

	code := run(
		[]string{"--session-id", sessionKey, mcpCommand, "--format", badFormat},
		strings.NewReader(""), &out, &errOut, env(nil),
	)
	if code != 1 || !strings.HasPrefix(errOut.String(), "Error: invalid format ") {
		t.Errorf("code = %d, stderr %q; want the format error, not an unknown flag", code, errOut.String())
	}
}

func TestMCPCommandReportsAMissingSessionFlag(t *testing.T) {
	t.Parallel()

	// This root lacks the flag, so the lookup in the command fails.
	root := &cobra.Command{Use: "awrel", SilenceUsage: true, SilenceErrors: true}
	root.AddCommand(newMCPCmdWithTimeout(strings.NewReader(""), env(nil), 0))
	root.SetArgs([]string{mcpCommand})

	err := root.ExecuteContext(t.Context())
	if err == nil || !strings.Contains(err.Error(), sessionFlag) {
		t.Errorf("err = %v, want a complaint about the %s flag", err, sessionFlag)
	}
}
