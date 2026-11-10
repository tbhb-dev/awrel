// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/spf13/cobra"

	"go.tbhb.dev/awrel/internal/mcpserver"
	"go.tbhb.dev/awrel/internal/persist"
	"go.tbhb.dev/awrel/internal/protocol"
	"go.tbhb.dev/awrel/internal/relayclient"
)

const (
	sessionKey = "key-one"
	savedEvent = `{"provider":"github","event":"issue_comment","repository":"o/r"}`
	stateHome  = "XDG_STATE_HOME"
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

// exists reports whether path is there.
func exists(path string) bool {
	_, err := os.Stat(path)

	return err == nil
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

func TestRestoreSessionKeepsANilStoreNil(t *testing.T) {
	t.Parallel()

	store, initial := restoreSession(nil, func(*persist.Store) []protocol.Subscription {
		t.Fatal("a nil Store was loaded")

		return nil
	})
	if store != nil || initial != nil {
		t.Errorf("restoreSession(nil) = %v, %v, want nil, nil", store, initial)
	}
}

func TestRestoreSessionLoadsAnOpenedStore(t *testing.T) {
	t.Parallel()

	saved := &persist.Store{}
	want := []protocol.Subscription{{Provider: "github", Event: "issue_comment", Repository: "o/r"}}

	store, initial := restoreSession(saved, func(got *persist.Store) []protocol.Subscription {
		if got != saved {
			t.Fatalf("loaded Store %p, want %p", got, saved)
		}

		return want
	})
	if store != saved || !slices.EqualFunc(initial, want, protocol.Subscription.Equal) {
		t.Errorf("restoreSession = %v, %v, want the opened Store and its subscriptions", store, initial)
	}
}

func TestServeMCPAnnouncesInjectedRestore(t *testing.T) {
	t.Parallel()

	reader, writer := io.Pipe()
	running := &mcpRun{
		in: writer, out: newLockedBuffer(), errOut: newLockedBuffer(),
		done: make(chan error, 1), cancel: nil,
	}
	ctx, cancel := context.WithCancel(t.Context())
	running.cancel = cancel
	t.Cleanup(cancel)
	t.Cleanup(func() { ignore(writer.Close()) })

	cmd := &cobra.Command{}
	cmd.SetContext(ctx)
	cmd.SetOut(running.out)
	cmd.SetErr(running.errOut)

	restored := make(chan struct{})
	//nolint:dupl // the injected connector must match the production signature
	connect := func(
		_ context.Context, _ *slog.Logger, _ string, _ *persist.Store, _ func(string) string, _ time.Duration,
		_ func(context.Context, protocol.Event), onRestored func(context.Context, int),
		onConnection func(context.Context, relayclient.ConnectionChange),
	) (mcpserver.Relay, func(context.Context)) {
		return unconnectedRelay{err: errNoEndpoint}, func(ctx context.Context) {
			select {
			case <-restored:
				onRestored(ctx, 2)

				since := time.Date(2026, time.November, 10, 12, 0, 0, 0, time.UTC)
				onConnection(ctx, relayclient.ConnectionChange{Up: false, Since: since, At: since})
				onConnection(ctx, relayclient.ConnectionChange{Up: true, Since: since, At: since.Add(time.Second)})
			case <-ctx.Done():
			}
		}
	}
	go func() {
		running.done <- serveMCP(cmd, reader, "", false, "", env(nil), 0, connect)

		ignore(reader.Close())
	}()

	running.handshake(t)
	close(restored)
	running.await(t, false, "restored 2 subscriptions from the previous run")
	running.await(t, false, "relay connection lost at 2026-11-10T12:00:00Z")
	running.await(t, false, "relay connection restored at 2026-11-10T12:00:01Z")
	running.await(t, false, `"provider":"awrel"`)
	running.await(t, false, `"event":"notice"`)

	err := running.finish(t)
	if err != nil {
		t.Fatalf("exit: %v", err)
	}
}
