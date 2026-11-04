// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package relayclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"go.tbhb.dev/awrel/internal/protocol"
	"go.tbhb.dev/awrel/internal/transport"
)

const (
	// waitLimit bounds every wait in these tests, so that a broken loop fails
	// the test before the mutation tester's own deadline.
	waitLimit = time.Second
	// tick is the unit of the short intervals the tests configure.
	tick          = 5 * time.Millisecond
	testToken     = "tok"
	typePing      = "ping"
	typeSubscribe = "subscribe"
)

// errDial is what the fake's refused dials return.
var errDial = errors.New("dial failed")

// frame is a client message as the fake relay sees it.
type frame struct {
	Subscription *protocol.Subscription `json:"subscription"`
	Type         string                 `json:"type"`
	ID           string                 `json:"id"`
}

// seen is a frame together with the connection it arrived on, counted from 1.
type seen struct {
	frame frame
	conn  int
}

// fakeRelay is a relay endpoint that records the frames it receives. Each
// connection starts with an empty subscription list, like the real relay.
type fakeRelay struct {
	server *httptest.Server
	// intercept, when it returns true, replaces the default reply. Its reply
	// is sent unless empty.
	intercept func(conn int, req frame) (string, bool)
	// refuse lists subscriptions for which the default reply is an error.
	current  *websocket.Conn
	closed   chan websocket.StatusCode
	refuse   []protocol.Subscription
	frames   []seen
	mu       sync.Mutex
	conns    int
	failDial atomic.Int32
	dials    atomic.Int32
	noPong   atomic.Bool
}

func newFakeRelay(t *testing.T) *fakeRelay {
	t.Helper()

	relay := &fakeRelay{
		intercept: nil,
		refuse:    nil,
		frames:    nil,
		current:   nil,
		closed:    make(chan websocket.StatusCode, 16),
		mu:        sync.Mutex{},
		conns:     0,
		failDial:  atomic.Int32{},
		dials:     atomic.Int32{},
		noPong:    atomic.Bool{},
		server:    nil,
	}
	relay.server = httptest.NewTLSServer(http.HandlerFunc(relay.serve))
	t.Cleanup(relay.server.Close)

	return relay
}

// dial is Options.Dial. A positive failure counter makes dials fail.
func (f *fakeRelay) dial(ctx context.Context) (*transport.Conn, error) {
	f.dials.Add(1)

	if f.failDial.Add(-1) >= 0 {
		return nil, errDial
	}

	f.failDial.Store(0)

	conn, err := (&transport.Dialer{HTTPClient: f.server.Client(), URL: f.server.URL, Token: testToken}).Dial(ctx)
	if err != nil {
		return nil, fmt.Errorf("dial the fake relay: %w", err)
	}

	return conn, nil
}

func (f *fakeRelay) serve(w http.ResponseWriter, req *http.Request) {
	conn, err := websocket.Accept(w, req, &websocket.AcceptOptions{Subprotocols: []string{protocol.Subprotocol}})
	if err != nil {
		return
	}

	defer func() { ignore(conn.CloseNow()) }()

	f.mu.Lock()
	f.conns++
	number := f.conns
	f.current = conn
	f.mu.Unlock()

	var list []protocol.Subscription

	for {
		_, data, readErr := conn.Read(req.Context())
		if readErr != nil {
			f.closed <- websocket.CloseStatus(readErr)

			return
		}

		var msg frame

		ignore(json.Unmarshal(data, &msg))

		f.mu.Lock()
		f.frames = append(f.frames, seen{frame: msg, conn: number})
		f.mu.Unlock()

		out := f.answer(number, msg, &list)
		if out == "" {
			continue
		}

		err = conn.Write(req.Context(), websocket.MessageText, []byte(out))
		if err != nil {
			return
		}
	}
}

// ignore drops an error that the fake has no way to report.
func ignore(error) {}

// answer is the reply to a message, or "" for none.
func (f *fakeRelay) answer(number int, msg frame, list *[]protocol.Subscription) string {
	f.mu.Lock()
	intercept := f.intercept
	refuse := slices.Clone(f.refuse)
	f.mu.Unlock()

	if intercept != nil {
		if out, handled := intercept(number, msg); handled {
			return out
		}
	}

	switch msg.Type {
	case typePing:
		if f.noPong.Load() {
			return ""
		}

		return `{"type":"pong"}`
	case typeSubscribe:
		if slices.Contains(refuse, *msg.Subscription) {
			return `{"type":"error","id":"` + msg.ID + `","code":"limit","message":"full"}`
		}

		if !slices.Contains(*list, *msg.Subscription) {
			*list = append(*list, *msg.Subscription)
		}
	case "unsubscribe":
		if !slices.Contains(*list, *msg.Subscription) {
			return `{"type":"error","id":"` + msg.ID + `","code":"not_subscribed","message":"absent"}`
		}

		*list = slices.DeleteFunc(*list, func(other protocol.Subscription) bool { return other == *msg.Subscription })
	}

	body, err := json.Marshal(*list)
	if err != nil || len(*list) == 0 {
		body = []byte("[]")
	}

	return `{"type":"subscriptions","id":"` + msg.ID + `","subscriptions":` + string(body) + `}`
}

// push sends a raw frame on the current connection.
func (f *fakeRelay) push(t *testing.T, text string) {
	t.Helper()

	f.mu.Lock()
	conn := f.current
	f.mu.Unlock()

	err := conn.Write(t.Context(), websocket.MessageText, []byte(text))
	if err != nil {
		t.Fatal(err)
	}
}

// drop ends the current connection without a closing handshake.
func (f *fakeRelay) drop() {
	f.mu.Lock()
	conn := f.current
	f.mu.Unlock()

	ignore(conn.CloseNow())
}

func (f *fakeRelay) setIntercept(fn func(conn int, req frame) (string, bool)) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.intercept = fn
}

func (f *fakeRelay) setRefuse(subs ...protocol.Subscription) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.refuse = subs
}

// received returns the frames seen so far, in arrival order.
func (f *fakeRelay) received() []seen {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.frames)
}

// onConn returns the frames of one connection, in order.
func (f *fakeRelay) onConn(number int) []frame {
	var out []frame

	for _, entry := range f.received() {
		if entry.conn == number {
			out = append(out, entry.frame)
		}
	}

	return out
}

// eventually retries check until it returns true, and fails the test after
// one second.
func eventually(t *testing.T, what string, check func() bool) {
	t.Helper()

	deadline := time.Now().Add(waitLimit)
	for !check() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}

		time.Sleep(time.Millisecond)
	}
}

func (f *fakeRelay) connCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.conns
}

// dialFirst returns a Dial that connects to the fake n times and then waits
// for its context to end.
func (f *fakeRelay) dialFirst(limit int32) func(context.Context) (*transport.Conn, error) {
	var calls atomic.Int32

	return func(ctx context.Context) (*transport.Conn, error) {
		if calls.Add(1) > limit {
			<-ctx.Done()

			return nil, ctx.Err()
		}

		return f.dial(ctx)
	}
}

// start runs a Client on the fake and returns it with a stop function that
// ends Run and checks that it returned nil.
func (f *fakeRelay) start(t *testing.T, opts Options) (*Client, func()) {
	t.Helper()

	if opts.Dial == nil {
		opts.Dial = f.dial
	}

	return f.run(t, New(opts))
}

// run is start for a Client that the test built.
func (f *fakeRelay) run(t *testing.T, client *Client) (*Client, func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	finished := make(chan error, 1)

	go func() { finished <- client.Run(ctx) }()

	stopped := false
	stop := func() {
		if stopped {
			return
		}

		stopped = true

		cancel()

		select {
		case err := <-finished:
			if err != nil {
				t.Errorf("Run = %v, want nil", err)
			}
		case <-time.After(waitLimit):
			t.Error("Run did not return")
		}
	}
	t.Cleanup(stop)

	return client, stop
}
