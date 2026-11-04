// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package relayclient

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"go.tbhb.dev/awrel/internal/protocol"
	"go.tbhb.dev/awrel/internal/transport"
)

const providerGitHub = "github"

func subA() protocol.Subscription {
	return protocol.Subscription{Provider: providerGitHub, Event: "push", Repository: "o/a"}
}

func subB() protocol.Subscription {
	return protocol.Subscription{Provider: providerGitHub, Event: "", Repository: "o/b"}
}

// quiet returns Options with short intervals and zero jitter. It discards
// events.
func quiet() Options {
	return Options{
		Dial:           nil,
		OnEvent:        func(context.Context, protocol.Event) {},
		Logger:         slog.New(slog.DiscardHandler),
		Rand:           func() float64 { return 0 },
		MinBackoff:     tick,
		MaxBackoff:     tick,
		PingInterval:   time.Hour,
		PongTimeout:    time.Hour,
		RequestTimeout: waitLimit,
	}
}

func TestNewDefaults(t *testing.T) {
	t.Parallel()

	var zero Options

	client := New(zero)

	tests := []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{"min backoff", client.opts.MinBackoff, time.Second},
		{"max backoff", client.opts.MaxBackoff, time.Minute},
		{"ping interval", client.opts.PingInterval, 30 * time.Second},
		{"pong timeout", client.opts.PongTimeout, 10 * time.Second},
		{"request timeout", client.opts.RequestTimeout, 10 * time.Second},
	}
	for _, test := range tests {
		if test.got != test.want {
			t.Errorf("%s = %v, want %v", test.name, test.got, test.want)
		}
	}

	if r := client.opts.Rand(); r < 0 || r >= 1 {
		t.Errorf("default Rand() = %v, want [0, 1)", r)
	}
}

func TestNewKeepsSetOptions(t *testing.T) {
	t.Parallel()

	opts := quiet()
	opts.MinBackoff = 3 * time.Second
	client := New(opts)

	if client.opts.MinBackoff != 3*time.Second {
		t.Errorf("MinBackoff = %v", client.opts.MinBackoff)
	}

	if client.opts.Rand() != 0 {
		t.Error("a set Rand was replaced")
	}
}

func TestBackoffDelay(t *testing.T) {
	t.Parallel()

	const (
		lowest  = time.Second
		highest = 60 * time.Second
	)

	tests := []struct {
		attempt int
		wantMax time.Duration // the delay before jitter
	}{
		{0, 1 * time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
		{5, 32 * time.Second},
		{6, 60 * time.Second},
		{7, 60 * time.Second},
		{1000, 60 * time.Second},
	}
	for _, test := range tests {
		for _, jitter := range []struct {
			r    float64
			frac float64
		}{{0, 0.5}, {0.5, 0.75}, {1, 1}} {
			got := backoffDelay(lowest, highest, test.attempt, jitter.r)
			want := time.Duration(float64(test.wantMax) * jitter.frac)

			if got != want {
				t.Errorf("backoffDelay(attempt %d, r %v) = %v, want %v", test.attempt, jitter.r, got, want)
			}
		}
	}
}

func TestBackoffDelayLowestAboveHighest(t *testing.T) {
	t.Parallel()

	if got := backoffDelay(10*time.Second, time.Second, 0, 1); got != time.Second {
		t.Errorf("got %v, want the cap", got)
	}
}

func TestNotConnectedError(t *testing.T) {
	t.Parallel()

	err := &NotConnectedError{Cause: errDial}

	if got, want := err.Error(), "not connected to the relay: dial failed"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}

	if !errors.Is(err, errDial) {
		t.Error("NotConnectedError does not unwrap to its cause")
	}
}

func TestNoDialBeforeFirstCall(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	client, _ := relay.start(t, quiet())

	time.Sleep(10 * tick)

	if relay.dials.Load() != 0 {
		t.Fatal("dialed before any request")
	}

	_, err := client.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if relay.dials.Load() != 1 {
		t.Errorf("dials = %d, want 1", relay.dials.Load())
	}
}

func TestRunStoppedBeforeFirstCall(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	_, stop := relay.start(t, quiet())

	stop()

	if relay.dials.Load() != 0 {
		t.Error("dialed although no request came")
	}
}

// silenceSubscribesOn returns an intercept that leaves the subscribes of the
// given connections unanswered.
func silenceSubscribesOn(conns ...int) func(int, frame) (string, bool) {
	return func(conn int, req frame) (string, bool) {
		return "", slices.Contains(conns, conn) && req.Type == typeSubscribe
	}
}

// currentSession returns the live session, or nil.
func currentSession(client *Client) *session {
	client.mu.Lock()
	defer client.mu.Unlock()

	return client.cur
}

// mirrorOf returns a copy of the client's list of wanted subscriptions.
func mirrorOf(client *Client) []protocol.Subscription {
	client.mu.Lock()
	defer client.mu.Unlock()

	return slices.Clone(client.mirror)
}

// awaitNewSession waits until a session other than old is live.
func awaitNewSession(t *testing.T, client *Client, old *session) {
	t.Helper()

	eventually(t, "a new session", func() bool {
		cur := currentSession(client)

		return cur != nil && cur != old
	})
}

// receive returns the next value from source, failing the test after one second.
//
//nolint:ireturn // a generic helper returns its type parameter
func receive[T any](t *testing.T, source <-chan T, what string) T {
	t.Helper()

	select {
	case value := <-source:
		return value
	case <-time.After(waitLimit):
		t.Fatalf("timed out waiting for %s", what)

		var zero T

		return zero
	}
}

// recorder is a slog handler that stores each record it receives.
type recorder struct {
	records []recorded
	mu      sync.Mutex
}

// recorded is one log record: its message and its attributes by key.
type recorded struct {
	attrs   map[string]slog.Value
	message string
}

func (r *recorder) Enabled(context.Context, slog.Level) bool { return true }

func (r *recorder) Handle(_ context.Context, record slog.Record) error {
	attrs := map[string]slog.Value{}

	record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value

		return true
	})

	r.mu.Lock()
	defer r.mu.Unlock()

	r.records = append(r.records, recorded{attrs: attrs, message: record.Message})

	return nil
}

func (r *recorder) WithAttrs([]slog.Attr) slog.Handler { return r }

func (r *recorder) WithGroup(string) slog.Handler { return r }

// matching returns the records with the given message.
func (r *recorder) matching(message string) []recorded {
	r.mu.Lock()
	defer r.mu.Unlock()

	var out []recorded

	for _, entry := range r.records {
		if entry.message == message {
			out = append(out, entry)
		}
	}

	return out
}

// loud returns Options like quiet, with the logger swapped for a recorder.
func loud() (Options, *recorder) {
	logs := &recorder{records: nil, mu: sync.Mutex{}}
	opts := quiet()
	opts.Logger = slog.New(logs)

	return opts, logs
}

// wantFailedClose checks that every record has a non-nil error attribute.
func wantFailedClose(t *testing.T, records []recorded) {
	t.Helper()

	for _, entry := range records {
		if value, ok := entry.attrs["error"]; !ok || value.Any() == nil {
			t.Errorf("record %+v has no error", entry)
		}
	}
}

// immediately is an After that fires at once and records the requested delay.
func immediately(delays chan<- time.Duration) func(time.Duration) <-chan time.Time {
	return func(delay time.Duration) <-chan time.Time {
		delays <- delay

		fired := make(chan time.Time)
		close(fired)

		return fired
	}
}

// mustSubscribe subscribes to each of subs, failing the test on an error.
func mustSubscribe(t *testing.T, client *Client, subs ...protocol.Subscription) {
	t.Helper()

	for _, sub := range subs {
		_, err := client.Subscribe(t.Context(), sub)
		if err != nil {
			t.Fatal(err)
		}
	}
}

// refusedSub is the subscription the fake relay refuses with a limit error.
func refusedSub() protocol.Subscription {
	return protocol.Subscription{Provider: providerGitHub, Event: "", Repository: "o/full"}
}

func TestSubscribeUnsubscribeList(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	relay.setRefuse(refusedSub())
	client, _ := relay.start(t, quiet())

	list, err := client.Subscribe(t.Context(), subA())
	if err != nil || !slices.Equal(list, []protocol.Subscription{subA()}) {
		t.Fatalf("Subscribe = %v, %v", list, err)
	}

	list, err = client.Subscribe(t.Context(), subB())
	if err != nil || !slices.Equal(list, []protocol.Subscription{subA(), subB()}) {
		t.Fatalf("Subscribe = %v, %v", list, err)
	}

	list, err = client.Unsubscribe(t.Context(), subA())
	if err != nil || !slices.Equal(list, []protocol.Subscription{subB()}) {
		t.Fatalf("Unsubscribe = %v, %v", list, err)
	}

	list, err = client.List(t.Context())
	if err != nil || !slices.Equal(list, []protocol.Subscription{subB()}) {
		t.Fatalf("List = %v, %v", list, err)
	}
}

func TestRelayErrorReplies(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	relay.setRefuse(refusedSub())
	client, _ := relay.start(t, quiet())

	_, err := client.Subscribe(t.Context(), subA())
	if err != nil {
		t.Fatal(err)
	}

	list, err := client.Unsubscribe(t.Context(), subB())
	if list != nil {
		t.Errorf("Unsubscribe of an absent subscription returned %v", list)
	}

	relayErr, ok := errors.AsType[*protocol.Error](err)
	if !ok || relayErr.Code != "not_subscribed" || relayErr.ID != "2" {
		t.Fatalf("err = %v, want a not_subscribed *protocol.Error for request 2", err)
	}

	_, err = client.Subscribe(
		t.Context(),
		protocol.Subscription{Provider: providerGitHub, Event: "", Repository: "o/full"},
	)
	if got := err.Error(); got != "limit: full" {
		t.Errorf("err = %q, want %q", got, "limit: full")
	}

	frames := relay.onConn(1)
	if len(frames) != 3 || frames[0].ID != "1" || frames[2].ID != "3" {
		t.Errorf("frames = %+v, want three requests with ids 1 to 3", frames)
	}
}

func TestRequestTimeoutDropsLateReply(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	relay.setIntercept(func(_ int, req frame) (string, bool) { return "", req.Type == typeSubscribe })

	opts := quiet()
	opts.RequestTimeout = 50 * time.Millisecond
	client, _ := relay.start(t, opts)

	_, err := client.Subscribe(t.Context(), subA())
	if !errors.Is(err, ErrNoAnswer) {
		t.Fatalf("err = %v, want ErrNoAnswer", err)
	}

	if got, want := err.Error(), "relay did not answer within 50ms"; got != want {
		t.Errorf("err = %q, want %q", got, want)
	}

	client.mu.Lock()
	left := len(client.pending)
	client.mu.Unlock()

	if left != 0 {
		t.Errorf("%d requests still pending", left)
	}

	// The late reply is dropped, and the client still works.
	relay.push(t, `{"type":"subscriptions","id":"1","subscriptions":[]}`)

	list, err := client.List(t.Context())
	if err != nil || len(list) != 0 {
		t.Fatalf("List = %v, %v", list, err)
	}
}

func TestNotConnectedCarriesTheDialError(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	relay.failDial.Store(1)

	opts := quiet()
	opts.MinBackoff = time.Hour
	opts.MaxBackoff = time.Hour
	opts.RequestTimeout = 100 * time.Millisecond
	client, _ := relay.start(t, opts)

	_, err := client.List(t.Context())
	if !errors.Is(err, errDial) {
		t.Fatalf("err = %v, want the dial error", err)
	}

	if _, ok := errors.AsType[*NotConnectedError](err); !ok {
		t.Errorf("err = %T, want *NotConnectedError", err)
	}

	if got, want := err.Error(), "not connected to the relay: dial failed"; got != want {
		t.Errorf("err = %q, want %q", got, want)
	}

	// The next request cuts the hour-long backoff short.
	_, err = client.List(t.Context())
	if err != nil {
		t.Fatalf("second List = %v", err)
	}
}

func TestNotConnectedWhileDialing(t *testing.T) {
	t.Parallel()

	opts := quiet()
	opts.RequestTimeout = 30 * time.Millisecond
	opts.Dial = func(ctx context.Context) (*transport.Conn, error) {
		<-ctx.Done()

		return nil, ctx.Err()
	}
	client, stop := newFakeRelay(t).start(t, opts)

	_, err := client.List(t.Context())
	if got, want := err.Error(), "not connected to the relay: still connecting"; got != want {
		t.Errorf("err = %q, want %q", got, want)
	}

	stop()
}

func TestRetriesAfterBackoffWithoutAKick(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	relay.failDial.Store(2)

	client, _ := relay.start(t, quiet())

	// The request waits through two failed dials and their retry delays.
	_, err := client.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if relay.dials.Load() != 3 {
		t.Errorf("dials = %d, want 3", relay.dials.Load())
	}
}

func TestReconnectResubscribesInOrder(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	client, _ := relay.start(t, quiet())

	mustSubscribe(t, client, subA(), subB())

	relay.drop()
	eventually(t, "two resubscribes", func() bool { return len(relay.onConn(2)) >= 2 })

	// A call made right after the reconnect comes after the resubscribes.
	list, err := client.List(t.Context())
	if err != nil || !slices.Equal(list, []protocol.Subscription{subA(), subB()}) {
		t.Fatalf("List = %v, %v", list, err)
	}

	frames := relay.onConn(2)
	if len(frames) != 3 {
		t.Fatalf("frames on the new connection = %+v", frames)
	}

	for i, sub := range []protocol.Subscription{subA(), subB()} {
		if frames[i].Type != typeSubscribe || frames[i].Subscription == nil || *frames[i].Subscription != sub {
			t.Errorf("frame %d = %+v, want a subscribe of %v", i, frames[i], sub)
		}
	}

	if frames[2].Type != "list" {
		t.Errorf("frame 2 = %+v, want the list call", frames[2])
	}
}

func TestRejectedResubscribeIsDropped(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	client, _ := relay.start(t, quiet())

	mustSubscribe(t, client, subA(), subB())

	old := currentSession(client)

	relay.setRefuse(subB())
	relay.drop()
	awaitNewSession(t, client, old)

	// Read the mirror directly, because a List reply would reset it.
	if got := mirrorOf(client); !slices.Equal(got, []protocol.Subscription{subA()}) {
		t.Fatalf("mirror = %v, want only the accepted subscription", got)
	}

	relay.drop()
	eventually(t, "a third connection", func() bool { return len(relay.onConn(3)) >= 1 })

	if frames := relay.onConn(3); frames[0].Type != typeSubscribe || *frames[0].Subscription != subA() {
		t.Errorf("first frame on the third connection = %+v", frames[0])
	}

	_, err := client.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	subscribes := 0

	for _, entry := range relay.onConn(3) {
		if entry.Type == typeSubscribe {
			subscribes++
		}
	}

	if subscribes != 1 {
		t.Errorf("third connection resubscribed %d times, want 1", subscribes)
	}
}

func TestEveryResubscribeRejected(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	client, _ := relay.start(t, quiet())

	mustSubscribe(t, client, subA(), subB())

	old := currentSession(client)

	relay.setRefuse(subA(), subB())
	relay.drop()
	awaitNewSession(t, client, old)

	if got := mirrorOf(client); len(got) != 0 {
		t.Fatalf("mirror = %v, want it empty", got)
	}

	relay.drop()
	eventually(t, "a third connection", func() bool { return relay.connCount() >= 3 })

	_, err := client.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	for _, entry := range relay.onConn(3) {
		if entry.Type == typeSubscribe {
			t.Errorf("third connection resubscribed %+v, want nothing", entry)
		}
	}
}

func TestResubscribeTimeoutReconnects(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	opts := quiet()
	opts.RequestTimeout = 100 * time.Millisecond
	client, _ := relay.start(t, opts)

	_, err := client.Subscribe(t.Context(), subA())
	if err != nil {
		t.Fatal(err)
	}

	// Connection 2 never answers a subscribe. Its resubscribe times out.
	relay.setIntercept(silenceSubscribesOn(2))
	relay.drop()
	eventually(t, "a third connection", func() bool { return relay.connCount() >= 3 })

	list, err := client.List(t.Context())
	if err != nil || !slices.Equal(list, []protocol.Subscription{subA()}) {
		t.Fatalf("List = %v, %v", list, err)
	}

	if relay.connCount() != 3 {
		t.Errorf("connections = %d, want 3", relay.connCount())
	}
}

func TestKeepalivePings(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	opts := quiet()
	opts.PingInterval = tick
	opts.PongTimeout = waitLimit
	client, _ := relay.start(t, opts)

	_, err := client.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	eventually(t, "three pings answered", func() bool {
		pings := 0

		for _, entry := range relay.onConn(1) {
			if entry.Type == typePing {
				pings++
			}
		}

		return pings >= 3
	})

	if relay.connCount() != 1 {
		t.Error("the connection was replaced although every ping was answered")
	}
}

func TestMissingPongReconnects(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	relay.noPong.Store(true)

	opts := quiet()
	opts.PingInterval = tick
	opts.PongTimeout = 2 * tick
	client, _ := relay.start(t, opts)

	_, err := client.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	eventually(t, "a second connection", func() bool { return relay.connCount() >= 2 })
}

func TestStalePongDoesNotAnswerAPing(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	relay.noPong.Store(true)

	opts := quiet()
	opts.PingInterval = 300 * time.Millisecond
	opts.PongTimeout = 100 * time.Millisecond
	client, _ := relay.start(t, opts)

	_, err := client.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	// Send unsolicited pongs before the first ping. The second finds the
	// one-slot channel full.
	relay.push(t, `{"type":"pong"}`)
	relay.push(t, `{"type":"pong"}`)

	eventually(t, "a second connection", func() bool { return relay.connCount() >= 2 })
}

func TestKeepaliveStopsOnWriteError(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	opts := quiet()
	opts.PingInterval = tick
	client := New(opts)

	conn, err := relay.dial(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	err = conn.Close()
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})

	go func() {
		defer close(done)

		client.keepalive(t.Context(), conn, func() { t.Error("aborted although the write failed first") })
	}()

	select {
	case <-done:
	case <-time.After(waitLimit):
		t.Fatal("keepalive kept running on a closed connection")
	}
}

func TestRoundTripWriteError(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	client := New(quiet())

	conn, err := relay.dial(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	err = conn.Close()
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), waitLimit)
	defer cancel()

	_, err = client.roundTrip(ctx, &session{conn: conn, done: make(chan struct{})}, protocol.EncodeList)
	if err == nil || errors.Is(err, ErrNoAnswer) {
		t.Fatalf("roundTrip on a closed connection = %v, want the write error", err)
	}

	client.mu.Lock()
	left := len(client.pending)
	client.mu.Unlock()

	if left != 0 {
		t.Errorf("%d requests still pending", left)
	}
}

func TestEventsInOrderAndNoise(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	events := make(chan protocol.Event, 8)
	opts, logs := loud()
	opts.OnEvent = func(_ context.Context, ev protocol.Event) { events <- ev }
	client, _ := relay.start(t, opts)

	_, err := client.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	relay.push(
		t,
		`{"type":"event","provider":"github","event":"push","delivery":"d1","repository":"o/a","payload":{"n":1}}`,
	)
	relay.push(t, `not json`)
	relay.push(t, `{"type":"future","x":1}`)
	relay.push(t, `{"type":"error","id":null,"code":"bad_message","message":"x"}`)
	relay.push(t, `{"type":"event","provider":"github","event":"ping","delivery":"d2","repository":null,"payload":2}`)

	for _, want := range []string{"d1", "d2"} {
		select {
		case ev := <-events:
			if ev.Delivery != want {
				t.Errorf("delivery = %q, want %q", ev.Delivery, want)
			}
		case <-time.After(waitLimit):
			t.Fatalf("no event %s", want)
		}
	}

	for _, message := range []string{"undecodable message from the relay", "relay error without an id"} {
		if len(logs.matching(message)) != 1 {
			t.Errorf("log %q appeared %d times, want 1", message, len(logs.matching(message)))
		}
	}
}

func TestShutdownClosesNormallyAndFailsPending(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	relay.setIntercept(func(_ int, req frame) (string, bool) { return "", req.Type == "list" })

	client, stop := relay.start(t, quiet())

	failed := make(chan error, 1)

	go func() {
		_, err := client.List(t.Context())
		failed <- err
	}()

	eventually(t, "the list request", func() bool { return len(relay.onConn(1)) == 1 })
	stop()

	select {
	case err := <-failed:
		if !errors.Is(err, ErrClosed) {
			t.Errorf("pending call err = %v, want ErrClosed", err)
		}
	case <-time.After(waitLimit):
		t.Fatal("pending call still waiting after shutdown")
	}

	select {
	case code := <-relay.closed:
		if code != websocket.StatusNormalClosure {
			t.Errorf("server saw close code %d, want %d", code, websocket.StatusNormalClosure)
		}
	case <-time.After(waitLimit):
		t.Fatal("server never saw the connection close")
	}
}

func TestShutdownDuringBackoff(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	relay.failDial.Store(100)

	opts := quiet()
	opts.MinBackoff = time.Hour
	opts.MaxBackoff = time.Hour
	opts.RequestTimeout = 20 * time.Millisecond
	client, stop := relay.start(t, opts)

	_, err := client.List(t.Context())
	if !errors.Is(err, errDial) {
		t.Fatalf("err = %v", err)
	}

	stop()
}

func TestShutdownWhileResubscribing(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	opts := quiet()
	opts.RequestTimeout = 5 * time.Second
	client, stop := relay.start(t, opts)

	_, err := client.Subscribe(t.Context(), subA())
	if err != nil {
		t.Fatal(err)
	}

	relay.setIntercept(silenceSubscribesOn(2))
	relay.drop()
	eventually(t, "the resubscribe", func() bool { return len(relay.onConn(2)) == 1 })
	stop()
}

// TestConcurrentCalls checks that requests from many goroutines get their own
// replies.
func TestConcurrentCalls(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	client, _ := relay.start(t, quiet())

	var group sync.WaitGroup

	for range 8 {
		group.Go(func() {
			_, err := client.List(t.Context())
			if err != nil {
				t.Error(err)
			}
		})
	}

	group.Wait()
}

func TestShutdownWhileAwaitingPong(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	relay.noPong.Store(true)

	opts := quiet()
	opts.PingInterval = tick
	client, stop := relay.start(t, opts)

	_, err := client.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	eventually(t, "a ping", func() bool { return len(relay.onConn(1)) >= 2 })
	stop()
}

func TestBackoffGrowsAndResets(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	delays := make(chan time.Duration, 16)
	opts := quiet()
	opts.MinBackoff = time.Second
	opts.MaxBackoff = 4 * time.Second
	opts.Dial = relay.dial
	client := New(opts)
	client.after = immediately(delays)

	relay.run(t, client)
	relay.failDial.Store(2)

	_, err := client.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	// The lost connection restarts the sequence, and the next two dials fail.
	relay.failDial.Store(2)
	relay.drop()

	want := []time.Duration{
		500 * time.Millisecond, time.Second,
		500 * time.Millisecond, time.Second, 2 * time.Second,
	}
	for i, wantDelay := range want {
		if got := receive(t, delays, "a delay"); got != wantDelay {
			t.Errorf("delay %d = %v, want %v", i, got, wantDelay)
		}
	}
}

func TestResubscribeFailuresKeepBackingOff(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	relay.setIntercept(silenceSubscribesOn(2, 3))

	delays := make(chan time.Duration, 16)
	opts := quiet()
	opts.MinBackoff = time.Second
	opts.MaxBackoff = 4 * time.Second
	opts.RequestTimeout = 50 * time.Millisecond
	opts.Dial = relay.dial
	client := New(opts)
	client.after = immediately(delays)

	relay.run(t, client)
	mustSubscribe(t, client, subA())
	relay.drop()

	want := []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second}
	for i, wantDelay := range want {
		if got := receive(t, delays, "a delay"); got != wantDelay {
			t.Errorf("delay %d = %v, want %v", i, got, wantDelay)
		}
	}
}

// eventuallyNotConnected waits until a request fails as not connected with the
// given cause.
func eventuallyNotConnected(t *testing.T, client *Client, cause error) {
	t.Helper()

	eventually(t, "a not-connected error with the cause", func() bool {
		_, err := client.List(t.Context())
		_, notConnected := errors.AsType[*NotConnectedError](err)

		return notConnected && errors.Is(err, cause)
	})
}

func TestCauseAfterResubscribeFailure(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	relay.setIntercept(silenceSubscribesOn(2))

	opts := quiet()
	opts.RequestTimeout = 50 * time.Millisecond
	opts.Dial = relay.dialFirst(2)
	client, _ := relay.start(t, opts)

	mustSubscribe(t, client, subA())
	relay.drop()

	eventuallyNotConnected(t, client, ErrNoAnswer)
}

func TestCauseAfterConnectionLoss(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	opts := quiet()
	opts.RequestTimeout = 50 * time.Millisecond
	opts.Dial = relay.dialFirst(1)
	client, _ := relay.start(t, opts)

	_, err := client.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	relay.drop()

	eventuallyNotConnected(t, client, ErrClosed)
}

func TestShutdownCloseIsLogged(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	opts, logs := loud()
	client, stop := relay.start(t, opts)

	_, err := client.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	stop()

	// The clean close succeeds. The second close, after the reader ended, fails.
	records := logs.matching("close the relay connection")
	if len(records) != 1 {
		t.Fatalf("close failures logged = %d, want 1", len(records))
	}

	wantFailedClose(t, records)
}

func TestRequestIDsAreDecimal(t *testing.T) {
	t.Parallel()

	const requests = 12

	relay := newFakeRelay(t)
	client, _ := relay.start(t, quiet())

	for range requests {
		_, err := client.List(t.Context())
		if err != nil {
			t.Fatal(err)
		}
	}

	frames := relay.onConn(1)
	if len(frames) != requests {
		t.Fatalf("frames = %d, want %d", len(frames), requests)
	}

	for i, entry := range frames {
		if want := strconv.Itoa(i + 1); entry.ID != want {
			t.Errorf("frame %d has id %q, want %q", i, entry.ID, want)
		}
	}
}
