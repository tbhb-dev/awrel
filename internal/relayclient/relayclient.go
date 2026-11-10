// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

// Package relayclient keeps one subscription session with the relay alive. It
// dials on the first request, or at once when it starts with subscriptions to
// restore, and redials after a loss with randomized exponential backoff. After
// a reconnect it resubscribes. Pings detect a dead socket, and an ID ties each
// reply to its request.
package relayclient

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.tbhb.dev/awrel/internal/delivery"
	"go.tbhb.dev/awrel/internal/protocol"
	"go.tbhb.dev/awrel/internal/transport"
)

const (
	defaultMinBackoff     = time.Second
	defaultMaxBackoff     = time.Minute
	defaultPingInterval   = 30 * time.Second
	defaultPongTimeout    = 10 * time.Second
	defaultRequestTimeout = 10 * time.Second
	// backoffFactor is the growth of the retry delay per attempt.
	backoffFactor      = 2
	requestList        = "list"
	requestSubscribe   = "subscribe"
	requestUnsubscribe = "unsubscribe"
	maxMessageBytes    = 4096
)

var (
	// ErrRelayTooOld refuses a connection that can't honor requested filters.
	ErrRelayTooOld = errors.New(
		"relay too old: subscription filter capability or canonical subscription echo missing or changed",
	)
	// ErrClosed is the cause of a request waiting when the connection ended.
	ErrClosed = errors.New("connection to the relay closed")
	// ErrNoAnswer is the cause of a request that the relay didn't answer in time.
	ErrNoAnswer = errors.New("relay did not answer")
	// errConnecting is the cause of a NotConnectedError when no dial has
	// failed yet, so the dial was still running.
	errConnecting = errors.New("still connecting")
)

// NotConnectedError means no connection came up within a request's budget.
// Cause is the last dial or connection failure.
type NotConnectedError struct {
	Cause error
}

// Error returns the text that tools show the agent.
func (e *NotConnectedError) Error() string {
	return "not connected to the relay: " + e.Cause.Error()
}

// Unwrap returns the cause.
func (e *NotConnectedError) Unwrap() error {
	return e.Cause
}

// Store saves the subscriptions that the relay acknowledged, so that the next
// process of the session can restore them. Each call applies one change.
type Store interface {
	Add(sub protocol.Subscription) error
	Remove(sub protocol.Subscription) error
	// Touch resets the age of the saved file. A Store without a file does nothing.
	Touch()
}

// nopStore is the Store of a client that saves nothing.
type nopStore struct{}

func (nopStore) Add(protocol.Subscription) error    { return nil }
func (nopStore) Remove(protocol.Subscription) error { return nil }
func (nopStore) Touch()                             {}

// Options configures a Client. Dial, OnEvent and Logger are required; a zero
// duration, a nil Rand, a nil Store or a nil OnRestored selects the default.
type Options struct {
	// Dial opens a connection to the relay.
	Dial func(context.Context) (*transport.Conn, error)
	// OnEvent receives each event, in order, on the reader goroutine.
	OnEvent func(context.Context, protocol.Event)
	Logger  *slog.Logger
	// OnRestored receives the number of subscriptions the mirror holds once the
	// first replay of Initial succeeds. It doesn't run when that number is zero.
	OnRestored func(ctx context.Context, count int)
	// Rand returns a float in [0, 1) for backoff jitter. Nil means rand.Float64.
	Rand func() float64
	// Store saves each acknowledged change. Nil saves nothing. The restore
	// replay never calls it.
	Store Store
	// Initial seeds the mirror. A client with a seed dials at once instead of
	// waiting for the first request.
	Initial []protocol.Subscription
	// MinBackoff is the first retry delay before jitter. Default one second.
	MinBackoff time.Duration
	// MaxBackoff caps the retry delay before jitter. Default 60 seconds.
	MaxBackoff time.Duration
	// PingInterval is the time between keepalive pings. Default 30 seconds.
	PingInterval time.Duration
	// PongTimeout is how long a ping waits for its pong. Default 10 seconds.
	PongTimeout time.Duration
	// RequestTimeout is the budget of one request, connecting included.
	// Default 10 seconds.
	RequestTimeout time.Duration
}

// session is one live connection.
type session struct {
	conn *transport.Conn
	// done closes when the reader exits.
	done chan struct{}
	// operations serializes subscription changes and their budget estimates.
	operations sync.Mutex //exhaustruct:optional
	capable    bool       //exhaustruct:optional
}

// reply is what the reader hands to a waiting request.
type reply struct {
	err           error
	subscriptions []protocol.Subscription
}

// Client is a relay session. Create it with New and start it with Run.
type Client struct {
	lastErr  error
	pong     chan struct{}
	cur      *session
	after    func(time.Duration) <-chan time.Time
	expected map[string]expectation
	kick     chan struct{}
	pending  map[string]chan reply
	ready    chan struct{}
	mirror   []protocol.Subscription
	opts     Options
	// gatedEvents counts events suppressed by the gate since process start.
	gatedEvents atomic.Uint64
	nextID      atomic.Uint64
	mu          sync.Mutex
	// incompatible stops retries and suppresses all later delivery.
	incompatible atomic.Bool
	// replaying freezes the complete mirror while replay replies contain partial lists.
	replaying atomic.Bool
	// gated counts filter requests and replay while their echoes are pending.
	gated atomic.Int32
	// restoring is set from a seeded start until OnRestored has run.
	restoring atomic.Bool
}

// New returns a Client. It starts nothing.
func New(opts Options) *Client {
	opts.MinBackoff = orDefault(opts.MinBackoff, defaultMinBackoff)
	opts.MaxBackoff = orDefault(opts.MaxBackoff, defaultMaxBackoff)
	opts.PingInterval = orDefault(opts.PingInterval, defaultPingInterval)
	opts.PongTimeout = orDefault(opts.PongTimeout, defaultPongTimeout)
	opts.RequestTimeout = orDefault(opts.RequestTimeout, defaultRequestTimeout)

	if opts.Rand == nil {
		opts.Rand = rand.Float64
	}

	if opts.Store == nil {
		opts.Store = nopStore{}
	}

	if opts.OnRestored == nil {
		opts.OnRestored = func(context.Context, int) {}
	}

	client := &Client{
		opts:         opts,
		after:        time.After,
		kick:         newSignal[struct{}](),
		pong:         newSignal[struct{}](),
		pending:      map[string]chan reply{},
		expected:     map[string]expectation{},
		ready:        make(chan struct{}),
		cur:          nil,
		lastErr:      nil,
		mirror:       cloneSubscriptions(opts.Initial),
		mu:           sync.Mutex{},
		nextID:       atomic.Uint64{},
		incompatible: atomic.Bool{},
		gated:        atomic.Int32{},
		gatedEvents:  atomic.Uint64{},
		replaying:    atomic.Bool{},
		restoring:    atomic.Bool{},
	}

	// A seed makes the client dial at once. The signal waits for Run.
	if len(opts.Initial) > 0 {
		client.restoring.Store(true)
		client.kickDial()
	}

	return client
}

// newSignal returns a channel with room for one pending value.
func newSignal[T any]() chan T {
	// gomutants:disable-next-line INTEGER_INCREMENT,INTEGER_DECREMENT reason="other sizes act the same"
	return make(chan T, 1)
}

func orDefault(value, fallback time.Duration) time.Duration {
	if value == 0 {
		return fallback
	}

	return value
}

// backoffDelay is the wait after a failed dial or a lost connection. The base
// starts at lowest, doubles with each attempt and stops at highest. The result
// lies between half the base and the whole base, placed by fraction, a number
// from 0 to 1.
func backoffDelay(lowest, highest time.Duration, attempt int, fraction float64) time.Duration {
	delay := lowest
	for range attempt {
		delay = min(delay*backoffFactor, highest)
	}

	delay = min(delay, highest)
	half := delay / backoffFactor

	return half + time.Duration(fraction*float64(half))
}

// Run connects when the first request arrives and keeps the connection until
// the context ends, then closes it and returns nil.
func (c *Client) Run(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case <-c.kick:
	}

	attempt := 0

	for {
		if c.session(ctx) {
			attempt = 0
		}

		if c.incompatible.Load() {
			return nil
		}

		delay := backoffDelay(c.opts.MinBackoff, c.opts.MaxBackoff, attempt, c.opts.Rand())
		attempt++

		select {
		case <-ctx.Done():
			return nil
		case <-c.kick:
		case <-c.after(delay):
		}
	}
}

// Subscribe adds sub and returns the relay's list after the change.
func (c *Client) Subscribe(ctx context.Context, sub protocol.Subscription) ([]protocol.Subscription, error) {
	return c.subscriptionCall(ctx, requestSubscribe, sub)
}

// Unsubscribe removes sub and returns the relay's list after the change.
func (c *Client) Unsubscribe(ctx context.Context, sub protocol.Subscription) ([]protocol.Subscription, error) {
	return c.subscriptionCall(ctx, "unsubscribe", sub)
}

// List returns the relay's current list.
func (c *Client) List(ctx context.Context) ([]protocol.Subscription, error) {
	return c.call(ctx, protocol.EncodeList)
}

// Status reads connection state, subscription count and gated event count locally.
func (c *Client) Status() (bool, int, uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.cur != nil, len(c.mirror), c.gatedEvents.Load()
}

// session dials once, resubscribes, and serves the connection until it ends.
// It reports whether the connection finished resubscribing.
func (c *Client) session(ctx context.Context) bool {
	conn, err := c.opts.Dial(ctx)
	if err != nil {
		c.setLastErr(err)
		c.opts.Logger.WarnContext(ctx, "dial the relay", slog.Any("error", err))

		return false
	}

	// The reader's context ignores the caller's cancellation, so shutdown can
	// close the socket cleanly first. Calling cancel aborts the reader for a
	// dead connection.
	readCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()

	sess := &session{conn: conn, done: make(chan struct{})}

	c.replaying.Store(true)
	c.gated.Add(1)

	go func() {
		defer close(sess.done)

		c.readLoop(readCtx, conn)
	}()

	err = c.resubscribe(ctx, sess)
	c.gated.Add(-1)
	c.replaying.Store(false)

	if err != nil {
		c.setLastErr(err)
		c.opts.Logger.WarnContext(ctx, "resubscribe to the relay", slog.Any("error", err))
		cancel()
		// Cancelling the reader's context also closes the socket.
		// gomutants:disable-next-line STATEMENT_REMOVE reason="the cancelled reader ends anyway"
		<-sess.done

		return false
	}

	c.opts.Store.Touch()
	c.markReady(sess)
	c.announceRestored(ctx)

	var keepers sync.WaitGroup

	keepers.Go(func() { c.keepalive(readCtx, conn, cancel) })

	select {
	case <-sess.done:
	case <-ctx.Done():
		c.closeConn(ctx, conn)
		// gomutants:disable-next-line STATEMENT_REMOVE reason="the cancelled reader ends anyway"
		<-sess.done
	}

	c.markNotReady()
	cancel()
	// gomutants:disable-next-line STATEMENT_REMOVE reason="the cancelled keepalive ends anyway"
	keepers.Wait()
	c.closeConn(ctx, conn)

	return true
}

// announceRestored tells OnRestored, once, how many subscriptions the replay
// of the seed left in the mirror.
func (c *Client) announceRestored(ctx context.Context) {
	if !c.restoring.CompareAndSwap(true, false) {
		return
	}

	count := len(c.snapshot())
	if count > 0 {
		c.opts.OnRestored(ctx, count)
	}
}

// closeConn closes conn and logs a failure at debug level, since the
// connection may already be gone.
func (c *Client) closeConn(ctx context.Context, conn *transport.Conn) {
	err := conn.Close()
	if err != nil {
		c.opts.Logger.DebugContext(ctx, "close the relay connection", slog.Any("error", err))
	}
}

func (c *Client) setLastErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.lastErr = err
}

func (c *Client) markReady(sess *session) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cur = sess
	c.lastErr = nil

	close(c.ready)
}

func (c *Client) markNotReady() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cur = nil
	c.lastErr = ErrClosed
	c.ready = make(chan struct{})
}

// kickDial wakes Run, from its first wait or from a backoff.
func (c *Client) kickDial() {
	select {
	case c.kick <- struct{}{}:
	default:
	}
}

// resubscribe replays the mirror on a new session. An entry that the relay
// rejects, filtered or not, is dropped from the mirror and the store, so that
// no later start replays it. The missing filter echo, not a rejection, marks a
// relay too old for filters.
func (c *Client) resubscribe(ctx context.Context, sess *session) error {
	for _, sub := range c.snapshot() {
		reqCtx, cancel := context.WithTimeout(ctx, c.opts.RequestTimeout)
		_, err := c.subscriptionTrip(reqCtx, sess, requestSubscribe, sub)

		// gomutants:disable-next-line STATEMENT_REMOVE reason="an uncancelled timer only waits"
		cancel()

		if relayErr, ok := errors.AsType[*protocol.Error](err); ok {
			c.opts.Logger.WarnContext(
				ctx, "relay rejected a restored subscription, dropping it",
				slog.Any("subscription", sub), slog.Any("error", relayErr),
			)
			c.dropFromMirror(sub)
			c.persist(ctx, requestUnsubscribe, sub)

			continue
		}

		if err != nil {
			return err
		}
	}

	return nil
}

// snapshot returns a copy of the mirror.
func (c *Client) snapshot() []protocol.Subscription {
	c.mu.Lock()
	defer c.mu.Unlock()

	return cloneSubscriptions(c.mirror)
}

// replaceMirror makes the mirror a copy of subs.
func (c *Client) replaceMirror(subs []protocol.Subscription) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.mirror = cloneSubscriptions(subs)
}

// current returns the live session, if any, and the channel that closes when
// the next session becomes ready.
func (c *Client) current() (*session, chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.cur, c.ready
}

// register records the channel that waits for the reply to requestID.
func (c *Client) register(requestID string, waiting chan reply, expected expectation) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.pending[requestID] = waiting
	c.expected[requestID] = expected
}

// forget removes the entry for requestID.
func (c *Client) forget(requestID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.pending, requestID)
	delete(c.expected, requestID)
}

// claim removes the entry for requestID and returns its channel.
func (c *Client) claim(requestID string) (chan reply, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	waiting, found := c.pending[requestID]
	// gomutants:disable-next-line STATEMENT_REMOVE reason="roundTrip also deletes the entry"
	delete(c.pending, requestID)

	return waiting, found
}

func (c *Client) dropFromMirror(sub protocol.Subscription) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.mirror = slices.DeleteFunc(c.mirror, func(other protocol.Subscription) bool { return other.Equal(sub) })
}

// readLoop is the only caller of Conn.Read. It returns when the read fails.
func (c *Client) readLoop(ctx context.Context, conn *transport.Conn) {
	for {
		data, err := conn.Read(ctx)
		// gomutants:disable-next-line BRANCH_IF reason="without this exit the loop spins on a dead connection"
		if err != nil {
			c.opts.Logger.DebugContext(ctx, "read from the relay", slog.Any("error", err))

			return
		}

		msg, err := protocol.Decode(data)
		if err != nil {
			c.opts.Logger.WarnContext(ctx, "undecodable message from the relay", slog.Any("error", err))

			continue
		}

		if !c.handle(ctx, msg) {
			return
		}
	}
}

func (c *Client) handle(ctx context.Context, msg protocol.Message) bool {
	switch message := msg.(type) {
	case protocol.Subscriptions:
		return c.handleSubscriptions(ctx, message)
	case protocol.Error:
		if message.ID == "" {
			c.opts.Logger.WarnContext(ctx, "relay error without an id", slog.Any("error", message))

			return true
		}

		c.route(ctx, message.ID, reply{subscriptions: nil, err: &message})
	case protocol.Pong:
		select {
		case c.pong <- struct{}{}:
		default:
		}
	case protocol.Event:
		if c.gated.Load() == 0 && !c.incompatible.Load() {
			c.opts.OnEvent(ctx, message)
		} else {
			reason := "filter request pending"
			if c.incompatible.Load() {
				reason = "relay incompatible"
			} else if c.replaying.Load() {
				reason = "replay pending"
			}

			c.gatedEvents.Add(1)
			delivery.Describe(message).Log(ctx, c.opts.Logger, "gated", "reason", reason)
		}
	default:
		c.opts.Logger.DebugContext(ctx, "ignored relay message", slog.Any("message", message))
	}

	return true
}

// route hands a reply to the request waiting for requestID.
func (c *Client) route(ctx context.Context, requestID string, result reply) {
	waiting, found := c.claim(requestID)
	if !found {
		c.opts.Logger.DebugContext(ctx, "reply to a request no longer waiting", slog.String("id", requestID))

		return
	}

	waiting <- result
}

// keepalive pings on an interval and calls cancel when a pong fails to arrive,
// which makes the reader fail.
func (c *Client) keepalive(ctx context.Context, conn *transport.Conn, cancel context.CancelFunc) {
	ticker := time.NewTicker(c.opts.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		// Discard a pong left over from before this ping.
		select {
		case <-c.pong:
		default:
		}

		err := conn.Write(ctx, []byte(protocol.PingFrame))
		if err != nil {
			return
		}

		select {
		case <-c.pong:
		case <-ctx.Done():
			return
		case <-time.After(c.opts.PongTimeout):
			c.opts.Logger.WarnContext(ctx, "no pong from the relay")
			cancel()

			return
		}
	}
}

// call sends the request that build makes and waits for its reply. A relay
// error reply comes back as *protocol.Error.
func (c *Client) call(ctx context.Context, build func(id string) []byte) ([]protocol.Subscription, error) {
	ctx, cancel := context.WithTimeout(ctx, c.opts.RequestTimeout)
	defer cancel()

	sess, err := c.acquire(ctx)
	if err != nil {
		return nil, err
	}

	return c.roundTrip(ctx, sess, build)
}

// acquire waits for a live session, asking Run to dial if there is none.
func (c *Client) acquire(ctx context.Context) (*session, error) {
	for {
		if c.incompatible.Load() {
			return nil, ErrRelayTooOld
		}

		sess, ready := c.current()
		if sess != nil {
			return sess, nil
		}

		c.kickDial()

		select {
		case <-ready:
		case <-ctx.Done():
			return nil, &NotConnectedError{Cause: c.cause()}
		}
	}
}

// cause is the reason a request has no connection.
func (c *Client) cause() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.lastErr == nil {
		return errConnecting
	}

	return c.lastErr
}

// roundTrip sends one request on the session and waits for its reply.
func (c *Client) roundTrip(
	ctx context.Context,
	sess *session,
	build func(id string) []byte,
) ([]protocol.Subscription, error) {
	return c.roundTripWith(
		ctx,
		sess,
		build,
		expectation{
			kind:         "",
			subscription: nil,
			filters:      false,
		},
	)
}

func (c *Client) roundTripWith(
	ctx context.Context,
	sess *session,
	build func(string) []byte,
	expected expectation,
) ([]protocol.Subscription, error) {
	requestID := strconv.FormatUint(c.nextID.Add(1), 10)
	results := newSignal[reply]()

	data := build(requestID)
	if len(data) > maxMessageBytes {
		return nil, &protocol.Error{ID: "", Code: "bad_message", Message: "messages must be at most 4096 bytes"}
	}

	c.register(requestID, results, expected)
	defer c.forget(requestID)

	err := sess.conn.Write(ctx, data)
	if err != nil {
		return nil, err //nolint:wrapcheck // transport.Conn.Write already names the operation
	}

	select {
	case result := <-results:
		return result.subscriptions, result.err
	case <-sess.done:
		if c.incompatible.Load() {
			return nil, ErrRelayTooOld
		}

		return nil, ErrClosed
	case <-ctx.Done():
		return nil, fmt.Errorf("%w within %s", ErrNoAnswer, c.opts.RequestTimeout)
	}
}

// expectation records the fields needed to check a successful reply.
type expectation struct {
	subscription *protocol.Subscription
	kind         string
	filters      bool
}

func (e expectation) accepts(reply protocol.Subscriptions) bool {
	if !reply.SubscriptionFilters {
		return false
	}

	if e.subscription == nil {
		return true
	}

	present := slices.ContainsFunc(
		reply.Subscriptions,
		func(sub protocol.Subscription) bool { return sub.Equal(*e.subscription) },
	)

	return present == (e.kind == requestSubscribe)
}

func cloneSubscriptions(subs []protocol.Subscription) []protocol.Subscription {
	result := slices.Clone(subs)
	for i, sub := range result {
		result[i] = sub.Canonical()
	}

	return result
}

func (c *Client) subscriptionCall(
	ctx context.Context,
	kind string,
	sub protocol.Subscription,
) ([]protocol.Subscription, error) {
	err := sub.ValidateFilters()
	if err != nil {
		return nil, fmt.Errorf("subscription: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, c.opts.RequestTimeout)
	defer cancel()

	sess, err := c.acquire(ctx)
	if err != nil {
		return nil, err
	}

	sub = sub.Canonical()

	subs, err := c.subscriptionTrip(ctx, sess, kind, sub)
	if err == nil {
		c.persist(ctx, kind, sub)
	}

	return subs, err
}

// persist saves the change that the relay acknowledged. A failure logs a
// warning and leaves the call successful.
func (c *Client) persist(ctx context.Context, kind string, sub protocol.Subscription) {
	save := c.opts.Store.Remove
	if kind == requestSubscribe {
		save = c.opts.Store.Add
	}

	err := save(sub)
	if err != nil {
		c.opts.Logger.WarnContext(ctx, "save the subscription", slog.Any("error", err))
	}
}

func (c *Client) subscriptionTrip(
	ctx context.Context,
	sess *session,
	kind string,
	sub protocol.Subscription,
) ([]protocol.Subscription, error) {
	sess.operations.Lock()
	defer sess.operations.Unlock()

	if c.incompatible.Load() {
		return nil, ErrRelayTooOld
	}

	if kind == requestSubscribe {
		candidate := c.snapshot()
		if !slices.ContainsFunc(candidate, sub.Equal) {
			candidate = append(candidate, sub)
		}

		if protocol.AttachmentBytes(candidate) > protocol.AttachmentBudget {
			return nil, &protocol.Error{ID: "", Code: "limit", Message: "subscriptions would exceed 12288 bytes"}
		}
	}

	if sub.HasFilters() {
		c.gated.Add(1)
		defer c.gated.Add(-1)

		if !sess.capable {
			_, err := c.roundTripWith(
				ctx,
				sess,
				protocol.EncodeFilterList,
				expectation{
					kind:         requestList,
					subscription: nil,
					filters:      true,
				},
			)
			if err != nil {
				c.closeConn(ctx, sess.conn)

				return nil, err
			}

			sess.capable = true
		}
	}

	build := func(id string) []byte { return protocol.EncodeUnsubscribe(id, sub) }
	if kind == requestSubscribe {
		build = func(id string) []byte { return protocol.EncodeSubscribe(id, sub) }
	}

	subs, err := c.roundTripWith(
		ctx,
		sess,
		build,
		expectation{kind: kind, subscription: &sub, filters: sub.HasFilters()},
	)
	if sub.HasFilters() && errors.Is(err, ErrNoAnswer) {
		c.closeConn(ctx, sess.conn)
	}

	return subs, err
}

func (c *Client) handleSubscriptions(ctx context.Context, message protocol.Subscriptions) bool {
	c.mu.Lock()
	expected, found := c.expected[message.ID]
	c.mu.Unlock()

	if expected.filters {
		if !expected.accepts(message) {
			c.incompatible.Store(true)

			return false
		}
	}

	if found && !c.replaying.Load() && (!expected.filters || expected.kind != requestList) {
		c.replaceMirror(message.Subscriptions)
	}

	c.route(ctx, message.ID, reply{subscriptions: message.Subscriptions, err: nil})

	return true
}
