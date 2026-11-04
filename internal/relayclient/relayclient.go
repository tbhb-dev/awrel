// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

// Package relayclient keeps one subscription session with the relay alive. It
// dials on the first request and redials after a loss with randomized
// exponential backoff. After a reconnect it resubscribes. Pings detect a dead
// socket, and an ID ties each reply to its request.
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
	backoffFactor = 2
)

var (
	// ErrClosed is the cause of a request that was waiting when the connection
	// ended.
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

// Options configures a Client. Dial, OnEvent and Logger are required; a zero
// duration or a nil Rand selects the default.
type Options struct {
	// Dial opens a connection to the relay.
	Dial func(context.Context) (*transport.Conn, error)
	// OnEvent receives each event, in order, on the reader goroutine.
	OnEvent func(context.Context, protocol.Event)
	Logger  *slog.Logger
	// Rand returns a float in [0, 1) for backoff jitter. Nil means rand.Float64.
	Rand func() float64
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
	// done is closed when the reader goroutine has exited.
	done chan struct{}
}

// reply is what the reader hands to a waiting request.
type reply struct {
	err           error
	subscriptions []protocol.Subscription
}

// Client is a relay session. Create it with New and start it with Run.
type Client struct {
	pending map[string]chan reply
	kick    chan struct{}
	pong    chan struct{}
	// ready is closed while cur is set, and replaced when the session ends.
	ready   chan struct{}
	cur     *session
	lastErr error
	mirror  []protocol.Subscription
	after   func(time.Duration) <-chan time.Time
	opts    Options
	mu      sync.Mutex
	nextID  atomic.Uint64
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

	return &Client{
		opts:    opts,
		after:   time.After,
		kick:    newSignal[struct{}](),
		pong:    newSignal[struct{}](),
		pending: map[string]chan reply{},
		ready:   make(chan struct{}),
		cur:     nil,
		lastErr: nil,
		mirror:  nil,
		mu:      sync.Mutex{},
		nextID:  atomic.Uint64{},
	}
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
//
//nolint:dupl // Mirrors Unsubscribe with a different encoder.
func (c *Client) Subscribe(ctx context.Context, sub protocol.Subscription) ([]protocol.Subscription, error) {
	return c.call(ctx, func(id string) []byte { return protocol.EncodeSubscribe(id, sub) })
}

// Unsubscribe removes sub and returns the relay's list after the change.
//
//nolint:dupl // Mirrors Subscribe with a different encoder.
func (c *Client) Unsubscribe(ctx context.Context, sub protocol.Subscription) ([]protocol.Subscription, error) {
	return c.call(ctx, func(id string) []byte { return protocol.EncodeUnsubscribe(id, sub) })
}

// List returns the relay's current list.
func (c *Client) List(ctx context.Context) ([]protocol.Subscription, error) {
	return c.call(ctx, protocol.EncodeList)
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

	go func() {
		defer close(sess.done)

		c.readLoop(readCtx, conn)
	}()

	err = c.resubscribe(ctx, sess)
	if err != nil {
		c.setLastErr(err)
		c.opts.Logger.WarnContext(ctx, "resubscribe to the relay", slog.Any("error", err))
		cancel()
		// Cancelling the reader's context also closes the socket.
		// gomutants:disable-next-line STATEMENT_REMOVE reason="the cancelled reader ends anyway"
		<-sess.done

		return false
	}

	c.markReady(sess)

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

// resubscribe replays the mirror on a new session. A relay error drops that
// entry from the mirror, and any other error ends the attempt.
func (c *Client) resubscribe(ctx context.Context, sess *session) error {
	for _, sub := range c.snapshot() {
		reqCtx, cancel := context.WithTimeout(ctx, c.opts.RequestTimeout)
		_, err := c.roundTrip(reqCtx, sess, func(id string) []byte { return protocol.EncodeSubscribe(id, sub) })

		// gomutants:disable-next-line STATEMENT_REMOVE reason="an uncancelled timer only waits"
		cancel()

		if relayErr, ok := errors.AsType[*protocol.Error](err); ok {
			c.opts.Logger.WarnContext(ctx, "relay rejected a resubscribe", slog.Any("error", relayErr))
			c.dropFromMirror(sub)

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

	return slices.Clone(c.mirror)
}

// replaceMirror makes the mirror a copy of subs.
func (c *Client) replaceMirror(subs []protocol.Subscription) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.mirror = slices.Clone(subs)
}

// current returns the live session, if any, and the channel that closes when
// the next session becomes ready.
func (c *Client) current() (*session, chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.cur, c.ready
}

// register records the channel that waits for the reply to requestID.
func (c *Client) register(requestID string, waiting chan reply) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.pending[requestID] = waiting
}

// forget removes the entry for requestID.
func (c *Client) forget(requestID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.pending, requestID)
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

	c.mirror = slices.DeleteFunc(c.mirror, func(other protocol.Subscription) bool { return other == sub })
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

		c.handle(ctx, msg)
	}
}

func (c *Client) handle(ctx context.Context, msg protocol.Message) {
	switch message := msg.(type) {
	case protocol.Subscriptions:
		c.replaceMirror(message.Subscriptions)
		c.route(ctx, message.ID, reply{subscriptions: message.Subscriptions, err: nil})
	case protocol.Error:
		if message.ID == "" {
			c.opts.Logger.WarnContext(ctx, "relay error without an id", slog.Any("error", message))

			return
		}

		c.route(ctx, message.ID, reply{subscriptions: nil, err: &message})
	case protocol.Pong:
		select {
		case c.pong <- struct{}{}:
		default:
		}
	case protocol.Event:
		c.opts.OnEvent(ctx, message)
	default:
		c.opts.Logger.DebugContext(ctx, "ignored relay message", slog.Any("message", message))
	}
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
	requestID := strconv.FormatUint(c.nextID.Add(1), 10)
	results := newSignal[reply]()

	c.register(requestID, results)
	defer c.forget(requestID)

	err := sess.conn.Write(ctx, build(requestID))
	if err != nil {
		return nil, err //nolint:wrapcheck // transport.Conn.Write already names the operation
	}

	select {
	case result := <-results:
		return result.subscriptions, result.err
	case <-sess.done:
		return nil, ErrClosed
	case <-ctx.Done():
		return nil, fmt.Errorf("%w within %s", ErrNoAnswer, c.opts.RequestTimeout)
	}
}
