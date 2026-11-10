// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"go.tbhb.dev/awrel/internal/buildmeta"
	"go.tbhb.dev/awrel/internal/mcpserver"
	"go.tbhb.dev/awrel/internal/persist"
	"go.tbhb.dev/awrel/internal/protocol"
	"go.tbhb.dev/awrel/internal/relayclient"
	"go.tbhb.dev/awrel/internal/transport"
)

const (
	endpointEnv   = "AWREL_ENDPOINT"
	formatEnv     = "AWREL_FORMAT"
	sessionFlag   = "session-id"
	formatCompact = "compact"
	formatFull    = "full"
	defaultFormat = formatCompact
	tokenEnv      = "AWREL_CONNECT_TOKEN"      //nolint:gosec // a variable name, not a credential
	tokenFileEnv  = "AWREL_CONNECT_TOKEN_FILE" //nolint:gosec // a variable name, not a credential
)

var (
	errNoEndpoint     = errors.New("no relay endpoint set: pass --endpoint or set " + endpointEnv)
	errNoToken        = errors.New("neither " + tokenEnv + " nor " + tokenFileEnv + " is set")
	errEmptyTokenFile = errors.New("is empty")
	errBadEndpoint    = errors.New("invalid endpoint")
	errEndpointScheme = errors.New("the scheme must be ws or wss")
	errEndpointNoHost = errors.New("the host is missing")
	errBadFormat      = errors.New("invalid format")
)

// resolveEndpoint returns the relay URL from the flag, else AWREL_ENDPOINT,
// else errNoEndpoint. A URL must be a WebSocket URL with a host.
func resolveEndpoint(flagValue string, getenv func(string) string) (string, error) {
	endpoint := flagValue
	if endpoint == "" {
		endpoint = getenv(endpointEnv)
	}

	if endpoint == "" {
		return "", errNoEndpoint
	}

	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("%w %q: %w", errBadEndpoint, endpoint, err)
	}

	if parsed.Scheme != "ws" && parsed.Scheme != "wss" {
		return "", fmt.Errorf("%w %q: %w", errBadEndpoint, endpoint, errEndpointScheme)
	}

	if parsed.Host == "" {
		return "", fmt.Errorf("%w %q: %w", errBadEndpoint, endpoint, errEndpointNoHost)
	}

	return endpoint, nil
}

// resolveFormat returns whether events are delivered compact, from the flag,
// else AWREL_FORMAT, else the default. The value is compact or full. A flag
// given on the command line takes precedence over the variable even when its
// value is empty, and the empty value is then an error like any other that
// isn't compact or full.
func resolveFormat(flagValue string, flagChanged bool, getenv func(string) string) (bool, error) {
	format := flagValue
	if !flagChanged {
		format = getenv(formatEnv)

		if format == "" {
			format = defaultFormat
		}
	}

	switch format {
	case formatCompact:
		return true, nil
	case formatFull:
		return false, nil
	default:
		return false, fmt.Errorf("%w %q: the value must be %s or %s", errBadFormat, format, formatCompact, formatFull)
	}
}

// resolveToken returns the connect token from AWREL_CONNECT_TOKEN, else the
// trimmed contents of the file that AWREL_CONNECT_TOKEN_FILE points to. A
// failure mentions the variable and the path, never the contents.
func resolveToken(getenv func(string) string) (string, error) {
	token := getenv(tokenEnv)
	if token != "" {
		return token, nil
	}

	path := getenv(tokenFileEnv)
	if path == "" {
		return "", errNoToken
	}

	data, err := os.ReadFile(path)
	if err != nil {
		// os.ReadFile always returns a path error, whose text repeats the
		// path. Wrapping the error beneath it keeps the path once.
		return "", fmt.Errorf("%s %s: %w", tokenFileEnv, path, errors.Unwrap(err))
	}

	token = strings.TrimSpace(string(data))
	if token == "" {
		return "", fmt.Errorf("%s %s %w", tokenFileEnv, path, errEmptyTokenFile)
	}

	return token, nil
}

// unconnectedRelay is the Relay of a server that has no endpoint or token.
// Subscription tools fail with the reason.
type unconnectedRelay struct {
	err error
}

//nolint:dupl // the Relay methods share a shape
func (r unconnectedRelay) Subscribe(context.Context, protocol.Subscription) ([]protocol.Subscription, error) {
	return nil, r.fail()
}

//nolint:dupl // the Relay methods share a shape
func (r unconnectedRelay) Unsubscribe(context.Context, protocol.Subscription) ([]protocol.Subscription, error) {
	return nil, r.fail()
}

func (r unconnectedRelay) Echo(context.Context, string) error {
	return r.fail()
}

func (r unconnectedRelay) List(context.Context) ([]protocol.Subscription, error) {
	return nil, r.fail()
}

func (unconnectedRelay) Status() (bool, int, uint64) {
	return false, 0, 0
}

func (r unconnectedRelay) fail() error {
	return &relayclient.NotConnectedError{Cause: r.err}
}

func newMCPCmd(stdin io.Reader, getenv func(string) string) *cobra.Command {
	// gomutants:disable-next-line INTEGER_INCREMENT,INTEGER_DECREMENT reason="zero picks the default"
	return newMCPCmdWithTimeout(stdin, getenv, 0)
}

// newMCPCmdWithTimeout is newMCPCmd with the relay request budget set, where
// zero means the default.
func newMCPCmdWithTimeout(stdin io.Reader, getenv func(string) string, requestTimeout time.Duration) *cobra.Command {
	var endpointFlag, formatFlag string

	cmd := &cobra.Command{
		Use:   "mcp",
		Short: "Serve MCP over stdio and relay webhook events to the agent",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			endpoint, err := resolveEndpoint(endpointFlag, getenv)
			// Like a missing token, a missing endpoint doesn't stop the
			// server. The endpoint is then empty and connectRelay fails
			// every tool with errNoEndpoint.
			if err != nil && !errors.Is(err, errNoEndpoint) {
				return err
			}

			compact, err := resolveFormat(formatFlag, cmd.Flags().Changed("format"), getenv)
			if err != nil {
				return err
			}

			sessionID, err := cmd.Flags().GetString(sessionFlag)
			if err != nil {
				return err //nolint:wrapcheck // pflag's message names the flag
			}

			return serveMCP(
				cmd,
				stdin,
				endpoint,
				compact,
				persist.Key(sessionID, getenv),
				getenv,
				requestTimeout,
				connectRelay,
			)
		},
	}
	cmd.Flags().
		StringVar(&endpointFlag, "endpoint", "", "relay URL, required (default $"+endpointEnv+")")
	cmd.Flags().
		StringVar(&formatFlag, "format", "", "event payloads: "+formatCompact+" or "+formatFull+
			" (default $"+formatEnv+", then "+defaultFormat+")")

	return cmd
}

// relayConnector supplies the tools' relay and its connection loop. Tests can
// inject restore notifications without opening a platform-specific Store.
type relayConnector func(
	context.Context, *slog.Logger, string, *persist.Store, func(string) string, time.Duration,
	func(context.Context, protocol.Event), func(context.Context, int),
) (mcpserver.Relay, func(context.Context))

// serveMCP runs the MCP server on stdin and stdout until the input ends or a
// signal arrives, and the relay connection alongside it.
func serveMCP(
	cmd *cobra.Command, stdin io.Reader, endpoint string, compact bool, sessionKey string,
	getenv func(string) string, requestTimeout time.Duration, connect relayConnector,
) error {
	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := slog.New(slog.NewTextHandler(cmd.ErrOrStderr(), nil))

	var server *mcpserver.Server

	saved, persistence := persist.Open(sessionKey, endpoint, getenv, logger, time.Now())

	relay, run := connect(
		ctx,
		logger,
		endpoint,
		saved,
		getenv,
		requestTimeout,
		func(ctx context.Context, event protocol.Event) {
			server.HandleEvent(ctx, event)
		},
		func(ctx context.Context, count int) {
			server.Restored(ctx, count)
		},
	)
	server = mcpserver.New(mcpserver.Options{
		Persistence: persistence,
		Logger:      logger,
		Relay:       relay,
		Version:     buildmeta.Get().Version,
		Compact:     compact,
	})

	relayCtx, cancelRelay := context.WithCancel(ctx)

	var running sync.WaitGroup

	running.Go(func() { run(relayCtx) })

	err := server.Serve(ctx, stdin, cmd.OutOrStdout())

	cancelRelay()
	// gomutants:disable-next-line STATEMENT_REMOVE reason="the process exits right after, which ends the relay"
	running.Wait()

	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("serve MCP: %w", err)
	}

	return nil
}

// connectRelay returns the Relay for the tools and the function that keeps its
// connection. If the endpoint or the token is missing, the Relay fails every call and the
// function returns at once. An open Store makes it save the subscriptions
// and restore the saved ones, and it reports how many through the restored callback.
//
//nolint:ireturn,dupl // the tools take the Relay interface; tests inject the same signature
func connectRelay(
	ctx context.Context,
	logger *slog.Logger,
	endpoint string,
	saved *persist.Store,
	getenv func(string) string,
	requestTimeout time.Duration,
	onEvent func(context.Context, protocol.Event),
	onRestored func(context.Context, int),
) (mcpserver.Relay, func(context.Context)) {
	if endpoint == "" {
		logger.ErrorContext(ctx, "no relay endpoint; every tool will fail", "error", errNoEndpoint)

		return unconnectedRelay{err: errNoEndpoint}, func(context.Context) {}
	}

	token, err := resolveToken(getenv)
	if err != nil {
		logger.ErrorContext(ctx, "no relay token; every tool will fail", "error", err)

		return unconnectedRelay{err: err}, func(context.Context) {}
	}

	dialer := &transport.Dialer{HTTPClient: nil, URL: endpoint, Token: token}

	store, initial := restoreSession(saved, (*persist.Store).Load)

	client := relayclient.New(relayclient.Options{
		Dial:       dialer.Dial,
		OnEvent:    onEvent,
		Logger:     logger,
		Store:      store,
		OnRestored: onRestored,
		Initial:    initial,
		Rand:       nil,
		// gomutants:disable-next-line INTEGER_INCREMENT,INTEGER_DECREMENT reason="zero picks the default"
		MinBackoff: 0,
		// gomutants:disable-next-line INTEGER_INCREMENT,INTEGER_DECREMENT reason="zero picks the default"
		MaxBackoff: 0,
		// gomutants:disable-next-line INTEGER_INCREMENT,INTEGER_DECREMENT reason="zero picks the default"
		PingInterval: 0,
		// gomutants:disable-next-line INTEGER_INCREMENT,INTEGER_DECREMENT reason="zero picks the default"
		PongTimeout:    0,
		RequestTimeout: requestTimeout,
	})

	return client, func(ctx context.Context) {
		client.Run(ctx) //nolint:errcheck,gosec // Run only ever returns nil
	}
}

// Load subscriptions only from an opened Store. Passing the
// loader lets tests cover the handoff on platforms where Open returns nil.
// A nil *persist.Store must not become a non-nil relayclient.Store.
//
//nolint:ireturn // the relay client takes the Store interface
func restoreSession(
	saved *persist.Store, load func(*persist.Store) []protocol.Subscription,
) (relayclient.Store, []protocol.Subscription) {
	if saved == nil {
		return nil, nil
	}

	return saved, load(saved)
}
