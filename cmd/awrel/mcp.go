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
	"go.tbhb.dev/awrel/internal/protocol"
	"go.tbhb.dev/awrel/internal/relayclient"
	"go.tbhb.dev/awrel/internal/transport"
)

const (
	defaultEndpoint = "wss://awrel.tbhb.dev/connect"
	endpointEnv     = "AWREL_ENDPOINT"
	tokenEnv        = "AWREL_CONNECT_TOKEN"      //nolint:gosec // a variable name, not a credential
	tokenFileEnv    = "AWREL_CONNECT_TOKEN_FILE" //nolint:gosec // a variable name, not a credential
)

var (
	errNoToken        = errors.New("neither " + tokenEnv + " nor " + tokenFileEnv + " is set")
	errEmptyTokenFile = errors.New("is empty")
	errBadEndpoint    = errors.New("invalid endpoint")
	errEndpointScheme = errors.New("the scheme must be ws or wss")
	errEndpointNoHost = errors.New("the host is missing")
)

// resolveEndpoint returns the relay URL from the flag, else AWREL_ENDPOINT,
// else the default. The URL must be a WebSocket URL with a host.
func resolveEndpoint(flagValue string, getenv func(string) string) (string, error) {
	endpoint := flagValue
	if endpoint == "" {
		endpoint = getenv(endpointEnv)
	}

	if endpoint == "" {
		endpoint = defaultEndpoint
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

// unconnectedRelay is the Relay of a server that has no token. Every tool
// fails with the reason.
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

func (r unconnectedRelay) List(context.Context) ([]protocol.Subscription, error) {
	return nil, r.fail()
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
	var endpointFlag string

	cmd := &cobra.Command{
		Use:   "mcp",
		Short: "Serve MCP over stdio and relay webhook events to the agent",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			endpoint, err := resolveEndpoint(endpointFlag, getenv)
			if err != nil {
				return err
			}

			return serveMCP(cmd, stdin, endpoint, getenv, requestTimeout)
		},
	}
	cmd.Flags().
		StringVar(&endpointFlag, "endpoint", "", "relay URL (default $"+endpointEnv+", then "+defaultEndpoint+")")

	return cmd
}

// serveMCP runs the MCP server on stdin and stdout until the input ends or a
// signal arrives, and the relay connection alongside it.
func serveMCP(
	cmd *cobra.Command, stdin io.Reader, endpoint string, getenv func(string) string, requestTimeout time.Duration,
) error {
	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := slog.New(slog.NewTextHandler(cmd.ErrOrStderr(), nil))

	var server *mcpserver.Server

	relay, run := connectRelay(
		ctx,
		logger,
		endpoint,
		getenv,
		requestTimeout,
		func(ctx context.Context, event protocol.Event) {
			server.HandleEvent(ctx, event)
		},
	)
	server = mcpserver.New(mcpserver.Options{
		Logger:  logger,
		Relay:   relay,
		Version: buildmeta.Get().Version,
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
// connection. Without a token the Relay fails every call and the function
// returns at once.
//
//nolint:ireturn // the tools take the Relay interface
func connectRelay(
	ctx context.Context,
	logger *slog.Logger,
	endpoint string,
	getenv func(string) string,
	requestTimeout time.Duration,
	onEvent func(context.Context, protocol.Event),
) (mcpserver.Relay, func(context.Context)) {
	token, err := resolveToken(getenv)
	if err != nil {
		logger.ErrorContext(ctx, "no relay token; every tool will fail", "error", err)

		return unconnectedRelay{err: err}, func(context.Context) {}
	}

	dialer := &transport.Dialer{HTTPClient: nil, URL: endpoint, Token: token}
	client := relayclient.New(relayclient.Options{
		Dial:    dialer.Dial,
		OnEvent: onEvent,
		Logger:  logger,
		Rand:    nil,
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
