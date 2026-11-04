// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package transport

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"go.tbhb.dev/awrel/internal/protocol"
)

const (
	testToken   = "secret-token"
	testTimeout = 10 * time.Second
	bigMessage  = 40000 // over the library's 32 KiB default read limit
)

// handler serves one accepted WebSocket.
type handler func(ctx context.Context, req *http.Request, conn *websocket.Conn)

// wsServer starts an HTTPS server that offers HTTP/2. It accepts a WebSocket
// that offers one of the given protocols and passes it to serve.
func wsServer(t *testing.T, protocols []string, serve handler) *httptest.Server {
	t.Helper()

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conn, err := websocket.Accept(w, req, &websocket.AcceptOptions{Subprotocols: protocols})
		if err != nil {
			return
		}

		defer func() { ignore(conn.CloseNow()) }()

		serve(req.Context(), req, conn)
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)

	return server
}

// ignore drops an error that a test server has no way to report.
func ignore(error) {}

// drain reads until the connection ends.
func drain(ctx context.Context, conn *websocket.Conn) {
	for {
		_, _, err := conn.Read(ctx)
		if err != nil {
			return
		}
	}
}

func idle(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
	drain(ctx, conn)
}

func dial(t *testing.T, server *httptest.Server) *Conn {
	t.Helper()

	conn, err := (&Dialer{HTTPClient: server.Client(), URL: server.URL, Token: testToken}).Dial(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	return conn
}

func TestReadLimit(t *testing.T) {
	t.Parallel()

	if ReadLimit != 33554432 {
		t.Errorf("ReadLimit = %d, want 32 MiB", ReadLimit)
	}
}

func TestDialHandshake(t *testing.T) {
	t.Parallel()

	type seen struct {
		auth  string
		major int
	}

	got := make(chan seen, 1)
	server := wsServer(
		t,
		[]string{protocol.Subprotocol},
		func(ctx context.Context, req *http.Request, conn *websocket.Conn) {
			got <- seen{auth: req.Header.Get("Authorization"), major: req.ProtoMajor}

			drain(ctx, conn)
		},
	)

	conn := dial(t, server)

	handshake := <-got
	if handshake.auth != "Bearer "+testToken {
		t.Errorf("Authorization = %q", handshake.auth)
	}

	// The test server offers HTTP/2. A WebSocket handshake must stay on 1.1.
	if handshake.major != 1 {
		t.Errorf("handshake used HTTP/%d, want HTTP/1.1", handshake.major)
	}

	err := conn.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestExchange(t *testing.T) {
	t.Parallel()

	server := wsServer(
		t,
		[]string{protocol.Subprotocol},
		func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}

			err = conn.Write(ctx, websocket.MessageText, bytes.ToUpper(data))
			if err != nil {
				return
			}

			err = conn.Write(ctx, websocket.MessageText, bytes.Repeat([]byte("x"), bigMessage))
			if err != nil {
				return
			}

			drain(ctx, conn)
		},
	)

	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	conn := dial(t, server)

	err := conn.Write(ctx, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}

	got, err := conn.Read(ctx)
	if err != nil || string(got) != "HELLO" {
		t.Fatalf("Read = %q, %v", got, err)
	}

	got, err = conn.Read(ctx)
	if err != nil || len(got) != bigMessage {
		t.Fatalf("big Read = %d bytes, %v", len(got), err)
	}

	err = conn.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestDialRefused(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version string
		want    string
		status  int
	}{
		{"worker 401", "v-test", "HTTP 401 from Worker version v-test", http.StatusUnauthorized},
		{"edge 404", "", "HTTP 404 from the edge (no Awrel-Worker-Version header)", http.StatusNotFound},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if test.version != "" {
					w.Header().Set(versionHeader, test.version)
				}

				w.WriteHeader(test.status)
			}))
			t.Cleanup(server.Close)

			_, err := (&Dialer{HTTPClient: server.Client(), URL: server.URL, Token: testToken}).Dial(t.Context())
			if !errors.Is(err, ErrRefused) {
				t.Fatalf("err = %v, want ErrRefused", err)
			}

			if !strings.Contains(err.Error(), test.want) {
				t.Errorf("err %q does not contain %q", err, test.want)
			}

			if strings.Contains(err.Error(), testToken) {
				t.Errorf("err %q leaks the token", err)
			}
		})
	}
}

func TestDialNoSubprotocol(t *testing.T) {
	t.Parallel()

	server := wsServer(t, nil, idle)

	_, err := (&Dialer{HTTPClient: server.Client(), URL: server.URL, Token: testToken}).Dial(t.Context())
	if !errors.Is(err, ErrSubprotocol) {
		t.Fatalf("err = %v, want ErrSubprotocol", err)
	}
}

func TestDialUnreachable(t *testing.T) {
	t.Parallel()

	server := httptest.NewTLSServer(http.NotFoundHandler())
	client := server.Client()
	target := server.URL

	server.Close()

	_, err := (&Dialer{HTTPClient: client, URL: target, Token: testToken}).Dial(t.Context())

	var opErr *net.OpError
	if !errors.As(err, &opErr) || errors.Is(err, ErrRefused) {
		t.Fatalf("err = %v, want a wrapped network error", err)
	}

	if !strings.Contains(err.Error(), "connect to relay") {
		t.Errorf("err %q lacks its context", err)
	}
}

func TestReadBinaryFrame(t *testing.T) {
	t.Parallel()

	server := wsServer(
		t,
		[]string{protocol.Subprotocol},
		func(ctx context.Context, _ *http.Request, conn *websocket.Conn) {
			err := conn.Write(ctx, websocket.MessageBinary, []byte{1})
			if err != nil {
				return
			}

			drain(ctx, conn)
		},
	)

	conn := dial(t, server)

	_, err := conn.Read(t.Context())
	if !errors.Is(err, ErrBinaryFrame) {
		t.Fatalf("err = %v, want ErrBinaryFrame", err)
	}
}

// wantClosed checks that err wraps net.ErrClosed and names its operation.
func wantClosed(t *testing.T, err error, operation string) {
	t.Helper()

	if !errors.Is(err, net.ErrClosed) {
		t.Errorf("%s: err = %v, want it to wrap net.ErrClosed", operation, err)

		return
	}

	if !strings.Contains(err.Error(), operation) {
		t.Errorf("err %q does not name %q", err, operation)
	}
}

func TestClosedConnection(t *testing.T) {
	t.Parallel()

	conn := dial(t, wsServer(t, []string{protocol.Subprotocol}, idle))

	err := conn.Close()
	if err != nil {
		t.Fatal(err)
	}

	wantClosed(t, conn.Write(t.Context(), []byte("x")), "write to relay")
	wantClosed(t, conn.Close(), "close relay connection")

	_, err = conn.Read(t.Context())
	wantClosed(t, err, "read from relay")
}
