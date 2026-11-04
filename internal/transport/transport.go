// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

// Package transport is the WebSocket client for the relay's /connect
// endpoint. It sends and receives text frames and leaves their content to the
// caller.
package transport

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/coder/websocket"

	"go.tbhb.dev/awrel/internal/protocol"
)

// ReadLimit is the largest message Conn.Read accepts, the Workers WebSocket
// message limit. The library default of 32 KiB is far below GitHub's 25 MB
// payload cap.
const ReadLimit = 32 * 1024 * 1024

// versionHeader is on every Worker response. The edge's own 404 lacks it.
const versionHeader = "Awrel-Worker-Version"

var (
	// ErrRefused is the cause of a Dial that got an HTTP answer instead of a
	// WebSocket.
	ErrRefused = errors.New("relay refused the connection")
	// ErrSubprotocol is the cause of a Dial that connected but didn't get the
	// subprotocol it asked for.
	ErrSubprotocol = errors.New("relay did not select the awrel.v1 subprotocol")
	// ErrBinaryFrame is the cause of a Read that got a binary frame.
	ErrBinaryFrame = errors.New("relay sent a binary frame")
)

// Dialer opens connections to the relay. The token only ever goes into the
// Authorization header.
type Dialer struct {
	// HTTPClient makes the handshake request. Nil means http.DefaultClient.
	// Production leaves it nil: net/http then keeps a WebSocket handshake on
	// HTTP/1.1, which the edge requires. A custom client must be backed by an
	// *http.Transport for the same reason.
	HTTPClient *http.Client
	URL        string
	Token      string
}

// Conn is one WebSocket connection to the relay.
type Conn struct {
	ws *websocket.Conn
}

// Dial connects and checks that the relay selected the awrel.v1 subprotocol.
func (d *Dialer) Dial(ctx context.Context) (*Conn, error) {
	header := http.Header{}
	header.Set("Authorization", "Bearer "+d.Token)

	//nolint:bodyclose // websocket.Dial owns resp.Body: it drains and closes it
	socket, resp, err := websocket.Dial(ctx, d.URL, &websocket.DialOptions{
		HTTPClient:   d.HTTPClient,
		HTTPHeader:   header,
		Subprotocols: []string{protocol.Subprotocol},
	})
	// Dial returns the 101 response on success too, so only a failed Dial with
	// a response means the relay answered over HTTP.
	if err != nil && resp != nil {
		return nil, refusal(resp)
	}

	if err != nil {
		return nil, fmt.Errorf("connect to relay: %w", err)
	}

	if socket.Subprotocol() != protocol.Subprotocol {
		return nil, errors.Join(ErrSubprotocol, socket.CloseNow())
	}

	socket.SetReadLimit(ReadLimit)

	return &Conn{ws: socket}, nil
}

// refusal describes a failed handshake, and says whether the answer came
// from the Worker or from the edge in front of it.
func refusal(resp *http.Response) error {
	version := resp.Header.Get(versionHeader)
	if version == "" {
		return fmt.Errorf("%w: HTTP %d from the edge (no %s header)", ErrRefused, resp.StatusCode, versionHeader)
	}

	return fmt.Errorf("%w: HTTP %d from Worker version %s", ErrRefused, resp.StatusCode, version)
}

// Read returns the next message. A binary frame is an error.
func (c *Conn) Read(ctx context.Context) ([]byte, error) {
	kind, data, err := c.ws.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("read from relay: %w", err)
	}

	if kind != websocket.MessageText {
		return nil, ErrBinaryFrame
	}

	return data, nil
}

// Write sends a message as one text frame.
func (c *Conn) Write(ctx context.Context, msg []byte) error {
	err := c.ws.Write(ctx, websocket.MessageText, msg)
	if err != nil {
		return fmt.Errorf("write to relay: %w", err)
	}

	return nil
}

// Close ends the connection with a normal closure.
func (c *Conn) Close() error {
	err := c.ws.Close(websocket.StatusNormalClosure, "")
	if err != nil {
		return fmt.Errorf("close relay connection: %w", err)
	}

	return nil
}
