// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package persist

import (
	"bytes"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
)

func TestWindowsPlatformKeepsSubscriptionsInTheSession(t *testing.T) {
	t.Parallel()

	checkSessionOnly(t, func(getenv func(string) string, logger *slog.Logger) *Store {
		return open("windows", "valid-session-key", "wss://relay.example/connect", getenv, logger, time.Now())
	})
}

// checkSessionOnly verifies both the platform decision and its explanation.
func checkSessionOnly(t *testing.T, openStore func(func(string) string, *slog.Logger) *Store) {
	t.Helper()

	state := t.TempDir()

	var logs bytes.Buffer

	logger := slog.New(slog.NewTextHandler(&logs, nil))
	getenv := func(name string) string {
		if name == stateEnv {
			return state
		}

		return ""
	}

	if got := openStore(getenv, logger); got != nil {
		t.Fatal("persistence returned a Store on Windows")
	}

	if got := logs.String(); strings.Count(got, "\n") != 1 ||
		!strings.Contains(got, `level=INFO msg="subscriptions last only for this session on this platform"`) {
		t.Errorf("logs = %q, want one Info line explaining session-only subscriptions", got)
	}

	entries, err := os.ReadDir(state)
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) != 0 {
		t.Errorf("persistence created files in the state directory: %v", entries)
	}
}

func TestLinuxPlatformOpensTheStore(t *testing.T) {
	t.Parallel()

	store := open("linux", "valid-session-key", "wss://relay.example/connect",
		func(string) string { return t.TempDir() }, slog.New(slog.DiscardHandler), time.Now())
	if store == nil {
		t.Fatal("persistence returned no Store on Linux")
	}
}
