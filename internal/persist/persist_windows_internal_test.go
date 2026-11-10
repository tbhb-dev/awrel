// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

//go:build windows

package persist

import (
	"log/slog"
	"testing"
	"time"
)

func TestOpenKeepsSubscriptionsInTheSessionOnWindows(t *testing.T) {
	t.Parallel()

	checkSessionOnly(t, func(getenv func(string) string, logger *slog.Logger) (*Store, string) {
		return Open("valid-session-key", "wss://relay.example/connect", getenv, logger, time.Now())
	})
}
