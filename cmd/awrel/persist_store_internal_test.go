// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

//go:build !windows

package main

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"time"

	"go.tbhb.dev/awrel/internal/persist"
	"go.tbhb.dev/awrel/internal/protocol"
)

const (
	harnessSession = "CLAUDE_CODE_SESSION_ID"
	listCall       = `{"jsonrpc":"2.0","id":5,"method":"tools/call",` +
		`"params":{"name":"list_subscriptions","arguments":{}}}` + "\n"
)

func awaitSubscription(t *testing.T, relay *sessionRelay) protocol.Subscription {
	t.Helper()

	select {
	case sub := <-relay.received:
		return sub
	case <-time.After(waitLimit):
		t.Fatal("the relay received no subscribe")

		return protocol.Subscription{Provider: "", Event: "", Repository: ""}
	}
}

func TestSeededSessionDialsAtOnce(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vars map[string]string
		name string
		args []string
	}{
		{map[string]string{}, "flag", []string{"--session-id", sessionKey}},
		{map[string]string{persist.SessionEnv: sessionKey}, "environment", nil},
		{map[string]string{harnessSession: sessionKey}, "harness variable", nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			state := t.TempDir()
			relay := newSessionRelay(t, true)
			path := seedFile(t, state, sessionKey, relay.url, savedEvent)

			seed, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}

			test.vars[tokenEnv] = sentinelValue
			test.vars[endpointEnv] = relay.url
			test.vars[stateHome] = state

			running := startSession(t, env(test.vars), 0, test.args...)

			driveRestore(t, running, relay)

			err = running.finish(t)
			if err != nil {
				t.Errorf("exit: %v", err)
			}

			checkUntouched(t, path, seed)
		})
	}
}

// driveRestore walks a seeded session through its restore and checks what the
// agent sees.
func driveRestore(t *testing.T, running *mcpRun, relay *sessionRelay) {
	t.Helper()

	// The relay sees the replay without any tool call. It delays the answer
	// until the agent has finished its handshake.
	if got := awaitSubscription(t, relay); got.Repository != "o/r" || got.Event != "issue_comment" {
		t.Errorf("replayed %+v, want the saved subscription", got)
	}

	running.handshake(t)
	close(relay.release)

	running.await(t, false, `restored 1 subscription from the previous run`)
	close(relay.push)
	running.await(t, false, `"provider":"github"`)

	running.send(t, listCall)
	running.await(t, false, `"id":5`)

	out := running.out.String()
	if !strings.Contains(out, `\"repository\":\"o/r\"`) && !strings.Contains(out, `"repository":"o/r"`) {
		t.Errorf("list_subscriptions lacks the restored entry:\n%s", out)
	}

	if relay.dials.Load() != 1 {
		t.Errorf("dials = %d, want 1", relay.dials.Load())
	}
}

// checkUntouched fails the test if the file differs from seed or has a lock
// beside it, since a process that only restores never writes or locks.
func checkUntouched(t *testing.T, path string, seed []byte) {
	t.Helper()

	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(after, seed) {
		t.Errorf("the restoring process changed the file: %s, %v", after, err)
	}

	if exists(path + ".lock") {
		t.Error("the restoring process took the lock")
	}
}

func TestSubscribeSavesTheSubscription(t *testing.T) {
	t.Parallel()

	state := t.TempDir()
	relay := newSessionRelay(t, false)
	vars := map[string]string{
		tokenEnv: sentinelValue, endpointEnv: relay.url, stateHome: state, persist.SessionEnv: sessionKey,
	}

	running := startSession(t, env(vars), 0)

	running.handshake(t)
	running.send(t, subscribeCall)
	running.await(t, false, `"id":2`)

	path := sessionFile(state, sessionKey)

	deadline := time.Now().Add(waitLimit)

	var data []byte

	for {
		var err error

		data, err = os.ReadFile(path)
		if err == nil {
			break
		}

		if time.Now().After(deadline) {
			t.Fatalf("no session file: %v\nstderr:\n%s", err, running.errOut.String())
		}

		time.Sleep(time.Millisecond)
	}

	want := `{"version":1,"endpoint":"` + relay.url + `","subscriptions":[` + savedEvent + `]}`
	if string(data) != want {
		t.Errorf("file = %s\nwant   %s", data, want)
	}

	err := running.finish(t)
	if err != nil {
		t.Errorf("exit: %v", err)
	}
}
