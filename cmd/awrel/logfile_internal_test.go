// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const payloadMarker = "payload-content-must-never-appear-865013"

func TestClientFileLogExcludesPayload(t *testing.T) {
	t.Parallel()
	state := t.TempDir()
	relay := newSessionRelay(t, false)
	running := startSession(
		t,
		env(map[string]string{tokenEnv: sentinelValue, endpointEnv: relay.url, stateHome: state}),
		0,
	)
	running.handshake(t)
	running.send(t, subscribeCall)
	running.await(t, false, `"id":2`)
	close(relay.push)
	running.await(t, true, "outcome=delivered delivery=d1")
	running.send(
		t,
		`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"check_delivery","arguments":{}}}`+"\n",
	)
	running.await(t, false, "Log file:")

	exitErr := running.finish(t)
	if exitErr != nil {
		t.Fatal(exitErr)
	}

	paths, err := filepath.Glob(filepath.Join(state, "awrel", "logs", "*.log"))
	if err != nil || len(paths) != 1 {
		t.Fatalf("paths = %v, %v", paths, err)
	}

	data, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatal(err)
	}

	logs := string(data)
	if strings.Contains(logs, payloadMarker) || !strings.Contains(logs, "msg=event outcome=delivered delivery=d1") ||
		!strings.Contains(logs, "repository=o/r action=created") {
		t.Fatalf("file log = %s", logs)
	}

	if !strings.Contains(running.out.String(), "Log file: "+strings.ReplaceAll(paths[0], `\`, `\\`)) {
		t.Fatalf("check_delivery lacks file path: %s", running.out.String())
	}
	// Every line sent to stderr went to the file, including startup messages.
	if logs != running.errOut.String() {
		t.Fatalf("file and stderr differ:\n%s\n%s", logs, running.errOut.String())
	}
}

func TestClientReportsUnavailableLog(t *testing.T) {
	t.Parallel()
	running := startSession(t, env(nil), 0)
	running.handshake(t)
	running.send(
		t,
		`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"check_delivery","arguments":{}}}`+"\n",
	)
	running.await(t, false, "Log file: unavailable (state directory unavailable)")

	err := running.finish(t)
	if err != nil {
		t.Fatal(err)
	}

	if got := strings.Count(running.errOut.String(), "client log file unavailable"); got != 1 {
		t.Fatalf("warnings = %d", got)
	}
}
