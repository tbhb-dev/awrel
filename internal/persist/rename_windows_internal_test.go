// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

//go:build windows

package persist

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

func TestRenameRetriesReaderErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		result    error
		busyError error
		name      string
		busy      int
		wantCalls int
	}{
		{nil, nil, "success", 0, 1},
		{nil, windows.ERROR_SHARING_VIOLATION, "sharing violation clears", 1, 2},
		{nil, windows.ERROR_ACCESS_DENIED, "access denied clears", 1, 2},
		{errBoom, nil, "permanent error", 0, 1},
		{windows.ERROR_FILE_NOT_FOUND, nil, "missing source", 0, 1},
		{windows.ERROR_SHARING_VIOLATION, windows.ERROR_SHARING_VIOLATION, "sharing violation persists", 10, 10},
		{windows.ERROR_ACCESS_DENIED, windows.ERROR_ACCESS_DENIED, "access denied persists", 10, 10},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			calls, pauses := 0, 0

			err := renameWithRetry("from", "to", func(oldPath, newPath string) error {
				calls++

				checkRenameCall(t, calls, oldPath, newPath)

				if calls <= test.busy {
					return &os.LinkError{Op: "rename", Old: oldPath, New: newPath, Err: test.busyError}
				}

				return test.result
			}, func(delay time.Duration) {
				pauses++

				checkRenamePause(t, delay)
			})
			if !errors.Is(err, test.result) || calls != test.wantCalls {
				t.Errorf("rename = %v after %d calls, want %v after %d", err, calls, test.result, test.wantCalls)
			}

			if pauses != calls-1 {
				t.Errorf("retry paused %d times after %d calls", pauses, calls)
			}
		})
	}
}

func TestRenameAfterAReaderCloses(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	oldPath, newPath := filepath.Join(dir, "old"), filepath.Join(dir, "new")
	mustWrite(t, oldPath, "replacement")
	mustWrite(t, newPath, "original")

	reader, err := os.Open(newPath)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = reader.Close() })

	calls := 0

	err = renameWithRetry(oldPath, newPath, func(from, to string) error {
		calls++
		moveErr := os.Rename(from, to)

		if calls == 1 {
			if !errors.Is(moveErr, windows.ERROR_SHARING_VIOLATION) &&
				!errors.Is(moveErr, windows.ERROR_ACCESS_DENIED) {
				t.Fatalf(
					"rename while the reader is open = %v, want ERROR_SHARING_VIOLATION or ERROR_ACCESS_DENIED",
					moveErr,
				)
			}

			closeErr := reader.Close()
			if closeErr != nil {
				t.Fatal(closeErr)
			}
		}

		return moveErr //nolint:wrapcheck // preserve the real rename error for the retry
	}, time.Sleep)
	if err != nil || calls != 2 {
		t.Fatalf("rename = %v after %d calls, want nil after 2", err, calls)
	}

	data, err := os.ReadFile(newPath)
	if err != nil || string(data) != "replacement" {
		t.Errorf("read replacement = %q, %v", data, err)
	}
}

func checkRenameCall(t *testing.T, calls int, oldPath, newPath string) {
	t.Helper()

	if calls > 11 {
		t.Fatal("rename kept retrying")
	}

	if oldPath != "from" || newPath != "to" {
		t.Errorf("rename(%q, %q), want (from, to)", oldPath, newPath)
	}
}

func checkRenamePause(t *testing.T, delay time.Duration) {
	t.Helper()

	if delay != 10*time.Millisecond {
		t.Errorf("retry pause = %s, want 10ms", delay)
	}
}
