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

func TestRenameRetriesSharingViolations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		result    error
		name      string
		busy      int
		wantCalls int
	}{
		{nil, "success", 0, 1},
		{nil, "reader closes", 1, 2},
		{errBoom, "permanent error", 0, 1},
		{windows.ERROR_SHARING_VIOLATION, "reader stays open", 10, 10},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			calls, pauses := 0, 0

			err := renameWithRetry("from", "to", func(oldPath, newPath string) error {
				calls++

				checkRenameCall(t, calls, oldPath, newPath)

				if calls <= test.busy {
					return &os.LinkError{Op: "rename", Old: oldPath, New: newPath, Err: windows.ERROR_SHARING_VIOLATION}
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
			if !errors.Is(moveErr, windows.ERROR_SHARING_VIOLATION) {
				t.Fatalf("rename while the reader is open = %v, want ERROR_SHARING_VIOLATION", moveErr)
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
