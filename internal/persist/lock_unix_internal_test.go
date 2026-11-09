// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

//go:build unix

package persist

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestLockExclusiveRetriesWhenInterrupted(t *testing.T) {
	t.Parallel()

	lockFile, err := os.Create(filepath.Join(t.TempDir(), "lock"))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = lockFile.Close() })

	calls := 0

	err = lockExclusive(lockFile, func(_, how int) error {
		calls++

		// A broken retry loop would spin forever, so stop it.
		if calls > 3 {
			t.Fatal("lockExclusive kept retrying")
		}

		if how != syscall.LOCK_EX {
			t.Errorf("how = %d, want LOCK_EX", how)
		}

		if calls < 3 {
			return syscall.EINTR
		}

		return nil
	})
	if err != nil || calls != 3 {
		t.Errorf("lockExclusive = %v after %d calls, want nil after 3", err, calls)
	}
}

func TestLockExclusiveReturnsOtherErrors(t *testing.T) {
	t.Parallel()

	lockFile, err := os.Create(filepath.Join(t.TempDir(), "lock"))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = lockFile.Close() })

	calls := 0

	err = lockExclusive(lockFile, func(int, int) error {
		calls++

		if calls > 1 {
			t.Fatal("lockExclusive retried an error that no signal caused")
		}

		return errBoom
	})
	if !errors.Is(err, errBoom) {
		t.Errorf("lockExclusive = %v, want the lock error", err)
	}
}

// TestFlockLocksTheFile checks the real system call. A second descriptor can't
// take the lock without waiting.
func TestFlockLocksTheFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "lock")

	first, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = first.Close() })

	second, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = second.Close() })

	err = flock(int(first.Fd()), syscall.LOCK_EX)
	if err != nil {
		t.Fatal(err)
	}

	err = flock(int(second.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if !errors.Is(err, syscall.EWOULDBLOCK) {
		t.Errorf("second lock = %v, want EWOULDBLOCK", err)
	}
}
