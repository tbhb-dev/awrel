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

func TestLockExclusiveRequestsABlockingExclusiveLock(t *testing.T) {
	t.Parallel()

	lockFile, err := os.Create(filepath.Join(t.TempDir(), "lock"))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = lockFile.Close() })

	calls := 0

	err = lockExclusive(lockFile, func(fd, how int) error {
		calls++

		if fd != int(lockFile.Fd()) || how != windows.LOCKFILE_EXCLUSIVE_LOCK {
			t.Errorf("lock(%d, %d), want (%d, LOCKFILE_EXCLUSIVE_LOCK)", fd, how, lockFile.Fd())
		}

		return errBoom
	})
	if !errors.Is(err, errBoom) || calls != 1 {
		t.Errorf("lockExclusive = %v after %d calls, want the lock error after 1", err, calls)
	}
}

func TestFlockLocksTheFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "lock")

	first, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = first.Close() })

	second, err := os.OpenFile(path, os.O_RDWR, fileMode)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = second.Close() })

	err = flock(int(first.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK)
	if err != nil {
		t.Fatal(err)
	}

	err = flock(int(second.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY)
	if !errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
		t.Fatalf("second lock = %v, want ERROR_LOCK_VIOLATION", err)
	}

	// The lock covers only the first byte, even though the file is empty.
	var nextByte windows.Overlapped

	nextByte.Offset = 1

	err = windows.LockFileEx(windows.Handle(second.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &nextByte)
	if err != nil {
		t.Fatalf("locking the next byte = %v, want nil", err)
	}

	err = first.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = boundedFlock(int(second.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK)
	if err != nil {
		t.Errorf("lock after closing the first handle = %v", err)
	}
}

// boundedFlock gives mutation tests a one-second limit even when a mutant
// fails to release a lock.
func boundedFlock(fd, how int) error {
	deadline := time.Now().Add(time.Second)

	for {
		err := flock(fd, how|windows.LOCKFILE_FAIL_IMMEDIATELY)
		if !errors.Is(err, windows.ERROR_LOCK_VIOLATION) || time.Now().After(deadline) {
			return err
		}

		time.Sleep(time.Millisecond)
	}
}
