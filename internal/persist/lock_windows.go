// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package persist

import (
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

// flock locks the first byte of the sibling lock file. Windows permits locks
// beyond the end of the file, so the lock file can be empty. The how argument
// specifies LockFileEx flags. Tests add LOCKFILE_FAIL_IMMEDIATELY to bound the wait.
func flock(fd, how int) error {
	//nolint:exhaustruct // the lock starts at offset zero, with no event
	var overlapped windows.Overlapped

	//nolint:gosec // how consists only of LockFileEx flags
	err := windows.LockFileEx(windows.Handle(fd), uint32(how), 0, 1, 0, &overlapped)
	if err != nil {
		return fmt.Errorf("flock: %w", err)
	}

	return nil
}

// lockExclusive blocks until the lock is available. Closing f releases it.
func lockExclusive(f *os.File, lock func(fd, how int) error) error {
	return lock(int(f.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK)
}
