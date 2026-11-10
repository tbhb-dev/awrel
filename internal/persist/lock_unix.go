// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

//go:build unix

package persist

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// flock is the system call that locks a file descriptor.
func flock(fd, how int) error {
	err := syscall.Flock(fd, how)
	if err != nil {
		return fmt.Errorf("flock: %w", err)
	}

	return nil
}

// lockExclusive waits for an exclusive advisory lock on f, using lock, and
// retries when a signal interrupts the wait.
func lockExclusive(f *os.File, lock func(fd, how int) error) error {
	for {
		err := lock(int(f.Fd()), syscall.LOCK_EX)
		// gomutants:disable-next-line REMOVE_LOGICAL_NOT,BRANCH_IF reason="without this exit the loop retries forever"
		if !errors.Is(err, syscall.EINTR) {
			return err
		}
	}
}
