// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

//go:build !unix

package persist

import "os"

// flock does nothing here, since the platform has no flock. Processes that
// share a session key can then lose each other's change on a write.
// A Store is never opened on Windows.
func flock(int, int) error {
	return nil
}

// lockExclusive takes no lock.
func lockExclusive(*os.File, func(fd, how int) error) error {
	return nil
}
