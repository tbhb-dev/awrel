// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

//go:build !unix

package persist

// boundedFlock is flock, which waits for nothing here.
func boundedFlock(fd, how int) error {
	return flock(fd, how)
}
