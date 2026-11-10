// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

//go:build !windows

package persist

import "os"

func rename(oldPath, newPath string) error {
	return os.Rename(oldPath, newPath) //nolint:wrapcheck // replace adds the session path and operation
}
