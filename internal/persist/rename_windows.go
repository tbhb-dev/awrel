// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package persist

import (
	"errors"
	"os"
	"time"

	"golang.org/x/sys/windows"
)

const (
	renameAttempts = 10
	renamePause    = 10 * time.Millisecond
)

// rename allows a Load in another process to finish its os.ReadFile, which
// closes promptly but temporarily prevents replacing the target on Windows.
// The sibling lock serializes writers. Sharing violations and access-denied
// errors retry, since an open reader can cause either error.
func rename(oldPath, newPath string) error {
	return renameWithRetry(oldPath, newPath, os.Rename, time.Sleep)
}

func renameWithRetry(
	oldPath, newPath string, move func(string, string) error, pause func(time.Duration),
) error {
	for attempt := 1; ; attempt++ {
		err := move(oldPath, newPath)
		if (!errors.Is(err, windows.ERROR_SHARING_VIOLATION) && !errors.Is(err, windows.ERROR_ACCESS_DENIED)) ||
			attempt >= renameAttempts {
			return err
		}

		pause(renamePause)
	}
}
