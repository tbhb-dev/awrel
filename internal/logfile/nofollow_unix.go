// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

//go:build unix

package logfile

import "syscall"

// noFollow makes the open fail when the final path element is a symlink, so a
// planted link can't redirect log writes to another file.
const noFollow = syscall.O_NOFOLLOW
