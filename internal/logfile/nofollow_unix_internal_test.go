// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

//go:build unix

package logfile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAppendRefusesSymlink(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	link := filepath.Join(dir, "test.log")

	requireOK(t, os.WriteFile(target, []byte("keep\n"), fileMode))
	requireOK(t, os.Symlink(target, link))

	file, size, err := appendFile(link, os.OpenFile)
	if err == nil || file != nil || size != 0 {
		t.Fatalf("append = %v, %d, %v", file, size, err)
	}

	got, err := os.ReadFile(target)

	requireOK(t, err)

	if string(got) != "keep\n" {
		t.Fatalf("target = %q", got)
	}
}
