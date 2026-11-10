// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package logfile

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.tbhb.dev/awrel/internal/persist"
)

const (
	failStat     = "stat"
	failWrite    = "write"
	failReopen   = "reopen"
	failRename   = "rename"
	testCap      = 1 << 20
	testStateEnv = "XDG_STATE_HOME"
	failClose    = "close"
	failMissing  = "missing"
)

var errTest = errors.New("test failure")

func env(vars map[string]string) func(string) string {
	return func(key string) string { return vars[key] }
}

func mustWrite(t *testing.T, writer io.Writer, text string) {
	t.Helper()

	n, err := io.WriteString(writer, text)
	if err != nil || n != len(text) {
		t.Fatalf("write = %d, %v", n, err)
	}
}

func read(t *testing.T, path string) string {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	return string(data)
}

func seed(t *testing.T, path, data string) {
	t.Helper()

	err := os.WriteFile(path, []byte(data), fileMode)
	if err != nil {
		t.Fatal(err)
	}
}

func newWriter(t *testing.T) (*writer, *bytes.Buffer) {
	t.Helper()

	if maxBytes != testCap {
		t.Fatalf("size cap = %d, want %d", maxBytes, testCap)
	}

	var stderr bytes.Buffer

	path := filepath.Join(t.TempDir(), "logs", "test.log")

	sink, err := open(path, &stderr, slog.New(slog.NewTextHandler(&stderr, nil)), time.Now())
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(sink.close)

	return sink, &stderr
}

func TestPathsAndAppend(t *testing.T) {
	t.Parallel()

	for _, key := range []string{"session-key", ""} {
		for _, variable := range []string{testStateEnv, "HOME"} {
			t.Run(key+variable, func(t *testing.T) {
				t.Parallel()
				checkPath(t, key, variable)
			})
		}
	}
}

func checkPath(t *testing.T, key, variable string) {
	t.Helper()
	root := t.TempDir()

	base := root
	if variable == "HOME" {
		base = filepath.Join(root, ".local", "state")
	}

	name := "pid-" + strconv.Itoa(os.Getpid())
	if key != "" {
		name = "1add55581aaea687f8c5e7919f217cb6640cba7d2c14e7e4d6d4df21460642cf"
	}

	want := filepath.Join(base, "awrel", "logs", name+".log")

	var stderr bytes.Buffer
	for range 2 {
		sink, closeLog, path := Open(key, env(map[string]string{variable: root}), &stderr, time.Now())
		if path != want {
			t.Fatalf("path = %s, want %s", path, want)
		}

		mustWrite(t, sink, "line\n")
		closeLog()
	}

	if got := read(t, want); got != strings.Repeat("line\n", 2) || stderr.String() != got {
		t.Fatalf("file = %q, stderr = %q", got, stderr.String())
	}

	checkModes(t, want)
}

func checkModes(t *testing.T, want string) {
	t.Helper()

	if runtime.GOOS != "windows" {
		for path, mode := range map[string]fs.FileMode{filepath.Dir(want): 0o700, want: 0o600} {
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}

			if info.Mode().Perm() != mode {
				t.Errorf("%s mode = %o, want %o", path, info.Mode().Perm(), mode)
			}
		}
	}
}

func TestOpenFailures(t *testing.T) {
	t.Parallel()

	for _, failure := range []string{"state", "directory", "file"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			vars := map[string]string{testStateEnv: root}
			want := "unavailable (cannot open log file)"

			switch failure {
			case "state":
				vars = nil
				want = "unavailable (state directory unavailable)"
			case "directory":
				seed(t, filepath.Join(root, "awrel"), "blocked")
			case "file":
				path := filepath.Join(root, "awrel", "logs", persist.Name("key")+".log")

				err := os.MkdirAll(path, dirMode)
				if err != nil {
					t.Fatal(err)
				}
			}

			var stderr bytes.Buffer

			sink, closeLog, description := Open("key", env(vars), &stderr, time.Now())
			closeLog()

			if description != want {
				t.Fatalf("description = %q", description)
			}

			logger := slog.New(slog.NewTextHandler(sink, nil))
			logger.Info("first")
			logger.Info("second")

			assertWarningAndLines(t, stderr.String())
		})
	}
}

func TestRotation(t *testing.T) {
	t.Parallel()
	sink, stderr := newWriter(t)
	seed(t, sink.path+".1", "old backup")

	first := strings.Repeat("a", testCap-1)
	mustWrite(t, sink, first)
	mustWrite(t, sink, "b")

	if got := read(t, sink.path); got != first+"b" {
		t.Fatal("rotated before the cap")
	}

	mustWrite(t, sink, "c")

	if got := read(t, sink.path+".1"); got != first+"b" {
		t.Fatal("backup differs")
	}

	if got := read(t, sink.path); got != "c" {
		t.Fatalf("current = %q", got)
	}

	mustWrite(t, sink, strings.Repeat("d", testCap+1))

	if got := read(t, sink.path+".1"); got != strings.Repeat("d", testCap) {
		t.Fatal("large-write backup differs")
	}

	if got := read(t, sink.path); got != "d" {
		t.Fatalf("large-write current = %q", got)
	}

	if strings.Contains(stderr.String(), "WARN") {
		t.Fatal(stderr.String())
	}

	entries, err := os.ReadDir(filepath.Dir(sink.path))
	if err != nil || len(entries) != 2 {
		t.Fatalf("entries = %v, %v", entries, err)
	}
}

func TestWriteFailures(t *testing.T) {
	t.Parallel()

	for _, failure := range []string{failStat, failWrite, failClose, failRename, failReopen} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			sink, stderr := newWriter(t)

			breakWriter(t, sink, failure)
			logger := slog.New(slog.NewTextHandler(sink, nil))
			logger.Info("first")
			assertWarnings(t, stderr.String(), 1)
			logger.Info("second")
			assertWarningAndLines(t, stderr.String())
			sink.close()
			sink.close()
			assertWarnings(t, stderr.String(), 1)
		})
	}
}

func breakWriter(t *testing.T, sink *writer, failure string) {
	t.Helper()

	switch failure {
	case failStat, failClose:
		err := sink.file.Close()
		if err != nil {
			t.Fatal(err)
		}
	case failWrite:
		requireOK(t, sink.file.Close())

		file, err := os.Open(sink.path)
		if err != nil {
			t.Fatal(err)
		}

		sink.file = file
	case failRename, failReopen:
		seed(t, sink.path, strings.Repeat("x", testCap))

		if failure == failRename {
			sink.rename = func(string, string) error { return errTest }
		} else {
			sink.openFile = func(string, int, fs.FileMode) (*os.File, error) { return nil, errTest }
		}
	}

	if failure == failClose {
		err := sink.rotate()
		if err == nil {
			t.Fatal("close failure lost")
		}
	}
}

func assertWarningAndLines(t *testing.T, got string) {
	t.Helper()

	if strings.Count(got, "level=WARN") != 1 || !strings.Contains(got, "error=") ||
		!strings.Contains(got, "msg=first") ||
		!strings.Contains(got, "msg=second") {
		t.Fatal(got)
	}
}

func TestStderrFailureAndEmptyWrite(t *testing.T) {
	t.Parallel()
	sink, _ := newWriter(t)
	mustWrite(t, sink, "")
	sink.stderr = errorWriter{}

	n, err := sink.Write([]byte("still saved"))
	if n != 2 || !errors.Is(err, errTest) {
		t.Fatalf("write = %d, %v", n, err)
	}

	if got := read(t, sink.path); got != "still saved" {
		t.Fatal(got)
	}
}

type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) { return 2, errTest }

func TestPrune(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	now := time.Now()

	ages := map[string]time.Duration{
		"old.log":      persist.MaxAge + time.Second,
		"old.log.1":    persist.MaxAge + time.Second,
		"new.log":      0,
		"boundary.log": persist.MaxAge,
		"other.txt":    persist.MaxAge + time.Second,
		"other.log.2":  persist.MaxAge + time.Second,
	}
	for name, age := range ages {
		path := filepath.Join(dir, name)
		seed(t, path, name)

		err := os.Chtimes(path, now.Add(-age), now.Add(-age))
		if err != nil {
			t.Fatal(err)
		}
	}

	requireOK(t, os.Mkdir(filepath.Join(dir, "directory.log"), dirMode))

	requireOK(
		t,
		os.Chtimes(
			filepath.Join(dir, "directory.log"),
			now.Add(-persist.MaxAge-time.Second),
			now.Add(-persist.MaxAge-time.Second),
		),
	)

	var logs bytes.Buffer

	logger := slog.New(slog.NewTextHandler(&logs, nil))
	prune(dir, now, logger, os.ReadDir, os.Remove)

	for _, name := range []string{"old.log", "old.log.1"} {
		_, statErr := os.Stat(filepath.Join(dir, name))
		if !errors.Is(statErr, fs.ErrNotExist) {
			t.Errorf("%s survived: %v", name, statErr)
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 5 || logs.Len() != 0 {
		t.Fatalf("entries = %v, %v; logs = %s", entries, err, &logs)
	}
}

type brokenEntry struct{ fs.DirEntry }

func (brokenEntry) Info() (fs.FileInfo, error) { return nil, errTest }

func TestPruneFailures(t *testing.T) {
	t.Parallel()

	for _, failure := range []string{failMissing, "read", "info", "remove"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			path := filepath.Join(dir, "old.log")
			seed(t, path, "")

			now := time.Now()

			past := now.Add(-persist.MaxAge - time.Second)
			requireOK(t, os.Chtimes(path, past, past))

			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}

			var logs bytes.Buffer

			readDir := func(string) ([]fs.DirEntry, error) { return entries, nil }
			remove := os.Remove

			switch failure {
			case failMissing:
				readDir = func(string) ([]fs.DirEntry, error) { return nil, fs.ErrNotExist }
			case "read":
				readDir = func(string) ([]fs.DirEntry, error) { return nil, errTest }
			case "info":
				entries = []fs.DirEntry{brokenEntry{DirEntry: entries[0]}}
			case "remove":
				remove = func(string) error { return errTest }
			}

			prune(dir, now, slog.New(slog.NewTextHandler(&logs, nil)), readDir, remove)

			count := 1
			if failure == failMissing {
				count = 0
			}

			assertWarnings(t, logs.String(), count)
		})
	}
}

func TestStartupPrunesAndCloses(t *testing.T) {
	t.Parallel()
	root := t.TempDir()

	dir := filepath.Join(root, "awrel", "logs")

	err := os.MkdirAll(dir, dirMode)
	if err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, "old.log.1")
	seed(t, path, "old")

	now := time.Now()

	past := now.Add(-persist.MaxAge - time.Second)

	err = os.Chtimes(path, past, past)
	if err != nil {
		t.Fatal(err)
	}

	var stderr bytes.Buffer

	output, closeLog, _ := Open("key", env(map[string]string{testStateEnv: root}), &stderr, now)

	_, statErr := os.Stat(path)
	if !errors.Is(statErr, fs.ErrNotExist) {
		t.Fatalf("startup did not prune: %v", statErr)
	}

	sink, ok := output.(*writer)
	if !ok {
		t.Fatalf("writer = %T", output)
	}

	closeLog()

	// A second Close reports fs.ErrClosed on every OS; Stat on Windows
	// reports an invalid handle instead.
	fileErr := sink.file.Close()
	if !errors.Is(fileErr, fs.ErrClosed) {
		t.Fatalf("close left file open: %v", fileErr)
	}

	if stderr.Len() != 0 {
		t.Fatal(stderr.String())
	}
}

func requireOK(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatal(err)
	}
}

func assertWarnings(t *testing.T, logs string, count int) {
	t.Helper()

	if got := strings.Count(logs, "level=WARN"); got != count {
		t.Fatalf("logs = %s", logs)
	}
}

func TestErrorCauses(t *testing.T) {
	t.Parallel()
	t.Run("directory", func(t *testing.T) {
		t.Parallel()
		path := filepath.Join(t.TempDir(), "blocked")
		seed(t, path, "")
		_, err := open(filepath.Join(path, "test.log"), io.Discard, slog.New(slog.DiscardHandler), time.Now())
		requirePathError(t, err)
	})
	t.Run("append", func(t *testing.T) {
		t.Parallel()

		_, err := appendFile("unused", func(string, int, fs.FileMode) (*os.File, error) { return nil, errTest })
		if !errors.Is(err, errTest) {
			t.Fatalf("cause lost: %v", err)
		}
	})

	for _, failure := range []string{failStat, failWrite, failClose, failRename, failReopen} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			sink, _ := newWriter(t)
			breakWriter(t, sink, failure)

			var err error

			switch failure {
			case failStat, failWrite, failRename, failReopen:
				err = sink.write([]byte("line"))
			case failClose:
				err = sink.rotate()
			}

			if failure == failRename || failure == failReopen {
				if !errors.Is(err, errTest) {
					t.Fatalf("cause lost: %v", err)
				}
			} else {
				requirePathError(t, err)
			}
		})
	}
}

func requirePathError(t *testing.T, err error) {
	t.Helper()

	if _, ok := errors.AsType[*os.PathError](err); !ok {
		t.Fatalf("path error lost: %v", err)
	}
}

func TestPruneContinuesAfterInfoFailure(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "old.log")
	seed(t, path, "")

	now := time.Now()
	past := now.Add(-persist.MaxAge - time.Second)
	requireOK(t, os.Chtimes(path, past, past))

	entries, err := os.ReadDir(dir)
	requireOK(t, err)

	readDir := func(string) ([]fs.DirEntry, error) {
		return []fs.DirEntry{brokenEntry{DirEntry: entries[0]}, entries[0]}, nil
	}

	var logs bytes.Buffer
	prune(dir, now, slog.New(slog.NewTextHandler(&logs, nil)), readDir, os.Remove)

	_, err = os.Stat(path)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("pruning stopped after info failure: %v", err)
	}

	assertWarnings(t, logs.String(), 1)
}
