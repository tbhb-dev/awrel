// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package persist

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.tbhb.dev/awrel/internal/protocol"
)

const (
	testEndpoint = "wss://relay.example/connect"
	testKey      = "session-one"
	// harnessVar is the one variable in today's detection list.
	harnessVar = "CLAUDE_CODE_SESSION_ID"

	fromEnv     = "from-env"
	fromHarness = "from-harness"
	windowsOS   = "windows"
	noModesNote = "Windows has no Unix modes"
)

var errBoom = errors.New("boom")

// lockedBuffer is a log sink that tests read while the code under test writes.
type lockedBuffer struct {
	buf bytes.Buffer
	mu  sync.Mutex
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p) //nolint:wrapcheck // a bytes.Buffer never fails
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

func newLogs() (*slog.Logger, *lockedBuffer) {
	logs := &lockedBuffer{buf: bytes.Buffer{}, mu: sync.Mutex{}}

	return slog.New(slog.NewTextHandler(logs, nil)), logs
}

func env(vars map[string]string) func(string) string {
	return func(key string) string { return vars[key] }
}

func sub(event, repo string) protocol.Subscription {
	return protocol.Subscription{Provider: "github", Event: event, Repository: repo}
}

// testStore returns a Store in a new directory, with its logs.
func testStore(t *testing.T) (*Store, *lockedBuffer) {
	t.Helper()

	logger, logs := newLogs()

	return newStore(filepath.Join(t.TempDir(), "awrel", "sessions"), testKey, testEndpoint, logger), logs
}

func mustLoad(t *testing.T, store *Store) []protocol.Subscription {
	t.Helper()

	subs, err := store.read()
	if err != nil {
		t.Fatal(err)
	}

	return subs
}

// sameSubs reports whether got is exactly want, in order.
func sameSubs(got []protocol.Subscription, want ...protocol.Subscription) bool {
	return slices.EqualFunc(got, want, protocol.Subscription.Equal)
}

// wantCause fails the test unless err wraps an error of type T.
func wantCause[T error](t *testing.T, err error) {
	t.Helper()

	_, ok := errors.AsType[T](err)
	if !ok {
		var zero T

		t.Errorf("err = %v, want a %T inside", err, zero)
	}
}

// savedFile is the JSON of a session file for an endpoint, with the given
// version and the subscriptions' JSON.
func savedFile(version int, endpoint, subs string) string {
	return `{"version":` + strconv.Itoa(version) + `,"endpoint":"` + endpoint + `","subscriptions":[` + subs + `]}`
}

func mustWrite(t *testing.T, path, text string) {
	t.Helper()

	err := os.MkdirAll(filepath.Dir(path), dirMode)
	if err != nil {
		t.Fatal(err)
	}

	err = os.WriteFile(path, []byte(text), fileMode)
	if err != nil {
		t.Fatal(err)
	}
}

func TestKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		flag string
		vars map[string]string
		want string
	}{
		{"flag", "from-flag", map[string]string{SessionEnv: fromEnv, harnessVar: fromHarness}, "from-flag"},
		{"environment", "", map[string]string{SessionEnv: fromEnv, harnessVar: fromHarness}, fromEnv},
		{"harness", "", map[string]string{harnessVar: fromHarness}, fromHarness},
		{"none", "", map[string]string{"OTHER": "x"}, ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if got := Key(test.flag, env(test.vars)); got != test.want {
				t.Errorf("Key = %q, want %q", got, test.want)
			}
		})
	}
}

func TestStateDir(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		vars    map[string]string
		want    string
		wantErr bool
	}{
		{"state home", map[string]string{stateEnv: "/s", homeEnv: "/h"}, "/s/awrel/sessions", false},
		{"empty state home", map[string]string{stateEnv: "", homeEnv: "/h"}, "/h/.local/state/awrel/sessions", false},
		{"no home", map[string]string{}, "", true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := stateDir(env(test.vars))
			if (err != nil) != test.wantErr || filepath.ToSlash(got) != test.want {
				t.Errorf("stateDir = %q, %v; want %q, error %v", got, err, test.want, test.wantErr)
			}
		})
	}
}

func TestNewStoreNamesTheFileByHash(t *testing.T) {
	t.Parallel()

	logger, _ := newLogs()
	store := newStore("/d", "abc", testEndpoint, logger)

	// sha256("abc")
	want := filepath.Join("/d", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad.json")
	if store.path != want {
		t.Errorf("path = %q, want %q", store.path, want)
	}
}

func TestOpen(t *testing.T) {
	t.Parallel()

	now := time.Now()

	t.Run("no key", func(t *testing.T) {
		t.Parallel()

		logger, _ := newLogs()
		if Open("", testEndpoint, env(map[string]string{stateEnv: t.TempDir()}), logger, now) != nil {
			t.Error("Open without a key returned a Store")
		}
	})

	t.Run("opted out", func(t *testing.T) {
		t.Parallel()

		state := t.TempDir()
		stale := filepath.Join(state, "awrel", "sessions", "old.json")
		mustWrite(t, stale, "{}")
		age(t, stale, now)

		logger, _ := newLogs()
		vars := map[string]string{PersistEnv: "0", stateEnv: state}

		if Open(testKey, testEndpoint, env(vars), logger, now) != nil {
			t.Error("Open with AWREL_PERSIST=0 returned a Store")
		}

		_, err := os.Stat(stale)
		if err != nil {
			t.Errorf("opting out still pruned: %v", err)
		}
	})

	t.Run("no state directory", func(t *testing.T) {
		t.Parallel()

		logger, logs := newLogs()
		if Open(testKey, testEndpoint, env(map[string]string{}), logger, now) != nil {
			t.Error("Open without a state directory returned a Store")
		}

		if !strings.Contains(logs.String(), "not saved") {
			t.Errorf("no warning:\n%s", logs.String())
		}
	})

	t.Run("opens and prunes", func(t *testing.T) {
		t.Parallel()

		state := t.TempDir()
		stale := filepath.Join(state, "awrel", "sessions", "old.json")
		mustWrite(t, stale, "{}")
		age(t, stale, now)

		logger, _ := newLogs()

		vars := map[string]string{PersistEnv: "1", stateEnv: state}

		store := Open(testKey, testEndpoint, env(vars), logger, now)
		if store == nil {
			t.Fatal("Open returned no Store")
		}

		_, err := os.Stat(stale)
		if !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("the stale file stayed: %v", err)
		}
	})
}

// age sets the file's modification time to just past MaxAge before now.
func age(t *testing.T, path string, now time.Time) {
	t.Helper()

	old := now.Add(-MaxAge - time.Hour)

	err := os.Chtimes(path, old, old)
	if err != nil {
		t.Fatal(err)
	}
}

func TestAddRemoveLoad(t *testing.T) {
	t.Parallel()

	store, logs := testStore(t)

	if got := store.Load(); got != nil {
		t.Errorf("Load of a missing file = %v", got)
	}

	first, second := sub("issues", "o/a"), sub("push", "o/b")

	for _, add := range []protocol.Subscription{first, second, first} {
		err := store.Add(add)
		if err != nil {
			t.Fatal(err)
		}
	}

	if got := store.Load(); !sameSubs(got, first, second) {
		t.Errorf("Load = %v, want %v and %v", got, first, second)
	}

	err := store.Remove(first)
	if err != nil {
		t.Fatal(err)
	}

	err = store.Remove(sub("issues", "o/missing"))
	if err != nil {
		t.Fatal(err)
	}

	if got := store.Load(); !sameSubs(got, second) {
		t.Errorf("Load after Remove = %v, want %v", got, second)
	}

	if logs.String() != "" {
		t.Errorf("unexpected logs:\n%s", logs.String())
	}
}

func TestFileContents(t *testing.T) {
	t.Parallel()

	store, _ := testStore(t)

	err := store.Add(protocol.Subscription{
		Provider: "github", Event: "check_run", Repository: "o/r",
		Action: []string{"completed"}, Conclusion: []string{"failure"}, Name: "build", WorkflowPath: "",
	})
	if err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(store.path)
	if err != nil {
		t.Fatal(err)
	}

	want := `{"version":1,"endpoint":"wss://relay.example/connect","subscriptions":[` +
		`{"provider":"github","event":"check_run","repository":"o/r",` +
		`"action":["completed"],"conclusion":["failure"],"name":"build"}]}`
	if string(data) != want {
		t.Errorf("file = %s\nwant   %s", data, want)
	}
}

func TestModes(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == windowsOS {
		t.Skip(noModesNote)
	}

	store, _ := testStore(t)

	err := store.Add(sub("push", "o/r"))
	if err != nil {
		t.Fatal(err)
	}

	// The modes are written out, so a change to the constants fails the test.
	for path, want := range map[string]fs.FileMode{
		filepath.Dir(store.path): 0o700,
		store.path:               0o600,
		store.path + lockSuffix:  0o600,
	} {
		info, statErr := os.Stat(path)
		if statErr != nil {
			t.Fatal(statErr)
		}

		if got := info.Mode().Perm(); got != want {
			t.Errorf("%s mode = %o, want %o", path, got, want)
		}
	}

	noTemporaryFiles(t, store)
}

func TestLoadIgnores(t *testing.T) {
	t.Parallel()

	const (
		push    = `{"provider":"github","event":"push","repository":"o/r"}`
		badPush = `{"provider":"github","event":"push","repository":"o/r","name":"x"}`
	)

	tests := []struct {
		name    string
		content string
		warns   string
	}{
		{"other version", savedFile(2, testEndpoint, push), ""},
		{"no version", `{"endpoint":"` + testEndpoint + `","subscriptions":[` + push + `]}`, ""},
		{"other endpoint", savedFile(1, "wss://other.example/connect", push), ""},
		{"invalid JSON", `{"version":1,`, "ignoring the saved subscriptions"},
		{"bad filter", savedFile(1, testEndpoint, badPush), "dropping a saved subscription"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store, logs := testStore(t)
			mustWrite(t, store.path, test.content)

			if got := store.Load(); len(got) != 0 {
				t.Errorf("Load = %v, want none", got)
			}

			if test.warns == "" && logs.String() != "" {
				t.Errorf("unexpected warning:\n%s", logs.String())
			}

			if !strings.Contains(logs.String(), test.warns) {
				t.Errorf("logs lack %q:\n%s", test.warns, logs.String())
			}
		})
	}
}

func TestLoadKeepsValidEntries(t *testing.T) {
	t.Parallel()

	store, _ := testStore(t)
	mustWrite(t, store.path, savedFile(1, testEndpoint,
		`{"provider":"github","event":"push","repository":"o/r"},`+
			`{"provider":"github","event":"push","repository":"o/r","name":"x"},`+
			`{"provider":"github","event":"issues","repository":"o/r"}`))

	if got := store.Load(); len(got) != 2 || got[0].Event != "push" || got[1].Event != "issues" {
		t.Errorf("Load = %v, want push and issues", got)
	}
}

func TestLoadWarnsWhenTheFileCannotBeRead(t *testing.T) {
	t.Parallel()

	store, logs := testStore(t)

	// A directory in place of the file makes the read fail.
	err := os.MkdirAll(store.path, dirMode)
	if err != nil {
		t.Fatal(err)
	}

	if got := store.Load(); got != nil {
		t.Errorf("Load = %v", got)
	}

	if !strings.Contains(logs.String(), "ignoring the saved subscriptions") {
		t.Errorf("no warning:\n%s", logs.String())
	}
}

func TestUpdateReplacesAnUnusableFile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		warns   bool
	}{
		{"invalid", "not json", true},
		{
			"other endpoint",
			savedFile(1, "wss://other.example/connect", `{"provider":"github","event":"push","repository":"o/old"}`),
			false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store, logs := testStore(t)
			mustWrite(t, store.path, test.content)

			err := store.Add(sub("issues", "o/new"))
			if err != nil {
				t.Fatal(err)
			}

			if got := mustLoad(t, store); len(got) != 1 || got[0].Repository != "o/new" {
				t.Errorf("file has %v, want only o/new", got)
			}

			warned := strings.Contains(logs.String(), "replacing the unreadable saved subscriptions")
			if warned != test.warns {
				t.Errorf("warned = %v, want %v; logs:\n%s", warned, test.warns, logs.String())
			}
		})
	}
}

func TestReadKeepsItsCause(t *testing.T) {
	t.Parallel()

	t.Run("unreadable", func(t *testing.T) {
		t.Parallel()

		store, _ := testStore(t)

		err := os.MkdirAll(store.path, dirMode)
		if err != nil {
			t.Fatal(err)
		}

		_, err = store.read()
		if err == nil || !strings.Contains(err.Error(), "read ") {
			t.Errorf("err = %v, want a read error", err)
		}

		wantCause[*fs.PathError](t, err)
	})

	t.Run("invalid", func(t *testing.T) {
		t.Parallel()

		store, _ := testStore(t)
		mustWrite(t, store.path, "not json")

		_, err := store.read()
		if err == nil || !strings.Contains(err.Error(), "parse ") {
			t.Errorf("err = %v, want a parse error", err)
		}

		wantCause[*json.SyntaxError](t, err)
	})
}

// TestProcessesApplyOnlyTheirOwnChange is two processes that share a key: each
// loaded the file before the other wrote, and neither may undo the other.
func TestProcessesApplyOnlyTheirOwnChange(t *testing.T) {
	t.Parallel()

	one, _ := testStore(t)
	two := newStore(filepath.Dir(one.path), testKey, testEndpoint, one.logger)

	kept, gone, added := sub("push", "o/kept"), sub("push", "o/gone"), sub("issues", "o/added")

	for _, seed := range []protocol.Subscription{kept, gone} {
		err := one.Add(seed)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Both processes know kept and gone. One removes gone, two adds added.
	for _, step := range []func() error{
		func() error { return one.Remove(gone) },
		func() error { return two.Add(added) },
	} {
		err := step()
		if err != nil {
			t.Fatal(err)
		}
	}

	if got := mustLoad(t, one); !sameSubs(got, kept, added) {
		t.Errorf("file has %v, want kept and added", got)
	}
}

func TestConcurrentAddsAreAllKept(t *testing.T) {
	t.Parallel()

	first, _ := testStore(t)

	const writers = 16

	var group sync.WaitGroup

	for i := range writers {
		// Each writer is its own process with its own Store.
		store := newStore(filepath.Dir(first.path), testKey, testEndpoint, first.logger)

		group.Go(func() {
			err := store.Add(sub("push", "o/r"+strings.Repeat("x", i+1)))
			if err != nil {
				t.Error(err)
			}
		})
	}

	group.Wait()

	if got := mustLoad(t, first); len(got) != writers {
		t.Errorf("file has %d subscriptions, want %d", len(got), writers)
	}
}

func TestUpdateFailsWithoutADirectory(t *testing.T) {
	t.Parallel()

	store, _ := testStore(t)
	// A file where the directory's parent should be.
	mustWrite(t, filepath.Dir(filepath.Dir(store.path)), "x")

	err := store.Add(sub("push", "o/r"))
	if err == nil || !strings.Contains(err.Error(), "create") {
		t.Errorf("err = %v", err)
	}

	wantCause[*fs.PathError](t, err)
}

func TestUpdateFailsWithoutALockFile(t *testing.T) {
	t.Parallel()

	store, _ := testStore(t)

	err := os.MkdirAll(store.path+lockSuffix, dirMode)
	if err != nil {
		t.Fatal(err)
	}

	err = store.Add(sub("push", "o/r"))
	if err == nil || !strings.Contains(err.Error(), "open") {
		t.Errorf("err = %v", err)
	}

	wantCause[*fs.PathError](t, err)
}

func TestUpdateFailsWhenTheLockIsRefused(t *testing.T) {
	t.Parallel()

	store, _ := testStore(t)
	store.flock = func(int, int) error { return errBoom }

	err := store.Add(sub("push", "o/r"))
	if !errors.Is(err, errBoom) {
		t.Errorf("err = %v, want the flock error", err)
	}
}

func TestUpdateFailsWithoutATemporaryFile(t *testing.T) {
	t.Parallel()

	store, _ := testStore(t)
	// The lock file exists, so only the temporary file needs a writable directory.
	mustWrite(t, store.path+lockSuffix, "")
	readOnly(t, filepath.Dir(store.path))

	err := store.Add(sub("push", "o/r"))
	if err == nil || !strings.Contains(err.Error(), "temporary") {
		t.Errorf("err = %v", err)
	}

	wantCause[*fs.PathError](t, err)
}

func TestUpdateFailsWhenTheRenameIsRefused(t *testing.T) {
	t.Parallel()

	store, _ := testStore(t)
	// A directory with a file inside can't be replaced by a file.
	mustWrite(t, filepath.Join(store.path, "inner"), "x")

	err := store.Add(sub("push", "o/r"))
	if err == nil || !strings.Contains(err.Error(), "write") {
		t.Errorf("err = %v", err)
	}

	wantCause[*os.LinkError](t, err)
	noTemporaryFiles(t, store)
}

func TestUpdateFailsWhenTheWriteFails(t *testing.T) {
	t.Parallel()

	store, _ := testStore(t)
	store.write = func(f *os.File, _ []byte) error { return errors.Join(errBoom, f.Close()) }

	err := store.Add(sub("push", "o/r"))
	if !errors.Is(err, errBoom) {
		t.Errorf("err = %v, want the write error", err)
	}

	noTemporaryFiles(t, store)

	_, statErr := os.Stat(store.path)
	if !errors.Is(statErr, fs.ErrNotExist) {
		t.Errorf("a failed write still made the file: %v", statErr)
	}
}

// noTemporaryFiles fails the test if a temporary file is left beside the
// session file.
func noTemporaryFiles(t *testing.T, store *Store) {
	t.Helper()

	entries, err := os.ReadDir(filepath.Dir(store.path))
	if err != nil {
		t.Fatal(err)
	}

	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".tmp") {
			t.Errorf("temporary file left: %s", entry.Name())
		}
	}
}

// readOnly makes dir refuse new entries until the test ends. It skips the test
// on Windows, which has no Unix modes.
func readOnly(t *testing.T, dir string) {
	t.Helper()

	if runtime.GOOS == windowsOS {
		t.Skip(noModesNote)
	}

	err := os.Chmod(dir, 0o500) //nolint:gosec // a directory needs its execute bit
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		restoreErr := os.Chmod(dir, dirMode)
		if restoreErr != nil {
			t.Error(restoreErr)
		}
	})
}

func TestWriteAllReportsAClosedFile(t *testing.T) {
	t.Parallel()

	temp, err := os.CreateTemp(t.TempDir(), "x")
	if err != nil {
		t.Fatal(err)
	}

	err = temp.Close()
	if err != nil {
		t.Fatal(err)
	}

	if writeAll(temp, []byte("x")) == nil {
		t.Error("writeAll on a closed file succeeded")
	}
}

func TestMustMarshalPanics(t *testing.T) {
	t.Parallel()

	defer func() {
		if recover() == nil {
			t.Error("mustMarshal of a channel did not panic")
		}
	}()

	mustMarshal(make(chan int))
}

// fakeEntry is a directory entry that fails when asked for its Info.
type fakeEntry struct {
	fs.DirEntry

	name string
}

func (e fakeEntry) Name() string { return e.name }
func (fakeEntry) IsDir() bool    { return false }

func (fakeEntry) Info() (fs.FileInfo, error) { return nil, errBoom }

func TestPrune(t *testing.T) {
	t.Parallel()

	logger, logs := newLogs()
	dir := t.TempDir()
	now := time.Now()

	oldFile, newFile := filepath.Join(dir, "old.json"), filepath.Join(dir, "new.json")
	other, folder := filepath.Join(dir, "old.txt"), filepath.Join(dir, "dir.json")

	for _, path := range []string{oldFile, oldFile + lockSuffix, newFile, newFile + lockSuffix, other} {
		mustWrite(t, path, "{}")
	}

	err := os.Mkdir(folder, dirMode)
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range []string{oldFile, other, folder} {
		age(t, path, now)
	}

	Prune(dir, now, logger)

	gone := map[string]bool{oldFile: true, oldFile + lockSuffix: true}

	for _, path := range []string{oldFile, oldFile + lockSuffix, newFile, newFile + lockSuffix, other, folder} {
		_, statErr := os.Stat(path)

		exists := statErr == nil
		if exists == gone[path] {
			t.Errorf("%s exists = %v, want %v", filepath.Base(path), exists, !gone[path])
		}
	}

	if logs.String() != "" {
		t.Errorf("unexpected logs:\n%s", logs.String())
	}
}

// TestPruneKeepsFilesUpToMaxAge pins the limit at 14 days, in days not in the
// constant that the code uses.
func TestPruneKeepsFilesUpToMaxAge(t *testing.T) {
	t.Parallel()

	const day = 24 * time.Hour

	tests := []struct {
		name string
		age  time.Duration
		gone bool
	}{
		{"13.5 days", 13*day + day/2, false},
		{"exactly 14 days", 14 * day, false},
		{"14.5 days", 14*day + day/2, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			logger, _ := newLogs()
			now := time.Unix(1_800_000_000, 0)
			path := filepath.Join(t.TempDir(), "s.json")

			mustWrite(t, path, "{}")

			err := os.Chtimes(path, now.Add(-test.age), now.Add(-test.age))
			if err != nil {
				t.Fatal(err)
			}

			Prune(filepath.Dir(path), now, logger)

			if gone := !exists(path); gone != test.gone {
				t.Errorf("gone = %v, want %v", gone, test.gone)
			}
		})
	}
}

// exists reports whether path is there.
func exists(path string) bool {
	_, err := os.Stat(path)

	return err == nil
}

func TestPruneWithoutALock(t *testing.T) {
	t.Parallel()

	logger, logs := newLogs()
	dir := t.TempDir()
	now := time.Now()
	path := filepath.Join(dir, "old.json")

	mustWrite(t, path, "{}")
	age(t, path, now)

	Prune(dir, now, logger)

	if exists(path) {
		t.Error("the stale file stayed")
	}

	if logs.String() != "" {
		t.Errorf("a missing lock produced logs:\n%s", logs.String())
	}
}

func TestPruneMissingDirectoryIsQuiet(t *testing.T) {
	t.Parallel()

	logger, logs := newLogs()

	Prune(filepath.Join(t.TempDir(), "none"), time.Now(), logger)

	if logs.String() != "" {
		t.Errorf("logs:\n%s", logs.String())
	}
}

func TestPruneWarns(t *testing.T) {
	t.Parallel()

	t.Run("unreadable directory", func(t *testing.T) {
		t.Parallel()

		logger, logs := newLogs()
		file := filepath.Join(t.TempDir(), "file")
		mustWrite(t, file, "x")

		// A file where the directory should be.
		Prune(file, time.Now(), logger)

		if !strings.Contains(logs.String(), "prune session files") {
			t.Errorf("no warning:\n%s", logs.String())
		}
	})

	t.Run("entry without info", func(t *testing.T) {
		t.Parallel()

		logger, logs := newLogs()

		prune("/none", time.Now(), logger, func(string) ([]fs.DirEntry, error) {
			return []fs.DirEntry{fakeEntry{DirEntry: nil, name: "x.json"}}, nil
		})

		if !strings.Contains(logs.String(), "boom") {
			t.Errorf("no warning:\n%s", logs.String())
		}
	})

	t.Run("entry without info does not stop the scan", func(t *testing.T) {
		t.Parallel()

		logger, logs := newLogs()
		dir := t.TempDir()
		now := time.Now()
		path := filepath.Join(dir, "old.json")

		mustWrite(t, path, "{}")
		age(t, path, now)

		prune(dir, now, logger, func(name string) ([]fs.DirEntry, error) {
			entries, err := os.ReadDir(name)

			return append([]fs.DirEntry{fakeEntry{DirEntry: nil, name: "x.json"}}, entries...), err
		})

		if exists(path) {
			t.Error("the entry after the failing one was not pruned")
		}

		if !strings.Contains(logs.String(), "boom") {
			t.Errorf("no warning:\n%s", logs.String())
		}
	})

	t.Run("lock removal refused", func(t *testing.T) {
		t.Parallel()

		logger, logs := newLogs()
		dir := t.TempDir()
		now := time.Now()
		path := filepath.Join(dir, "old.json")

		mustWrite(t, path, "{}")
		age(t, path, now)
		// A directory with a file inside can't be removed like a file.
		mustWrite(t, filepath.Join(path+lockSuffix, "inner"), "x")

		Prune(dir, now, logger)

		if !strings.Contains(logs.String(), "prune session files") {
			t.Errorf("no warning:\n%s", logs.String())
		}
	})

	t.Run("removal refused", func(t *testing.T) {
		t.Parallel()

		logger, logs := newLogs()
		dir := t.TempDir()
		now := time.Now()
		path := filepath.Join(dir, "old.json")

		mustWrite(t, path, "{}")
		age(t, path, now)
		readOnly(t, dir)

		Prune(dir, now, logger)

		if !strings.Contains(logs.String(), "prune session files") {
			t.Errorf("no warning:\n%s", logs.String())
		}
	})
}

// TestHarnessDetectionList pins the harness variables, in the order Key
// checks them.
func TestHarnessDetectionList(t *testing.T) {
	t.Parallel()

	if got := harnessEnvs(); !slices.Equal(got, []string{harnessVar}) {
		t.Errorf("harnessEnvs = %v", got)
	}
}
