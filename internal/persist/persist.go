// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

// Package persist saves a session's subscriptions on disk, so that the next
// awrel process of the session can restore them. The session key picks the
// file. Each change locks the file, rereads it and applies only its own
// change, so two processes that share a key don't overwrite each other.
package persist

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"time"

	"go.tbhb.dev/awrel/internal/protocol"
)

const (
	// SessionEnv names the session key in the environment.
	SessionEnv = "AWREL_SESSION_ID"
	// PersistEnv turns persistence off when its value is 0.
	PersistEnv = "AWREL_PERSIST"
	stateEnv   = "XDG_STATE_HOME"
	homeEnv    = "HOME"

	fileVersion = 1
	// MaxAge is the age past which Prune deletes a session file.
	MaxAge   = 14 * 24 * time.Hour
	dirMode  = 0o700
	fileMode = 0o600
	// fileSuffix ends a session file's name. The suffix for the lock beside it
	// follows the session file's whole name.
	fileSuffix = ".json"
	lockSuffix = ".lock"
)

// harnessEnvs returns the variables that agent harnesses set to name their
// session, in the order Key checks them. Another harness takes one more line.
func harnessEnvs() []string {
	return []string{"CLAUDE_CODE_SESSION_ID"}
}

var errNoHome = errors.New("neither XDG_STATE_HOME nor HOME is set")

// file is the JSON form of a session file.
//
//nolint:govet // field order matches the file's JSON
type file struct {
	Version       int                     `json:"version"`
	Endpoint      string                  `json:"endpoint"`
	Subscriptions []protocol.Subscription `json:"subscriptions"`
}

// Key returns the flag value if there is one, else the value of
// AWREL_SESSION_ID, else the value of the first harness variable with a value.
// With none of them, it returns the empty string.
func Key(flagValue string, getenv func(string) string) string {
	if flagValue != "" {
		return flagValue
	}

	for _, name := range append([]string{SessionEnv}, harnessEnvs()...) {
		value := getenv(name)
		if value != "" {
			return value
		}
	}

	return ""
}

// Store is one session's file.
type Store struct {
	logger     *slog.Logger
	createTemp func(dir, pattern string) (*os.File, error)
	flock      func(fd, how int) error
	write      func(f *os.File, data []byte) error
	path       string
	endpoint   string
}

// Open returns the Store for the session key and the relay endpoint, after it
// refreshes the session's own file and prunes old session files. It returns nil when persistence is off, when the
// key is empty, or when no state directory can be found. In the last case it
// logs a warning.
func Open(
	key, endpoint string, getenv func(string) string, logger *slog.Logger, now time.Time,
) *Store {
	if key == "" || getenv(PersistEnv) == "0" {
		return nil
	}

	dir, err := stateDir(getenv)
	if err != nil {
		logger.Warn("session subscriptions not saved", slog.Any("error", err))

		return nil
	}

	store := newStore(dir, key, endpoint, logger)

	// Refresh first, so that the session's own restart never prunes its file.
	store.touch(now)
	Prune(dir, now, logger)

	return store
}

// stateDir is $XDG_STATE_HOME/awrel/sessions, with ~/.local/state when
// XDG_STATE_HOME is unset or empty.
func stateDir(getenv func(string) string) (string, error) {
	state := getenv(stateEnv)
	if state == "" {
		home := getenv(homeEnv)
		if home == "" {
			return "", errNoHome
		}

		state = filepath.Join(home, ".local", "state")
	}

	return filepath.Join(state, "awrel", "sessions"), nil
}

func newStore(dir, key, endpoint string, logger *slog.Logger) *Store {
	sum := sha256.Sum256([]byte(key))

	return &Store{
		logger:     logger,
		createTemp: os.CreateTemp,
		flock:      flock,
		write:      writeAll,
		path:       filepath.Join(dir, hex.EncodeToString(sum[:])+fileSuffix),
		endpoint:   endpoint,
	}
}

// Prune deletes the session files in dir that are older than MaxAge at now,
// and the locks beside them. A failure logs a warning.
func Prune(dir string, now time.Time, logger *slog.Logger) {
	prune(dir, now, logger, os.ReadDir)
}

// prune is Prune with the directory read passed in, so a test can make the
// directory read or an entry's Info fail without depending on OS permissions.
func prune(dir string, now time.Time, logger *slog.Logger, readDir func(string) ([]fs.DirEntry, error)) {
	entries, err := readDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return
	}

	if err != nil {
		logger.Warn("prune session files", slog.Any("error", err))

		return
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != fileSuffix {
			continue
		}

		info, infoErr := entry.Info()
		if infoErr != nil {
			logger.Warn("prune session files", slog.Any("error", infoErr))

			continue
		}

		if now.Sub(info.ModTime()) <= MaxAge {
			continue
		}

		path := filepath.Join(dir, entry.Name())

		err = errors.Join(os.Remove(path), ignoreMissing(os.Remove(path+lockSuffix)))
		if err != nil {
			logger.Warn("prune session files", slog.Any("error", err))
		}
	}
}

// ignoreMissing drops the error of removing a file that was already gone.
func ignoreMissing(err error) error {
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}

	return err
}

// Load returns the saved subscriptions. It returns none for a missing file or
// a file of another version or endpoint. A file that fails to read or parse
// also yields none, with a warning.
func (s *Store) Load() []protocol.Subscription {
	subs, err := s.read()
	if err != nil {
		s.logger.Warn("ignoring the saved subscriptions", slog.String("path", s.path), slog.Any("error", err))

		return nil
	}

	return s.valid(subs)
}

// Add saves sub in the file, unless the file has it already.
func (s *Store) Add(sub protocol.Subscription) error {
	return s.update(func(subs []protocol.Subscription) []protocol.Subscription {
		if slices.ContainsFunc(subs, sub.Equal) {
			return subs
		}

		return append(subs, sub)
	})
}

// Touch sets the file's modification time to now, so that Prune keeps the
// file of a live session. A missing file is fine. Another failure logs a
// warning.
func (s *Store) Touch() {
	s.touch(time.Now())
}

// Remove deletes sub from the file.
func (s *Store) Remove(sub protocol.Subscription) error {
	return s.update(func(subs []protocol.Subscription) []protocol.Subscription {
		return slices.DeleteFunc(subs, sub.Equal)
	})
}

func (s *Store) touch(now time.Time) {
	err := os.Chtimes(s.path, now, now)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		s.logger.Warn("refresh the session file's age", slog.String("path", s.path), slog.Any("error", err))
	}
}

// read returns the file's subscriptions: none when the file is missing or
// belongs to another version or endpoint.
func (s *Store) read() ([]protocol.Subscription, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("read %s: %w", s.path, err)
	}

	var saved file

	err = json.Unmarshal(data, &saved)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", s.path, err)
	}

	if saved.Version != fileVersion || saved.Endpoint != s.endpoint {
		return nil, nil
	}

	return saved.Subscriptions, nil
}

// valid keeps the subscriptions that pass the filter checks. The relay
// refuses a bad filter, and a refused filter would stop every retry.
func (s *Store) valid(subs []protocol.Subscription) []protocol.Subscription {
	return slices.DeleteFunc(subs, func(sub protocol.Subscription) bool {
		err := sub.ValidateFilters()
		if err != nil {
			s.logger.Warn("dropping a saved subscription", slog.String("path", s.path), slog.Any("error", err))
		}

		return err != nil
	})
}

// update applies change to the file's subscriptions under an exclusive lock
// and writes the result through a temporary file. A file that fails to read is
// replaced, since Load would return nothing from it.
func (s *Store) update(change func([]protocol.Subscription) []protocol.Subscription) error {
	dir := filepath.Dir(s.path)

	err := os.MkdirAll(dir, dirMode)
	if err != nil {
		return fmt.Errorf("create %s: %w", dir, err)
	}

	unlock, err := s.lock()
	if err != nil {
		return err
	}
	defer unlock()

	current, err := s.read()
	if err != nil {
		s.logger.Warn("replacing the unreadable saved subscriptions", slog.Any("error", err))
	}

	return s.replace(dir, mustMarshal(file{
		Version:       fileVersion,
		Endpoint:      s.endpoint,
		Subscriptions: change(slices.Clone(current)),
	}))
}

// mustMarshal encodes value, which consists of strings, numbers and lists of
// them, so the encoding can't fail.
func mustMarshal(value any) []byte {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}

	return data
}

// replace writes data to a temporary file in dir, then renames the file over
// the session file. CreateTemp makes the file readable by its owner only.
func (s *Store) replace(dir string, data []byte) error {
	temp, err := s.createTemp(dir, "session-*.tmp")
	if err != nil {
		return fmt.Errorf("create a temporary file: %w", err)
	}

	err = s.write(temp, data)
	if err == nil {
		err = rename(temp.Name(), s.path)
	}

	if err != nil {
		return errors.Join(fmt.Errorf("write %s: %w", s.path, err), os.Remove(temp.Name()))
	}

	return nil
}

// writeAll writes data to f and closes it.
func writeAll(f *os.File, data []byte) error {
	_, err := f.Write(data)

	return errors.Join(err, f.Close())
}

// lock takes the exclusive lock beside the session file and returns the
// function that releases it.
func (s *Store) lock() (func(), error) {
	path := s.path + lockSuffix

	lockFile, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, fileMode)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}

	err = lockExclusive(lockFile, s.flock)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("lock %s: %w", path, err), lockFile.Close())
	}

	// Closing the file releases the lock. The close error says nothing more.
	return func() { _ = lockFile.Close() }, nil
}
