// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

// Package logfile copies client logs to a bounded file while preserving stderr.
// Open returns a writer whose result always reflects the write to stderr. A
// file failure produces one warning directly on stderr and stops further file
// writes. Each file has one backup, which rotation replaces. Writes larger
// than the cap span successive files, so even a large record can't
// grow either file past the cap.
package logfile

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.tbhb.dev/awrel/internal/persist"
)

const (
	maxBytes = 1024 * 1024
	dirMode  = 0o700
	fileMode = 0o600
)

// Open returns a writer that always writes to stderr, a close function, and
// the path or a short explanation of why the file is unavailable.
func Open(key string, getenv func(string) string, stderr io.Writer, now time.Time) (io.Writer, func(), string) {
	logger := slog.New(slog.NewTextHandler(stderr, nil))

	base, err := persist.StateDir(getenv)
	if err != nil {
		logger.Warn("client log file unavailable", "error", err)

		return stderr, func() {}, "unavailable (state directory unavailable)"
	}

	name := persist.Name(key)
	if key == "" {
		name = "pid-" + strconv.Itoa(os.Getpid())
	}

	dir := filepath.Join(base, "logs")
	path := filepath.Join(dir, name+".log")

	writer, err := open(path, stderr, logger, now)
	if err != nil {
		logger.Warn("client log file unavailable", "error", err)

		return stderr, func() {}, "unavailable (cannot open log file)"
	}

	return writer, writer.close, path
}

type writer struct {
	stderr   io.Writer
	file     *os.File
	logger   *slog.Logger
	openFile func(string, int, fs.FileMode) (*os.File, error)
	rename   func(string, string) error
	path     string
	mu       sync.Mutex
	stopped  bool
	size     int64
}

func open(path string, stderr io.Writer, logger *slog.Logger, now time.Time) (*writer, error) {
	dir := filepath.Dir(path)

	err := os.MkdirAll(dir, dirMode)
	if err != nil {
		return nil, fmt.Errorf("create log directory: %w", err)
	}

	prune(dir, now, logger, os.ReadDir, os.Remove)

	file, size, err := appendFile(path, os.OpenFile)
	if err != nil {
		return nil, err
	}

	return &writer{
		stderr: stderr, file: file, logger: logger, openFile: os.OpenFile,
		rename: os.Rename, path: path, mu: sync.Mutex{}, stopped: false, size: size,
	}, nil
}

func appendFile(path string, openFile func(string, int, fs.FileMode) (*os.File, error)) (*os.File, int64, error) {
	file, err := openFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, fileMode)
	if err != nil {
		return nil, 0, fmt.Errorf("open log file: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		return nil, 0, fmt.Errorf("stat log file: %w", errors.Join(err, file.Close()))
	}

	return file, info.Size(), nil
}

// Write copies each record to stderr and the file. Warnings use stderr directly to
// avoid recursing through the failed writer.
func (w *writer) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	written, err := w.stderr.Write(data)
	if !w.stopped {
		w.fail(w.write(data))
	}

	if err != nil {
		return written, fmt.Errorf("write stderr: %w", err)
	}

	return written, nil
}

func (w *writer) write(data []byte) error {
	for chunk := range slices.Chunk(data, maxBytes) {
		if w.size+int64(len(chunk)) > maxBytes {
			err := w.rotate()
			if err != nil {
				return err
			}
		}

		written, err := w.file.Write(chunk)
		w.size += int64(written)

		if err != nil {
			return fmt.Errorf("write log file: %w", err)
		}
	}

	return nil
}

func (w *writer) rotate() error {
	err := w.file.Close()
	if err != nil {
		return fmt.Errorf("close log file: %w", err)
	}
	// Rename replaces an existing backup on Unix and Windows.
	err = w.rename(w.path, w.path+".1")
	if err != nil {
		return fmt.Errorf("rotate log file: %w", err)
	}

	file, _, err := appendFile(w.path, w.openFile)
	if err != nil {
		return err
	}

	w.file = file
	w.size = 0

	return nil
}

func (w *writer) fail(err error) {
	if err != nil && !w.stopped {
		w.logger.Warn("client log file stopped", "error", err)
		w.stopped = true
	}
}

func (w *writer) close() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.fail(w.file.Close())
}

// prune follows the session-file pruning shape, with filesystem operations
// passed in so failures can be tested without depending on permissions.
func prune(dir string, now time.Time, logger *slog.Logger,
	readDir func(string) ([]fs.DirEntry, error), remove func(string) error,
) {
	entries, err := readDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return
	}

	if err != nil {
		logger.Warn("prune log files", "error", err)

		return
	}

	for _, entry := range entries {
		if entry.IsDir() || (!strings.HasSuffix(entry.Name(), ".log") && !strings.HasSuffix(entry.Name(), ".log.1")) {
			continue
		}

		info, infoErr := entry.Info()
		if infoErr != nil {
			logger.Warn("prune log files", "error", infoErr)

			continue
		}

		if now.Sub(info.ModTime()) <= persist.MaxAge {
			continue
		}

		err = remove(filepath.Join(dir, entry.Name()))
		if err != nil {
			logger.Warn("prune log files", "error", err)
		}
	}
}
