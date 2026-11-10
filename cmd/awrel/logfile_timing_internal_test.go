// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.tbhb.dev/awrel/internal/logfile"
)

// These counters time synchronous file writes separately from stderr
// and time waiting for serialized writes. In-flight operations and stacks on failure
// distinguish a blocked write from open, close, or a later protocol wait.
type logTimings struct {
	mu      sync.Mutex
	opened  time.Duration
	closed  time.Duration
	total   time.Duration
	slowest time.Duration
	writes  int
	active  int
	opening bool
	closing bool
	stderr  atomic.Int64
	waited  time.Duration
}

func timedLogOpener(t *testing.T) logOpener {
	t.Helper()

	timings := &logTimings{
		mu: sync.Mutex{}, opened: 0, closed: 0, total: 0,
		slowest: 0, writes: 0, active: 0, opening: false, closing: false, stderr: atomic.Int64{}, waited: 0,
	}

	t.Cleanup(func() { timings.report(t) })

	return timings.open
}

func (m *logTimings) open(
	key string,
	getenv func(string) string,
	stderr io.Writer,
	now time.Time,
) (io.Writer, func(), string) {
	m.mu.Lock()
	m.opening = true
	m.mu.Unlock()

	started := time.Now()
	sink, closeLog, path := logfile.Open(key, getenv, &timedStderr{writer: stderr, elapsed: &m.stderr}, now)
	m.mu.Lock()
	m.opened = time.Since(started)
	m.opening = false
	m.mu.Unlock()

	return &timedLog{writer: sink, timings: m, mu: sync.Mutex{}}, func() {
		m.mu.Lock()
		m.closing = true
		m.mu.Unlock()

		closeStarted := time.Now()

		closeLog()
		m.mu.Lock()
		m.closed = time.Since(closeStarted)
		m.closing = false
		m.mu.Unlock()
	}, path
}

type timedStderr struct {
	writer  io.Writer
	elapsed *atomic.Int64
}

func (s *timedStderr) Write(data []byte) (int, error) {
	started := time.Now()
	written, err := s.writer.Write(data)
	s.elapsed.Add(int64(time.Since(started)))

	if err != nil {
		return written, fmt.Errorf("timed stderr write: %w", err)
	}

	return written, nil
}

type timedLog struct {
	writer  io.Writer
	timings *logTimings
	mu      sync.Mutex
}

func (w *timedLog) Write(data []byte) (int, error) {
	timings := w.timings
	timings.mu.Lock()
	timings.active++
	timings.mu.Unlock()

	queued := time.Now()

	w.mu.Lock()
	defer w.mu.Unlock()

	started := time.Now()
	stderrBefore := timings.stderr.Load()
	written, err := w.writer.Write(data)
	elapsed := time.Since(started) - time.Duration(timings.stderr.Load()-stderrBefore)

	timings.mu.Lock()
	timings.waited += started.Sub(queued)
	timings.total += elapsed
	timings.slowest = max(timings.slowest, elapsed)
	timings.writes++
	timings.active--
	timings.mu.Unlock()

	if err != nil {
		return written, fmt.Errorf("timed log write: %w", err)
	}

	return written, nil
}

func (m *logTimings) report(t *testing.T) {
	t.Helper()
	m.mu.Lock()
	t.Logf(
		"log file timing: open=%s opening=%t writes=%d file_total=%s slowest=%s "+
			"stderr=%s lock_wait=%s active=%d close=%s closing=%t",
		m.opened,
		m.opening,
		m.writes,
		m.total,
		m.slowest,
		time.Duration(m.stderr.Load()),
		m.waited,
		m.active,
		m.closed,
		m.closing,
	)
	m.mu.Unlock()

	if t.Failed() {
		stacks := make([]byte, 1<<20)
		t.Logf("goroutines at failure:\n%s", stacks[:runtime.Stack(stacks, true)])
	}
}
