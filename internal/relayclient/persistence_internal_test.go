// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package relayclient

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.tbhb.dev/awrel/internal/protocol"
)

var errSaveFailed = errors.New("disk full")

// ledger is a list that a test reads while the code under test appends to it.
type ledger[T any] struct {
	items []T
	mu    sync.Mutex
}

func (l *ledger[T]) add(item T) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.items = append(l.items, item)
}

func (l *ledger[T]) seen() []T {
	l.mu.Lock()
	defer l.mu.Unlock()

	return slices.Clone(l.items)
}

// recordingStore is a Store that remembers its calls.
type recordingStore struct {
	err   error
	calls ledger[string]
}

func newRecordingStore(err error) *recordingStore {
	return &recordingStore{err: err, calls: ledger[string]{items: nil, mu: sync.Mutex{}}}
}

func (s *recordingStore) Add(sub protocol.Subscription) error    { return s.record("add", sub) }
func (s *recordingStore) Remove(sub protocol.Subscription) error { return s.record("remove", sub) }

func (s *recordingStore) record(verb string, sub protocol.Subscription) error {
	s.calls.add(verb + " " + sub.Repository)

	return s.err
}

// restoreRecorder collects the counts that OnRestored receives.
type restoreRecorder struct {
	counts ledger[int]
}

func newRestoreRecorder() *restoreRecorder {
	return &restoreRecorder{counts: ledger[int]{items: nil, mu: sync.Mutex{}}}
}

func (r *restoreRecorder) onRestored(_ context.Context, count int) {
	r.counts.add(count)
}

// wantSubscribes fails the test unless frames are subscribes of want, in order.
func wantSubscribes(t *testing.T, frames []frame, want ...protocol.Subscription) {
	t.Helper()

	if len(frames) < len(want) {
		t.Fatalf("frames = %+v, want %d subscribes", frames, len(want))
	}

	for i, sub := range want {
		got := frames[i]
		if got.Type != typeSubscribe || got.Subscription == nil || !got.Subscription.Equal(sub) {
			t.Errorf("frame %d = %+v, want a subscribe of %v", i, got, sub)
		}
	}
}

func TestSeededClientDialsAtOnce(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	recorder := newRestoreRecorder()
	store := newRecordingStore(nil)

	opts := quiet()
	opts.Initial = []protocol.Subscription{subA(), {Provider: providerGitHub, Event: pushEvent, Repository: "O/B"}}
	opts.OnRestored = recorder.onRestored
	opts.Store = store

	client, _ := relay.start(t, opts)

	// The seed alone causes the dial and the replay.
	eventually(t, "the restore notice", func() bool { return len(recorder.counts.seen()) == 1 })

	if got := recorder.counts.seen(); got[0] != 2 {
		t.Errorf("restored count = %d, want 2", got[0])
	}

	frames := relay.onConn(1)
	if len(frames) != 2 {
		t.Fatalf("frames on the first connection = %+v, want two subscribes", frames)
	}

	wantSubscribes(t, frames, subA(), subB())

	if got := mirrorOf(client); !sameSubscriptions(got, subA(), subB()) || got[1].Repository != "o/b" {
		t.Errorf("mirror = %v, want the canonical seed", got)
	}

	if calls := store.calls.seen(); len(calls) != 0 {
		t.Errorf("the restore replay saved %v, want nothing", calls)
	}
}

func TestRestoreIsAnnouncedOnce(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	recorder := newRestoreRecorder()

	opts := quiet()
	opts.Initial = []protocol.Subscription{subA()}
	opts.OnRestored = recorder.onRestored

	client, _ := relay.start(t, opts)

	eventually(t, "the restore notice", func() bool { return len(recorder.counts.seen()) == 1 })

	old := currentSession(client)

	relay.drop()
	awaitNewSession(t, client, old)

	if got := recorder.counts.seen(); len(got) != 1 {
		t.Errorf("OnRestored ran %d times after a reconnect, want once", len(got))
	}
}

func TestRestoreIsAnnouncedAfterAFailedReplay(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	relay.setIntercept(silenceSubscribesOn(1))

	recorder := newRestoreRecorder()

	opts := quiet()
	opts.Initial = []protocol.Subscription{subA()}
	opts.OnRestored = recorder.onRestored
	opts.RequestTimeout = 50 * time.Millisecond

	relay.start(t, opts)

	eventually(t, "the restore notice", func() bool { return len(recorder.counts.seen()) == 1 })

	if relay.connCount() < 2 {
		t.Errorf("connections = %d, want a retry after the silent replay", relay.connCount())
	}
}

func TestNoRestoreNoticeWithoutASeed(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	recorder := newRestoreRecorder()

	opts := quiet()
	opts.OnRestored = recorder.onRestored

	client, _ := relay.start(t, opts)
	awaitConnected(t, client)

	if got := recorder.counts.seen(); len(got) != 0 {
		t.Errorf("OnRestored ran with %v, want never", got)
	}
}

func TestNoRestoreNoticeWhenEveryRestoreIsRefused(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	relay.setRefuse(subA())

	recorder := newRestoreRecorder()

	opts := quiet()
	opts.Initial = []protocol.Subscription{subA()}
	opts.OnRestored = recorder.onRestored

	client, _ := relay.start(t, opts)
	awaitReady(t, client)

	if got := recorder.counts.seen(); len(got) != 0 {
		t.Errorf("OnRestored ran with %v, want never for an empty mirror", got)
	}

	if got := mirrorOf(client); len(got) != 0 {
		t.Errorf("mirror = %v, want it empty", got)
	}
}

func TestStoreSeesAcknowledgedChanges(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	relay.setRefuse(refusedSub())

	store := newRecordingStore(nil)

	opts := quiet()
	opts.Store = store

	client, _ := relay.start(t, opts)

	mustSubscribe(t, client, subA())

	_, err := client.Subscribe(t.Context(), refusedSub())
	if err == nil {
		t.Fatal("the refused subscribe succeeded")
	}

	_, err = client.Unsubscribe(t.Context(), subA())
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if got, want := strings.Join(store.calls.seen(), ","), "add o/a,remove o/a"; got != want {
		t.Errorf("store calls = %s, want %s", got, want)
	}
}

func TestStoreSeesTheCanonicalSubscription(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)
	store := newRecordingStore(nil)

	opts := quiet()
	opts.Store = store

	client, _ := relay.start(t, opts)
	upper := protocol.Subscription{Provider: providerGitHub, Event: pushEvent, Repository: "O/A"}

	mustSubscribe(t, client, upper)

	_, err := client.Unsubscribe(t.Context(), upper)
	if err != nil {
		t.Fatal(err)
	}

	if got, want := strings.Join(store.calls.seen(), ","), "add o/a,remove o/a"; got != want {
		t.Errorf("store calls = %s, want %s", got, want)
	}
}

func TestSeededClientNeedsNoRestoreHook(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)

	opts := quiet()
	opts.Initial = []protocol.Subscription{subA()}

	client, _ := relay.start(t, opts)
	awaitReady(t, client)

	if got := mirrorOf(client); !sameSubscriptions(got, subA()) {
		t.Errorf("mirror = %v, want the seed", got)
	}
}

func TestFailedSaveLeavesTheCallSuccessful(t *testing.T) {
	t.Parallel()

	relay := newFakeRelay(t)

	logs := &bytes.Buffer{}
	lockedLogs := &lockedWriter{w: logs, mu: sync.Mutex{}}

	opts := quiet()
	opts.Store = newRecordingStore(errSaveFailed)
	opts.Logger = slog.New(slog.NewTextHandler(lockedLogs, nil))

	client, _ := relay.start(t, opts)

	list, err := client.Subscribe(t.Context(), subA())
	if err != nil || !sameSubscriptions(list, subA()) {
		t.Fatalf("Subscribe = %v, %v, want success", list, err)
	}

	got := lockedLogs.String()
	if !strings.Contains(got, "save the subscription") || !strings.Contains(got, "disk full") {
		t.Errorf("no warning in the logs:\n%s", got)
	}
}

// lockedWriter serializes writes to a buffer that the test also reads.
type lockedWriter struct {
	w  *bytes.Buffer
	mu sync.Mutex
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.w.Write(p) //nolint:wrapcheck // a bytes.Buffer never fails
}

func (l *lockedWriter) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.w.String()
}
