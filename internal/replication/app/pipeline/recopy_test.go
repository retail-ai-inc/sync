package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// A stored position the source cannot continue from. Redis is where this
// happens: its replication history lives in memory, so a restart of the source
// ends the history every stored offset belongs to, and no amount of retrying
// gets that history back.

// sweepingSnapshotter is a snapshotter that can also remove what the target
// holds and the source does not, which is what makes a recovering copy safe.
type sweepingSnapshotter struct {
	fakeSnapshotter
	swept int
}

func (s *sweepingSnapshotter) SweepStale(context.Context) error {
	s.calls = append(s.calls, "sweep")
	s.swept++
	return nil
}

// openFailsOnce is a reader that refuses the stored position the first time and
// accepts whatever it is given afterwards, which is how the source behaves: the
// history is gone, a fresh handshake is not.
type openFailsOnce struct {
	fakeReader
	refuse error
	opens  []domain.Position
}

func (r *openFailsOnce) Open(ctx context.Context, from domain.Position) error {
	r.opens = append(r.opens, from)
	if len(r.opens) == 1 && r.refuse != nil {
		return r.refuse
	}
	return r.fakeReader.Open(ctx, from)
}

func storedPosition(t *testing.T, store *fakeStore, payload string) {
	t.Helper()
	if err := store.Save(context.Background(), "", payload); err != nil {
		t.Fatalf("Save: %v", err)
	}
}

func TestAPositionTheSourceCannotContinueFromIsRecoveredByCopying(t *testing.T) {
	withStored(t, Tuning{RecopyOnUnusablePosition: true})

	snap := &sweepingSnapshotter{}
	snap.pinned = domain.Position{Payload: "fresh"}
	reader := &openFailsOnce{refuse: domain.PositionUnusable("the history that offset belongs to is gone")}
	store := newStore()
	storedPosition(t, store, "stale")

	r := newRunner(t, reader, &fakeApplier{}, store)
	r.Snapshotter = snap

	if err := runFor(t, r, 80*time.Millisecond); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(reader.opens) != 2 {
		t.Fatalf("the stream was opened %d times, want the stale position and then "+
			"the fresh one", len(reader.opens))
	}
	if reader.opens[0].Payload != "stale" || reader.opens[1].Payload != "fresh" {
		t.Errorf("opened %q then %q, want the stored position then the pinned one",
			reader.opens[0].Payload, reader.opens[1].Payload)
	}
	// A copy over a target that already holds data, and only then the sweep:
	// anything the source deleted while the task was away is on the target and
	// no copy would mention it.
	want := []string{"pin", "copy", "sweep"}
	if len(snap.calls) != len(want) {
		t.Fatalf("the snapshot did %v, want %v", snap.calls, want)
	}
	for i, call := range want {
		if snap.calls[i] != call {
			t.Fatalf("the snapshot did %v, want %v", snap.calls, want)
		}
	}
	if got := store.value(""); got != "fresh" {
		t.Errorf("recorded position %q, want the point the new copy was pinned at", got)
	}
}

// Off is the older behaviour, and it is also what a deployment gets when the
// settings cannot be read: rebuilding a target is not something to do on a
// guess.
func TestWithTheSettingOffThePositionStopsTheTask(t *testing.T) {
	withStored(t, Tuning{RecopyOnUnusablePosition: false})

	snap := &sweepingSnapshotter{}
	snap.pinned = domain.Position{Payload: "fresh"}
	reader := &openFailsOnce{refuse: domain.PositionUnusable("the history is gone")}
	store := newStore()
	storedPosition(t, store, "stale")

	r := newRunner(t, reader, &fakeApplier{}, store)
	r.Snapshotter = snap

	err := runFor(t, r, 80*time.Millisecond)
	if err == nil {
		t.Fatal("the task carried on with a position the source cannot continue from")
	}
	if !domain.IsUnrecoverable(err) {
		t.Errorf("the task stopped with %v, want it reported as needing intervention", err)
	}
	if len(snap.calls) != 0 {
		t.Errorf("the snapshot did %v, want nothing", snap.calls)
	}
	if got := store.value(""); got != "stale" {
		t.Errorf("the stored position is now %q; it should have been left alone", got)
	}
}

// A snapshotter that cannot remove what the source has deleted must not be used
// to recover a position: the copy would write everything the source has over a
// target that also holds what the source no longer has, and nothing would ever
// report the difference.
func TestAnEngineThatCannotSweepDoesNotRecoverByCopying(t *testing.T) {
	withStored(t, Tuning{RecopyOnUnusablePosition: true})

	snap := &fakeSnapshotter{pinned: domain.Position{Payload: "fresh"}}
	reader := &openFailsOnce{refuse: domain.PositionUnusable("the history is gone")}
	store := newStore()
	storedPosition(t, store, "stale")

	r := newRunner(t, reader, &fakeApplier{}, store)
	r.Snapshotter = snap

	if err := runFor(t, r, 80*time.Millisecond); err == nil {
		t.Fatal("the task copied over a target it cannot sweep")
	}
	if len(snap.calls) != 0 {
		t.Errorf("the snapshot did %v, want nothing", snap.calls)
	}
}

// Any other failure to open the stream is not this: a connection that dropped
// is worth retrying, and rebuilding the target over one would be absurd.
func TestAnOrdinaryFailureToOpenIsNotRecoveredByCopying(t *testing.T) {
	withStored(t, Tuning{RecopyOnUnusablePosition: true})

	snap := &sweepingSnapshotter{}
	reader := &openFailsOnce{refuse: context.DeadlineExceeded}
	store := newStore()
	storedPosition(t, store, "stale")

	r := newRunner(t, reader, &fakeApplier{}, store)
	r.Snapshotter = snap

	if err := runFor(t, r, 80*time.Millisecond); err == nil {
		t.Fatal("a stream that would not open was treated as a position to recover")
	}
	if len(snap.calls) != 0 {
		t.Errorf("the snapshot did %v, want nothing", snap.calls)
	}
}

// A first copy sweeps nothing: the target starts empty, and a sweep would be a
// scan of the whole of it for no reason.
func TestAFirstCopyDoesNotSweep(t *testing.T) {
	withStored(t, Tuning{RecopyOnUnusablePosition: true})

	snap := &sweepingSnapshotter{}
	snap.pinned = domain.Position{Payload: "pinned"}
	r := newRunner(t, &fakeReader{}, &fakeApplier{}, newStore())
	r.Snapshotter = snap

	if err := runFor(t, r, 60*time.Millisecond); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if snap.swept != 0 {
		t.Errorf("a first copy swept the target %d times", snap.swept)
	}
}
