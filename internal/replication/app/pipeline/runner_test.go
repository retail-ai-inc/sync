package pipeline

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// ------------------------------------------------------------------ fixtures

// fakeReader hands out a fixed list of events and then blocks until cancelled,
// which is what a live stream with nothing to say looks like.
type fakeReader struct {
	events  []*domain.Event
	i       int
	opened  domain.Position
	openErr error
	closed  bool
}

func (f *fakeReader) Open(_ context.Context, from domain.Position) error {
	f.opened = from
	return f.openErr
}

func (f *fakeReader) Next(ctx context.Context) (*domain.Event, error) {
	if f.i < len(f.events) {
		e := f.events[f.i]
		f.i++
		return e, nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (f *fakeReader) Close() error { f.closed = true; return nil }

// fakeApplier records what it was asked to write.
type fakeApplier struct {
	mu sync.Mutex
	// batches holds the runs of each batch it received.
	batches [][][]*domain.Event
	// positions holds the position handed over with each batch.
	positions []domain.Position
	// commits makes Apply claim it recorded the position itself.
	commits bool
	// err, when set, fails every call.
	err error
	// block, when non-nil, is waited on before the first batch is applied.
	block chan struct{}
}

func (f *fakeApplier) Apply(_ context.Context, runs [][]*domain.Event, pos domain.Position) (bool, error) {
	if f.block != nil {
		<-f.block
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return false, f.err
	}
	f.batches = append(f.batches, runs)
	f.positions = append(f.positions, pos)
	return f.commits, nil
}

func (f *fakeApplier) applied() [][][]*domain.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][][]*domain.Event, len(f.batches))
	copy(out, f.batches)
	return out
}

// fakeStore is an in-memory checkpoint store.
type fakeStore struct {
	mu      sync.Mutex
	values  map[string]string
	saves   int
	saveErr error
}

func newStore() *fakeStore { return &fakeStore{values: map[string]string{}} }

func (s *fakeStore) Load(_ context.Context, key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.values[key], nil
}

func (s *fakeStore) Save(_ context.Context, key, payload string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saveErr != nil {
		return s.saveErr
	}
	s.values[key] = payload
	s.saves++
	return nil
}

func (s *fakeStore) value(key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.values[key]
}

// fakeSnapshotter records the order Pin and Copy were called in.
type fakeSnapshotter struct {
	calls   []string
	pinned  domain.Position
	pinErr  error
	copyErr error
}

func (f *fakeSnapshotter) Pin(context.Context) (domain.Position, error) {
	f.calls = append(f.calls, "pin")
	return f.pinned, f.pinErr
}

func (f *fakeSnapshotter) Copy(context.Context) error {
	f.calls = append(f.calls, "copy")
	return f.copyErr
}

func quietLogger() logrus.FieldLogger {
	l := logrus.New()
	l.SetLevel(logrus.PanicLevel)
	return l
}

// event builds one change, ending its transaction unless told otherwise.
func event(ns, key, pos string, opts ...func(*domain.Event)) *domain.Event {
	e := &domain.Event{
		NS:              domain.Namespace{DB: "shop", Object: ns},
		Op:              domain.OpInsert,
		Key:             key,
		Pos:             domain.Position{Payload: pos},
		SourceTime:      time.Now(),
		EndsTransaction: true,
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

func midTransaction(e *domain.Event) { e.EndsTransaction = false }
func heartbeat(e *domain.Event)      { e.Heartbeat = true; e.Key = ""; e.SourceTime = time.Time{} }
func schema(e *domain.Event)         { e.Op = domain.OpSchema; e.Key = "" }

func newRunner(t *testing.T, r domain.Reader, a domain.Applier, s Checkpoints) *Runner {
	t.Helper()
	return &Runner{
		Reader:      r,
		Applier:     a,
		Checkpoints: s,
		Opts: Options{
			Limits:         Limits{MaxEvents: 2},
			FlushInterval:  20 * time.Millisecond,
			ReportInterval: 5 * time.Millisecond,
			Labels:         metrics.Labels{"task": t.Name()},
			Logger:         quietLogger(),
			Engine:         "Test",
		},
	}
}

// runFor runs the runner until it settles, then cancels and returns its error.
func runFor(t *testing.T, r *Runner, d time.Duration) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	time.Sleep(d)
	cancel()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after the context was cancelled")
		return nil
	}
}

// ------------------------------------------------------- transaction cutting

// TestABatchIsNotCutInsideASourceTransaction is the guarantee the whole design
// rests on. A transaction split across two batches shows the target the order
// without its payment — a state the source was never in, and one nothing
// downstream is written to cope with.
func TestABatchIsNotCutInsideASourceTransaction(t *testing.T) {
	reader := &fakeReader{events: []*domain.Event{
		event("orders", "1", "p1", midTransaction),
		event("orders", "2", "p2", midTransaction),
		event("orders", "3", "p3", midTransaction),
	}}
	applier := &fakeApplier{}
	store := newStore()

	if err := runFor(t, newRunner(t, reader, applier, store), 120*time.Millisecond); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := len(applier.applied()); got != 0 {
		t.Errorf("applied %d batches, want 0: the transaction never ended", got)
	}
	if got := store.value(""); got != "" {
		t.Errorf("recorded position %q, want none", got)
	}
}

// TestTheBatchIsCutOnceTheTransactionEnds is the other half: waiting is only
// correct if it stops waiting.
func TestTheBatchIsCutOnceTheTransactionEnds(t *testing.T) {
	reader := &fakeReader{events: []*domain.Event{
		event("orders", "1", "p1", midTransaction),
		event("orders", "2", "p2", midTransaction),
		event("payments", "9", "p3"),
	}}
	applier := &fakeApplier{}
	store := newStore()

	if err := runFor(t, newRunner(t, reader, applier, store), 120*time.Millisecond); err != nil {
		t.Fatalf("Run: %v", err)
	}

	batches := applier.applied()
	if len(batches) != 1 {
		t.Fatalf("applied %d batches, want 1", len(batches))
	}
	var count int
	for _, run := range batches[0] {
		count += len(run)
	}
	if count != 3 {
		t.Errorf("applied %d events, want all 3 of the transaction", count)
	}
	if got := store.value(""); got != "p3" {
		t.Errorf("recorded position %q, want p3", got)
	}
}

// TestARunawayTransactionIsRefusedRatherThanBuffered covers the safety valve.
// A batch cannot be cut inside a transaction, so an unbounded one would be held
// until the process died — in the middle of applying a payment batch.
func TestARunawayTransactionIsRefusedRatherThanBuffered(t *testing.T) {
	var events []*domain.Event
	for i := 0; i < 20; i++ {
		events = append(events, event("orders", "k", "p", midTransaction))
	}
	r := newRunner(t, &fakeReader{events: events}, &fakeApplier{}, newStore())
	r.Opts.Limits.MaxTransactionEvents = 5

	err := runFor(t, r, 120*time.Millisecond)
	if !domain.IsUnrecoverable(err) {
		t.Fatalf("Run returned %v, want an unrecoverable error", err)
	}
}

// ------------------------------------------------------------ position moves

// TestThePositionIsNotRecordedWhenTheApplyFails is what keeps a restart from
// resuming past changes that never reached the target.
func TestThePositionIsNotRecordedWhenTheApplyFails(t *testing.T) {
	reader := &fakeReader{events: []*domain.Event{event("orders", "1", "p1")}}
	applier := &fakeApplier{err: errors.New("target refused the write")}
	store := newStore()

	err := runFor(t, newRunner(t, reader, applier, store), 120*time.Millisecond)
	if err == nil {
		t.Fatal("Run returned nil, want the applier's failure")
	}
	if got := store.value(""); got != "" {
		t.Errorf("recorded position %q after a failed apply, want none", got)
	}
}

// TestAnApplierThatCommitsThePositionIsNotAskedTwice covers the MySQL path,
// where the position is written in the same transaction as the data.
func TestAnApplierThatCommitsThePositionIsNotAskedTwice(t *testing.T) {
	reader := &fakeReader{events: []*domain.Event{event("orders", "1", "p1")}}
	applier := &fakeApplier{commits: true}
	store := newStore()

	if err := runFor(t, newRunner(t, reader, applier, store), 120*time.Millisecond); err != nil {
		t.Fatalf("Run: %v", err)
	}

	store.mu.Lock()
	saves := store.saves
	store.mu.Unlock()
	if saves != 0 {
		t.Errorf("the store was written %d times, want 0: the applier committed the position", saves)
	}
}

// TestAnApplierThatCannotCommitThePositionHasItRecordedForIt covers the other
// path, which is at-least-once and relies on idempotent writes.
func TestAnApplierThatCannotCommitThePositionHasItRecordedForIt(t *testing.T) {
	reader := &fakeReader{events: []*domain.Event{event("orders", "1", "p1")}}
	applier := &fakeApplier{commits: false}
	store := newStore()

	if err := runFor(t, newRunner(t, reader, applier, store), 120*time.Millisecond); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := store.value(""); got != "p1" {
		t.Errorf("recorded position %q, want p1", got)
	}
}

// ---------------------------------------------------------------- heartbeats

// TestAHeartbeatIsNotWrittenToTheTarget covers the event the syncer makes up
// itself. Applying it would replicate the syncer's own bookkeeping into the
// payment data.
func TestAHeartbeatIsNotWrittenToTheTarget(t *testing.T) {
	reader := &fakeReader{events: []*domain.Event{
		event("", "", "p1", heartbeat),
	}}
	applier := &fakeApplier{}
	store := newStore()

	if err := runFor(t, newRunner(t, reader, applier, store), 120*time.Millisecond); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := len(applier.applied()); got != 0 {
		t.Errorf("applied %d batches, want 0: a heartbeat is not a change", got)
	}
}

// TestAHeartbeatStillMovesThePosition is why heartbeats are worth having at
// all: they prove the link is alive, and the position they carry means a
// restart does not re-read a stretch of log that held nothing.
func TestAHeartbeatStillMovesThePosition(t *testing.T) {
	reader := &fakeReader{events: []*domain.Event{
		event("", "", "hb1", heartbeat),
	}}
	store := newStore()

	if err := runFor(t, newRunner(t, reader, &fakeApplier{}, store), 120*time.Millisecond); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := store.value(""); got != "hb1" {
		t.Errorf("recorded position %q, want hb1", got)
	}
}

// ------------------------------------------------------------------- gauges

// TestTheLagClimbsWhileTheApplierIsStuck covers a defect in the gauge itself.
//
// The applied lag used to be set only when a batch landed, so a task that had
// stopped applying left it frozen at whatever it last was. An alert on a frozen
// gauge never fires, which made "replication has stopped" the one condition the
// monitoring could not see.
func TestTheLagClimbsWhileTheApplierIsStuck(t *testing.T) {
	release := make(chan struct{})
	reader := &fakeReader{events: []*domain.Event{event("orders", "1", "p1")}}
	applier := &fakeApplier{block: release}
	labels := metrics.Labels{"task": t.Name()}

	r := newRunner(t, reader, applier, newStore())
	r.Opts.Labels = labels
	defer metrics.Default.Forget(labels)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	time.Sleep(60 * time.Millisecond)
	first := gauge(t, metrics.LagSeconds, labels)
	time.Sleep(120 * time.Millisecond)
	second := gauge(t, metrics.LagSeconds, labels)

	close(release)
	cancel()
	<-done

	if !(second > first) {
		t.Errorf("lag went %v → %v while the applier was stuck; it has to climb", first, second)
	}
}

// TestSilenceFromTheSourceIsVisible covers the other half of the same problem:
// a stream delivering nothing at all looks exactly like one that is up to date.
func TestSilenceFromTheSourceIsVisible(t *testing.T) {
	labels := metrics.Labels{"task": t.Name()}
	r := newRunner(t, &fakeReader{}, &fakeApplier{}, newStore())
	r.Opts.Labels = labels
	defer metrics.Default.Forget(labels)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	time.Sleep(40 * time.Millisecond)
	first := gauge(t, metrics.LastEventAgeSeconds, labels)
	time.Sleep(120 * time.Millisecond)
	second := gauge(t, metrics.LastEventAgeSeconds, labels)

	cancel()
	<-done

	if !(second > first) {
		t.Errorf("silence age went %v → %v; it has to climb while nothing arrives", first, second)
	}
}

func gauge(t *testing.T, name string, labels metrics.Labels) float64 {
	t.Helper()
	for _, s := range metrics.Default.Snapshot(name) {
		if s.Labels.Key() == labels.Key() {
			return s.Value
		}
	}
	t.Fatalf("no sample for %s", name)
	return 0
}

// ------------------------------------------------------------------ snapshot

// TestTheSnapshotPinsBeforeItCopies is the ordering that decides whether the
// writes made during the copy belong to anybody. Reading the position
// afterwards loses every one of them.
func TestTheSnapshotPinsBeforeItCopies(t *testing.T) {
	snap := &fakeSnapshotter{pinned: domain.Position{Payload: "pinned"}}
	store := newStore()
	r := newRunner(t, &fakeReader{}, &fakeApplier{}, store)
	r.Snapshotter = snap

	if err := runFor(t, r, 60*time.Millisecond); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(snap.calls) != 2 || snap.calls[0] != "pin" || snap.calls[1] != "copy" {
		t.Errorf("snapshot did %v, want pin then copy", snap.calls)
	}
	if got := store.value(""); got != "pinned" {
		t.Errorf("recorded position %q, want the pinned point", got)
	}
}

// TestAnInterruptedCopyRecordsNoPosition means the copy is redone rather than
// resumed from a point it never reached.
func TestAnInterruptedCopyRecordsNoPosition(t *testing.T) {
	snap := &fakeSnapshotter{
		pinned:  domain.Position{Payload: "pinned"},
		copyErr: errors.New("connection reset"),
	}
	store := newStore()
	r := newRunner(t, &fakeReader{}, &fakeApplier{}, store)
	r.Snapshotter = snap

	if err := runFor(t, r, 60*time.Millisecond); err == nil {
		t.Fatal("Run returned nil, want the copy's failure")
	}
	if got := store.value(""); got != "" {
		t.Errorf("recorded position %q after a failed copy, want none", got)
	}
}

// TestAStoredPositionSkipsTheSnapshot covers a restart: the copy is made once.
func TestAStoredPositionSkipsTheSnapshot(t *testing.T) {
	snap := &fakeSnapshotter{}
	store := newStore()
	store.values[""] = "already-here"
	reader := &fakeReader{}
	r := newRunner(t, reader, &fakeApplier{}, store)
	r.Snapshotter = snap

	if err := runFor(t, r, 60*time.Millisecond); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(snap.calls) != 0 {
		t.Errorf("snapshot did %v, want nothing", snap.calls)
	}
	if reader.opened.Payload != "already-here" {
		t.Errorf("opened the stream at %q, want the stored position", reader.opened.Payload)
	}
}

// TestAQuietSourceDoesNotLookLikeALag covers a defect the real cluster showed.
//
// The applied lag was measured from the last change applied, so a source nobody
// had written to for an hour reported an hour of lag while being perfectly up to
// date. Alerting on that pages somebody every quiet Sunday, and an alert that
// cries wolf is worse than none. When there is nothing waiting the lag is
// measured from the newest thing the stream has reported — which heartbeats keep
// fresh precisely so that this works.
func TestAQuietSourceDoesNotLookLikeALag(t *testing.T) {
	labels := metrics.Labels{"task": t.Name()}
	// One change from long ago, applied; then a heartbeat from just now, which
	// is what a quiet but healthy stream looks like.
	old := time.Now().Add(-time.Hour)
	reader := &fakeReader{events: []*domain.Event{
		{NS: domain.Namespace{DB: "shop", Object: "orders"}, Op: domain.OpInsert, Key: "1",
			Pos: domain.Position{Payload: "p1"}, SourceTime: old, EndsTransaction: true},
		{Heartbeat: true, EndsTransaction: true, SourceTime: time.Now()},
	}}

	r := newRunner(t, reader, &fakeApplier{}, newStore())
	r.Opts.Labels = labels
	defer metrics.Default.Forget(labels)

	if err := runFor(t, r, 150*time.Millisecond); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := gauge(t, metrics.LagSeconds, labels); got > 60 {
		t.Errorf("lag = %.0fs for a quiet but caught-up stream; the last change being "+
			"an hour old is not a lag", got)
	}
}
