package pipeline

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// fakeReader hands out a fixed list of events and then blocks until cancelled,
// which is what a live stream with nothing to say looks like.
type fakeReader struct {
	events  []*domain.Event
	i       int
	opened  domain.Position
	openErr error
	closed  bool
	// onOpen, when non-nil, is called as the stream opens, so a test can assert
	// when that happened relative to the copy.
	onOpen func()
}

func (f *fakeReader) Open(_ context.Context, from domain.Position) error {
	f.opened = from
	if f.onOpen != nil {
		f.onOpen()
	}
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
	// onApply, when non-nil, is called on every batch, so a test can assert on
	// when a batch arrives rather than only that it did.
	onApply func()
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
	if f.onApply != nil {
		f.onApply()
	}
	return f.commits, nil
}

func (f *fakeApplier) applied() [][][]*domain.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][][]*domain.Event, len(f.batches))
	copy(out, f.batches)
	return out
}

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

type fakeSnapshotter struct {
	calls   []string
	pinned  domain.Position
	pinErr  error
	copyErr error
	// onCopy, when non-nil, is called while the copy is running, which is when a
	// test can see what the rest of the pipeline is doing underneath it.
	onCopy func()
}

func (f *fakeSnapshotter) Pin(context.Context) (domain.Position, error) {
	f.calls = append(f.calls, "pin")
	return f.pinned, f.pinErr
}

func (f *fakeSnapshotter) Copy(context.Context) error {
	f.calls = append(f.calls, "copy")
	if f.onCopy != nil {
		f.onCopy()
	}
	return f.copyErr
}

func quietLogger() logrus.FieldLogger {
	l := logrus.New()
	l.SetLevel(logrus.PanicLevel)
	return l
}

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

// A transaction split across two batches shows the target the order without
// its payment — a state the source was never in, and one nothing downstream is
// written to cope with.
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

// TestAnApplierThatCommitsThePositionIsNotAskedTwice covers the MySQL path.
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

// TestAHeartbeatIsNotWrittenToTheTarget covers the event the syncer makes up
// itself.
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

// The applied lag used to be set only when a batch landed, so a task that had
// stopped applying left it frozen at whatever it last was.
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

// gaugeOK reports a gauge's value and whether it was set at all. Never set and
// set to zero are different answers, and some of these tests turn on which.
func gaugeOK(t *testing.T, name string, labels metrics.Labels) (float64, bool) {
	t.Helper()
	for _, s := range metrics.Default.Snapshot(name) {
		if s.Labels.Key() == labels.Key() {
			return s.Value, true
		}
	}
	return 0, false
}

func gauge(t *testing.T, name string, labels metrics.Labels) float64 {
	t.Helper()
	value, ok := gaugeOK(t, name, labels)
	if !ok {
		t.Fatalf("no sample for %s", name)
	}
	return value
}

// TestTheSnapshotPinsBeforeItCopies is the ordering that decides whether the
// writes made during the copy belong to anybody.
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

// TestTheStreamOpensBeforeTheCopyStarts is what keeps a long copy alive. The
// copy's own writes push the source's log past the pinned point -- on a sharded
// source they push the smallest shard's log past it in minutes -- so a stream
// opened after the copy can find the point already gone. By then the copy has
// run for hours and the position is recorded, which leaves the task able
// neither to resume nor to copy again.
func TestTheStreamOpensBeforeTheCopyStarts(t *testing.T) {
	snap := &fakeSnapshotter{pinned: domain.Position{Payload: "pinned"}}
	reader := &fakeReader{}
	reader.onOpen = func() { snap.calls = append(snap.calls, "open") }
	store := newStore()
	r := newRunner(t, reader, &fakeApplier{}, store)
	r.Snapshotter = snap

	if err := runFor(t, r, 60*time.Millisecond); err != nil {
		t.Fatalf("Run: %v", err)
	}

	want := []string{"pin", "open", "copy"}
	if !reflect.DeepEqual(snap.calls, want) {
		t.Errorf("the run did %v, want %v", snap.calls, want)
	}
	if reader.opened.Payload != "pinned" {
		t.Errorf("opened the stream at %q, want the pinned point", reader.opened.Payload)
	}
}

// TestTheCopyHoldsTheChangesMadeWhileItRuns is the other half of opening early.
// The stream is read during the copy, so those changes must wait for the copy to
// lay the base down under them: applying one first would write a record the copy
// then overwrites with the state it had before the change.
func TestTheCopyHoldsTheChangesMadeWhileItRuns(t *testing.T) {
	applier := &fakeApplier{}
	snap := &fakeSnapshotter{pinned: domain.Position{Payload: "pinned"}}
	during := -1
	snap.onCopy = func() { during = len(applier.applied()) }

	reader := &fakeReader{events: []*domain.Event{
		event("orders", "1", "p1"),
		event("orders", "2", "p2"),
	}}
	r := newRunner(t, reader, applier, newStore())
	r.Snapshotter = snap

	if err := runFor(t, r, 200*time.Millisecond); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if during != 0 {
		t.Errorf("%d batches reached the target while the copy ran, want none", during)
	}
	if got := len(applier.applied()); got == 0 {
		t.Error("no batch reached the target after the copy, want the held changes")
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

// The applied lag was measured from the last change applied, so a source
// nobody had written to for an hour reported an hour of lag while being
// perfectly up to date.
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

	// Caught up is zero, not "how long since we last heard anything". The
	// heartbeat is a second old and the last change an hour old; neither is a
	// lag, because nothing is waiting to be applied.
	if got := gauge(t, metrics.LagSeconds, labels); got > 1 {
		t.Errorf("lag = %.2fs for a quiet but caught-up stream; nothing is waiting "+
			"to be applied, so nothing is behind", got)
	}
}

// windowedReader is read from the test while the reporting goroutine calls it,
// so every field goes through the mutex.
type windowedReader struct {
	*fakeReader
	mu     sync.Mutex
	window time.Duration
	err    error
	calls  int
}

func (w *windowedReader) Window(context.Context) (time.Duration, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls++
	return w.window, w.err
}

func (w *windowedReader) answer(window time.Duration, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.window, w.err = window, err
}

func (w *windowedReader) asked() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.calls
}

func headroomOf(t *testing.T, labels metrics.Labels) (float64, bool) {
	t.Helper()
	return gaugeOK(t, metrics.RetentionHeadroomSeconds, labels)
}

// runReporting drives one reporting tick for a task that is behind, with its
// oldest unapplied change made at oldestPending.
//
// The backlog is what the headroom is computed from: a task that is caught up
// holds a position at the head of the source's log and can afford the whole
// window, and one carrying an unapplied change from an hour ago has an hour
// less. It used to be set up by ageing lastReadAt instead, which measured how
// long since anything was heard -- a different quantity, and one a heartbeat
// resets.
func runReporting(t *testing.T, r *Runner, oldestPending time.Time) {
	t.Helper()
	r.mu.Lock()
	r.oldestPending = oldestPending
	r.lastReadAt = oldestPending
	r.mu.Unlock()

	stop := r.report(context.Background())
	defer stop()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := headroomOf(t, r.Opts.Labels); ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestTheHeadroomIsTheWindowLessTheLag covers the number an operator reads
// while deciding whether a stopped task can still be restarted.
func TestTheHeadroomIsTheWindowLessTheLag(t *testing.T) {
	labels := metrics.Labels{"task": "headroom-basic"}
	defer metrics.Default.Forget(labels)

	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	r := &Runner{
		Reader: &windowedReader{fakeReader: &fakeReader{}, window: 24 * time.Hour},
		clock:  func() time.Time { return now },
		Opts:   Options{Labels: labels, ReportInterval: time.Millisecond},
	}

	runReporting(t, r, now.Add(-60*time.Second))

	got, ok := headroomOf(t, labels)
	if !ok {
		t.Fatal("no headroom was published")
	}
	if want := (24 * time.Hour).Seconds() - 60; got != want {
		t.Errorf("headroom = %v, want %v", got, want)
	}
}

// A task an hour past its window and one a week past it need different answers.
func TestTheHeadroomGoesNegativeOnceTheWindowIsPast(t *testing.T) {
	labels := metrics.Labels{"task": "headroom-negative"}
	defer metrics.Default.Forget(labels)

	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	r := &Runner{
		Reader: &windowedReader{fakeReader: &fakeReader{}, window: time.Hour},
		clock:  func() time.Time { return now },
		Opts:   Options{Labels: labels, ReportInterval: time.Millisecond},
	}

	runReporting(t, r, now.Add(-3*time.Hour))

	got, ok := headroomOf(t, labels)
	if !ok {
		t.Fatal("no headroom was published")
	}
	if want := -2 * time.Hour.Seconds(); got != want {
		t.Errorf("headroom = %v, want %v", got, want)
	}
}

// A guessed window would read exactly like a measured one.
func TestASourceThatCannotSayPublishesNothing(t *testing.T) {
	labels := metrics.Labels{"task": "headroom-unknown"}
	defer metrics.Default.Forget(labels)

	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	source := &windowedReader{fakeReader: &fakeReader{}, err: errors.New("not through mongos")}
	r := &Runner{
		Reader: source,
		clock:  func() time.Time { return now },
		Opts:   Options{Labels: labels, ReportInterval: time.Millisecond},
	}

	stop := r.report(context.Background())
	time.Sleep(60 * time.Millisecond)
	stop()

	if _, ok := headroomOf(t, labels); ok {
		t.Error("a headroom was published for a source that could not say what its window is")
	}
	if asked := source.asked(); asked != 1 {
		t.Errorf("the source was asked %d times, want once — a source that cannot answer "+
			"should not be asked, or logged about, on every tick", asked)
	}
}

// TestAReaderThatKnowsNothingOfRetentionIsLeftAlone keeps the metric optional:
// Redis and PostgreSQL readers do not implement it.
func TestAReaderThatKnowsNothingOfRetentionIsLeftAlone(t *testing.T) {
	labels := metrics.Labels{"task": "headroom-absent"}
	defer metrics.Default.Forget(labels)

	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	r := &Runner{
		Reader: &fakeReader{},
		clock:  func() time.Time { return now },
		Opts:   Options{Labels: labels, ReportInterval: time.Millisecond},
	}

	stop := r.report(context.Background())
	time.Sleep(40 * time.Millisecond)
	stop()

	if _, ok := headroomOf(t, labels); ok {
		t.Error("a headroom was published for a reader that knows nothing of retention")
	}
}

// TestTheWindowIsNotReReadEveryTick keeps a per-second gauge refresh from
// becoming a per-second query against the source.
func TestTheWindowIsNotReReadEveryTick(t *testing.T) {
	labels := metrics.Labels{"task": "headroom-cached"}
	defer metrics.Default.Forget(labels)

	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	source := &windowedReader{fakeReader: &fakeReader{}, window: time.Hour}
	r := &Runner{
		Reader: source,
		clock:  func() time.Time { return now },
		Opts:   Options{Labels: labels, ReportInterval: time.Millisecond},
	}

	runReporting(t, r, now.Add(-time.Minute))
	time.Sleep(50 * time.Millisecond)

	if asked := source.asked(); asked != 1 {
		t.Errorf("the source was asked %d times over many ticks, want once", asked)
	}
}

// Splitting a batch into runs lets an applier parallelise within a run, which
// is correct when every event is an idempotent write of a whole record.
func TestStreamOrderHandsTheBatchOverUnsplit(t *testing.T) {
	applier := &fakeApplier{commits: true}
	// Two events on the same key, which orderedRuns would put in separate runs.
	events := []*domain.Event{
		{NS: domain.Namespace{DB: "0"}, Op: domain.OpUpdate, Key: "counter",
			Pos: domain.Position{Payload: "1"}, EndsTransaction: true},
		{NS: domain.Namespace{DB: "0"}, Op: domain.OpUpdate, Key: "counter",
			Pos: domain.Position{Payload: "2"}, EndsTransaction: true},
	}

	r := &Runner{
		Reader:      &fakeReader{events: events},
		Applier:     applier,
		Checkpoints: newStore(),
		Opts: Options{
			Engine:        "Redis",
			StreamOrder:   true,
			FlushInterval: 5 * time.Millisecond,
			Logger:        quietLogger(),
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = r.Run(ctx)

	applier.mu.Lock()
	defer applier.mu.Unlock()
	if len(applier.batches) == 0 {
		t.Fatal("nothing was applied")
	}
	for i, runs := range applier.batches {
		if len(runs) != 1 {
			t.Errorf("batch %d arrived as %d runs, want 1 — a command stream cannot "+
				"be reordered", i, len(runs))
		}
	}
}

// TestWithoutStreamOrderTheBatchIsStillSplit keeps the change from altering what
// MySQL and MongoDB see.
func TestWithoutStreamOrderTheBatchIsStillSplit(t *testing.T) {
	applier := &fakeApplier{commits: true}
	events := []*domain.Event{
		{NS: domain.Namespace{DB: "shop", Object: "orders"}, Op: domain.OpUpdate, Key: "1",
			Pos: domain.Position{Payload: "1"}, EndsTransaction: true},
		{NS: domain.Namespace{DB: "shop", Object: "orders"}, Op: domain.OpUpdate, Key: "1",
			Pos: domain.Position{Payload: "2"}, EndsTransaction: true},
	}

	r := &Runner{
		Reader:      &fakeReader{events: events},
		Applier:     applier,
		Checkpoints: newStore(),
		Opts: Options{Engine: "MySQL", FlushInterval: 5 * time.Millisecond,
			Logger: quietLogger()},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = r.Run(ctx)

	applier.mu.Lock()
	defer applier.mu.Unlock()
	var split bool
	for _, runs := range applier.batches {
		if len(runs) > 1 {
			split = true
		}
	}
	if !split {
		t.Error("two events on the same key were not split into separate runs")
	}
}

// TestASourceThatCannotSayYetIsAskedAgain separates "not yet" from "cannot".
func TestASourceThatCannotSayYetIsAskedAgain(t *testing.T) {
	labels := metrics.Labels{"task": "headroom-not-yet"}
	defer metrics.Default.Forget(labels)

	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	source := &windowedReader{
		fakeReader: &fakeReader{},
		err:        fmt.Errorf("measured once: %w", domain.ErrWindowNotYet),
	}
	r := &Runner{
		Reader: source,
		clock:  func() time.Time { return now },
		Opts:   Options{Labels: labels, ReportInterval: time.Millisecond, Logger: quietLogger()},
	}

	stop := r.report(context.Background())
	time.Sleep(80 * time.Millisecond)
	stop()

	if asked := source.asked(); asked < 2 {
		t.Errorf("the source was asked %d time(s); a source that says 'not yet' has "+
			"to be asked again, or the metric never appears", asked)
	}
	if _, ok := headroomOf(t, labels); ok {
		t.Error("a headroom was published before the source could say what its window is")
	}
}

func TestASourceThatSaysNotYetAndThenAnswersIsPublished(t *testing.T) {
	labels := metrics.Labels{"task": "headroom-eventually"}
	defer metrics.Default.Forget(labels)

	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	source := &windowedReader{
		fakeReader: &fakeReader{},
		err:        fmt.Errorf("measured once: %w", domain.ErrWindowNotYet),
	}
	r := &Runner{
		Reader: source,
		clock:  func() time.Time { return now },
		Opts:   Options{Labels: labels, ReportInterval: time.Millisecond, Logger: quietLogger()},
	}
	// Thirty seconds of backlog, so the headroom is the window less that.
	r.mu.Lock()
	r.oldestPending = now.Add(-30 * time.Second)
	r.lastReadAt = now.Add(-30 * time.Second)
	r.mu.Unlock()

	stop := r.report(context.Background())
	time.Sleep(30 * time.Millisecond)

	// The second measurement arrives.
	source.answer(2*time.Hour, nil)

	deadline := time.Now().Add(2 * time.Second)
	var published bool
	for time.Now().Before(deadline) {
		if _, ok := headroomOf(t, labels); ok {
			published = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	stop()

	if !published {
		t.Fatal("the headroom was never published after the source could answer")
	}
	got, _ := headroomOf(t, labels)
	if want := (2 * time.Hour).Seconds() - 30; got != want {
		t.Errorf("headroom = %v, want %v", got, want)
	}
}

// The window used to be paid on every batch that did not fill, which is every
// batch on a quiet source.
func TestAnIdleBatchIsSentWithoutWaitingForTheWindow(t *testing.T) {
	applied := make(chan struct{}, 1)
	reader := &fakeReader{events: []*domain.Event{event("orders", "1", "p1")}}
	applier := &fakeApplier{onApply: func() {
		select {
		case applied <- struct{}{}:
		default:
		}
	}}

	runner := newRunner(t, reader, applier, newStore())
	// Far longer than the deadline below: reaching it would mean the batch waited.
	runner.Opts.FlushInterval = 30 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	select {
	case <-applied:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("the batch was still waiting after 500ms: an idle queue must be flushed at once")
	}
	cancel()
	<-done
}

// TestAnIdleFlushStillWillNotCutInsideATransaction is the guard on the one
// above: sending early must not become a way to show the target half a source
// transaction.
func TestAnIdleFlushStillWillNotCutInsideATransaction(t *testing.T) {
	applied := make(chan struct{}, 1)
	reader := &fakeReader{events: []*domain.Event{
		event("orders", "1", "p1", midTransaction),
		event("orders", "2", "p2", midTransaction),
	}}
	applier := &fakeApplier{onApply: func() {
		select {
		case applied <- struct{}{}:
		default:
		}
	}}

	runner := newRunner(t, reader, applier, newStore())
	runner.Opts.FlushInterval = 30 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	select {
	case <-applied:
		t.Fatal("applied a batch that ends inside a source transaction")
	case <-time.After(300 * time.Millisecond):
	}
	cancel()
	<-done
}

// Ending the run over a failed write also stopped the reader, and the reader
// is what keeps the source's replication log from rolling past the position.
func TestATargetThatIsNotReadyDoesNotStopTheReader(t *testing.T) {
	// onApply only fires on a successful write, so receiving from this channel
	// means the batch that was refused earlier was retried and got through.
	applied := make(chan struct{}, 1)
	applier := &fakeApplier{onApply: func() {
		select {
		case applied <- struct{}{}:
		default:
		}
	}}
	// Fail the first two attempts the way an unreachable target does.
	applier.err = errors.New("dial tcp 10.0.0.1:6379: i/o timeout")
	reader := &fakeReader{events: []*domain.Event{event("orders", "1", "p1")}}

	runner := newRunner(t, reader, applier, newStore())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	// Let it fail twice, then let the target come back.
	time.Sleep(700 * time.Millisecond)
	applier.mu.Lock()
	applier.err = nil
	applier.mu.Unlock()

	select {
	case <-applied:
	case <-time.After(10 * time.Second):
		t.Fatal("the batch was never retried: a target that is not ready must not end the run")
	}
	if reader.closed {
		t.Error("the reader was closed while the target was unavailable")
	}
	cancel()
	<-done
}

// TestAPoisonedEventBlocksTheTaskRatherThanBeingSteppedOver covers the other
// half: retrying forever is only right for a target that might come back.
func TestAPoisonedEventBlocksTheTaskRatherThanBeingSteppedOver(t *testing.T) {
	applier := &fakeApplier{err: errors.New(
		"write slot 3030: WRONGTYPE Operation against a key holding the wrong kind of value")}
	store := newStore()
	reader := &fakeReader{events: []*domain.Event{event("orders", "1", "p1")}}

	err := runFor(t, newRunner(t, reader, applier, store), 900*time.Millisecond)
	if !domain.IsUnrecoverable(err) {
		t.Fatalf("Run returned %v, want an unrecoverable error so the task is blocked", err)
	}
	if got := store.value(""); got != "" {
		t.Errorf("recorded position %q, want none: a failed event must not be stepped over", got)
	}
}

// A failure is not proof the write did not happen.
func TestARetryAsksTheTargetWhatItHoldsFirst(t *testing.T) {
	store := &refreshingStore{fakeStore: newStore()}
	applied := make(chan struct{}, 1)
	applier := &fakeApplier{onApply: func() {
		select {
		case applied <- struct{}{}:
		default:
		}
	}}
	applier.err = errors.New("read tcp 10.0.0.1:6379: i/o timeout")
	reader := &fakeReader{events: []*domain.Event{event("orders", "1", "p1")}}

	runner := newRunner(t, reader, applier, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	time.Sleep(700 * time.Millisecond)
	applier.mu.Lock()
	applier.err = nil
	applier.mu.Unlock()

	select {
	case <-applied:
	case <-time.After(10 * time.Second):
		t.Fatal("the batch was never retried")
	}
	cancel()
	<-done

	if got := store.refreshes(); got == 0 {
		t.Error("the target was never re-read before retrying, so a landed write " +
			"would be applied twice")
	}
}

// refreshingStore is a checkpoint store that counts how often it was asked to
// re-read the target.
type refreshingStore struct {
	*fakeStore
	mu sync.Mutex
	n  int
}

func (s *refreshingStore) Refresh(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n++
	return nil
}

func (s *refreshingStore) refreshes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n
}

// TestATargetMissingTheTableBlocksRatherThanRetryingForever covers the other
// half of the retry: waiting is only right for a target that might come back.
func TestATargetMissingTheTableBlocksRatherThanRetryingForever(t *testing.T) {
	applier := &fakeApplier{err: errors.New(
		"apply 1 changes: Error 1146 (42S02): Table 'bench.nopk' doesn't exist")}
	store := newStore()
	reader := &fakeReader{events: []*domain.Event{event("nopk", "1", "p1")}}

	err := runFor(t, newRunner(t, reader, applier, store), 900*time.Millisecond)
	if !domain.IsUnrecoverable(err) {
		t.Fatalf("Run returned %v, want an unrecoverable error so the task is blocked", err)
	}
	if got := store.value(""); got != "" {
		t.Errorf("recorded position %q, want none", got)
	}
}

// A foreign key that exists on the target and not on the source refuses the
// row every time it is offered.
func TestAConstraintTheTargetAloneHoldsBlocksTheTask(t *testing.T) {
	applier := &fakeApplier{err: errors.New(
		"apply 1 changes: Error 1452 (23000): Cannot add or update a child row: " +
			"a foreign key constraint fails")}
	store := newStore()
	reader := &fakeReader{events: []*domain.Event{event("fk_child", "1", "p1")}}

	err := runFor(t, newRunner(t, reader, applier, store), 900*time.Millisecond)
	if !domain.IsUnrecoverable(err) {
		t.Fatalf("Run returned %v, want an unrecoverable error so the task is blocked", err)
	}
	if got := store.value(""); got != "" {
		t.Errorf("recorded position %q, want none", got)
	}
}

// TestPermanentApplyFailureIsDecidedBySQLState pins the classification down to
// the two SQLSTATE classes that never become applicable by being retried,
// rather than to the error numbers that happened to be met so far.
func TestPermanentApplyFailureIsDecidedBySQLState(t *testing.T) {
	permanent := []string{
		"Error 1452 (23000): Cannot add or update a child row",
		"Error 1062 (23000): Duplicate entry '5' for key 'uk'",
		"Error 1048 (23000): Column 'c' cannot be null",
		"Error 1146 (42S02): Table 'bench.nopk' doesn't exist",
		"Error 1054 (42S22): Unknown column 'gone' in 'field list'",
		"Error 1142 (42000): INSERT command denied to user",
	}
	for _, text := range permanent {
		if !permanentApplyFailure(errors.New(text)) {
			t.Errorf("permanentApplyFailure(%q) = false, want true", text)
		}
	}

	transient := []string{
		"dial tcp 10.0.0.1:3306: i/o timeout",
		"Error 1205 (HY000): Lock wait timeout exceeded",
		"Error 1213 (40001): Deadlock found when trying to get lock",
		"invalid connection",
	}
	for _, text := range transient {
		if permanentApplyFailure(errors.New(text)) {
			t.Errorf("permanentApplyFailure(%q) = true, want false: retrying may well work", text)
		}
	}
}

func counter(t *testing.T, name string, labels metrics.Labels) float64 {
	t.Helper()
	for _, s := range metrics.Default.Snapshot(name) {
		if s.Labels.Key() == labels.Key() {
			return s.Value
		}
	}
	return 0
}

// "10,000 changes applied" hides the case where every one of them was a
// delete, which is exactly what a botched migration looks like from outside.
func TestTheStreamsEventsAreCountedByOperation(t *testing.T) {
	reader := &fakeReader{events: []*domain.Event{
		{NS: domain.Namespace{DB: "shop", Object: "orders"}, Op: domain.OpInsert, Payload: "a"},
		{NS: domain.Namespace{DB: "shop", Object: "orders"}, Op: domain.OpInsert, Payload: "b"},
		{NS: domain.Namespace{DB: "shop", Object: "orders"}, Op: domain.OpUpdate, Payload: "c", EndsTransaction: true,
			Pos: domain.Position{Payload: "p1"}},
	}}
	r := newRunner(t, reader, &fakeApplier{}, newStore())
	labels := r.Opts.Labels
	defer metrics.Default.Forget(labels)
	defer metrics.Default.Forget(metrics.Labels{"task": t.Name(), "op": "insert"})
	defer metrics.Default.Forget(metrics.Labels{"task": t.Name(), "op": "update"})

	if err := runFor(t, r, 150*time.Millisecond); err != nil {
		t.Fatalf("Run: %v", err)
	}

	inserts := metrics.Labels{"task": t.Name(), "op": "insert"}
	updates := metrics.Labels{"task": t.Name(), "op": "update"}
	if got := counter(t, metrics.EventsTotal, inserts); got != 2 {
		t.Errorf("insert events = %v, want 2", got)
	}
	if got := counter(t, metrics.EventsTotal, updates); got != 1 {
		t.Errorf("update events = %v, want 1", got)
	}
}

// The defect this pipeline shipped with moved a checkpoint past a transaction
// whose rows were never read.
func TestSourceTransactionsAreCounted(t *testing.T) {
	reader := &fakeReader{events: []*domain.Event{
		{NS: domain.Namespace{DB: "shop", Object: "orders"}, Op: domain.OpInsert, Payload: "a", EndsTransaction: true,
			Pos: domain.Position{Payload: "p1"}},
		{NS: domain.Namespace{DB: "shop", Object: "orders"}, Op: domain.OpInsert, Payload: "b", EndsTransaction: true,
			Pos: domain.Position{Payload: "p2"}},
	}}
	r := newRunner(t, reader, &fakeApplier{}, newStore())
	labels := r.Opts.Labels
	defer metrics.Default.Forget(labels)
	defer metrics.Default.Forget(metrics.Labels{"task": t.Name(), "op": "insert"})

	if err := runFor(t, r, 150*time.Millisecond); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := counter(t, metrics.TransactionsCommittedTotal, labels); got != 2 {
		t.Errorf("committed transactions = %v, want 2", got)
	}
}

// TestABatchThatRollsBackCountsItsTransactionsAsRolledBack keeps refused work
// visible.
func TestABatchThatRollsBackCountsItsTransactionsAsRolledBack(t *testing.T) {
	reader := &fakeReader{events: []*domain.Event{
		{NS: domain.Namespace{DB: "shop", Object: "orders"}, Op: domain.OpInsert, Payload: "a", EndsTransaction: true,
			Pos: domain.Position{Payload: "p1"}},
	}}
	applier := &fakeApplier{err: domain.Unrecoverable("the target refused it")}
	r := newRunner(t, reader, applier, newStore())
	labels := r.Opts.Labels
	defer metrics.Default.Forget(labels)
	defer metrics.Default.Forget(metrics.Labels{"task": t.Name(), "op": "insert"})

	if err := runFor(t, r, 150*time.Millisecond); err == nil {
		t.Fatal("Run returned no error although the target refused the batch")
	}

	if got := counter(t, metrics.TransactionsRolledBackTotal, labels); got != 1 {
		t.Errorf("rolled back transactions = %v, want 1", got)
	}
}

// TestTheQueueReportsTheBytesItHolds: a queue can be shallow in events and huge
// in bytes.
func TestTheQueueReportsTheBytesItHolds(t *testing.T) {
	labels := metrics.Labels{"task": t.Name()}
	defer metrics.Default.Forget(labels)

	metrics.SetQueueBytes(labels, 2048)
	if got := gauge(t, metrics.QueueBytes, labels); got != 2048 {
		t.Errorf("queue bytes = %v, want 2048", got)
	}
}

// MySQL had SQLSTATE classification and Redis had its own list of refusals;
// MongoDB had neither, so every error it returned was treated as "the target
// is briefly unavailable, try again".
func TestADuplicateKeyOnMongoDBIsPermanent(t *testing.T) {
	for _, text := range []string{
		`bulk write exception: write errors: [E11000 duplicate key error collection: bench.orders index: uq_u dup key: { u: "conflict" }]`,
		"E11001 duplicate key on update",
		"DocumentValidationFailure: Document failed validation",
		"BSONObjectTooLarge: object to insert exceeds cappedMaxSize",
	} {
		if !permanentApplyFailure(errors.New(text)) {
			t.Errorf("not classified as permanent, so it would be retried for ever:\n  %s", text)
		}
	}
}

// TestAnOrdinaryMongoDBFailureStaysRetryable.
func TestAnOrdinaryMongoDBFailureStaysRetryable(t *testing.T) {
	for _, text := range []string{
		"server selection error: context deadline exceeded",
		"connection() error occurred during connection handshake",
		"(NotWritablePrimary) not primary",
		"socket was unexpectedly closed",
	} {
		if permanentApplyFailure(errors.New(text)) {
			t.Errorf("classified as permanent, so a passing outage would stop the task:\n  %s", text)
		}
	}
}

// driverApplier refuses a call on a cancelled context, which is what every
// database driver does and what the fixtures above do not: fakeApplier ignores
// the context it is handed, so no test using it could tell a live context from
// a dead one.
type driverApplier struct {
	mu       sync.Mutex
	events   int
	refused  bool
	deadline bool
}

func (d *driverApplier) Apply(ctx context.Context, runs [][]*domain.Event, _ domain.Position) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := ctx.Err(); err != nil {
		d.refused = true
		return false, err
	}
	if _, ok := ctx.Deadline(); ok {
		d.deadline = true
	}
	for _, run := range runs {
		d.events += len(run)
	}
	return false, nil
}

// The applier was handed the run's own context, so at a stop it was handed a
// context that had just been cancelled.
func TestTheLastBatchIsWrittenAfterTheRunIsAskedToStop(t *testing.T) {
	const trials = 50
	applied := 0

	for i := 0; i < trials; i++ {
		applier := &driverApplier{}
		r := newRunner(t, &fakeReader{}, applier, newStore())
		// Neither the timer nor the size limit may be what writes this batch.
		r.Opts.FlushInterval = time.Hour
		r.Opts.Limits = Limits{MaxEvents: 1000}

		queue := make(chan *domain.Event, 8)
		queue <- event("orders", "1", "p1")
		queue <- event("orders", "2", "p2")
		queue <- event("orders", "3", "p3")

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		if err := r.apply(ctx, queue); err != nil {
			t.Fatalf("trial %d: apply returned %v; being asked to stop is not a failure", i, err)
		}
		if applier.refused {
			t.Fatalf("trial %d: the applier was handed a cancelled context, so the "+
				"batch never reached the target", i)
		}
		if applier.events > 0 {
			applied++
			if !applier.deadline {
				t.Fatalf("trial %d: the batch was written through a context with no "+
					"deadline; a stop must not be able to outlast the pod's grace period", i)
			}
		}
	}

	if applied == 0 {
		t.Fatalf("no trial out of %d wrote anything, so this proved nothing about "+
			"what a stop does with a batch it is holding", trials)
	}
}

// TestTheQueueDepthIsReadLiveNotCached records why the gauge lied.
//
// The depth used to be cached when an event was dequeued, and the applier stops
// dequeuing the moment a batch cannot be applied -- it retries with a backoff
// instead. So the queue filled while the gauge held whatever it last saw:
// "near-empty" exactly while replication was stuck with a full queue, which is
// the one reading that makes the problem invisible.
func TestTheQueueDepthIsReadLiveNotCached(t *testing.T) {
	r := &Runner{}
	queue := make(chan *domain.Event, 8)
	r.mu.Lock()
	r.queue = queue
	r.queueCap = 8
	r.mu.Unlock()

	depth := func() int {
		r.mu.Lock()
		held := r.queue
		r.mu.Unlock()
		return len(held)
	}

	if got := depth(); got != 0 {
		t.Fatalf("an empty queue reads %d", got)
	}
	for i := 0; i < 5; i++ {
		queue <- &domain.Event{}
	}
	// Nothing has been dequeued: the old reading would still be 0.
	if got := depth(); got != 5 {
		t.Errorf("queue depth = %d after 5 events were queued and none taken, want 5", got)
	}
	<-queue
	if got := depth(); got != 4 {
		t.Errorf("queue depth = %d after one was taken, want 4", got)
	}
}

// scriptedReader hands out exactly what the test sends it, one step at a time.
type scriptedReader struct{ steps chan scriptedStep }

type scriptedStep struct {
	event *domain.Event
	err   error
}

func (s *scriptedReader) Open(context.Context, domain.Position) error { return nil }

func (s *scriptedReader) Next(ctx context.Context) (*domain.Event, error) {
	select {
	case step := <-s.steps:
		return step.event, step.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *scriptedReader) Close() error { return nil }

func (s *scriptedReader) send(e *domain.Event) { s.steps <- scriptedStep{event: e} }

// producing reads onto an unbuffered queue: each receive is the moment that event was queued.
func producing(t *testing.T) (*Runner, *scriptedReader, chan *domain.Event, context.Context) {
	t.Helper()
	stream := &scriptedReader{steps: make(chan scriptedStep)}
	r := newRunner(t, stream, &fakeApplier{}, newStore())
	r.turn = make(turn, 1)
	r.held = newBudget(1 << 20)
	queue := make(chan *domain.Event)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.read(ctx, queue) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("read: %v", err)
		}
	})
	return r, stream, queue, ctx
}

// receive takes the next n events off the queue in the order they were queued.
func receive(queue <-chan *domain.Event, n int) []*domain.Event {
	got := make([]*domain.Event, 0, n)
	for len(got) < n {
		got = append(got, <-queue)
	}
	return got
}

// batchesOf applies events in the order given and reports each batch, flattened.
func batchesOf(t *testing.T, r *Runner, events []*domain.Event) ([][]*domain.Event, error) {
	t.Helper()
	applier := &fakeApplier{}
	r.Applier = applier
	queue := make(chan *domain.Event, len(events))
	for _, e := range events {
		queue <- e
	}
	close(queue)
	err := r.apply(context.Background(), queue)

	var batches [][]*domain.Event
	for _, runs := range applier.applied() {
		var batch []*domain.Event
		for _, run := range runs {
			batch = append(batch, run...)
		}
		batches = append(batches, batch)
	}
	return batches, err
}

func holds(batch []*domain.Event, e *domain.Event) bool {
	for _, held := range batch {
		if held == e {
			return true
		}
	}
	return false
}

func sameOrder(got, want []*domain.Event) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func TestAChunkIsNotQueuedInsideASourceTransaction(t *testing.T) {
	r, stream, queue, ctx := producing(t)

	opening := event("orders", "1", "", midTransaction)
	closing := event("payments", "9", "p1")
	stream.send(opening)
	if got := <-queue; got != opening {
		t.Fatalf("queued %v first, want the transaction's first event", got.Key)
	}

	chunk := []*domain.Event{chunkRow("a"), chunkRow("b"), chunkRow("c")}
	handed := make(chan error, 1)
	go func() { handed <- r.queueChunk(ctx, queue, chunk) }()
	stream.send(closing)

	got := append([]*domain.Event{opening}, receive(queue, 1+len(chunk))...)
	if err := <-handed; err != nil {
		t.Fatalf("queueChunk: %v", err)
	}
	if want := append([]*domain.Event{opening, closing}, chunk...); !sameOrder(got, want) {
		t.Fatalf("queued %v, want the whole transaction and then the chunk", keysIn(got))
	}

	batches, err := batchesOf(t, r, got)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	for _, batch := range batches {
		if holds(batch, opening) != holds(batch, closing) {
			t.Errorf("batch %v holds half of a source transaction", keysIn(batch))
		}
	}
}

func TestASchemaChangeReadDuringAChunkIsAppliedAlone(t *testing.T) {
	r, stream, queue, ctx := producing(t)

	chunk := []*domain.Event{chunkRow("a"), chunkRow("b"), chunkRow("c"), chunkRow("d")}
	handed := make(chan error, 1)
	go func() { handed <- r.queueChunk(ctx, queue, chunk) }()
	if got := <-queue; got != chunk[0] {
		t.Fatalf("queued %v first, want the chunk's first row", got.Key)
	}

	change := event("orders", "", "p1", schema)
	stream.send(change)

	got := append([]*domain.Event{chunk[0]}, receive(queue, len(chunk))...)
	if err := <-handed; err != nil {
		t.Fatalf("queueChunk: %v", err)
	}
	if want := append(append([]*domain.Event{}, chunk...), change); !sameOrder(got, want) {
		t.Fatalf("queued %v, want the whole chunk and then the schema change", keysIn(got))
	}

	batches, err := batchesOf(t, r, got)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	applied := false
	for _, batch := range batches {
		if !holds(batch, change) {
			continue
		}
		applied = true
		if len(batch) != 1 {
			t.Errorf("the schema change shared its batch: %v", keysIn(batch))
		}
	}
	if !applied {
		t.Error("the schema change was never applied")
	}
}

func TestASchemaChangeBehindABatchThatCannotBeCutIsRefused(t *testing.T) {
	applier := &fakeApplier{}
	store := newStore()
	r := newRunner(t, &fakeReader{}, applier, store)
	r.Opts.FlushInterval = time.Hour

	queue := make(chan *domain.Event, 2)
	queue <- event("orders", "1", "", midTransaction)
	queue <- event("orders", "", "p1", schema)
	close(queue)

	if err := r.apply(context.Background(), queue); err == nil {
		t.Fatal("apply took a schema change into a batch that ends inside a transaction")
	}
	if got := len(applier.applied()); got != 0 {
		t.Errorf("applied %d batches, want none", got)
	}
	if got := store.value(""); got != "" {
		t.Errorf("recorded position %q, want none", got)
	}
}

func TestASchemaChangeThatLeavesItsTransactionOpenIsRefused(t *testing.T) {
	applier := &fakeApplier{}
	store := newStore()
	r := newRunner(t, &fakeReader{}, applier, store)
	r.Opts.FlushInterval = time.Hour

	queue := make(chan *domain.Event, 2)
	queue <- event("orders", "", "p1", schema, midTransaction)
	queue <- event("orders", "1", "p2")
	close(queue)

	if err := r.apply(context.Background(), queue); err == nil {
		t.Fatal("apply let a schema change share its batch with the event after it")
	}
	if got := len(applier.applied()); got != 0 {
		t.Errorf("applied %d batches, want none", got)
	}
	if got := store.value(""); got != "" {
		t.Errorf("recorded position %q, want none", got)
	}
}

// A chunk queued after the stream stopped part-way would close the half transaction.
func TestAReaderThatStopsInsideATransactionKeepsTheTurn(t *testing.T) {
	stream := &scriptedReader{steps: make(chan scriptedStep)}
	r := newRunner(t, stream, &fakeApplier{}, newStore())
	r.turn = make(turn, 1)
	queue := make(chan *domain.Event)

	done := make(chan error, 1)
	go func() { done <- r.read(context.Background(), queue) }()

	opening := event("orders", "1", "", midTransaction)
	stream.send(opening)
	<-queue
	failure := errors.New("connection reset")
	stream.steps <- scriptedStep{err: failure}
	if err := <-done; !errors.Is(err, failure) {
		t.Fatalf("read returned %v, want the stream's failure", err)
	}
	if len(r.turn) != 1 {
		t.Fatal("the reader gave the turn back with half a transaction queued")
	}
}

// A chunk left part-way in the queue could never be cut.
func TestAChunkWithANilRowQueuesNothing(t *testing.T) {
	r := newRunner(t, &fakeReader{}, &fakeApplier{}, newStore())
	r.turn = make(turn, 1)
	queue := make(chan *domain.Event, 3)

	chunk := []*domain.Event{chunkRow("a"), nil, chunkRow("c")}
	if err := r.queueChunk(context.Background(), queue, chunk); err == nil {
		t.Fatal("queueChunk accepted a chunk holding a nil row")
	}
	if got := len(queue); got != 0 {
		t.Errorf("queued %d rows of the chunk, want none", got)
	}
	if len(r.turn) != 0 {
		t.Error("queueChunk kept the turn after refusing the chunk")
	}
}

// Nothing else would stop a re-copy waiting for a stream that has failed.
func TestAFailedStreamEndsTheRunWhileAReCopyWaitsForIt(t *testing.T) {
	failure := errors.New("connection reset")
	stream := &scriptedReader{steps: make(chan scriptedStep, 2)}
	stream.steps <- scriptedStep{event: event("orders", "1", "p1", func(e *domain.Event) {
		e.SourceTime = time.Now().Add(-time.Hour)
	})}
	stream.steps <- scriptedStep{err: failure}

	r := newRunner(t, stream, &fakeApplier{}, newStore())
	r.Resyncs = []*Resync{{
		NS: domain.Namespace{DB: "shop", Object: "orders"},
		Reader: &fakeChunks{chunks: []Chunk{
			{Events: []*domain.Event{chunkRow("a")}, After: "a", ReadAt: time.Now()},
		}},
	}}

	done := make(chan error, 1)
	go func() { done <- r.Run(context.Background()) }()
	select {
	case err := <-done:
		if !errors.Is(err, failure) {
			t.Errorf("Run returned %v, want the stream's failure", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not end after the stream failed")
	}
}

// While the copy or the applier outlives a failed reader, the gauges are the only live signal.
func TestTheGaugesKeepRefreshingAfterTheStreamFails(t *testing.T) {
	for _, during := range []string{"copy", "apply"} {
		t.Run(during, func(t *testing.T) {
			stream := &scriptedReader{steps: make(chan scriptedStep)}
			hold := make(chan struct{})
			applier := &fakeApplier{}
			r := newRunner(t, stream, applier, newStore())
			if during == "copy" {
				r.Snapshotter = &fakeSnapshotter{pinned: domain.Position{Payload: "pinned"},
					onCopy: func() { <-hold }}
			} else {
				applier.block = hold
			}

			var mu sync.Mutex
			var ticked chan struct{}
			r.clock = func() time.Time {
				mu.Lock()
				c := ticked
				mu.Unlock()
				if c != nil {
					select {
					case c <- struct{}{}:
					default:
					}
				}
				return time.Now()
			}

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- r.Run(ctx) }()
			defer func() {
				cancel()
				close(hold)
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("Run did not end after it was asked to stop")
				}
			}()

			stream.send(event("orders", "1", "p1"))
			stream.steps <- scriptedStep{err: errors.New("connection reset")}

			c := make(chan struct{})
			mu.Lock()
			ticked = c
			mu.Unlock()
			deadline := time.After(5 * time.Second)
			for i := 0; i < 10; i++ {
				select {
				case <-c:
				case <-deadline:
					t.Fatalf("the gauges refreshed %d times after the stream failed, then stopped", i)
				}
			}
		})
	}
}

// Every producer blocks on room or on the queue while holding the turn; none may stall.
func TestATightBudgetStallsNeitherTheStreamNorAReCopy(t *testing.T) {
	const transactions = 40
	var events []*domain.Event
	for i := 0; i < transactions; i++ {
		for j := 0; j < 3; j++ {
			e := event("orders", fmt.Sprintf("%d-%d", i, j), fmt.Sprintf("p%d", i))
			e.Bytes = 8
			if j < 2 {
				e.EndsTransaction = false
				e.Pos = domain.Position{}
			}
			events = append(events, e)
		}
	}
	var chunks []Chunk
	var rows []*domain.Event
	for i := 0; i < 5; i++ {
		var chunk []*domain.Event
		for j := 0; j < 4; j++ {
			row := chunkRow(fmt.Sprintf("r%d-%d", i, j))
			row.Bytes = 8
			chunk = append(chunk, row)
		}
		rows = append(rows, chunk...)
		chunks = append(chunks, Chunk{Events: chunk, After: chunk[len(chunk)-1].Key,
			ReadAt: time.Now().Add(-time.Hour)})
	}

	want := len(events) + len(rows)
	all := make(chan struct{})
	var once sync.Once
	applier := &fakeApplier{}
	applier.onApply = func() {
		n := 0
		for _, runs := range applier.batches {
			for _, run := range runs {
				n += len(run)
			}
		}
		if n >= want {
			once.Do(func() { close(all) })
		}
	}

	r := newRunner(t, &fakeReader{events: events}, applier, newStore())
	r.Opts.QueueBytes = 16
	r.Opts.QueueCapacity = 2
	r.Resyncs = []*Resync{{NS: domain.Namespace{DB: "shop", Object: "orders"},
		Reader: &fakeChunks{chunks: chunks}}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	select {
	case <-all:
	case <-time.After(10 * time.Second):
		t.Fatal("the stream and the re-copy stalled against the byte budget")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	for _, runs := range applier.applied() {
		var batch []*domain.Event
		for _, run := range runs {
			batch = append(batch, run...)
		}
		for i := 0; i+2 < len(events); i += 3 {
			if holds(batch, events[i]) != holds(batch, events[i+2]) {
				t.Errorf("batch %v holds part of a source transaction", keysIn(batch))
			}
		}
	}
}

func keysIn(events []*domain.Event) []string {
	keys := make([]string, 0, len(events))
	for _, e := range events {
		if e.Op == domain.OpSchema {
			keys = append(keys, "DDL")
			continue
		}
		keys = append(keys, e.Key)
	}
	return keys
}

// chunkFunc lets a test act while a re-copy reads its chunk.
type chunkFunc func() (Chunk, error)

func (f chunkFunc) NextChunk(context.Context, domain.Namespace, string, int) (Chunk, error) {
	return f()
}

func flattened(batches [][][]*domain.Event) []*domain.Event {
	var events []*domain.Event
	for _, runs := range batches {
		for _, run := range runs {
			events = append(events, run...)
		}
	}
	return events
}

func TestAChangeReadAfterTheChunkIsNotOverwrittenByIt(t *testing.T) {
	for _, when := range []string{"while the chunk is read", "while the chunk waits for the stream"} {
		t.Run(when, func(t *testing.T) {
			readAt := time.Now().Truncate(time.Second)
			row := chunkRow("7")
			change := event("orders", "7", "p1", func(e *domain.Event) {
				e.SourceTime = readAt.Add(time.Second)
			})

			changeLanded, bothLanded := make(chan struct{}), make(chan struct{})
			var changeOnce, bothOnce sync.Once
			applier := &fakeApplier{}
			applier.onApply = func() {
				applied := flattened(applier.batches)
				if holds(applied, change) {
					changeOnce.Do(func() { close(changeLanded) })
					if holds(applied, row) {
						bothOnce.Do(func() { close(bothLanded) })
					}
				}
			}

			stream := &scriptedReader{steps: make(chan scriptedStep)}
			read := make(chan struct{})
			chunks := chunkFunc(func() (Chunk, error) {
				if when == "while the chunk is read" {
					stream.send(change)
					select {
					case <-changeLanded:
					case <-time.After(300 * time.Millisecond):
					}
				}
				close(read)
				return Chunk{Events: []*domain.Event{row}, After: "7", ReadAt: readAt, Done: true}, nil
			})

			r := newRunner(t, stream, applier, newStore())
			r.Resyncs = []*Resync{{NS: domain.Namespace{DB: "shop", Object: "orders"}, Reader: chunks}}

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- r.Run(ctx) }()
			defer func() {
				cancel()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("Run did not end after it was asked to stop")
				}
			}()

			<-read
			if when == "while the chunk waits for the stream" {
				stream.send(change)
			}
			select {
			case <-bothLanded:
			case <-time.After(5 * time.Second):
				t.Fatal("the chunk's row and the stream's change did not both reach the target")
			}

			var last *domain.Event
			for _, e := range flattened(applier.applied()) {
				if e.Key == "7" {
					last = e
				}
			}
			// The chunk's row is older than the change, so writing it last leaves the target stale.
			if last != change {
				t.Errorf("the re-copy's row was written over a newer change the stream read after it")
			}
		})
	}
}
