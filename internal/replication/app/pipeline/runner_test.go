package pipeline

import (
	"context"
	"errors"
	"fmt"
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

// ------------------------------------------------------- retention headroom

// windowedReader is a reader that can say how far back its source's log reaches.
type windowedReader struct {
	*fakeReader
	window time.Duration
	err    error
	calls  int
}

func (w *windowedReader) Window(context.Context) (time.Duration, error) {
	w.calls++
	return w.window, w.err
}

func headroomOf(t *testing.T, labels metrics.Labels) (float64, bool) {
	t.Helper()
	for _, s := range metrics.Default.Snapshot(metrics.RetentionHeadroomSeconds) {
		if s.Labels.Key() == labels.Key() {
			return s.Value, true
		}
	}
	return 0, false
}

// runReporting drives one report tick against a runner whose clock is fixed.
func runReporting(t *testing.T, r *Runner, lastRead time.Time) {
	t.Helper()
	r.mu.Lock()
	r.lastReadAt = lastRead
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
//
// A day of binlog and a minute behind leaves a day less a minute. Once that
// reaches zero the saved position has been purged and the only way back is
// copying the database again, which is a decision worth making before the
// deadline rather than after it.
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

// TestTheHeadroomGoesNegativeOnceTheWindowIsPast keeps the metric from being
// clamped at zero.
//
// A task an hour past its window and one a week past it need different answers:
// the first is a re-copy, the second is a re-copy plus a conversation about how
// nobody noticed for a week. Clamping loses that, and it also loses the slope an
// alert would have fired on.
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

// TestASourceThatCannotSayPublishesNothing covers the sharded MongoDB case,
// where the oplog is not reachable through mongos.
//
// A guessed window would read exactly like a measured one, and it is read at
// the moment somebody is deciding whether they still have time. Absence is the
// honest answer.
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
	if source.calls != 1 {
		t.Errorf("the source was asked %d times, want once — a source that cannot answer "+
			"should not be asked, or logged about, on every tick", source.calls)
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

	if source.calls != 1 {
		t.Errorf("the source was asked %d times over many ticks, want once", source.calls)
	}
}

// TestStreamOrderHandsTheBatchOverUnsplit covers the engine whose log is a
// command stream rather than a set of record writes.
//
// Splitting a batch into runs lets an applier parallelise within a run, which is
// correct when every event is an idempotent write of a whole record. A command
// stream has neither property: replaying INCR adds again, and two commands on
// different keys may have been one atomic act at the source. So the batch has to
// arrive in the order it was read, as one run.
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
//
// The refusal path gives up for the lifetime of the process, which is right for
// a source that can never answer and wrong for one that needs measuring twice
// before it can work out a rate. Conflating them meant the headroom metric never
// appeared at all for such a source: the very first question failed, by design,
// and nothing asked again.
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

	if source.calls < 2 {
		t.Errorf("the source was asked %d time(s); a source that says 'not yet' has "+
			"to be asked again, or the metric never appears", source.calls)
	}
	if _, ok := headroomOf(t, labels); ok {
		t.Error("a headroom was published before the source could say what its window is")
	}
}

// TestASourceThatSaysNotYetAndThenAnswersIsPublished covers the whole sequence.
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
	r.mu.Lock()
	r.lastReadAt = now.Add(-30 * time.Second)
	r.mu.Unlock()

	stop := r.report(context.Background())
	time.Sleep(30 * time.Millisecond)

	// The second measurement arrives.
	source.err = nil
	source.window = 2 * time.Hour

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

// TestAnIdleBatchIsSentWithoutWaitingForTheWindow covers the flush that happens
// because nothing else is queued.
//
// The window used to be paid on every batch that did not fill, which is every
// batch on a quiet source. Measured against Memorystore across regions at 500
// writes a second, it put 263 ms of the 500 ms window into the recovery point
// for a target that was idle at one percent of a core — latency bought nothing,
// because there was no second event to amortise the round trip over.
//
// The assertion has to be about *when* the batch arrives, not that it arrives:
// cancelling the context flushes what is whole, so any batch is applied
// eventually. The window here is 30s and the deadline 500ms, so only an
// immediate send can pass.
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

// TestATargetThatIsNotReadyDoesNotStopTheReader covers the decoupling.
//
// Ending the run over a failed write also stopped the reader, and the reader is
// what keeps the source's replication log from rolling past the position. On
// Memorystore that log is a fixed ring measured at 10–30 KB, so a target that
// was briefly away cost a full re-copy of the whole keyspace.
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
//
// A WRONGTYPE will be refused identically for as long as anybody retries it,
// and stepping over it leaves the target permanently different from the source
// with nothing blocked and nothing alarming — which is what was measured.
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

// TestARetryAsksTheTargetWhatItHoldsFirst covers the duplicate that retrying
// in place would otherwise create.
//
// A failure is not proof the write did not happen: a timeout can arrive after
// the transaction landed. Re-applying then repeats a command that is not
// idempotent, which was measured as an RPUSH landing three times too often
// under packet loss. Restarting the task always re-read the target; retrying in
// place has to do the same.
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
//
// A table that does not exist on the target will not appear by being asked
// again, and holding the batch forever blocks every later event behind it while
// the task still reports itself up. Measured against a table that existed only
// on the source: the pipeline sat retrying "Table 'bench.nopk' doesn't exist",
// task_up stayed 1, task_blocked stayed 0, and nothing downstream was applied.
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

// TestAConstraintTheTargetAloneHoldsBlocksTheTask covers the other permanent
// refusal the target can produce.
//
// A foreign key that exists on the target and not on the source refuses the row
// every time it is offered. Measured against one: the pipeline retried "Cannot
// add or update a child row" on a backoff for as long as it was left running,
// with task_up at 1 and task_blocked at 0, so nothing said the replication had
// stopped making progress.
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

// ---------------------------------------------------- what the stream carried

// counter reads a counter series the way a scrape would.
func counter(t *testing.T, name string, labels metrics.Labels) float64 {
	t.Helper()
	for _, s := range metrics.Default.Snapshot(name) {
		if s.Labels.Key() == labels.Key() {
			return s.Value
		}
	}
	return 0
}

// TestTheStreamsEventsAreCountedByOperation is the Debezium split this pipeline
// had no answer for. "10,000 changes applied" hides the case where every one of
// them was a delete, which is exactly what a botched migration looks like from
// outside.
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

// TestSourceTransactionsAreCounted is the other half of the pair that catches a
// transaction going missing.
//
// The defect this pipeline shipped with moved a checkpoint past a transaction
// whose rows were never read. The applied count looked healthy — everything
// that arrived was applied. Counting transactions carried through gives the
// number to compare against the source's own, and the comparison is what makes
// the loss visible instead of leaving it to a reconciliation pass hours later.
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
// visible. The applied counter only ever counts what succeeded, so without this
// a batch that failed leaves no trace on any graph.
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
// in bytes. One large document filling the buffer and a hundred thousand small
// ones need different answers, and an event count alone cannot tell them apart.
func TestTheQueueReportsTheBytesItHolds(t *testing.T) {
	labels := metrics.Labels{"task": t.Name()}
	defer metrics.Default.Forget(labels)

	metrics.SetQueueBytes(labels, 2048)
	if got := gauge(t, metrics.QueueBytes, labels); got != 2048 {
		t.Errorf("queue bytes = %v, want 2048", got)
	}
}

// TestADuplicateKeyOnMongoDBIsPermanent is the gap a sharded-cluster run found.
//
// MySQL had SQLSTATE classification and Redis had its own list of refusals;
// MongoDB had neither, so every error it returned was treated as "the target is
// briefly unavailable, try again". Measured against a real pair: a document
// that collided with a unique index on the target held its batch and retried it
// for ever, with task_blocked at 0 and task_up at 1. Nothing was lost — the
// position does not move — but nothing was replicated either, and the only sign
// was the lag climbing. That is the failure this classification exists to turn
// into a stop with a reason attached.
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

// TestAnOrdinaryMongoDBFailureStaysRetryable. The other half of the line: a
// target that is merely unreachable has to be waited out, not stopped for.
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
