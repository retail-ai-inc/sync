package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// Every limit here has a default, and a task's configuration is allowed to
// leave any of them out -- the whole API silently dropped seven task settings
// once, so which value a zero resolves to is worth pinning rather than
// assuming. A default that quietly became zero would mean an unbounded queue
// or a batch that is never cut.

func TestAnUnsetLimitResolvesToItsDefault(t *testing.T) {
	var limits Limits
	if got := limits.maxEvents(); got != defaultMaxEvents {
		t.Errorf("maxEvents() = %d, want %d", got, defaultMaxEvents)
	}
	if got := limits.maxBytes(); got != defaultMaxBytes {
		t.Errorf("maxBytes() = %d, want %d", got, defaultMaxBytes)
	}
	if got := limits.maxTransactionEvents(); got != defaultMaxTransactionEvents {
		t.Errorf("maxTransactionEvents() = %d, want %d", got, defaultMaxTransactionEvents)
	}
}

func TestASetLimitIsUsed(t *testing.T) {
	limits := Limits{MaxEvents: 7, MaxBytes: 1024, MaxTransactionEvents: 3}
	if got := limits.maxEvents(); got != 7 {
		t.Errorf("maxEvents() = %d, want 7", got)
	}
	if got := limits.maxBytes(); got != 1024 {
		t.Errorf("maxBytes() = %d, want 1024", got)
	}
	if got := limits.maxTransactionEvents(); got != 3 {
		t.Errorf("maxTransactionEvents() = %d, want 3", got)
	}
}

func TestAnUnsetOptionResolvesToItsDefault(t *testing.T) {
	var options Options
	for name, got := range map[string]interface{}{
		"flushInterval":         options.flushInterval(),
		"queueBytes":            options.queueBytes(),
		"queueCapacity":         options.queueCapacity(),
		"snapshotQueueCapacity": options.snapshotQueueCapacity(),
		"reportInterval":        options.reportInterval(),
		"shutdownGrace":         options.shutdownGrace(),
	} {
		switch value := got.(type) {
		case time.Duration:
			if value <= 0 {
				t.Errorf("%s() = %v; an unset option must not resolve to zero", name, value)
			}
		case int:
			if value <= 0 {
				t.Errorf("%s() = %d; an unset option must not resolve to zero", name, value)
			}
		case int64:
			if value <= 0 {
				t.Errorf("%s() = %d; an unset option must not resolve to zero", name, value)
			}
		}
	}

	if got := options.flushInterval(); got != defaultFlushInterval {
		t.Errorf("flushInterval() = %v, want %v", got, defaultFlushInterval)
	}
	if got := options.queueBytes(); got != defaultQueueBytes {
		t.Errorf("queueBytes() = %d, want %d", got, defaultQueueBytes)
	}
	if got := options.queueCapacity(); got != defaultQueueCapacity {
		t.Errorf("queueCapacity() = %d, want %d", got, defaultQueueCapacity)
	}
	if got := options.snapshotQueueCapacity(); got != defaultSnapshotQueueCapacity {
		t.Errorf("snapshotQueueCapacity() = %d, want %d", got, defaultSnapshotQueueCapacity)
	}
	if got := options.shutdownGrace(); got != defaultShutdownGrace {
		t.Errorf("shutdownGrace() = %v, want %v", got, defaultShutdownGrace)
	}
}

func TestASetOptionIsUsed(t *testing.T) {
	options := Options{
		FlushInterval:         2 * time.Second,
		QueueBytes:            1 << 20,
		QueueCapacity:         11,
		SnapshotQueueCapacity: 13,
		ReportInterval:        3 * time.Second,
		ShutdownGrace:         17 * time.Second,
	}
	if got := options.flushInterval(); got != 2*time.Second {
		t.Errorf("flushInterval() = %v", got)
	}
	if got := options.queueBytes(); got != 1<<20 {
		t.Errorf("queueBytes() = %d", got)
	}
	if got := options.queueCapacity(); got != 11 {
		t.Errorf("queueCapacity() = %d", got)
	}
	if got := options.snapshotQueueCapacity(); got != 13 {
		t.Errorf("snapshotQueueCapacity() = %d", got)
	}
	if got := options.reportInterval(); got != 3*time.Second {
		t.Errorf("reportInterval() = %v", got)
	}
	if got := options.shutdownGrace(); got != 17*time.Second {
		t.Errorf("shutdownGrace() = %v", got)
	}
}

// TestAShutdownGraceFitsInsideKubernetesTerminationPeriod: a grace longer than
// the pod's thirty seconds means SIGKILL arrives mid-drain, and whatever the
// task had buffered goes with it.
func TestAShutdownGraceFitsInsideKubernetesTerminationPeriod(t *testing.T) {
	if defaultShutdownGrace >= 30*time.Second {
		t.Errorf("defaultShutdownGrace = %v, which is not inside the default "+
			"thirty-second termination grace period", defaultShutdownGrace)
	}
}

func TestAnUnsetResyncChunkSizeResolvesToItsDefault(t *testing.T) {
	resync := &Resync{}
	if got := resync.chunkSize(); got != defaultChunkSize {
		t.Errorf("chunkSize() = %d, want %d", got, defaultChunkSize)
	}
	resync.ChunkSize = 250
	if got := resync.chunkSize(); got != 250 {
		t.Errorf("chunkSize() = %d, want 250", got)
	}
}

// TestAnEmptyBatchIsNotCuttable: cutting an empty batch would commit a position
// nothing had been applied for, so a restart would resume past events the
// target never received.
func TestAnEmptyBatchIsNotCuttable(t *testing.T) {
	if (&batch{}).cuttable() {
		t.Error("an empty batch reported itself cuttable")
	}
}

func TestABatchIsCuttableOnlyAtATransactionBoundary(t *testing.T) {
	mid := &batch{events: []*domain.Event{
		{EndsTransaction: false},
	}}
	if mid.cuttable() {
		t.Error("a batch ending mid-transaction reported itself cuttable")
	}

	whole := &batch{events: []*domain.Event{
		{EndsTransaction: false},
		{EndsTransaction: true},
	}}
	if !whole.cuttable() {
		t.Error("a batch ending on a transaction boundary reported itself not cuttable")
	}
}

// TestHoldsSchemaChangeFindsOneAnywhere covers the check that keeps a schema
// change from sharing a batch with rows. MongoDB's catalogue is not
// transactional, so a batch carrying both cannot be applied atomically.
func TestHoldsSchemaChangeFindsOneAnywhere(t *testing.T) {
	for name, events := range map[string][]*domain.Event{
		"first":  {{Op: domain.OpSchema}, {Op: domain.OpInsert}},
		"last":   {{Op: domain.OpInsert}, {Op: domain.OpSchema}},
		"middle": {{Op: domain.OpInsert}, {Op: domain.OpSchema}, {Op: domain.OpUpdate}},
		"alone":  {{Op: domain.OpSchema}},
	} {
		t.Run(name, func(t *testing.T) {
			if !holdsSchemaChange(events) {
				t.Error("a batch carrying a schema change reported none")
			}
		})
	}

	for name, events := range map[string][]*domain.Event{
		"rows only": {{Op: domain.OpInsert}, {Op: domain.OpUpdate}, {Op: domain.OpDelete}},
		"empty":     {},
		"nil":       nil,
	} {
		t.Run(name, func(t *testing.T) {
			if holdsSchemaChange(events) {
				t.Error("a batch with no schema change reported one")
			}
		})
	}
}

// What the lag figure means. It answers one question -- how far behind is the
// target -- and the whole point of these is that it must not answer a different
// one when the first has no interesting answer.

func TestLagIsTheAgeOfTheOldestUnappliedChange(t *testing.T) {
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	oldest := now.Add(-90 * time.Second)

	got, known := lagSeconds(now, oldest, now.Add(-time.Second), now.Add(-time.Hour))
	if !known {
		t.Fatal("a task with a backlog reported an unknown lag")
	}
	if got != 90 {
		t.Errorf("lag = %v, want 90 -- the age of the oldest change still waiting", got)
	}
}

// TestCaughtUpIsZeroNotTheAgeOfTheLastHeartbeat is the fix this exists for.
//
// Nothing waiting means nothing is behind. Reporting the age of the newest
// thing heard instead answered a different question, and on a quiet source the
// newest thing heard is the last heartbeat -- so the gauge walked from zero up
// to the heartbeat interval and dropped back, reading as three to five seconds
// of steady lag on links measured end to end at about a tenth of a second.
func TestCaughtUpIsZeroNotTheAgeOfTheLastHeartbeat(t *testing.T) {
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)

	for name, c := range map[string]struct{ read, applied time.Time }{
		"heartbeat nine seconds ago":      {now.Add(-9 * time.Second), now.Add(-time.Hour)},
		"nothing read for an hour":        {now.Add(-time.Hour), now.Add(-time.Hour)},
		"applied long ago, read just now": {now, now.Add(-24 * time.Hour)},
	} {
		t.Run(name, func(t *testing.T) {
			got, known := lagSeconds(now, time.Time{}, c.read, c.applied)
			if !known {
				t.Fatal("a task that has read something reported an unknown lag")
			}
			if got != 0 {
				t.Errorf("lag = %v with nothing waiting to be applied; the target is "+
					"not behind, and this is measuring how long since we last heard "+
					"from the source instead", got)
			}
		})
	}
}

// TestALagIsUnknownBeforeAnythingIsRead: a task that has not reached its source
// is not caught up, and publishing zero for it would show a healthy figure for
// a link that has never worked.
func TestALagIsUnknownBeforeAnythingIsRead(t *testing.T) {
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)

	if _, known := lagSeconds(now, time.Time{}, time.Time{}, time.Time{}); known {
		t.Error("a task that has read nothing published a lag")
	}
}

// TestABacklogWinsOverEverythingElse: once something is waiting, that is the
// answer regardless of how recently the stream was heard from.
func TestABacklogWinsOverEverythingElse(t *testing.T) {
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)

	got, known := lagSeconds(now, now.Add(-10*time.Minute), now, now)
	if !known || got != 600 {
		t.Errorf("lag = %v (known=%v), want 600 -- a fresh heartbeat does not clear "+
			"a ten-minute backlog", got, known)
	}
}

// TestATransactionLargerThanTheBudgetStillReplicates covers a deadlock, so it
// is written to fail by timing out rather than by an assertion.
//
// The applier may not cut a batch inside a source transaction, so while one is
// open it releases nothing. A reader that waited for budget room before handing
// over that transaction's last event was therefore waiting for a release that
// only its own next event could bring about: neither side could move and
// replication stopped for good. Two events against a budget that fits one
// reproduces it, and a large enough MULTI block or MySQL transaction does the
// same against the real default.
func TestATransactionLargerThanTheBudgetStillReplicates(t *testing.T) {
	ns := domain.Namespace{DB: "shop", Object: "orders"}
	reader := &fakeReader{events: []*domain.Event{
		// One transaction of three events, none of which ends it until the last.
		{NS: ns, Op: domain.OpInsert, Key: "1", Bytes: 6, EndsTransaction: false},
		{NS: ns, Op: domain.OpInsert, Key: "2", Bytes: 6, EndsTransaction: false},
		{NS: ns, Op: domain.OpInsert, Key: "3", Bytes: 6,
			Pos: domain.Position{Payload: "p1"}, EndsTransaction: true},
	}}
	applier := &fakeApplier{}

	r := newRunner(t, reader, applier, newStore())
	labels := metrics.Labels{"task": t.Name()}
	r.Opts.Labels = labels
	// A budget that fits one event of the three.
	r.Opts.QueueBytes = 10
	t.Cleanup(func() { metrics.Default.Forget(labels) })

	done := make(chan error, 1)
	go func() { done <- runFor(t, r, 300*time.Millisecond) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the runner never returned: the reader is waiting for budget room " +
			"that only its own next event can release, and the applier cannot cut " +
			"a batch inside the transaction")
	}

	applier.mu.Lock()
	applied := 0
	for _, batch := range applier.batches {
		for _, run := range batch {
			applied += len(run)
		}
	}
	applier.mu.Unlock()
	if applied != 3 {
		t.Errorf("applied %d events, want the whole transaction of 3", applied)
	}
}

// TestARecopyRecordsProgressOnlyAfterTheRowsLand covers a re-copy quietly
// skipping part of what it was asked to repair.
//
// Progress is stored so an interrupted run resumes where it stopped. Handing a
// chunk to the queue is not applying it, and recording progress on the
// hand-over meant a crash between the two lost those rows for good: the next
// run resumed past them. The comment above the save said the opposite -- that
// an interrupted run repeats a chunk rather than skipping one -- which is the
// guarantee this restores.
func TestARecopyRecordsProgressOnlyAfterTheRowsLand(t *testing.T) {
	ns := domain.Namespace{DB: "shop", Object: "orders"}
	progress := newStore()

	// An applier that refuses to accept anything until released, so the chunk
	// is in the queue and unapplied for as long as the test needs.
	blocked := make(chan struct{})
	applier := &fakeApplier{block: blocked}

	resync := &Resync{
		NS:          ns,
		ProgressKey: "orders",
		Progress:    progress,
		Reader: &oneChunkReader{chunk: Chunk{
			ReadAt: time.Now().Add(-time.Hour),
			Events: []*domain.Event{{NS: ns, Op: domain.OpInsert, Key: "1", Bytes: 1}},
			After:  "1",
			Done:   true,
		}},
	}

	// The chunk is held until the stream has been read past the moment it was
	// read at, so the stream has to have delivered something newer.
	stream := &fakeReader{events: []*domain.Event{
		{NS: ns, Op: domain.OpInsert, Key: "stream", Bytes: 1,
			SourceTime: time.Now(), EndsTransaction: true,
			Pos: domain.Position{Payload: "p1"}},
	}}

	r := newRunner(t, stream, applier, newStore())
	labels := metrics.Labels{"task": t.Name()}
	r.Opts.Labels = labels
	r.Resyncs = []*Resync{resync}
	t.Cleanup(func() { metrics.Default.Forget(labels) })

	go func() { _ = runFor(t, r, 2*time.Second) }()

	// While the applier is blocked the chunk cannot have landed, so nothing may
	// have been recorded.
	time.Sleep(250 * time.Millisecond)
	if stored, _ := progress.Load(context.Background(), "orders"); stored != "" {
		t.Errorf("progress was recorded as %q while the rows were still in the "+
			"queue; a crash here loses them and the next run resumes past them",
			stored)
	}
	close(blocked)
}

// oneChunkReader hands out a single chunk and then reports it is done.
type oneChunkReader struct {
	chunk Chunk
	given bool
}

func (o *oneChunkReader) NextChunk(context.Context, domain.Namespace, string, int) (Chunk, error) {
	if o.given {
		return Chunk{Done: true}, nil
	}
	o.given = true
	return o.chunk, nil
}
