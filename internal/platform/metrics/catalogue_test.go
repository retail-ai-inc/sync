package metrics

import (
	"strings"
	"testing"
	"time"
)

// value reads one series out of the default registry, which is where the
// catalogue's helpers record.
func value(t *testing.T, name string, want Labels) (float64, bool) {
	t.Helper()

	for _, s := range Default.Snapshot(name) {
		if s.Labels.Key() == want.Key() {
			return s.Value, true
		}
	}
	return 0, false
}

func mustValue(t *testing.T, name string, labels Labels) float64 {
	t.Helper()

	v, ok := value(t, name, labels)
	if !ok {
		t.Fatalf("%s{%s} was never recorded", name, labels.Key())
	}
	return v
}

// TestEventsAreCountedByOperation is the Debezium split that a single applied
// counter cannot give: three attributes there, one metric with an op label
// here.
func TestEventsAreCountedByOperation(t *testing.T) {
	labels := Labels{"task": "opsplit", "engine": "mysql"}
	c := NewEventCounters(labels)

	c.Count("insert", 3)
	c.Count("update", 2)
	c.Count("delete", 1)
	c.Count("insert", 1)

	insert := Labels{"task": "opsplit", "engine": "mysql", "op": "insert"}
	update := Labels{"task": "opsplit", "engine": "mysql", "op": "update"}
	remove := Labels{"task": "opsplit", "engine": "mysql", "op": "delete"}

	if got := mustValue(t, EventsTotal, insert); got != 4 {
		t.Errorf("insert count = %v, want 4", got)
	}
	if got := mustValue(t, EventsTotal, update); got != 2 {
		t.Errorf("update count = %v, want 2", got)
	}
	if got := mustValue(t, EventsTotal, remove); got != 1 {
		t.Errorf("delete count = %v, want 1", got)
	}
}

// TestCountingAnEventDoesNotTouchTheCallersLabels guards the hot path.
func TestCountingAnEventDoesNotTouchTheCallersLabels(t *testing.T) {
	labels := Labels{"task": "shared", "engine": "redis"}
	c := NewEventCounters(labels)
	c.Count("insert", 1)

	if _, ok := labels["op"]; ok {
		t.Fatalf("the caller's labels gained an op: %v", labels)
	}
	if len(labels) != 2 {
		t.Fatalf("the caller's labels were changed: %v", labels)
	}
}

// TestAnUnpreparedOperationIsStillCounted: an operation this code does not know
// about is exactly the one somebody needs to see, so it must not be dropped for
// want of a prepared label set.
func TestAnUnpreparedOperationIsStillCounted(t *testing.T) {
	labels := Labels{"task": "novel", "engine": "mongodb"}
	c := NewEventCounters(labels)

	c.Count("replace", 2)

	want := Labels{"task": "novel", "engine": "mongodb", "op": "replace"}
	if got := mustValue(t, EventsTotal, want); got != 2 {
		t.Errorf("replace count = %v, want 2", got)
	}
}

// TestTransactionsAndEventsAreCountedSeparately is the pair that catches a
// whole source transaction going missing.
func TestTransactionsAndEventsAreCountedSeparately(t *testing.T) {
	labels := Labels{"task": "tx", "engine": "mysql"}

	CountTransaction(labels, 2)
	CountEvent(labels, "insert", 6)

	if got := mustValue(t, TransactionsCommittedTotal, labels); got != 2 {
		t.Errorf("committed transactions = %v, want 2", got)
	}
	want := Labels{"task": "tx", "engine": "mysql", "op": "insert"}
	if got := mustValue(t, EventsTotal, want); got != 6 {
		t.Errorf("events = %v, want 6", got)
	}
}

// TestARolledBackBatchIsCounted keeps the refused work visible.
func TestARolledBackBatchIsCounted(t *testing.T) {
	labels := Labels{"task": "rollback", "engine": "mysql"}

	CountRolledBack(labels, 3)
	CountRolledBack(labels, 0) // zero must not create a series full of noise

	if got := mustValue(t, TransactionsRolledBackTotal, labels); got != 3 {
		t.Errorf("rolled back = %v, want 3", got)
	}
}

// TestConnectedIsNotTheSameAsUp is the distinction Debezium draws with
// Connected and this codebase used to miss: a task can be up and disconnected
// while it retries, and that is the window where the source's log rolls past a
// position nobody is reading.
func TestConnectedIsNotTheSameAsUp(t *testing.T) {
	labels := Labels{"task": "conn", "engine": "redis"}

	SetTaskUp(labels, true)
	SetConnected(labels, false)

	if got := mustValue(t, TaskUp, labels); got != 1 {
		t.Errorf("task_up = %v, want 1", got)
	}
	if got := mustValue(t, Connected, labels); got != 0 {
		t.Errorf("connected = %v, want 0 — an up task that is disconnected must "+
			"be distinguishable from a healthy one", got)
	}
}

// TestASnapshotReportsRunningThenCompleted walks the states Debezium's snapshot
// context exposes.
func TestASnapshotReportsRunningThenCompleted(t *testing.T) {
	labels := Labels{"task": "snap", "engine": "mongodb"}

	SnapshotStarted(labels, 4)
	if got := mustValue(t, SnapshotRunning, labels); got != 1 {
		t.Errorf("running = %v, want 1", got)
	}
	if got := mustValue(t, SnapshotObjectsRemaining, labels); got != 4 {
		t.Errorf("remaining = %v, want 4", got)
	}

	SnapshotProgress(labels, 500, 2, 12)
	if got := mustValue(t, SnapshotRowsScannedTotal, labels); got != 500 {
		t.Errorf("rows scanned = %v, want 500", got)
	}
	if got := mustValue(t, SnapshotObjectsRemaining, labels); got != 2 {
		t.Errorf("remaining = %v, want 2", got)
	}
	if got := mustValue(t, SnapshotDurationSeconds, labels); got != 12 {
		t.Errorf("duration = %v, want 12", got)
	}

	SnapshotFinished(labels, true, 30)
	if got := mustValue(t, SnapshotRunning, labels); got != 0 {
		t.Errorf("running after finishing = %v, want 0", got)
	}
	if got := mustValue(t, SnapshotCompleted, labels); got != 1 {
		t.Errorf("completed = %v, want 1", got)
	}
	if got := mustValue(t, SnapshotAborted, labels); got != 0 {
		t.Errorf("aborted = %v, want 0", got)
	}
	if got := mustValue(t, SnapshotObjectsRemaining, labels); got != 0 {
		t.Errorf("remaining after finishing = %v, want 0", got)
	}
}

// TestAnAbandonedSnapshotIsNotReportedAsCompleted is the case that matters
// operationally: a copy that stopped half way must not look like one that
// finished, or the stream starts from a position the target never reached.
func TestAnAbandonedSnapshotIsNotReportedAsCompleted(t *testing.T) {
	labels := Labels{"task": "abandoned", "engine": "redis"}

	SnapshotStarted(labels, 3)
	SnapshotFinished(labels, false, 7)

	if got := mustValue(t, SnapshotCompleted, labels); got != 0 {
		t.Errorf("completed = %v, want 0", got)
	}
	if got := mustValue(t, SnapshotAborted, labels); got != 1 {
		t.Errorf("aborted = %v, want 1", got)
	}
	if got := mustValue(t, SnapshotRunning, labels); got != 0 {
		t.Errorf("running = %v, want 0", got)
	}
}

// TestSchemaChangesAreCountedApartFromRefusals: carrying a DDL and refusing to
// carry one are opposite outcomes, and a single counter would add them
// together. The refusal is the one that needs a human.
func TestSchemaChangesAreCountedApartFromRefusals(t *testing.T) {
	labels := Labels{"task": "ddl", "engine": "mysql"}

	CountSchemaChange(labels, 2)
	CountSchemaRefused(labels, "blocked")
	CountSchemaRefused(labels, "blocked")
	CountSchemaRefused(labels, "skipped")

	if got := mustValue(t, SchemaChangesTotal, labels); got != 2 {
		t.Errorf("applied schema changes = %v, want 2", got)
	}
	blocked := Labels{"task": "ddl", "engine": "mysql", "reason": "blocked"}
	if got := mustValue(t, SchemaChangesRefusedTotal, blocked); got != 2 {
		t.Errorf("blocked refusals = %v, want 2", got)
	}
	skipped := Labels{"task": "ddl", "engine": "mysql", "reason": "skipped"}
	if got := mustValue(t, SchemaChangesRefusedTotal, skipped); got != 1 {
		t.Errorf("skipped refusals = %v, want 1", got)
	}
}

// TestTheSourceInfoSeriesCarriesTheSlowMovingPartsOfThePosition guards the
// cardinality decision.
func TestTheSourceInfoSeriesCarriesTheSlowMovingPartsOfThePosition(t *testing.T) {
	labels := Labels{"task": "pos", "engine": "mysql"}

	SetSourcePosition(labels, 4096)
	SetSourceInfo(labels, "mysql-bin.000052", "10.60.0.5:3306/bench")

	if got := mustValue(t, SourcePositionBytes, labels); got != 4096 {
		t.Errorf("position = %v, want 4096", got)
	}
	want := Labels{
		"task": "pos", "engine": "mysql",
		"file": "mysql-bin.000052", "server": "10.60.0.5:3306/bench",
	}
	if got := mustValue(t, SourceInfo, want); got != 1 {
		t.Errorf("info series = %v, want 1", got)
	}
}

// TestTheQueueReportsDepthAndBytes: a queue can be shallow in events and huge
// in bytes, and the two lead to different answers.
func TestTheQueueReportsDepthAndBytes(t *testing.T) {
	labels := Labels{"task": "queue", "engine": "mongodb"}

	SetQueue(labels, 12, 1000)
	SetQueueBytes(labels, 4_000_000)

	if got := mustValue(t, QueueUsedEvents, labels); got != 12 {
		t.Errorf("used = %v, want 12", got)
	}
	if got := mustValue(t, QueueCapacityEvents, labels); got != 1000 {
		t.Errorf("capacity = %v, want 1000", got)
	}
	if got := mustValue(t, QueueBytes, labels); got != 4_000_000 {
		t.Errorf("bytes = %v, want 4000000", got)
	}
}

// TestTheRedisOffsetsAlsoPublishTheNeutralNames keeps one dashboard panel
// working across engines: the Redis relay measures in stream bytes, and those
// same numbers appear under the engine-neutral position names.
func TestTheRedisOffsetsAlsoPublishTheNeutralNames(t *testing.T) {
	labels := Labels{"task": "redispos", "engine": "redis"}

	SetStreamOffset(labels, 900, 128)
	SetAppliedOffset(labels, 850)

	if got := mustValue(t, StreamOffsetBytes, labels); got != 900 {
		t.Errorf("stream offset = %v, want 900", got)
	}
	if got := mustValue(t, AppliedPositionBytes, labels); got != 850 {
		t.Errorf("neutral applied position = %v, want 850", got)
	}
	if got := mustValue(t, BufferBytes, labels); got != 128 {
		t.Errorf("buffer bytes = %v, want 128", got)
	}
}

// TestEveryCatalogueMetricRendersWithHelpAndType is what makes the exposition
// readable by Grafana's metric browser: a series with no HELP or TYPE line is
// one nobody can find without knowing its name already.
func TestEveryCatalogueMetricRendersWithHelpAndType(t *testing.T) {
	r := New()
	labels := Labels{"task": "render", "engine": "mysql"}

	r.SetGauge(LagSeconds, helpLag, labels, 1)
	r.AddCounter(EventsTotal, helpEvents, withLabel(labels, "op", "insert"), 1)
	r.SetGauge(SnapshotRunning, helpSnapshotRunning, labels, 1)
	r.AddCounter(SchemaChangesTotal, helpSchemaChanges, labels, 1)

	out := exposition(t, r)
	for _, name := range []string{LagSeconds, EventsTotal, SnapshotRunning, SchemaChangesTotal} {
		if !strings.Contains(out, "# HELP "+name+" ") {
			t.Errorf("%s has no HELP line", name)
		}
		if !strings.Contains(out, "# TYPE "+name+" ") {
			t.Errorf("%s has no TYPE line", name)
		}
	}
	if !strings.Contains(out, "# TYPE "+EventsTotal+" counter") {
		t.Errorf("%s is not typed as a counter", EventsTotal)
	}
	if !strings.Contains(out, "# TYPE "+SnapshotRunning+" gauge") {
		t.Errorf("%s is not typed as a gauge", SnapshotRunning)
	}
}

// TestObserveBatchRecordsEveryPartOfTheCost keeps the batch sums together: a
// batch that is slow because of round trips and one that is slow because of the
// commit need different fixes.
func TestObserveBatchRecordsEveryPartOfTheCost(t *testing.T) {
	labels := Labels{"task": "batch", "engine": "mysql"}

	ObserveBatch(labels, 200*time.Millisecond, 50*time.Millisecond, 3, 2, 40)

	if got := mustValue(t, BatchApplyCount, labels); got != 1 {
		t.Errorf("batches = %v, want 1", got)
	}
	if got := mustValue(t, BatchApplySeconds, labels); got != 0.2 {
		t.Errorf("apply seconds = %v, want 0.2", got)
	}
	if got := mustValue(t, BatchCommitSeconds, labels); got != 0.05 {
		t.Errorf("commit seconds = %v, want 0.05", got)
	}
	if got := mustValue(t, BatchRoundTripsSum, labels); got != 3 {
		t.Errorf("round trips = %v, want 3", got)
	}
	if got := mustValue(t, BatchEventsSum, labels); got != 40 {
		t.Errorf("events = %v, want 40", got)
	}
}
