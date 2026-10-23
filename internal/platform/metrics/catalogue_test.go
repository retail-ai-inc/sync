package metrics

import (
	"testing"
	"time"
)

// The catalogue is thin -- each entry names a metric and writes it -- and thin
// is what makes it worth pinning. A setter that writes the wrong metric name,
// or a counter helper that lets a zero through and creates a series nobody
// meant, is invisible in the code and shows up as a panel that is empty or a
// series that never goes away.

func labelsFor(t *testing.T) Labels {
	return Labels{"task": t.Name(), "engine": "test"}
}

func TestTheGaugeSettersWriteTheirOwnMetric(t *testing.T) {
	labels := labelsFor(t)

	for _, c := range []struct {
		name   string
		set    func()
		want   float64
		metric string
	}{
		{"last event age", func() { SetLastEventAge(labels, 12.5) }, 12.5, LastEventAgeSeconds},
		{"captured tables", func() { SetCapturedTables(labels, 7) }, 7, CapturedTables},
		{"schema change age", func() { SetSchemaChangeAge(labels, 900) }, 900, SchemaChangeAgeSeconds},
		{"dead lettered", func() { SetDeadLettered(labels, 3) }, 3, DeadLettered},
		{"unreplicated", func() { SetUnreplicated(labels, 4) }, 4, Unreplicated},
		{"source lag", func() { SetSourceLag(labels, 8192) }, 8192, SourceLagBytes},
		{"reconcile difference", func() { SetReconcileDifference(labels, 11) }, 11, ReconcileDifference},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Cleanup(func() { Default.Forget(labels) })
			c.set()
			if got := sampleValue(t, c.metric, labels); got != c.want {
				t.Errorf("%s = %v, want %v", c.metric, got, c.want)
			}
		})
	}
}

// TestSetRetentionWritesBothHalves covers the one setter that writes two
// metrics. A headroom left unwritten reads as zero, which is the value that
// means "the window is exactly as long as the lag" -- an alarm state reported
// for a link that is fine.
func TestSetRetentionWritesBothHalves(t *testing.T) {
	labels := labelsFor(t)
	t.Cleanup(func() { Default.Forget(labels) })

	SetRetention(labels, 25092, 25086)

	if got := sampleValue(t, RetentionWindowSeconds, labels); got != 25092 {
		t.Errorf("window = %v, want 25092", got)
	}
	if got := sampleValue(t, RetentionHeadroomSeconds, labels); got != 25086 {
		t.Errorf("headroom = %v, want 25086", got)
	}
}

// TestTheCounterHelpersIgnoreAZero covers why they test n > 0: a counter set
// created for a zero is a series that exists forever, reporting nothing, on
// every task that never filtered or skipped anything.
func TestTheCounterHelpersIgnoreAZero(t *testing.T) {
	labels := labelsFor(t)
	t.Cleanup(func() { Default.Forget(labels) })

	CountFiltered(labels, 0)
	CountSkipped(labels, 0)
	CountValueRepairs(labels, 0)

	for _, name := range []string{EventsFilteredTotal, EventsSkippedTotal, ValueRepairsTotal} {
		if _, ok := seriesFor(Default, name, labels); ok {
			t.Errorf("%s was created for a count of zero", name)
		}
	}
}

func TestTheCounterHelpersAdvanceOnAPositiveCount(t *testing.T) {
	labels := labelsFor(t)
	t.Cleanup(func() { Default.Forget(labels) })

	CountFiltered(labels, 3)
	CountFiltered(labels, 2)
	CountSkipped(labels, 1)
	CountValueRepairs(labels, 4)
	CountDisconnect(labels)
	CountDisconnect(labels)

	for name, want := range map[string]float64{
		EventsFilteredTotal: 5,
		EventsSkippedTotal:  1,
		ValueRepairsTotal:   4,
		DisconnectsTotal:    2,
	} {
		if got := sampleValue(t, name, labels); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
}

// TestTheTargetReadinessGaugesAreAlwaysWritten covers the point of having them
// at all: a target that is set up correctly has to report 0, not nothing. An
// absent series cannot be distinguished from a task that never checked, so an
// alert on it would fire for every stopped task.
func TestTheTargetReadinessGaugesAreAlwaysWritten(t *testing.T) {
	labels := labelsFor(t)
	t.Cleanup(func() { Default.Forget(labels) })

	SetTargetEvictsKeys(labels, false)
	SetTargetTooSmall(labels, false)
	SetTargetMissingModules(labels, 0)

	for _, name := range []string{TargetEvictsKeys, TargetTooSmall, TargetMissingModules} {
		got, ok := seriesFor(Default, name, labels)
		if !ok {
			t.Errorf("%s was not written for a target that is set up correctly", name)
			continue
		}
		if got != 0 {
			t.Errorf("%s = %v, want 0", name, got)
		}
	}
}

func TestTheTargetReadinessGaugesReportAProblem(t *testing.T) {
	labels := labelsFor(t)
	t.Cleanup(func() { Default.Forget(labels) })

	SetTargetEvictsKeys(labels, true)
	SetTargetTooSmall(labels, true)
	SetTargetMissingModules(labels, 2)

	for name, want := range map[string]float64{
		TargetEvictsKeys:     1,
		TargetTooSmall:       1,
		TargetMissingModules: 2,
	} {
		if got := sampleValue(t, name, labels); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
}

func sampleValue(t *testing.T, name string, labels Labels) float64 {
	t.Helper()
	value, ok := seriesFor(Default, name, labels)
	if !ok {
		t.Fatalf("%s{%v} is not reported", name, labels)
	}
	return value
}

// TestWithLabelDoesNotMutateTheCallersMap covers what the copy is for: a task
// holds one label set for its whole life and passes it to every setter. A
// setter that added its own label in place would leave every later metric
// carrying it, so one schema refusal would put a reason label on the lag.
func TestWithLabelDoesNotMutateTheCallersMap(t *testing.T) {
	held := Labels{"task": "39", "engine": "mongodb"}

	with := withLabel(held, "op", "insert")

	if len(held) != 2 {
		t.Errorf("the caller's map grew to %v", held)
	}
	if with["op"] != "insert" || with["task"] != "39" {
		t.Errorf("withLabel produced %v", with)
	}
	with["task"] = "changed"
	if held["task"] != "39" {
		t.Error("the two maps share storage")
	}
}

func TestSetConnectedIsABoolean(t *testing.T) {
	labels := labelsFor(t)
	t.Cleanup(func() { Default.Forget(labels) })

	SetConnected(labels, true)
	if got := sampleValue(t, Connected, labels); got != 1 {
		t.Errorf("connected = %v, want 1", got)
	}
	SetConnected(labels, false)
	if got := sampleValue(t, Connected, labels); got != 0 {
		t.Errorf("disconnected = %v, want 0", got)
	}
}

// TestSetTaskInfoCarriesTheEndpointsAsLabels covers the info metric's shape: the
// value is always 1 and the endpoints are labels, which is how a dashboard shows
// them as text.
func TestSetTaskInfoCarriesTheEndpointsAsLabels(t *testing.T) {
	labels := labelsFor(t)
	with := Labels{"task": t.Name(), "engine": "test",
		"source": "10.118.192.8:3306", "target": "10.60.117.91:6379"}
	t.Cleanup(func() { Default.Forget(labels) })

	SetTaskInfo(labels, "10.118.192.8:3306", "10.60.117.91:6379")

	if got := sampleValue(t, TaskInfo, with); got != 1 {
		t.Errorf("task info = %v, want 1", got)
	}
	if len(labels) != 2 {
		t.Errorf("the caller's label set was changed to %v", labels)
	}
}

func TestSetSourceInfoCarriesTheFileAndServer(t *testing.T) {
	labels := labelsFor(t)
	t.Cleanup(func() { Default.Forget(labels) })

	SetSourceInfo(labels, "mysql-bin.000123", "10.118.192.8:3306")

	with := Labels{"task": t.Name(), "engine": "test",
		"file": "mysql-bin.000123", "server": "10.118.192.8:3306"}
	if got := sampleValue(t, SourceInfo, with); got != 1 {
		t.Errorf("source info = %v, want 1", got)
	}
}

func TestThePositionAndOffsetSetters(t *testing.T) {
	labels := labelsFor(t)
	t.Cleanup(func() { Default.Forget(labels) })

	SetSourcePosition(labels, 4096)
	SetAppliedPosition(labels, 4000)
	SetQueue(labels, 12, 2000)
	SetQueueBytes(labels, 1<<20)

	for name, want := range map[string]float64{
		SourcePositionBytes:  4096,
		AppliedPositionBytes: 4000,
		QueueUsedEvents:      12,
		QueueCapacityEvents:  2000,
		QueueBytes:           1 << 20,
	} {
		if got := sampleValue(t, name, labels); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
}

// TestTheRedisOffsetSettersPublishTwice pins a deliberate double write. Redis
// positions are byte offsets in a replication stream, which is engine-specific,
// but the dashboard's cross-engine panels read the generic position and buffer
// metrics -- so each setter writes both, and a Redis task appears in both
// places. Dropping either leaves one of them permanently empty for Redis.
func TestTheRedisOffsetSettersPublishTwice(t *testing.T) {
	labels := labelsFor(t)
	t.Cleanup(func() { Default.Forget(labels) })

	SetStreamOffset(labels, 1276319, 8192)
	SetAppliedOffset(labels, 1276000)
	SetUnappliedBytes(labels, 1276319-1276000)

	for name, want := range map[string]float64{
		StreamOffsetBytes: 1276319,
		// What the buffer keeps and what is waiting to be applied are different
		// numbers: the buffer holds history on purpose, so reporting its size as
		// the backlog made an idle task look like one falling behind.
		BufferHeldBytes:      8192,
		BufferBytes:          319,
		AppliedOffsetBytes:   1276000,
		AppliedPositionBytes: 1276000,
	} {
		if got := sampleValue(t, name, labels); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
}

// An applied offset ahead of what has been received -- a position restored
// from the target after a restart, before the stream has caught up with it --
// is nothing waiting, not a negative backlog.
func TestNothingWaitingIsReportedAsZero(t *testing.T) {
	labels := labelsFor(t)
	t.Cleanup(func() { Default.Forget(labels) })

	SetUnappliedBytes(labels, -512)
	if got := sampleValue(t, BufferBytes, labels); got != 0 {
		t.Errorf("%s = %v, want 0", BufferBytes, got)
	}
}

// TestEventCountersCountPerOperation covers the op label: a task that only ever
// deletes and one that only ever inserts are the same total without it.
func TestEventCountersCountPerOperation(t *testing.T) {
	labels := labelsFor(t)
	counters := NewEventCounters(labels)
	t.Cleanup(func() { Default.Forget(labels) })

	counters.Count("insert", 5)
	counters.Count("insert", 2)
	counters.Count("delete", 1)

	for op, want := range map[string]float64{"insert": 7, "delete": 1} {
		with := withLabel(labels, "op", op)
		if got := sampleValue(t, EventsTotal, with); got != want {
			t.Errorf("%s events = %v, want %v", op, got, want)
		}
	}
}

// TestAnUnpreparedOperationIsStillCounted covers the fallback. An operation
// nobody listed is exactly the one worth seeing, so it is counted under its own
// name rather than dropped or folded into "unknown".
func TestAnUnpreparedOperationIsStillCounted(t *testing.T) {
	labels := labelsFor(t)
	counters := NewEventCounters(labels)
	t.Cleanup(func() { Default.Forget(labels) })

	counters.Count("replace", 3)

	with := withLabel(labels, "op", "replace")
	if got := sampleValue(t, EventsTotal, with); got != 3 {
		t.Errorf("replace events = %v, want 3", got)
	}
}

func TestEventCountersIgnoreAZeroAndANilReceiver(t *testing.T) {
	labels := labelsFor(t)
	counters := NewEventCounters(labels)
	t.Cleanup(func() { Default.Forget(labels) })

	counters.Count("insert", 0)
	if _, ok := seriesFor(Default, EventsTotal, withLabel(labels, "op", "insert")); ok {
		t.Error("a series was created for a count of zero")
	}

	var absent *EventCounters
	absent.Count("insert", 1)
}

// TestASnapshotThatStartsIsRunningAndNotYetDone covers the three-way state the
// dashboard reads. All three are written at once, because a task restarting
// into a fresh copy has to clear the previous run's "completed".
func TestASnapshotThatStartsIsRunningAndNotYetDone(t *testing.T) {
	labels := labelsFor(t)
	t.Cleanup(func() { Default.Forget(labels) })

	SnapshotFinished(labels, true, 100) // a previous run
	SnapshotStarted(labels, 12)

	for name, want := range map[string]float64{
		SnapshotRunning:          1,
		SnapshotCompleted:        0,
		SnapshotAborted:          0,
		SnapshotObjectsTotal:     12,
		SnapshotObjectsRemaining: 12,
		SnapshotDurationSeconds:  0,
	} {
		if got := sampleValue(t, name, labels); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
}

func TestSnapshotProgressAdvancesTheRowsAndCountsDownTheObjects(t *testing.T) {
	labels := labelsFor(t)
	t.Cleanup(func() { Default.Forget(labels) })

	SnapshotStarted(labels, 12)
	SnapshotProgress(labels, 5000, 9, 30)
	SnapshotProgress(labels, 4000, 7, 61)

	for name, want := range map[string]float64{
		SnapshotRowsScannedTotal: 9000,
		SnapshotObjectsRemaining: 7,
		SnapshotDurationSeconds:  61,
	} {
		if got := sampleValue(t, name, labels); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
}

// TestASnapshotThatFinishesCleanlyHasNothingLeft: a completed copy with objects
// still outstanding is a contradiction the dashboard would draw as a stalled
// bar.
func TestASnapshotThatFinishesCleanlyHasNothingLeft(t *testing.T) {
	labels := labelsFor(t)
	t.Cleanup(func() { Default.Forget(labels) })

	SnapshotStarted(labels, 12)
	SnapshotProgress(labels, 100, 4, 10)
	SnapshotFinished(labels, true, 120)

	for name, want := range map[string]float64{
		SnapshotRunning:          0,
		SnapshotCompleted:        1,
		SnapshotAborted:          0,
		SnapshotObjectsRemaining: 0,
		SnapshotDurationSeconds:  120,
	} {
		if got := sampleValue(t, name, labels); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
}

// TestAnAbortedSnapshotKeepsWhatWasLeft: how much was outstanding when it gave
// up is the diagnostic, so it is not zeroed.
func TestAnAbortedSnapshotKeepsWhatWasLeft(t *testing.T) {
	labels := labelsFor(t)
	t.Cleanup(func() { Default.Forget(labels) })

	SnapshotStarted(labels, 12)
	SnapshotProgress(labels, 100, 4, 10)
	SnapshotFinished(labels, false, 45)

	for name, want := range map[string]float64{
		SnapshotRunning:          0,
		SnapshotCompleted:        0,
		SnapshotAborted:          1,
		SnapshotObjectsRemaining: 4,
	} {
		if got := sampleValue(t, name, labels); got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
}

func TestSchemaChangesAreCountedAndRefusalsCarryTheReason(t *testing.T) {
	labels := labelsFor(t)
	t.Cleanup(func() { Default.Forget(labels) })

	CountSchemaChange(labels, 0)
	if _, ok := seriesFor(Default, SchemaChangesTotal, labels); ok {
		t.Error("a series was created for zero schema changes")
	}

	CountSchemaChange(labels, 2)
	if got := sampleValue(t, SchemaChangesTotal, labels); got != 2 {
		t.Errorf("schema changes = %v, want 2", got)
	}

	CountSchemaRefused(labels, "drops a column")
	CountSchemaRefused(labels, "drops a column")
	with := withLabel(labels, "reason", "drops a column")
	if got := sampleValue(t, SchemaChangesRefusedTotal, with); got != 2 {
		t.Errorf("refusals = %v, want 2", got)
	}
}

// TestObserveBatchAdvancesEverySum covers the six counters a batch writes. They
// are sums rather than averages so a dashboard can divide by the count over any
// window; a missing one makes the ratio that uses it wrong rather than absent.
func TestObserveBatchAdvancesEverySum(t *testing.T) {
	labels := labelsFor(t)
	t.Cleanup(func() { Default.Forget(labels) })

	ObserveBatch(labels, 200*time.Millisecond, 50*time.Millisecond, 3, 2, 500)
	ObserveBatch(labels, 100*time.Millisecond, 25*time.Millisecond, 1, 1, 100)

	for name, want := range map[string]float64{
		BatchApplyCount:    2,
		BatchApplySeconds:  0.3,
		BatchCommitSeconds: 0.075,
		BatchRoundTripsSum: 4,
		BatchNamespacesSum: 3,
		BatchEventsSum:     600,
	} {
		got := sampleValue(t, name, labels)
		if diff := got - want; diff > 1e-9 || diff < -1e-9 {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
}
