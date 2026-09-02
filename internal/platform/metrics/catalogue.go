// The metric catalogue, modelled on Debezium's names and its three contexts —
// snapshot, streaming, schema history — so an operator who has run Debezium
// need not learn a second vocabulary. Two departures.
package metrics

import "time"

// ---------------------------------------------------------------- connector

// Whether the task is running at all, and whether it is running because
// somebody restarted it. Debezium answers this through Kafka Connect's
// connector state rather than an MBean; the distinction between "stopped and
// restarted" and "stopped and staying stopped" is the one an operator acts on,
// so it is a metric here.
const (
	TaskUp = "sync_task_up"
	// TaskBlocked is 1 while a task is stopped for a reason retrying cannot fix.
	//
	// No Debezium equivalent: a failed connector is FAILED whatever the cause,
	// and the two need separate alerts. A blocked task needs somebody to look
	// at one event; a task that is merely down needs the process restarted.
	TaskBlocked   = "sync_task_blocked"
	RestartsTotal = "sync_task_restarts_total"
	// Connected is 1 while the source stream is established.
	//
	// Debezium: Connected. It is not the same as TaskUp — a task can be up and
	// disconnected while it retries, and that is exactly the window where the
	// source's log is rolling past a position nobody is reading.
	Connected = "sync_source_connected"

	helpTaskUp    = "1 while the task is replicating, 0 once it has stopped"
	helpBlocked   = "1 while a task is stopped for a reason retrying cannot fix"
	helpRestarts  = "Times a task has been restarted after stopping by itself"
	helpConnected = "1 while the source stream is established, 0 while disconnected"
)

func SetTaskUp(labels Labels, up bool) { setBool(TaskUp, helpTaskUp, labels, up) }

func SetTaskBlocked(labels Labels, blocked bool) {
	setBool(TaskBlocked, helpBlocked, labels, blocked)
}

func CountRestart(labels Labels) { Default.AddCounter(RestartsTotal, helpRestarts, labels, 1) }

func SetConnected(labels Labels, connected bool) {
	setBool(Connected, helpConnected, labels, connected)
}

func setBool(name, help string, labels Labels, on bool) {
	v := 0.0
	if on {
		v = 1
	}
	Default.SetGauge(name, help, labels, v)
}

// ---------------------------------------------------------------- streaming

// What the stream is delivering and how far behind it is. This is the context
// an operator watches once replication is steady.
const (
	// LagSeconds is how far behind the source the target is, measured from the
	// timestamp the source put on the change.
	//
	// Debezium: MilliSecondsBehindSource, in seconds.
	LagSeconds = "sync_replication_lag_seconds"
	// ReadLagSeconds is how old an event was when the syncer read it. The
	// difference between this and LagSeconds is the syncer's own backlog rather
	// than the source's or the network's.
	ReadLagSeconds = "sync_source_event_age_seconds"
	// LastEventAgeSeconds is how long since anything arrived, heartbeats
	// included (Debezium: MilliSecondsSinceLastEvent). The applied lag cannot
	// answer this: a stream that stops delivering leaves it frozen, and an alert
	// on a frozen gauge never fires.
	LastEventAgeSeconds = "sync_source_last_event_age_seconds"

	// EventsTotal counts source events by operation. Debezium keeps one
	// attribute per operation; an op label is the Prometheus spelling of it.
	EventsTotal = "sync_source_events_total"
	// EventsFilteredTotal counts events dropped because nothing maps them.
	//
	// Debezium: NumberOfEventsFiltered.
	EventsFilteredTotal = "sync_source_events_filtered_total"
	// EventsSkippedTotal counts events the reader could not interpret and
	// stepped over.
	//
	// Debezium: NumberOfSkippedEvents.
	EventsSkippedTotal = "sync_source_events_skipped_total"
	// TransactionsCommittedTotal counts source transactions carried through.
	// Paired with EventsTotal it catches a whole transaction going missing —
	// a checkpoint that moved past rows nobody read, which this shipped with.
	TransactionsCommittedTotal = "sync_source_transactions_committed_total"
	// TransactionsRolledBackTotal counts source transactions rolled back.
	//
	// Debezium: NumberOfRolledBackTransactions.
	TransactionsRolledBackTotal = "sync_source_transactions_rolled_back_total"
	// CapturedTables is how many tables or collections the task is watching.
	//
	// Debezium: CapturedTables.
	CapturedTables = "sync_captured_tables"
	// DisconnectsTotal counts source stream drops.
	//
	// Debezium: NumberOfDisconnects.
	DisconnectsTotal = "sync_source_disconnects_total"

	// AppliedTotal counts changes written to the target.
	//
	// No Debezium equivalent — it hands events to Kafka and the sink is
	// somebody else's problem. Here the target is the point of the exercise, so
	// what reached it is counted separately from what was read.
	AppliedTotal = "sync_changes_applied_total"
	FailedTotal  = "sync_changes_failed_total"

	helpLag          = "Seconds between a change being made at the source and applied at the target"
	helpReadLag      = "Seconds between a change being made at the source and read by the syncer"
	helpLastEventAge = "Seconds since any event, heartbeat included, arrived from the source"
	helpEvents       = "Source events read, by operation"
	helpFiltered     = "Source events dropped because no mapping covers them"
	helpSkipped      = "Source events the reader could not interpret and stepped over"
	helpCommitted    = "Source transactions carried through to the target"
	helpRolledBack   = "Source transactions that were rolled back"
	helpCaptured     = "Tables or collections this task is watching"
	helpDisconnects  = "Times the source stream dropped and had to be re-established"
	helpApplied      = "Changes written to the target"
	helpFailed       = "Changes the target refused"
)

// The op label uses the words the domain already uses — insert, update,
// delete, schema. Debezium says "create" where this says "insert"; renaming to
// match would leave the metric disagreeing with every log line and every error
// message in this codebase, which is a worse kind of confusion than the one it
// would fix.

// EventCounters holds one prepared label set per operation.
//
// Counting an event is on the hot path — a thousand a second is an ordinary
// afternoon — and building a label map per event would allocate a map per
// event. The label sets are built once, when the task starts.
type EventCounters struct {
	byOp map[string]Labels
	base Labels
}

func NewEventCounters(labels Labels) *EventCounters {
	c := &EventCounters{byOp: make(map[string]Labels, 5), base: labels}
	for _, op := range []string{"insert", "update", "delete", "schema", "unknown"} {
		c.byOp[op] = withLabel(labels, "op", op)
	}
	return c
}

func (c *EventCounters) Count(op string, n int) {
	if c == nil || n == 0 {
		return
	}
	labels, ok := c.byOp[op]
	if !ok {
		// An operation nobody prepared for. Counting it under its own name
		// costs one allocation and is still better than not counting it: an
		// operation this code does not know about is exactly what somebody
		// needs to see.
		labels = withLabel(c.base, "op", op)
		c.byOp[op] = labels
	}
	Default.AddCounter(EventsTotal, helpEvents, labels, float64(n))
}

func SetLag(labels Labels, seconds float64) { Default.SetGauge(LagSeconds, helpLag, labels, seconds) }

func SetReadLag(labels Labels, seconds float64) {
	Default.SetGauge(ReadLagSeconds, helpReadLag, labels, seconds)
}

func SetLastEventAge(labels Labels, seconds float64) {
	Default.SetGauge(LastEventAgeSeconds, helpLastEventAge, labels, seconds)
}

// CountEvent counts source events of one operation, for callers that count
// rarely enough not to care about the allocation. On a hot path use
// EventCounters instead.
func CountEvent(labels Labels, op string, n int) {
	if n == 0 {
		return
	}
	Default.AddCounter(EventsTotal, helpEvents, withLabel(labels, "op", op), float64(n))
}

func CountFiltered(labels Labels, n int) {
	if n > 0 {
		Default.AddCounter(EventsFilteredTotal, helpFiltered, labels, float64(n))
	}
}

func CountSkipped(labels Labels, n int) {
	if n > 0 {
		Default.AddCounter(EventsSkippedTotal, helpSkipped, labels, float64(n))
	}
}

func CountTransaction(labels Labels, n int) {
	if n > 0 {
		Default.AddCounter(TransactionsCommittedTotal, helpCommitted, labels, float64(n))
	}
}

func CountRolledBack(labels Labels, n int) {
	if n > 0 {
		Default.AddCounter(TransactionsRolledBackTotal, helpRolledBack, labels, float64(n))
	}
}

func SetCapturedTables(labels Labels, n int) {
	Default.SetGauge(CapturedTables, helpCaptured, labels, float64(n))
}

func CountDisconnect(labels Labels) {
	Default.AddCounter(DisconnectsTotal, helpDisconnects, labels, 1)
}

func Applied(labels Labels, n int) { Default.AddCounter(AppliedTotal, helpApplied, labels, float64(n)) }

func Failed(labels Labels, n int) { Default.AddCounter(FailedTotal, helpFailed, labels, float64(n)) }

// withLabel copies a label set with one more label, so a caller's map is never
// mutated behind its back — the same map is usually held for the task's life.
func withLabel(labels Labels, name, value string) Labels {
	out := make(Labels, len(labels)+1)
	for k, v := range labels {
		out[k] = v
	}
	out[name] = value
	return out
}

// ------------------------------------------------------------------ position

// Where in the source's log the task has got to. Only a number can be graphed,
// so the position is bytes; slow-changing strings go on an info series and the
// GTID set stays in the logs, where it cannot leak cardinality.
const (
	// SourcePositionBytes is the offset read to inside the source's current log
	// segment. It is not cumulative across segments, so it drops to near zero
	// when MySQL rotates a binlog — the shape mysqld_exporter also publishes.
	// Read it with SourceInfo's file label; alert on lag and retention instead.
	SourcePositionBytes = "sync_source_position_bytes"
	// AppliedPositionBytes is the position the target has actually recorded.
	// Within one log segment the gap between the two is the backlog in the
	// units the source measures it in, which is what decides whether a stopped
	// task can still resume.
	AppliedPositionBytes = "sync_applied_position_bytes"
	// SourceInfo is 1, carrying the slow-moving parts of the position as
	// labels: the log file the stream is in and the server it came from.
	SourceInfo = "sync_source_info"

	helpSourcePosition  = "Byte offset read to within the source's current log segment"
	helpAppliedPosition = "Byte offset the target has recorded as applied"
	helpSourceInfo      = "1, labelled with the source log file and server identity"
)

func SetSourcePosition(labels Labels, offset int64) {
	Default.SetGauge(SourcePositionBytes, helpSourcePosition, labels, float64(offset))
}

func SetAppliedPosition(labels Labels, offset int64) {
	Default.SetGauge(AppliedPositionBytes, helpAppliedPosition, labels, float64(offset))
}

// SetSourceInfo publishes the identity of the position being read.
//
// Only pass values that change on the order of hours — a log file name, a
// server id. Anything that changes per transaction makes a new series every
// time and leaks cardinality until the scrape fails.
func SetSourceInfo(labels Labels, file, server string) {
	with := withLabel(withLabel(labels, "file", file), "server", server)
	Default.SetGauge(SourceInfo, helpSourceInfo, with, 1)
}

// -------------------------------------------------------------------- queue

// The reader's hand-off to the applier. Debezium exposes QueueTotalCapacity,
// QueueRemainingCapacity, CurrentQueueSizeInBytes and MaxQueueSizeInBytes; a
// queue that is persistently full is the signal that the target, not the
// source, is the limit.
const (
	QueueCapacityEvents = "sync_queue_capacity_events"
	// QueueUsedEvents is how many it holds now. Debezium reports the remainder
	// instead; used is the direction that reads as "pressure" on a graph, and
	// the remainder is one subtraction away.
	QueueUsedEvents = "sync_queue_used_events"
	// QueueBytes is how much unapplied change data is held, in memory and on
	// disk together.
	QueueBytes = "sync_queue_bytes"
	// BufferBytes is the part of that which is on local disk.
	//
	// No Debezium equivalent — it has no disk buffer, its queue is bounded and
	// it applies backpressure to the source instead. Here the Redis path spools
	// to disk, and disk that fills is an outage with no warning otherwise.
	BufferBytes = "sync_buffer_bytes"

	helpQueueCapacity = "Events the reader-to-applier queue may hold"
	helpQueueUsed     = "Events the reader-to-applier queue holds now"
	helpQueueBytes    = "Bytes of unapplied change data held, in memory and on disk"
	HelpBufferBytes   = "Bytes of change data on local disk waiting to be applied to the target"
)

func SetQueue(labels Labels, used, capacity int) {
	Default.SetGauge(QueueUsedEvents, helpQueueUsed, labels, float64(used))
	Default.SetGauge(QueueCapacityEvents, helpQueueCapacity, labels, float64(capacity))
}

func SetQueueBytes(labels Labels, bytes int64) {
	Default.SetGauge(QueueBytes, helpQueueBytes, labels, float64(bytes))
}

// ----------------------------------------------------------------- snapshot

// The initial copy. Debezium's snapshot context answers "is it running, how far
// has it got, did it finish or give up" — questions this codebase could not
// answer at all, which mattered the moment a Redis shard had to be re-copied
// because the source's backlog had rolled past its position.
const (
	SnapshotRunning          = "sync_snapshot_running"
	SnapshotCompleted        = "sync_snapshot_completed"
	SnapshotAborted          = "sync_snapshot_aborted"
	SnapshotDurationSeconds  = "sync_snapshot_duration_seconds"
	SnapshotRowsScannedTotal = "sync_snapshot_rows_scanned_total"
	// SnapshotObjectsTotal is how many tables, collections or shards the copy
	// covers, and SnapshotObjectsRemaining how many it has left.
	SnapshotObjectsTotal     = "sync_snapshot_objects_total"
	SnapshotObjectsRemaining = "sync_snapshot_objects_remaining"

	helpSnapshotRunning   = "1 while an initial copy is in progress"
	helpSnapshotCompleted = "1 once an initial copy has finished cleanly"
	helpSnapshotAborted   = "1 if an initial copy stopped without finishing"
	helpSnapshotDuration  = "Seconds the current or last initial copy has taken"
	helpSnapshotRows      = "Rows, documents or keys read by the initial copy"
	helpSnapshotObjects   = "Tables, collections or shards the initial copy covers"
	helpSnapshotRemaining = "Tables, collections or shards the initial copy has left"
)

func SnapshotStarted(labels Labels, objects int) {
	setBool(SnapshotRunning, helpSnapshotRunning, labels, true)
	setBool(SnapshotCompleted, helpSnapshotCompleted, labels, false)
	setBool(SnapshotAborted, helpSnapshotAborted, labels, false)
	Default.SetGauge(SnapshotObjectsTotal, helpSnapshotObjects, labels, float64(objects))
	Default.SetGauge(SnapshotObjectsRemaining, helpSnapshotRemaining, labels, float64(objects))
	Default.SetGauge(SnapshotDurationSeconds, helpSnapshotDuration, labels, 0)
}

func SnapshotProgress(labels Labels, rowsScanned, objectsRemaining int, elapsed float64) {
	if rowsScanned > 0 {
		Default.AddCounter(SnapshotRowsScannedTotal, helpSnapshotRows, labels, float64(rowsScanned))
	}
	Default.SetGauge(SnapshotObjectsRemaining, helpSnapshotRemaining, labels, float64(objectsRemaining))
	Default.SetGauge(SnapshotDurationSeconds, helpSnapshotDuration, labels, elapsed)
}

func SnapshotFinished(labels Labels, completed bool, elapsed float64) {
	setBool(SnapshotRunning, helpSnapshotRunning, labels, false)
	setBool(SnapshotCompleted, helpSnapshotCompleted, labels, completed)
	setBool(SnapshotAborted, helpSnapshotAborted, labels, !completed)
	Default.SetGauge(SnapshotDurationSeconds, helpSnapshotDuration, labels, elapsed)
	if completed {
		Default.SetGauge(SnapshotObjectsRemaining, helpSnapshotRemaining, labels, 0)
	}
}

// ----------------------------------------------------------- schema history

// Schema changes. Debezium's schema-history context reports what it has
// recovered and applied; here the number that matters is how many DDL
// statements have been carried to the target and when the last one landed,
// because a schema change that did not arrive is how rows silently land in the
// wrong columns.
const (
	SchemaChangesTotal     = "sync_schema_changes_applied_total"
	SchemaChangeAgeSeconds = "sync_schema_last_change_age_seconds"
	// SchemaChangesRefusedTotal counts schema changes deliberately not carried —
	// a DROP that would empty the target, a rename that would orphan it. A
	// decision nobody can see is a decision nobody can audit.
	SchemaChangesRefusedTotal = "sync_schema_changes_refused_total"

	helpSchemaChanges = "Schema changes carried through to the target"
	helpSchemaAge     = "Seconds since the last schema change was carried through"
	helpSchemaRefused = "Schema changes deliberately not carried through, by reason"
)

func CountSchemaChange(labels Labels, n int) {
	if n > 0 {
		Default.AddCounter(SchemaChangesTotal, helpSchemaChanges, labels, float64(n))
	}
}

func SetSchemaChangeAge(labels Labels, seconds float64) {
	Default.SetGauge(SchemaChangeAgeSeconds, helpSchemaAge, labels, seconds)
}

func CountSchemaRefused(labels Labels, reason string) {
	Default.AddCounter(SchemaChangesRefusedTotal, helpSchemaRefused,
		withLabel(labels, "reason", reason), 1)
}

// ------------------------------------------------------- source retention

// How long a stopped task has before its position is unusable. On Memorystore
// the backlog measured five to twenty kilobytes — a fraction of a second at
// load — so this decides whether a restart is routine or means a re-copy.
const (
	RetentionWindowSeconds = "sync_source_retention_window_seconds"
	// RetentionHeadroomSeconds is that window less the current lag: how long
	// the task could stay stopped before its saved position is purged.
	RetentionHeadroomSeconds = "sync_source_retention_headroom_seconds"

	helpRetentionWindow   = "Seconds of history the source still holds (binlog expiry, oplog window)"
	helpRetentionHeadroom = "Seconds a stopped task has left before its position falls out of the source's log"
)

func SetRetention(labels Labels, window, headroom float64) {
	Default.SetGauge(RetentionWindowSeconds, helpRetentionWindow, labels, window)
	Default.SetGauge(RetentionHeadroomSeconds, helpRetentionHeadroom, labels, headroom)
}

// ------------------------------------------------------------ correctness

// What comparing the two sides found, and what had to be set aside. Every
// stream defect found here was found by comparing, not by the stream saying so.
const (
	ReconcileDifference = "sync_redis_reconcile_difference"
	// ValueRepairsTotal counts keys copied whole rather than by replaying a
	// command. A rate above zero outside the first copy means something is
	// being repaired.
	ValueRepairsTotal = "sync_redis_value_repairs_total"
	// DeadLettered is how many operations were set aside rather than applied.
	// Each one is a hole in the replica.
	DeadLettered = "sync_dead_lettered_operations"
	// Unreplicated is how many source objects nothing carries. A table nobody
	// mapped is a table that will not exist after a failover.
	Unreplicated = "sync_unreplicated_tables"

	helpReconcileDiff = "Objects found to differ by the last full comparison"
	helpValueRepairs  = "Objects copied by value instead of by replaying a command"
	helpDeadLettered  = "Operations that could not be applied to the target and are held for retry"
	helpUnreplicated  = "Source objects that no mapping carries to the target"
)

func SetReconcileDifference(labels Labels, objects float64) {
	Default.SetGauge(ReconcileDifference, helpReconcileDiff, labels, objects)
}

func CountValueRepairs(labels Labels, n int) {
	if n > 0 {
		Default.AddCounter(ValueRepairsTotal, helpValueRepairs, labels, float64(n))
	}
}

func SetDeadLettered(labels Labels, count float64) {
	Default.SetGauge(DeadLettered, helpDeadLettered, labels, count)
}

func SetUnreplicated(labels Labels, count float64) {
	Default.SetGauge(Unreplicated, helpUnreplicated, labels, count)
}

// ----------------------------------------------------------- Redis stream

// The Redis relay measures progress in stream bytes, because a Redis
// replication stream carries no timestamps. These are the byte-denominated
// equivalents of the position metrics above, kept under their own names because
// they mean something only for that engine.
const (
	StreamOffsetBytes  = "sync_redis_stream_offset_bytes"
	AppliedOffsetBytes = "sync_redis_applied_offset_bytes"
	// SourceLagBytes is how far the target is behind the source's own write
	// offset, taken from master_repl_offset rather than derived from what this
	// process received. The difference of the two above freezes when the reader
	// stops: measured against a blocked target it held at 0.4 MB while the real
	// distance passed 21 MB.
	SourceLagBytes = "sync_redis_source_lag_bytes"
	// BufferHeldBytes is how much of the stream is on disk, which is what turns
	// a briefly unavailable target into a partial resync instead of a full one.
	BufferHeldBytes = "sync_redis_buffer_held_bytes"

	helpStreamOffset  = "Replication stream offset the relay has received and written to disk"
	helpAppliedOffset = "Replication stream offset applied to the target"
	helpSourceLag     = "Bytes the target is behind the source's own write offset"
	helpBufferHeld    = "Bytes of the replication stream held on disk"
)

func SetStreamOffset(labels Labels, offset, held int64) {
	Default.SetGauge(StreamOffsetBytes, helpStreamOffset, labels, float64(offset))
	Default.SetGauge(BufferHeldBytes, helpBufferHeld, labels, float64(held))
	Default.SetGauge(BufferBytes, HelpBufferBytes, labels, float64(held))
}

func SetSourceLag(labels Labels, lag int64) {
	Default.SetGauge(SourceLagBytes, helpSourceLag, labels, float64(lag))
}

func SetAppliedOffset(labels Labels, offset int64) {
	Default.SetGauge(AppliedOffsetBytes, helpAppliedOffset, labels, float64(offset))
	SetAppliedPosition(labels, offset)
}

// ------------------------------------------------------------------ batch

// What one batch cost. Debezium has no batch: it emits records one at a time
// and Kafka does the grouping. Here a batch is the unit of atomicity, so its
// size and the round trips it takes are what an operator tunes against.
const (
	BatchApplyCount    = "sync_batch_apply_count"
	BatchApplySeconds  = "sync_batch_apply_seconds_sum"
	BatchCommitSeconds = "sync_batch_commit_seconds_sum"
	BatchRoundTripsSum = "sync_batch_round_trips_sum"
	BatchNamespacesSum = "sync_batch_namespaces_sum"
	BatchEventsSum     = "sync_batch_events_sum"

	helpBatchCount      = "Batches applied to the target"
	helpBatchApply      = "Seconds spent applying batches"
	helpBatchCommit     = "Seconds spent committing batches"
	helpBatchRoundTrips = "Round trips to the target spent applying batches"
	helpBatchNamespaces = "Tables or collections touched by applied batches"
	helpBatchEvents     = "Events carried by applied batches"
)

func ObserveBatch(labels Labels, apply, commit time.Duration, roundTrips, namespaces, events int) {
	Default.AddCounter(BatchApplyCount, helpBatchCount, labels, 1)
	Default.AddCounter(BatchApplySeconds, helpBatchApply, labels, apply.Seconds())
	Default.AddCounter(BatchCommitSeconds, helpBatchCommit, labels, commit.Seconds())
	Default.AddCounter(BatchRoundTripsSum, helpBatchRoundTrips, labels, float64(roundTrips))
	Default.AddCounter(BatchNamespacesSum, helpBatchNamespaces, labels, float64(namespaces))
	Default.AddCounter(BatchEventsSum, helpBatchEvents, labels, float64(events))
}
