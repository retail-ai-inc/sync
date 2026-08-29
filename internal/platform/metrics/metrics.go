// Package metrics exposes what the syncer is doing in the Prometheus text
// format.
//
// Until now the only things a running deployment reported were Slack messages
// and a row-count table in SQLite, so there was no way to put a number on the
// recovery point objective: how far behind the Osaka copy is at any moment,
// whether it is falling further behind, and whether a task has stopped applying
// anything at all. Those are the questions a disaster-recovery setup exists to
// answer, and none of them could be graphed or alerted on.
//
// The exposition is written by hand rather than pulled in with a client
// library. The format is a handful of lines, this needs four metric types
// between them, and a control plane for a payment system is a poor place to add
// a large dependency for that.
package metrics

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// Kind is how a value changes: a gauge moves in both directions, a counter only
// upwards. Prometheus treats them differently, so the exposition names which.
type Kind string

const (
	Gauge   Kind = "gauge"
	Counter Kind = "counter"
)

// Labels distinguishes one series of a metric from another — one task, one
// engine, one table.
type Labels map[string]string

// Key renders labels into a stable identity, so the same label set always
// addresses the same series.
func (l Labels) Key() string {
	if len(l) == 0 {
		return ""
	}
	names := make([]string, 0, len(l))
	for name := range l {
		names = append(names, name)
	}
	sort.Strings(names)

	var b strings.Builder
	for i, name := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(name)
		b.WriteByte('=')
		b.WriteString(l[name])
	}
	return b.String()
}

// render writes the labels the way the exposition format spells them.
func (l Labels) render() string {
	if len(l) == 0 {
		return ""
	}
	names := make([]string, 0, len(l))
	for name := range l {
		names = append(names, name)
	}
	sort.Strings(names)

	parts := make([]string, 0, len(names))
	for _, name := range names {
		// escape has already produced what belongs inside the quotes, so the
		// quotes are added here rather than by %q, which would escape them again.
		parts = append(parts, name+`="`+escape(l[name])+`"`)
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// escape makes a label value safe for the exposition format.
func escape(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `"`, `\"`)
	return strings.ReplaceAll(v, "\n", `\n`)
}

// series is one metric with one label set.
type series struct {
	labels Labels
	value  float64
}

// metric is one named metric and every label set seen for it.
type metric struct {
	name string
	kind Kind
	help string
	// series is keyed by the label identity, so a repeated observation updates
	// the value rather than adding a line to the scrape.
	series map[string]*series
}

// Registry holds what the next scrape will report.
type Registry struct {
	mu      sync.Mutex
	metrics map[string]*metric
}

// New returns an empty registry.
func New() *Registry {
	return &Registry{metrics: map[string]*metric{}}
}

// Default is the registry the syncer records into.
var Default = New()

// find returns the series for a metric and label set, creating it if needed.
// The caller holds the lock.
func (r *Registry) find(name string, kind Kind, help string, labels Labels) *series {
	m, ok := r.metrics[name]
	if !ok {
		m = &metric{name: name, kind: kind, help: help, series: map[string]*series{}}
		r.metrics[name] = m
	}

	key := labels.Key()
	s, ok := m.series[key]
	if !ok {
		s = &series{labels: labels}
		m.series[key] = s
	}
	return s
}

// SetGauge records the current value of something that moves both ways.
func (r *Registry) SetGauge(name, help string, labels Labels, value float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.find(name, Gauge, help, labels).value = value
}

// AddCounter advances a total. A negative delta is ignored: a counter that goes
// backwards makes every rate computed from it wrong.
func (r *Registry) AddCounter(name, help string, labels Labels, delta float64) {
	if delta < 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.find(name, Counter, help, labels).value += delta
}

// Forget removes every series of a metric carrying the given labels, so a task
// that has stopped does not go on reporting the lag it had when it did.
func (r *Registry) Forget(labels Labels) {
	want := labels.Key()

	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range r.metrics {
		for key := range m.series {
			if strings.Contains(key, want) {
				delete(m.series, key)
			}
		}
	}
}

// Write emits the exposition.
//
// It is not named WriteTo, because that name belongs to io.WriterTo and this
// does not report a byte count.
func (r *Registry) Write(w io.Writer) error {
	r.mu.Lock()
	names := make([]string, 0, len(r.metrics))
	for name := range r.metrics {
		names = append(names, name)
	}
	sort.Strings(names)

	type line struct{ text string }
	var lines []line
	for _, name := range names {
		m := r.metrics[name]
		if len(m.series) == 0 {
			continue
		}
		lines = append(lines,
			line{fmt.Sprintf("# HELP %s %s", m.name, m.help)},
			line{fmt.Sprintf("# TYPE %s %s", m.name, m.kind)})

		keys := make([]string, 0, len(m.series))
		for key := range m.series {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			s := m.series[key]
			lines = append(lines,
				line{fmt.Sprintf("%s%s %g", m.name, s.labels.render(), s.value)})
		}
	}
	r.mu.Unlock()

	for _, l := range lines {
		if _, err := io.WriteString(w, l.text+"\n"); err != nil {
			return err
		}
	}
	return nil
}

// Handler serves the exposition.
//
// It sits outside /api and takes no credential, which is the convention every
// scraper expects: Prometheus has no way to present one. The port it listens on
// is what has to be kept off the public network — the same requirement the
// probes already carry.
func Handler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_ = Default.Write(w)
}

// The metric names the syncer records. They are constants because a name that
// varies between two call sites produces two graphs that should have been one.
const (
	// LagSeconds is how far behind the source a task's applied data is, taken
	// from the timestamp the source put on the change.
	LagSeconds = "sync_replication_lag_seconds"
	// ReadLagSeconds is how old an event was when the syncer read it, before
	// it was applied. The difference between the two is the syncer's own
	// backlog rather than the network's.
	ReadLagSeconds = "sync_source_event_age_seconds"
	// AppliedTotal counts the changes written to the target.
	AppliedTotal = "sync_changes_applied_total"
	// FailedTotal counts the changes that could not be written.
	FailedTotal = "sync_changes_failed_total"
	// TaskUp is 1 while a task is replicating and 0 once it has stopped.
	TaskUp = "sync_task_up"
)

// Help strings, kept next to the names they describe.
const (
	helpLag     = "Seconds between a change being made at the source and applied at the target"
	helpReadLag = "Seconds between a change being made at the source and read by the syncer"
	helpApplied = "Changes written to the target"
	helpFailed  = "Changes that could not be written to the target"
	helpTaskUp  = "1 while the task is replicating, 0 once it has stopped"
)

// SetLag records the applied lag for one table or collection.
func SetLag(labels Labels, seconds float64) {
	Default.SetGauge(LagSeconds, helpLag, labels, seconds)
}

// SetReadLag records how old an event was when it was read.
func SetReadLag(labels Labels, seconds float64) {
	Default.SetGauge(ReadLagSeconds, helpReadLag, labels, seconds)
}

// Applied counts changes written to the target.
func Applied(labels Labels, n int) {
	Default.AddCounter(AppliedTotal, helpApplied, labels, float64(n))
}

// Failed counts changes that could not be written.
func Failed(labels Labels, n int) {
	Default.AddCounter(FailedTotal, helpFailed, labels, float64(n))
}

// SetTaskUp records whether a task is replicating.
func SetTaskUp(labels Labels, up bool) {
	value := 0.0
	if up {
		value = 1
	}
	Default.SetGauge(TaskUp, helpTaskUp, labels, value)
}

// Sample is one series as a scrape would see it.
type Sample struct {
	Name   string
	Labels Labels
	Value  float64
}

// Snapshot reports every series currently recorded, so something in-process can
// act on the same numbers a scraper reads — alerting on the replication lag,
// for instance, without waiting for Prometheus to be wired up.
func (r *Registry) Snapshot(name string) []Sample {
	r.mu.Lock()
	defer r.mu.Unlock()

	m, ok := r.metrics[name]
	if !ok {
		return nil
	}
	out := make([]Sample, 0, len(m.series))
	for _, s := range m.series {
		labels := make(Labels, len(s.labels))
		for k, v := range s.labels {
			labels[k] = v
		}
		out = append(out, Sample{Name: name, Labels: labels, Value: s.value})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Labels.Key() < out[j].Labels.Key() })
	return out
}

// Supervision metrics. A task that stops and is restarted, and a task that
// stops and is not, are different events and an operator needs to tell them
// apart: the first is noise until it becomes a pattern, the second means
// replication has halted and nothing will resume it.
const (
	// RestartsTotal counts how often a task has been restarted after stopping
	// by itself.
	RestartsTotal = "sync_task_restarts_total"
	// TaskBlocked is 1 while a task is stopped for a reason retrying cannot fix.
	TaskBlocked = "sync_task_blocked"

	helpRestarts = "Times a task has been restarted after stopping by itself"
	helpBlocked  = "1 while a task is stopped for a reason retrying cannot fix"
)

// CountRestart records that a task was restarted.
func CountRestart(labels Labels) {
	Default.AddCounter(RestartsTotal, helpRestarts, labels, 1)
}

// SetTaskBlocked records whether a task is stopped and will not be retried.
func SetTaskBlocked(labels Labels, blocked bool) {
	value := 0.0
	if blocked {
		value = 1
	}
	Default.SetGauge(TaskBlocked, helpBlocked, labels, value)
}

// Coverage metrics. A task that names its tables replicates those and no more,
// so a table added at the source afterwards is simply absent from the replica —
// which is not something to discover during a failover.
const (
	// Unreplicated is how many tables or collections the source holds that a
	// task does not carry.
	Unreplicated = "sync_unreplicated_tables"

	helpUnreplicated = "Tables or collections at the source that this task does not replicate"
)

// SetUnreplicated records how much of the source a task is not carrying.
func SetUnreplicated(labels Labels, count float64) {
	Default.SetGauge(Unreplicated, helpUnreplicated, labels, count)
}

// Buffer metrics. Change data waiting on local disk is the one thing that grows
// when the target cannot keep up, and it is invisible until the volume fills.
const (
	// BufferBytes is how much unapplied change data is on disk.
	BufferBytes = "sync_buffer_bytes"

	// HelpBufferBytes describes it for a scrape.
	HelpBufferBytes = "Bytes of change data on local disk waiting to be applied to the target"
)

// Dead letter metrics. An operation that could not be applied to the target and
// was set aside is a hole in the replica. It used to exist only as a file on the
// syncer's local disk, so nobody found out about it until a comparison ran or a
// switchover went wrong.
const (
	// DeadLettered is how many operations are waiting in the dead letter queue.
	DeadLettered = "sync_dead_lettered_operations"

	helpDeadLettered = "Operations that could not be applied to the target and are held for retry"
)

// Stream health. A replication task that is reading nothing looks exactly like
// one that is up to date, so these three say which it is.
const (
	// LastEventAgeSeconds is how long since anything at all arrived from the
	// source, heartbeats included.
	//
	// The applied lag cannot answer this. It is measured from the events that
	// arrive, so a stream that has stopped delivering leaves it frozen at
	// whatever it last was — and an alert on a frozen gauge never fires. This
	// one grows whenever the link is silent, which is the condition worth
	// waking somebody for.
	LastEventAgeSeconds = "sync_source_last_event_age_seconds"
	helpLastEventAge    = "Seconds since any event, heartbeat included, arrived from the source"

	// RetentionWindowSeconds is how far back the source's log reaches.
	RetentionWindowSeconds = "sync_source_retention_window_seconds"
	helpRetentionWindow    = "Seconds of history the source still holds (binlog expiry, oplog window)"
	// RetentionHeadroomSeconds is that window less the current lag: how long
	// the task could stay stopped before its saved position is purged.
	RetentionHeadroomSeconds = "sync_source_retention_headroom_seconds"
	helpRetentionHeadroom    = "Seconds a stopped task has left before its position falls out of the source's log"

	// The Redis relay measures its progress in stream bytes rather than in time,
	// because a Redis replication stream carries no timestamps.
	//
	// The applied lag is therefore StreamOffsetBytes minus AppliedOffsetBytes,
	// computed where the metrics are read: the two are published by different
	// parts of the pipeline, and joining them here would mean one waiting for the
	// other. sync_replication_lag_seconds still exists for Redis, but it measures
	// from when the relay read a change rather than from when the source made it,
	// so it does not include the time spent crossing the region.
	StreamOffsetBytes  = "sync_redis_stream_offset_bytes"
	helpStreamOffset   = "Replication stream offset the relay has received and written to disk"
	AppliedOffsetBytes = "sync_redis_applied_offset_bytes"

	// SourceLagBytes is how far the target is behind the source's own write
	// offset, read from the source rather than derived from what this process
	// has managed to receive.
	//
	// StreamOffsetBytes minus AppliedOffsetBytes only measures the part of the
	// backlog this process is already holding. When the target goes away the
	// reader stops too, so both of those freeze and their difference freezes
	// with them: measured against a blocked target, they held steady at 0.4 MB
	// while the real distance grew past 21 MB. This one is taken from the
	// source's master_repl_offset, so it keeps climbing for as long as the
	// source keeps writing, which is the whole point of a lag alarm.
	SourceLagBytes    = "sync_redis_source_lag_bytes"
	helpAppliedOffset = "Replication stream offset applied to the target"
	helpSourceLag     = "How far the target is behind the source's own write offset, in bytes."
	// BufferHeldBytes is how much of the stream is on disk, which is what turns a
	// briefly unavailable target into a partial resync instead of a full one.
	BufferHeldBytes = "sync_redis_buffer_held_bytes"
	helpBufferHeld  = "Bytes of the replication stream held on disk"
	// ValueRepairsTotal counts keys copied whole rather than by replaying a
	// command. A rate above zero outside the first copy means something is being
	// repaired, which is worth knowing about.
	ValueRepairsTotal = "sync_redis_value_repairs_total"
	helpValueRepairs  = "Keys copied by value instead of by replaying a command"
	// ReconcileDifference is what the last full comparison found. It is the
	// backstop for everything else in this package being wrong.
	ReconcileDifference = "sync_redis_reconcile_difference"
	helpReconcileDiff   = "Keys found to differ by the last full comparison"

	// QueueUsed is how much of the reader's hand-off queue is occupied. It
	// filling up is what back pressure looks like from outside.
	QueueUsed  = "sync_reader_queue_used"
	helpQueue  = "Events read from the source and not yet applied"
	QueueTotal = "sync_reader_queue_capacity"
	helpQueueT = "Capacity of the queue between the reader and the applier"

	// DisconnectsTotal counts how often the source stream had to be reopened.
	// A climbing count is the shape of a link about to fail for good.
	DisconnectsTotal = "sync_source_disconnects_total"
	helpDisconnects  = "Times the source stream was reopened after an error"
)

// SetLastEventAge records how long the source has been silent.
func SetLastEventAge(labels Labels, seconds float64) {
	Default.SetGauge(LastEventAgeSeconds, helpLastEventAge, labels, seconds)
}

// SetQueue records the reader hand-off queue's depth and capacity.
// SetStreamOffset publishes how far the relay has received and written to disk.
func SetStreamOffset(labels Labels, offset, held int64) {
	Default.SetGauge(StreamOffsetBytes, helpStreamOffset, labels, float64(offset))
	Default.SetGauge(BufferHeldBytes, helpBufferHeld, labels, float64(held))
}

// SetSourceLag publishes how far behind the source's own offset the target is.
func SetSourceLag(labels Labels, lag int64) {
	Default.SetGauge(SourceLagBytes, helpSourceLag, labels, float64(lag))
}

// SetAppliedOffset publishes how far the target has been written.
func SetAppliedOffset(labels Labels, offset int64) {
	Default.SetGauge(AppliedOffsetBytes, helpAppliedOffset, labels, float64(offset))
}

// CountValueRepairs records keys copied whole.
func CountValueRepairs(labels Labels, n int) {
	if n > 0 {
		Default.AddCounter(ValueRepairsTotal, helpValueRepairs, labels, float64(n))
	}
}

// SetReconcileDifference publishes what the last full comparison found.
func SetReconcileDifference(labels Labels, keys float64) {
	Default.SetGauge(ReconcileDifference, helpReconcileDiff, labels, keys)
}

// SetRetention publishes how much source history is left to fall back on.
//
// Headroom is the window less the lag: with a day of binlog kept and a minute
// of lag, a task has just under a day to be fixed before its position falls out
// of the log and the target has to be built again from scratch. It can go
// negative, and that is the point — the number crossing zero is the moment the
// answer changes from "restart it" to "re-copy everything", and an alert wants
// to fire well before it does.
func SetRetention(labels Labels, window, headroom float64) {
	Default.SetGauge(RetentionWindowSeconds, helpRetentionWindow, labels, window)
	Default.SetGauge(RetentionHeadroomSeconds, helpRetentionHeadroom, labels, headroom)
}

func SetQueue(labels Labels, used, capacity int) {
	Default.SetGauge(QueueUsed, helpQueue, labels, float64(used))
	Default.SetGauge(QueueTotal, helpQueueT, labels, float64(capacity))
}

// CountDisconnect records one reopening of the source stream.
func CountDisconnect(labels Labels) {
	Default.AddCounter(DisconnectsTotal, helpDisconnects, labels, 1)
}

// SetDeadLettered records how much this task could not apply.
func SetDeadLettered(labels Labels, count float64) {
	Default.SetGauge(DeadLettered, helpDeadLettered, labels, count)
}

// Batch shape and cost. These exist so that two decisions can be made from
// numbers rather than from opinion: whether applying batches concurrently would
// help, and whether one request per object is costing anything worth removing.
const (
	// BatchApplySeconds over BatchApplyCount is the mean time to apply a batch.
	BatchApplySeconds   = "sync_batch_apply_seconds_sum"
	helpBatchApply      = "Total seconds spent applying batches"
	BatchApplyCount     = "sync_batch_apply_count"
	helpBatchApplyCount = "Batches applied"

	// BatchCommitSeconds is the part of that spent committing rather than
	// writing.
	//
	// On a sharded target a batch spanning shards commits through a two-phase
	// protocol. If that is where the time goes, applying batches concurrently
	// would make several two-phase commits contend with each other rather than
	// make anything faster — so this is the number that decides it.
	BatchCommitSeconds = "sync_batch_commit_seconds_sum"
	helpBatchCommit    = "Total seconds spent committing batches, of the time spent applying them"

	// BatchRoundTrips is how many requests a batch takes: one per object, plus
	// one for the position. If the mean is near one there is nothing to be gained
	// from a command that writes several objects at once.
	BatchRoundTrips     = "sync_batch_round_trips_sum"
	helpBatchRoundTrips = "Total requests sent to the target while applying batches"

	// BatchNamespaces is how many objects a batch touches. It decides whether a
	// cross-object write would save anything, and on a sharded target it is also
	// what decides how often a commit has to span shards.
	BatchNamespaces     = "sync_batch_namespaces_sum"
	helpBatchNamespaces = "Total distinct objects touched by applied batches"

	// BatchEvents is how many changes a batch carries, so the means above can be
	// read per change as well as per batch.
	BatchEvents     = "sync_batch_events_sum"
	helpBatchEvents = "Total changes carried by applied batches"
)

// ObserveBatch records what one applied batch cost, and what shape it was.
func ObserveBatch(labels Labels, apply, commit time.Duration, roundTrips, namespaces, events int) {
	Default.AddCounter(BatchApplySeconds, helpBatchApply, labels, apply.Seconds())
	Default.AddCounter(BatchApplyCount, helpBatchApplyCount, labels, 1)
	Default.AddCounter(BatchCommitSeconds, helpBatchCommit, labels, commit.Seconds())
	Default.AddCounter(BatchRoundTrips, helpBatchRoundTrips, labels, float64(roundTrips))
	Default.AddCounter(BatchNamespaces, helpBatchNamespaces, labels, float64(namespaces))
	Default.AddCounter(BatchEvents, helpBatchEvents, labels, float64(events))
}
