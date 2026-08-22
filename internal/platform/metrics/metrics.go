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

// SetDeadLettered records how much this task could not apply.
func SetDeadLettered(labels Labels, count float64) {
	Default.SetGauge(DeadLettered, helpDeadLettered, labels, count)
}
