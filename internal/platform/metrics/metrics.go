// Package metrics exposes what the syncer is doing in the Prometheus text
// format. Until now the only things a running deployment reported were Slack
// messages and a row-count table in SQLite, so there was no way to put a
// number on the recovery point objective: how far behind the Osaka copy is at
// any moment, whether it is falling further behind, and whether a task has
// stopped applying anything at all.
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

func escape(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `"`, `\"`)
	return strings.ReplaceAll(v, "\n", `\n`)
}

type series struct {
	labels Labels
	value  float64
}

type metric struct {
	name string
	kind Kind
	help string
	// series is keyed by the label identity, so a repeated observation updates
	// the value rather than adding a line to the scrape.
	series map[string]*series
}

type Registry struct {
	mu      sync.Mutex
	metrics map[string]*metric
}

func New() *Registry {
	return &Registry{metrics: map[string]*metric{}}
}

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
