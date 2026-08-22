package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// exposition renders a registry the way a scrape would read it.
func exposition(t *testing.T, r *Registry) string {
	t.Helper()

	var b strings.Builder
	if err := r.Write(&b); err != nil {
		t.Fatalf("Write: %v", err)
	}
	return b.String()
}

func TestAGaugeReportsItsLatestValue(t *testing.T) {
	r := New()
	labels := Labels{"task": "1", "engine": "mysql"}

	r.SetGauge(LagSeconds, helpLag, labels, 12)
	r.SetGauge(LagSeconds, helpLag, labels, 3.5)

	got := exposition(t, r)
	if !strings.Contains(got, `sync_replication_lag_seconds{engine="mysql",task="1"} 3.5`) {
		t.Errorf("exposition =\n%s", got)
	}
	if strings.Contains(got, " 12\n") {
		t.Errorf("the earlier value is still reported:\n%s", got)
	}
}

func TestACounterAccumulates(t *testing.T) {
	r := New()
	labels := Labels{"task": "1"}

	r.AddCounter(AppliedTotal, helpApplied, labels, 3)
	r.AddCounter(AppliedTotal, helpApplied, labels, 4)

	if got := exposition(t, r); !strings.Contains(got, `sync_changes_applied_total{task="1"} 7`) {
		t.Errorf("exposition =\n%s", got)
	}
}

// TestACounterNeverGoesBackwards pins the one rule a counter has: Prometheus
// reads a decrease as a restart and every rate computed across it is wrong.
func TestACounterNeverGoesBackwards(t *testing.T) {
	r := New()
	labels := Labels{"task": "1"}

	r.AddCounter(AppliedTotal, helpApplied, labels, 5)
	r.AddCounter(AppliedTotal, helpApplied, labels, -3)

	if got := exposition(t, r); !strings.Contains(got, `sync_changes_applied_total{task="1"} 5`) {
		t.Errorf("exposition =\n%s", got)
	}
}

// TestEachLabelSetIsItsOwnSeries is what lets one graph show every task.
func TestEachLabelSetIsItsOwnSeries(t *testing.T) {
	r := New()

	r.SetGauge(LagSeconds, helpLag, Labels{"task": "1"}, 1)
	r.SetGauge(LagSeconds, helpLag, Labels{"task": "2"}, 2)

	got := exposition(t, r)
	for _, want := range []string{`{task="1"} 1`, `{task="2"} 2`} {
		if !strings.Contains(got, want) {
			t.Errorf("exposition does not carry %s:\n%s", want, got)
		}
	}
	if n := strings.Count(got, "# TYPE"); n != 1 {
		t.Errorf("the metric was declared %d times, want once:\n%s", n, got)
	}
}

// TestTheLabelOrderIsStable matters because an unstable order would make every
// scrape look like a different series to a diff, and because the label key is
// what identifies a series internally.
func TestTheLabelOrderIsStable(t *testing.T) {
	r := New()
	labels := Labels{"z": "1", "a": "2", "m": "3"}

	r.SetGauge(LagSeconds, helpLag, labels, 1)

	got := exposition(t, r)
	if !strings.Contains(got, `{a="2",m="3",z="1"}`) {
		t.Errorf("labels are not in order:\n%s", got)
	}
	for i := 0; i < 10; i++ {
		if exposition(t, r) != got {
			t.Fatal("two renderings of the same registry differ")
		}
	}
}

// TestALabelValueIsEscaped covers the characters that would otherwise end the
// label early and produce an exposition a scraper rejects. A table name is
// operator-supplied, so this is reachable.
func TestALabelValueIsEscaped(t *testing.T) {
	r := New()

	r.SetGauge(LagSeconds, helpLag, Labels{"table": `we"ird\name` + "\n"}, 1)

	got := exposition(t, r)
	if strings.Count(got, "\n") != 3 { // HELP, TYPE, the sample
		t.Errorf("a newline reached the exposition:\n%q", got)
	}
	if !strings.Contains(got, `we\"ird\\name\n`) {
		t.Errorf("the value is not escaped:\n%s", got)
	}
}

// TestForgetDropsATasksSeries is what stops a task that has stopped from going
// on reporting the lag it had when it was running, which would read as a stale
// alert for ever.
func TestForgetDropsATasksSeries(t *testing.T) {
	r := New()
	r.SetGauge(LagSeconds, helpLag, Labels{"task": "1", "table": "orders"}, 5)
	r.SetGauge(LagSeconds, helpLag, Labels{"task": "2", "table": "orders"}, 6)

	r.Forget(Labels{"task": "1"})

	got := exposition(t, r)
	if strings.Contains(got, `task="1"`) {
		t.Errorf("the forgotten task is still reported:\n%s", got)
	}
	if !strings.Contains(got, `task="2"`) {
		t.Errorf("another task was forgotten too:\n%s", got)
	}
}

func TestAnEmptyRegistryRendersNothing(t *testing.T) {
	if got := exposition(t, New()); got != "" {
		t.Errorf("exposition = %q, want empty", got)
	}
}

func TestAMetricWithNoLabelsRenders(t *testing.T) {
	r := New()
	r.SetGauge(TaskUp, helpTaskUp, nil, 1)

	if got := exposition(t, r); !strings.Contains(got, "sync_task_up 1\n") {
		t.Errorf("exposition =\n%s", got)
	}
}

// TestTheRegistryIsSafeForConcurrentUse matters because every syncer records
// from its own goroutine while a scrape reads.
func TestTheRegistryIsSafeForConcurrentUse(t *testing.T) {
	r := New()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				r.AddCounter(AppliedTotal, helpApplied, Labels{"task": "1"}, 1)
				r.SetGauge(LagSeconds, helpLag, Labels{"task": "1"}, float64(n))
				var b strings.Builder
				_ = r.Write(&b)
			}
		}(i)
	}
	wg.Wait()

	if got := exposition(t, r); !strings.Contains(got, "sync_changes_applied_total{task=\"1\"} 1600") {
		t.Errorf("exposition =\n%s", got)
	}
}

// ------------------------------------------------------------- the syncer API

func TestTheSyncerHelpersRecordIntoTheDefaultRegistry(t *testing.T) {
	labels := Labels{"task": "helpers-test"}
	t.Cleanup(func() { Default.Forget(labels) })

	SetLag(labels, 4)
	SetReadLag(labels, 2)
	Applied(labels, 10)
	Failed(labels, 1)
	SetTaskUp(labels, true)

	var b strings.Builder
	if err := Default.Write(&b); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got := b.String()

	for _, want := range []string{
		`sync_replication_lag_seconds{task="helpers-test"} 4`,
		`sync_source_event_age_seconds{task="helpers-test"} 2`,
		`sync_changes_applied_total{task="helpers-test"} 10`,
		`sync_changes_failed_total{task="helpers-test"} 1`,
		`sync_task_up{task="helpers-test"} 1`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("exposition does not carry %s:\n%s", want, got)
		}
	}

	SetTaskUp(labels, false)
	b.Reset()
	_ = Default.Write(&b)
	if !strings.Contains(b.String(), `sync_task_up{task="helpers-test"} 0`) {
		t.Errorf("a stopped task is not reported as down:\n%s", b.String())
	}
}

// ----------------------------------------------------------------- handler

func TestTheHandlerServesTheExposition(t *testing.T) {
	labels := Labels{"task": "handler-test"}
	t.Cleanup(func() { Default.Forget(labels) })
	SetLag(labels, 7)

	rec := httptest.NewRecorder()
	Handler(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type = %q, want the exposition media type", ct)
	}
	if !strings.Contains(rec.Body.String(), `sync_replication_lag_seconds{task="handler-test"} 7`) {
		t.Errorf("body =\n%s", rec.Body.String())
	}
}
