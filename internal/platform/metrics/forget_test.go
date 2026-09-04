package metrics

import "testing"

// TestForgetDoesNotClearAnotherTaskThatSharesAPrefix covers a task id that is a
// prefix of another. Key() renders name=value with nothing around the value, so
// matching on the rendered key found task=42 when clearing task=4 -- stopping
// task 4 blanked task 42's dashboard row.
func TestForgetDoesNotClearAnotherTaskThatSharesAPrefix(t *testing.T) {
	r := New()
	four := Labels{"task": "4", "engine": "mysql"}
	fortyTwo := Labels{"task": "42", "engine": "mysql"}
	r.SetGauge(LagSeconds, "", four, 1)
	r.SetGauge(LagSeconds, "", fortyTwo, 2)

	r.Forget(four)

	if got := gaugeValue(t, r, LagSeconds, fortyTwo); got != 2 {
		t.Errorf("task 42's lag = %v after clearing task 4, want 2", got)
	}
	if _, ok := seriesFor(r, LagSeconds, four); ok {
		t.Error("task 4's lag is still reported")
	}
}

// TestForgetReachesEveryShard covers a Redis task, whose lag is per shard. The
// clear is addressed by {task, engine}, which is not contiguous in a key that
// also carries shard, so matching on the rendered key found nothing and every
// shard went on reporting.
func TestForgetReachesEveryShard(t *testing.T) {
	r := New()
	task := Labels{"task": "44", "engine": "redis"}
	for _, shard := range []string{"0-5460", "5461-10922", "10923-16383"} {
		r.SetGauge(LagSeconds, "", Labels{"task": "44", "engine": "redis", "shard": shard}, 5)
	}

	r.Forget(task)

	if n := len(r.metrics[LagSeconds].series); n != 0 {
		t.Errorf("%d shard series survived clearing the task", n)
	}
}

func TestForgetStaleClearsWhatGoesStale(t *testing.T) {
	r := New()
	task := Labels{"task": "39", "engine": "mongodb"}
	r.SetGauge(LagSeconds, "", task, 5.7)
	r.SetGauge(QueueUsedEvents, "", task, 120)
	r.SetGauge(RetentionWindowSeconds, "", task, 25092)

	r.ForgetStale(task)

	for _, name := range []string{LagSeconds, QueueUsedEvents, RetentionWindowSeconds} {
		if _, ok := seriesFor(r, name, task); ok {
			t.Errorf("%s survived; a stopped task goes on reporting it and an alert "+
				"reading it can never fire", name)
		}
	}
}

// TestForgetStaleKeepsWhatSaysTheTaskStopped is the point of clearing the rest:
// task_up=0 has to be there to be alerted on.
func TestForgetStaleKeepsWhatSaysTheTaskStopped(t *testing.T) {
	r := New()
	task := Labels{"task": "39", "engine": "mongodb"}
	r.SetGauge(TaskUp, "", task, 0)
	r.SetGauge(TaskBlocked, "", task, 1)
	r.SetGauge(LagSeconds, "", task, 5.7)

	r.ForgetStale(task)

	if got := gaugeValue(t, r, TaskUp, task); got != 0 {
		t.Errorf("task_up = %v, want it kept at 0", got)
	}
	if got := gaugeValue(t, r, TaskBlocked, task); got != 1 {
		t.Errorf("task_blocked = %v, want it kept at 1", got)
	}
	if _, ok := seriesFor(r, LagSeconds, task); ok {
		t.Error("the lag was kept")
	}
}

// TestForgetStaleKeepsCounters covers why counters are not cleared: deleting one
// makes Prometheus read the next value as a reset, so every rate over that
// window is wrong.
func TestForgetStaleKeepsCounters(t *testing.T) {
	r := New()
	task := Labels{"task": "41", "engine": "mysql"}
	r.AddCounter(AppliedTotal, "", task, 9000)
	r.AddCounter(EventsTotal, "", task, 9100)

	r.ForgetStale(task)

	if got := gaugeValue(t, r, AppliedTotal, task); got != 9000 {
		t.Errorf("applied total = %v, want 9000 kept", got)
	}
	if got := gaugeValue(t, r, EventsTotal, task); got != 9100 {
		t.Errorf("events total = %v, want 9100 kept", got)
	}
}

// TestForgetStaleKeepsWhatAlreadyHappened covers the snapshot gauges. "Did the
// initial copy ever finish" is asked about a task that is stopped, which is
// exactly when the answer would otherwise be gone.
func TestForgetStaleKeepsWhatAlreadyHappened(t *testing.T) {
	r := New()
	task := Labels{"task": "39", "engine": "mongodb"}
	r.SetGauge(SnapshotCompleted, "", task, 1)
	r.SetGauge(SnapshotDurationSeconds, "", task, 9420)
	r.SetGauge(SnapshotRunning, "", task, 1)

	r.ForgetStale(task)

	if got := gaugeValue(t, r, SnapshotCompleted, task); got != 1 {
		t.Errorf("snapshot_completed = %v, want it kept", got)
	}
	if got := gaugeValue(t, r, SnapshotDurationSeconds, task); got != 9420 {
		t.Errorf("snapshot_duration = %v, want it kept", got)
	}
	if _, ok := seriesFor(r, SnapshotRunning, task); ok {
		t.Error("snapshot_running was kept; nothing is running in a stopped task")
	}
}

// TestForgetStaleLeavesOtherTasksAlone: one task stopping must not blank
// another's row, which is what the whole deployment shares a registry for.
func TestForgetStaleLeavesOtherTasksAlone(t *testing.T) {
	r := New()
	stopped := Labels{"task": "42", "engine": "redis"}
	running := Labels{"task": "39", "engine": "mongodb"}
	r.SetGauge(LagSeconds, "", stopped, 0.001)
	r.SetGauge(LagSeconds, "", running, 5.7)

	r.ForgetStale(stopped)

	if got := gaugeValue(t, r, LagSeconds, running); got != 5.7 {
		t.Errorf("the running task's lag = %v, want 5.7", got)
	}
}

// TestAGaugeAddedLaterIsClearedByDefault pins the safe default: a new gauge is
// cleared unless somebody puts it in keptOnStop, because the cost of clearing
// one that could have stayed is a gap, and the cost of keeping one that should
// have gone is an alert that cannot fire.
func TestAGaugeAddedLaterIsClearedByDefault(t *testing.T) {
	r := New()
	task := Labels{"task": "39", "engine": "mongodb"}
	r.SetGauge("sync_something_nobody_has_classified", "", task, 1)

	r.ForgetStale(task)

	if _, ok := seriesFor(r, "sync_something_nobody_has_classified", task); ok {
		t.Error("an unclassified gauge was kept; the default has to be to clear it")
	}
}

func seriesFor(r *Registry, name string, labels Labels) (float64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.metrics[name]
	if !ok {
		return 0, false
	}
	s, ok := m.series[labels.Key()]
	if !ok {
		return 0, false
	}
	return s.value, true
}

func gaugeValue(t *testing.T, r *Registry, name string, labels Labels) float64 {
	t.Helper()
	value, ok := seriesFor(r, name, labels)
	if !ok {
		t.Fatalf("%s{%v} is not reported", name, labels)
	}
	return value
}
