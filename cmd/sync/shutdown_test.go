package main

import (
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/config"
)

// TestEveryStuckTaskSeesTheShutdownDeadline covers the shape that hung
// shutdown: the deadline was a time.After channel, which carries exactly one
// value. The first task to time out consumed it, and every task after that
// waited on a channel that would never fire again -- so two stuck tasks left
// stopAll blocked for ever, and the clean-up after the loop, which clears the
// running set and cancels the monitors, was never reached.
func TestEveryStuckTaskSeesTheShutdownDeadline(t *testing.T) {
	restore := drainTimeout
	drainTimeout = 200 * time.Millisecond
	t.Cleanup(func() { drainTimeout = restore })

	s := &supervisor{
		log:     quietLogger(),
		running: map[int]*runningTask{},
	}
	// Two tasks that never finish: their done channels are never closed.
	for _, id := range []int{1, 2, 3} {
		s.running[id] = &runningTask{cancel: func() {}, done: make(chan struct{})}
	}

	finished := make(chan struct{})
	go func() { s.stopAll(); close(finished) }()

	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("stopAll did not return once the deadline passed, so shutdown " +
			"never reached the clean-up after the wait loop")
	}

	if len(s.running) != 0 {
		t.Errorf("the running set still holds %d tasks", len(s.running))
	}
}

// The monitors walk the task list on every tick, through a function rather than
// a configuration captured when they started. These cover that the function
// tracks reloads -- the alternative, folding the task list into the monitor
// fingerprint, would restart every monitor whenever any one task was edited,
// which is the split globalFingerprint deliberately keeps.

func TestTheMonitorsSeeATaskAddedAfterTheyStarted(t *testing.T) {
	s := &supervisor{log: quietLogger(), running: map[int]*runningTask{}}
	tasks := s.currentTasks

	s.setTasks([]config.SyncConfig{{ID: 39, Type: "mongodb"}})
	if got := len(tasks()); got != 1 {
		t.Fatalf("the monitors see %d tasks, want 1", got)
	}

	s.setTasks([]config.SyncConfig{
		{ID: 39, Type: "mongodb"},
		{ID: 41, Type: "mysql"},
	})
	if got := len(tasks()); got != 2 {
		t.Errorf("the monitors still see %d tasks after one was added; a new task "+
			"would go unmonitored", got)
	}
}

// TestTheMonitorsStopSeeingARemovedTask is the sharp one: with repairs enabled
// the consistency checker writes to the target it is comparing, so a task taken
// out of service must stop being compared.
func TestTheMonitorsStopSeeingARemovedTask(t *testing.T) {
	s := &supervisor{log: quietLogger(), running: map[int]*runningTask{}}

	s.setTasks([]config.SyncConfig{{ID: 39}, {ID: 41}})
	s.setTasks([]config.SyncConfig{{ID: 39}})

	got := s.currentTasks()
	if len(got) != 1 || got[0].ID != 39 {
		t.Errorf("the monitors see %v, want only task 39 -- a removed task would "+
			"go on being compared, and repaired", got)
	}
}

func TestATaskEditStillDoesNotRestartTheMonitors(t *testing.T) {
	base := &config.Config{MonitorInterval: time.Minute,
		SyncConfigs: []config.SyncConfig{{ID: 39, Type: "mongodb"}}}
	edited := &config.Config{MonitorInterval: time.Minute,
		SyncConfigs: []config.SyncConfig{{ID: 39, Type: "mongodb"}, {ID: 41}}}

	if globalFingerprint(base) != globalFingerprint(edited) {
		t.Error("a task edit changed the monitor fingerprint, so every monitor " +
			"would be torn down and restarted mid-sweep")
	}
}
