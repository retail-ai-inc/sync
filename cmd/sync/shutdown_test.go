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

// The monitors keep the configuration they were started with: the row count
// monitor and the consistency checker walk cfg.SyncConfigs on every tick, from
// the pointer they were handed. So the fingerprint that decides whether to
// restart them has to notice the task list changing.

func configWith(tasks ...config.SyncConfig) *config.Config {
	return &config.Config{
		EnableTableRowCountMonitoring: true,
		MonitorInterval:               time.Minute,
		SyncConfigs:                   tasks,
	}
}

func TestTheMonitorsNoticeATaskBeingAdded(t *testing.T) {
	before := globalFingerprint(configWith(config.SyncConfig{ID: 39, Type: "mongodb"}))
	after := globalFingerprint(configWith(
		config.SyncConfig{ID: 39, Type: "mongodb"},
		config.SyncConfig{ID: 41, Type: "mysql"},
	))

	if before == after {
		t.Error("adding a task left the fingerprint unchanged, so the monitors " +
			"would keep the old task list and the new task would go unmonitored")
	}
}

// TestTheMonitorsNoticeATaskBeingDisabled is the sharp one: with repairs
// enabled, a checker still holding a disabled task writes to the target it was
// taken off.
func TestTheMonitorsNoticeATaskBeingDisabled(t *testing.T) {
	before := globalFingerprint(configWith(config.SyncConfig{ID: 39, Enable: true}))
	after := globalFingerprint(configWith(config.SyncConfig{ID: 39, Enable: false}))

	if before == after {
		t.Error("disabling a task left the fingerprint unchanged")
	}
}

func TestTheMonitorsNoticeATaskBeingEditedOrRemoved(t *testing.T) {
	base := configWith(config.SyncConfig{ID: 39, Type: "mongodb",
		TargetConnection: "mongodb://osaka:27017/bk"})

	edited := configWith(config.SyncConfig{ID: 39, Type: "mongodb",
		TargetConnection: "mongodb://elsewhere:27017/bk"})
	if globalFingerprint(base) == globalFingerprint(edited) {
		t.Error("repointing a task's target left the fingerprint unchanged")
	}

	if globalFingerprint(base) == globalFingerprint(configWith()) {
		t.Error("removing the last task left the fingerprint unchanged")
	}
}

// TestAnUnchangedConfigurationKeepsItsFingerprint: the monitors must not be
// torn down and restarted on every reload, which happens every ten seconds.
func TestAnUnchangedConfigurationKeepsItsFingerprint(t *testing.T) {
	tasks := []config.SyncConfig{{ID: 39, Type: "mongodb"}, {ID: 41, Type: "mysql"}}
	if globalFingerprint(configWith(tasks...)) != globalFingerprint(configWith(tasks...)) {
		t.Error("the same configuration produced two fingerprints, so the monitors " +
			"would restart on every reload")
	}
}
