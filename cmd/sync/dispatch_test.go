package main

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// quietLog returns a logger that discards output.
func quietLog() *logrus.Logger {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return l
}

// waitWithin reports whether wg reached zero inside the timeout, so a leaked
// WaitGroup counter surfaces as a failure rather than a hung test.
func waitWithin(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

func TestStartSyncTasksSkipsDisabledTasks(t *testing.T) {
	task := baseTask()
	task.Enable = false

	var wg sync.WaitGroup
	startSyncTasks(context.Background(), cfgWith(task), &wg, quietLog())

	if !waitWithin(&wg, time.Second) {
		t.Fatal("a disabled task still added to the WaitGroup")
	}
}

func TestStartSyncTasksOnAnEmptyConfig(t *testing.T) {
	var wg sync.WaitGroup
	startSyncTasks(context.Background(), cfgWith(), &wg, quietLog())

	if !waitWithin(&wg, time.Second) {
		t.Fatal("an empty config added to the WaitGroup")
	}
}

// An unrecognised sync type is logged at error level and the WaitGroup counter
// is released, so the process keeps running with that task simply absent. There
// is no failed state anywhere: the task shows as enabled in the API and
// nothing replicates.
func TestAnUnknownSyncTypeIsSilentlyDropped(t *testing.T) {
	task := baseTask()
	task.Type = "cassandra"

	var wg sync.WaitGroup
	startSyncTasks(context.Background(), cfgWith(task), &wg, quietLog())

	if !waitWithin(&wg, time.Second) {
		t.Fatal("an unknown sync type leaked a WaitGroup counter — the default branch appears to have changed")
	}
}

// The type switch is case-sensitive and has no alias handling, so a config
// value that differs only in case is treated as unknown and the task never
// runs. The API stores what the UI sends, and nothing normalises it.
func TestSyncTypeMatchingIsCaseSensitive(t *testing.T) {
	for _, typ := range []string{"MongoDB", "MySQL", "MariaDB", "PostgreSQL", "Redis", "postgres", "mongo"} {
		task := baseTask()
		task.Type = typ

		var wg sync.WaitGroup
		startSyncTasks(context.Background(), cfgWith(task), &wg, quietLog())

		// A recognised type would start a syncer goroutine that outlives this
		// call; an unrecognised one releases the counter immediately.
		if !waitWithin(&wg, 500*time.Millisecond) {
			t.Fatalf("type %q now starts a syncer — the switch appears to normalise case; assert that instead", typ)
		}
	}
}

func TestStartSyncTasksDropsUnknownTypesIndividually(t *testing.T) {
	unknown := baseTask()
	unknown.ID = 1
	unknown.Type = "cassandra"

	disabled := baseTask()
	disabled.ID = 2
	disabled.Enable = false

	alsoUnknown := baseTask()
	alsoUnknown.ID = 3
	alsoUnknown.Type = ""

	var wg sync.WaitGroup
	startSyncTasks(context.Background(), cfgWith(unknown, disabled, alsoUnknown), &wg, quietLog())

	if !waitWithin(&wg, time.Second) {
		t.Fatal("a WaitGroup counter was leaked across the mixed config")
	}
}

func TestConfigsEqualIgnoresEverythingOutsideSyncConfigs(t *testing.T) {
	a := cfgWith(baseTask())
	b := cfgWith(baseTask())

	a.LogLevel = "debug"
	b.LogLevel = "trace"
	a.EnableTableRowCountMonitoring = true
	b.EnableTableRowCountMonitoring = false
	a.MonitorInterval = time.Minute
	b.MonitorInterval = time.Hour

	if !configsEqual(a, b) {
		t.Error("configsEqual compared fields outside SyncConfigs")
	}
}

// runSyncTasks reloads the configuration every ten seconds and restarts the
// syncers only when SyncConfigs changed. Everything else — log level, the
// row-count monitoring switch, the monitor interval — is read once at startup
// and a later edit is picked up by nothing until the process restarts or an
// unrelated SyncConfigs edit happens to trigger a reload.
func TestNonSyncConfigEditsNeverTriggerARestart(t *testing.T) {
	a := cfgWith(baseTask())
	b := cfgWith(baseTask())
	b.EnableTableRowCountMonitoring = !a.EnableTableRowCountMonitoring

	if !configsEqual(a, b) {
		t.Fatal("configsEqual now notices the monitoring switch — the reload appears to have been widened")
	}
}
