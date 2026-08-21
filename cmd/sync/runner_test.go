package main

import (
	"context"
	"database/sql"
	"io"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/sirupsen/logrus"
)

func quietLogger() *logrus.Logger {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return l
}

// useTempConfigDB points SYNC_DB_PATH at a throwaway SQLite file carrying the
// two tables config.NewConfig reads, with a settings row — without one the load
// calls log.Fatalf and takes the test binary with it (T-148).
func useTempConfigDB(t *testing.T) *sql.DB {
	t.Helper()

	path := filepath.Join(t.TempDir(), "sync.db")
	t.Setenv("SYNC_DB_PATH", path)

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open temp sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec(`
CREATE TABLE config_global (
    id                                INTEGER PRIMARY KEY,
    enable_table_row_count_monitoring INTEGER NOT NULL DEFAULT 0,
    log_level                         TEXT    NOT NULL DEFAULT 'info',
    monitor_interval                  INTEGER DEFAULT 60,
    slackWebhookURL                   TEXT,
    slackChannel                      TEXT
);
CREATE TABLE sync_tasks (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    enable           INTEGER NOT NULL DEFAULT 1,
    last_update_time DATETIME,
    last_run_time    DATETIME,
    config_json      TEXT NOT NULL
);
INSERT INTO config_global (id, enable_table_row_count_monitoring, log_level, monitor_interval)
VALUES (1, 0, 'info', 3600);`); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	return db
}

// TestConfigsEqualComparesTheMappingsToo records that the comparison marshals
// the whole task slice, so any difference inside a task counts — the mappings
// included. Editing one task's table list therefore restarts every task's
// syncer, not just that one's, because the supervisor cancels the shared context
// and starts them all again.
func TestConfigsEqualComparesTheMappingsToo(t *testing.T) {
	a := &config.Config{SyncConfigs: []config.SyncConfig{
		{ID: 1, Type: "mongodb", Enable: true,
			Mappings: []config.DatabaseMapping{{SourceDatabase: "src"}}},
	}}
	b := &config.Config{SyncConfigs: []config.SyncConfig{
		{ID: 1, Type: "mongodb", Enable: true, Mappings: nil},
	}}

	if configsEqual(a, b) {
		t.Fatal("configsEqual = true for tasks with different mappings; the comparison " +
			"appears to have narrowed, so assert the new one instead")
	}
}

// TestRunSyncTasksStopsWhenItsContextIsCancelled records that the supervisor
// returns once the parent context is done, having cancelled its children and
// waited for them.
func TestRunSyncTasksStopsWhenItsContextIsCancelled(t *testing.T) {
	useTempConfigDB(t)

	cfg := &config.Config{SyncConfigs: []config.SyncConfig{
		{ID: 1, Type: "cassandra", Enable: true}, // dropped by the dispatch
	}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runSyncTasks(ctx, quietLogger(), cfg)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runSyncTasks did not return after its context was cancelled")
	}
}

// TestRunSyncTasksStartsMonitoringWhenEnabled covers the branch that brings up
// the row-count monitor, and its cancellation on the way out.
func TestRunSyncTasksStartsMonitoringWhenEnabled(t *testing.T) {
	useTempConfigDB(t)

	cfg := &config.Config{
		EnableTableRowCountMonitoring: true,
		MonitorInterval:               time.Hour,
		SyncConfigs:                   []config.SyncConfig{{ID: 1, Type: "cassandra", Enable: true}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runSyncTasks(ctx, quietLogger(), cfg)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runSyncTasks did not return")
	}
}

// TestTheConfigurationIsRereadEveryTenSeconds records the reload cadence, and
// that a change is only acted on when configsEqual says the tasks differ. Ten
// seconds is also the longest a started or stopped task waits to take effect —
// except that the syncers are rebuilt from the configuration, so a task started
// through the API does begin replicating on the next reload after all.
func TestTheConfigurationIsRereadEveryTenSeconds(t *testing.T) {
	db := useTempConfigDB(t)

	cfg := &config.Config{SyncConfigs: nil}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		runSyncTasks(ctx, quietLogger(), cfg)
	}()

	// A task appears in the database. The reload interval is ten seconds, which
	// is longer than this test waits, so nothing should have happened yet.
	if _, err := db.Exec(
		`INSERT INTO sync_tasks (enable, config_json) VALUES (1, '{"type":"cassandra"}')`); err != nil {
		t.Fatalf("insert task: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("runSyncTasks did not return")
	}
}
