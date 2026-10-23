package main

import (
	"context"
	"database/sql"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
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

// useControlDB points SYNC_DB_PATH at a fresh control database with the real
// schema, and returns a handle to edit it with.
func useControlDB(t *testing.T) *sql.DB {
	t.Helper()

	t.Setenv("SYNC_DB_PATH", filepath.Join(t.TempDir(), "sync.db"))
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		t.Fatalf("open the control database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func execOrFail(t *testing.T, db *sql.DB, statement string) {
	t.Helper()

	if _, err := db.Exec(statement); err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
}

// reloadFailures receives each "could not re-read" the reload loop logs.
type reloadFailures chan string

func (reloadFailures) Levels() []logrus.Level { return []logrus.Level{logrus.ErrorLevel} }

func (r reloadFailures) Fire(entry *logrus.Entry) error {
	if strings.HasPrefix(entry.Message, "Could not re-read the configuration") {
		select {
		case r <- entry.Message:
		default:
		}
	}
	return nil
}

// superviseStoredTasks runs the reload loop over the control database every few
// milliseconds. Each task is a stub that hands its context to started.
func superviseStoredTasks(t *testing.T, started chan<- context.Context) reloadFailures {
	t.Helper()

	failures := make(reloadFailures, 16)
	log := quietLogger()
	log.AddHook(failures)

	s := newSupervisor(log)
	s.reloadEvery = 5 * time.Millisecond
	s.build = func(config.SyncConfig, *config.Config, *logrus.Logger) func(context.Context) error {
		return func(ctx context.Context) error {
			started <- ctx
			<-ctx.Done()
			return nil
		}
	}

	cfg, err := config.NewConfig()
	if err != nil {
		t.Fatalf("read the configuration: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.run(ctx, cfg)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return failures
}

func startedTask(t *testing.T, started <-chan context.Context) context.Context {
	t.Helper()

	select {
	case ctx := <-started:
		return ctx
	case <-time.After(5 * time.Second):
		t.Fatal("the stored task was never started")
		return nil
	}
}

// A failure means a stop written to the database, promotion's included, never reaches the running task.
func TestADisabledTaskIsStoppedAtTheNextReload(t *testing.T) {
	db := useControlDB(t)
	execOrFail(t, db, `INSERT INTO sync_tasks (enable, config_json) VALUES (1, '{"type":"mysql"}')`)
	started := make(chan context.Context, 8)
	superviseStoredTasks(t, started)
	task := startedTask(t, started)

	execOrFail(t, db, `UPDATE sync_tasks SET enable = 0`)

	select {
	case <-task.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the task was still running five seconds after it was disabled")
	}
}

// A failure means one unreadable read stops replication, or stops the reloads that come after it.
func TestAnUnreadableReloadKeepsTasksRunning(t *testing.T) {
	db := useControlDB(t)
	execOrFail(t, db, `INSERT INTO sync_tasks (enable, config_json) VALUES (1, '{"type":"mysql"}')`)
	started := make(chan context.Context, 8)
	failures := superviseStoredTasks(t, started)
	task := startedTask(t, started)

	execOrFail(t, db, `ALTER TABLE config_global RENAME TO config_global_aside`)
	for i := 0; i < 3; i++ {
		select {
		case <-failures:
		case <-time.After(5 * time.Second):
			t.Fatalf("%d reloads failed and then none: the loop stopped re-reading", i)
		}
	}
	if task.Err() != nil {
		t.Fatal("a reload that could not read the configuration stopped the task")
	}
	select {
	case <-started:
		t.Fatal("a reload that could not read the configuration restarted the task")
	default:
	}

	execOrFail(t, db, `ALTER TABLE config_global_aside RENAME TO config_global`)
	execOrFail(t, db, `UPDATE sync_tasks SET enable = 0`)
	select {
	case <-task.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("once the configuration was readable again, disabling the task did not stop it")
	}
}
