package config

import (
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// useTempConfigDB points SYNC_DB_PATH at a throwaway SQLite file carrying the
// two tables NewConfig reads, and returns a handle so a test can seed rows.
func useTempConfigDB(t *testing.T) *sql.DB {
	t.Helper()

	path := filepath.Join(t.TempDir(), "sync.db")
	t.Setenv("SYNC_DB_PATH", path)

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open temp sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const schema = `
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
);`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	return db
}

// TestNewConfigReadsBothTables records that the constructor loads the global
// settings and the sync tasks in one go.
func TestNewConfigReadsBothTables(t *testing.T) {
	db := useTempConfigDB(t)
	if _, err := db.Exec(
		`INSERT INTO config_global
		   (id, enable_table_row_count_monitoring, log_level, monitor_interval, slackWebhookURL, slackChannel)
		 VALUES (1, 1, 'debug', 60, 'https://hooks.example.test/x', '#ops')`); err != nil {
		t.Fatalf("insert settings: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO sync_tasks (enable, config_json) VALUES (1, ?)`,
		`{"type":"mongodb","taskName":"orders",
		  "sourceConn":{"host":"h","port":"1","database":"d"},
		  "targetConn":{"host":"h","port":"1","database":"d"},"mappings":[]}`); err != nil {
		t.Fatalf("insert task: %v", err)
	}

	cfg := NewConfig()

	if !cfg.EnableTableRowCountMonitoring {
		t.Error("EnableTableRowCountMonitoring = false")
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %q", cfg.LogLevel)
	}
	if cfg.MonitorInterval != 60*time.Second {
		t.Errorf("MonitorInterval = %v, want 60s", cfg.MonitorInterval)
	}
	if cfg.GetSlackWebhookURL() != "https://hooks.example.test/x" || cfg.GetSlackChannel() != "#ops" {
		t.Errorf("slack settings = %q / %q", cfg.GetSlackWebhookURL(), cfg.GetSlackChannel())
	}
	if len(cfg.SyncConfigs) != 1 {
		t.Fatalf("SyncConfigs holds %d tasks, want 1", len(cfg.SyncConfigs))
	}
	if cfg.SyncConfigs[0].Type != "mongodb" {
		t.Errorf("the task type is %q", cfg.SyncConfigs[0].Type)
	}
	if cfg.Logger == nil {
		t.Error("Logger is nil")
	}
}

// TestAnEmptyConfigGlobalTableIsFatal records a defect this test suite found.
//
// loadGlobalConfig answers sql.ErrNoRows with log.Fatalf, so a database that has
// the tables but no settings row kills the process at start-up. A fresh install,
// or a migration that created the schema without seeding it, cannot start — and
// the message is "Failed to load config_global: sql: no rows in result set",
// which does not say what to do about it.
//
// There are five log.Fatalf calls on this path: the database open, this row, the
// sync_tasks query, the row scan and the row iteration. Any of them takes the
// whole process down, including the API server that could have been used to fix
// the configuration.
//
// The exit cannot be exercised in-process, so this runs the load in a
// subprocess.
func TestAnEmptyConfigGlobalTableIsFatal(t *testing.T) {
	if os.Getenv("SYNC_TEST_FATAL_CHILD") == "1" {
		// Child: the tables exist, the settings row does not.
		useTempConfigDBAt(os.Getenv("SYNC_DB_PATH"))
		_ = NewConfig()
		return
	}

	path := filepath.Join(t.TempDir(), "sync.db")
	cmd := exec.Command(os.Args[0], "-test.run=TestAnEmptyConfigGlobalTableIsFatal")
	cmd.Env = append(os.Environ(), "SYNC_TEST_FATAL_CHILD=1", "SYNC_DB_PATH="+path)
	out, err := cmd.CombinedOutput()

	if err == nil {
		t.Fatalf("the child survived an empty config_global table; the load appears "+
			"to return an error now, so assert that instead (output: %s)", out)
	}
	if !strings.Contains(string(out), "Failed to load config_global") {
		t.Errorf("the child died for another reason: %s", out)
	}
}

// TestNewConfigDoesNotHoldTheDatabaseOpen records that the handle is closed
// before the configuration is returned, so the single-connection SQLite pool is
// released for the next caller. Two calls in a row have to work.
func TestNewConfigDoesNotHoldTheDatabaseOpen(t *testing.T) {
	db := useTempConfigDB(t)
	// A settings row is mandatory: without one the load calls log.Fatalf and
	// takes the test binary with it. See TestAnEmptyConfigGlobalTableIsFatal.
	if _, err := db.Exec(
		`INSERT INTO config_global (id, enable_table_row_count_monitoring, log_level, monitor_interval)
		 VALUES (1, 0, 'info', 60)`); err != nil {
		t.Fatalf("insert settings: %v", err)
	}

	_ = NewConfig()
	_ = NewConfig()
}

// TestNewConfigCallsFatalWhenTheDatabaseCannotBeOpened records that the
// constructor answers an unopenable database with log.Fatalf, which exits the
// process. No caller is given the chance to handle it: the API server and every
// replication task die together, at start-up, with one line of output.
//
// The exit cannot be exercised in-process, so this is the record that the path
// is still there rather than a test of it. Turning it into a returned error is a
// behaviour change.
func TestNewConfigCallsFatalWhenTheDatabaseCannotBeOpened(t *testing.T) {
	t.Log("NewConfig calls log.Fatalf on an unopenable database; see config.go")
}

// useTempConfigDBAt creates the two tables at a given path, without a *testing.T
// so the subprocess above can call it.
func useTempConfigDBAt(path string) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	const schema = `
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
);`
	if _, err := db.Exec(schema); err != nil {
		panic(err)
	}
}
