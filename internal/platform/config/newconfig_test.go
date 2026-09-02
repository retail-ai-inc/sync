package config

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

// useTempConfigDB points SYNC_DB_PATH at a throwaway SQLite file carrying the
// two tables NewConfig reads, and returns a handle so a test can seed rows.
func useTempConfigDB(t *testing.T) *sql.DB {
	t.Helper()

	path := filepath.Join(t.TempDir(), "sync.db")
	t.Setenv("SYNC_DB_PATH", path)

	// Through the real opener, so the fixture carries the schema the program
	// creates rather than a copy of it that can drift. That also settles the
	// schema for this file, so a table a test drops afterwards stays dropped.
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		t.Fatalf("open temp sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// The opener seeds a settings row. The tests here supply their own, or
	// depend on there being none, so start from an empty table.
	if _, err := db.Exec(`DELETE FROM config_global`); err != nil {
		t.Fatalf("clear config_global: %v", err)
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

	cfg, err := NewConfig()
	if err != nil {
		t.Fatalf("NewConfig: %v", err)
	}

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

// The load answered sql.ErrNoRows with log.Fatalf, so a database with the
// tables but no settings row killed the process — including the API server
// that could have been used to fix the configuration.
func TestAnEmptyConfigGlobalTableIsReported(t *testing.T) {
	useTempConfigDB(t)

	cfg, err := NewConfig()
	if err == nil {
		t.Fatal("NewConfig succeeded with no settings row")
	}
	if cfg != nil {
		t.Error("a configuration was returned alongside the error")
	}
	if !strings.Contains(err.Error(), "config_global") {
		t.Errorf("error = %v, want it to name the table", err)
	}
}

func TestAnUnopenableDatabaseIsReported(t *testing.T) {
	// A directory where the file should be: openable by name, unusable as a
	// database.
	dir := filepath.Join(t.TempDir(), "sync.db")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	t.Setenv("SYNC_DB_PATH", dir)

	if _, err := NewConfig(); err == nil {
		t.Error("NewConfig succeeded against a database that cannot be opened")
	}
}

func TestAMissingSyncTasksTableIsReported(t *testing.T) {
	db := useTempConfigDB(t)
	if _, err := db.Exec(
		`INSERT INTO config_global (id, enable_table_row_count_monitoring, log_level, monitor_interval)
		 VALUES (1, 0, 'info', 60)`); err != nil {
		t.Fatalf("insert settings: %v", err)
	}
	if _, err := db.Exec(`DROP TABLE sync_tasks`); err != nil {
		t.Fatalf("drop sync_tasks: %v", err)
	}

	if _, err := NewConfig(); err == nil {
		t.Error("NewConfig succeeded with no sync_tasks table")
	}
}

// TestNewConfigDoesNotHoldTheDatabaseOpen records that the handle is closed
// before the configuration is returned, so the single-connection SQLite pool is
// released for the next caller. Two calls in a row have to work.
func TestNewConfigDoesNotHoldTheDatabaseOpen(t *testing.T) {
	db := useTempConfigDB(t)
	// A settings row is mandatory: without one the load reports an error. See
	// TestAnEmptyConfigGlobalTableIsReported.
	if _, err := db.Exec(
		`INSERT INTO config_global (id, enable_table_row_count_monitoring, log_level, monitor_interval)
		 VALUES (1, 0, 'info', 60)`); err != nil {
		t.Fatalf("insert settings: %v", err)
	}

	if _, err := NewConfig(); err != nil {
		t.Fatalf("first NewConfig: %v", err)
	}
	if _, err := NewConfig(); err != nil {
		t.Fatalf("second NewConfig: %v", err)
	}
}
