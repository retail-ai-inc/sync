package infra

import (
	"database/sql"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// useTempTaskDB points SYNC_DB_PATH at a throwaway SQLite file carrying the two
// tables this store reads, and returns a handle so a test can seed rows and read
// them back independently of the store under test.
func useTempTaskDB(t *testing.T) *sql.DB {
	t.Helper()

	path := filepath.Join(t.TempDir(), "sync.db")
	t.Setenv("SYNC_DB_PATH", path)

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open temp sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const schema = `
CREATE TABLE sync_tasks (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    enable           INTEGER NOT NULL DEFAULT 1,
    last_update_time DATETIME,
    last_run_time    DATETIME,
    config_json      TEXT NOT NULL
);
CREATE TABLE monitoring_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    sync_task_id  INTEGER NOT NULL,
    logged_at     DATETIME NOT NULL,
    src_table     TEXT,
    tgt_table     TEXT,
    src_row_count INTEGER,
    tgt_row_count INTEGER
);`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	return db
}

// emptyTaskDB points SYNC_DB_PATH at a file with no tables at all.
func emptyTaskDB(t *testing.T) {
	t.Helper()
	t.Setenv("SYNC_DB_PATH", filepath.Join(t.TempDir(), "empty.db"))
}

// unopenableDB points SYNC_DB_PATH at a path whose parent is a regular file, so
// the directory creation inside OpenSQLiteDB fails immediately.
func unopenableDB(t *testing.T) {
	t.Helper()

	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatalf("write blocker: %v", err)
	}
	t.Setenv("SYNC_DB_PATH", filepath.Join(blocker, "sub", "sync.db"))
}

// insertTask seeds one sync_tasks row.
func insertTask(t *testing.T, db *sql.DB, enable int, cfg string) int64 {
	t.Helper()

	res, err := db.Exec(
		`INSERT INTO sync_tasks (enable, last_update_time, last_run_time, config_json)
		 VALUES (?, '2026-08-21 00:00:00', '2026-08-21 01:00:00', ?)`, enable, cfg)
	if err != nil {
		t.Fatalf("insert sync task: %v", err)
	}
	id, _ := res.LastInsertId()
	return id
}

// insertMonitoringRow seeds one monitoring_log row.
func insertMonitoringRow(t *testing.T, db *sql.DB, taskID int, loggedAt, table string, src, tgt int64) {
	t.Helper()

	if _, err := db.Exec(
		`INSERT INTO monitoring_log (sync_task_id, logged_at, src_table, tgt_table, src_row_count, tgt_row_count)
		 VALUES (?, ?, ?, ?, ?, ?)`, taskID, loggedAt, table, table, src, tgt); err != nil {
		t.Fatalf("insert monitoring row: %v", err)
	}
}

// readConfig returns one row's stored configuration document.
func readConfig(t *testing.T, db *sql.DB, id int64) string {
	t.Helper()

	var cfg string
	if err := db.QueryRow(`SELECT config_json FROM sync_tasks WHERE id=?`, id).Scan(&cfg); err != nil {
		t.Fatalf("read config_json: %v", err)
	}
	return cfg
}

// readTimestamp reads a DATETIME column back as the string the store wrote. The
// go-sqlite3 driver converts a DATETIME to time.Time, so scanning straight into
// a string yields RFC 3339 rather than the value that went in.
func readTimestamp(t *testing.T, db *sql.DB, column string, id int64) string {
	t.Helper()

	var ts time.Time
	if err := db.QueryRow(`SELECT `+column+` FROM sync_tasks WHERE id=?`, id).Scan(&ts); err != nil {
		t.Fatalf("read %s: %v", column, err)
	}
	return ts.UTC().Format("2006-01-02 15:04:05")
}

// stageOf returns the stage a store failure was tagged with, or the empty string
// when the error is not a Fault.
func stageOf(err error) string {
	if f, ok := err.(*Fault); ok {
		return f.Stage
	}
	return ""
}

func contains(haystack, needle string) bool { return strings.Contains(haystack, needle) }

func itoa(id int64) string { return strconv.FormatInt(id, 10) }
