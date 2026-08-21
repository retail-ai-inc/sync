package app

import (
	"database/sql"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// isolateCrontab empties PATH so the crontab command cannot be found. Every
// write use case calls SyncCrontab after answering, which shells out; without
// this the tests would rewrite the crontab of whoever runs the suite.
func isolateCrontab(t *testing.T) {
	t.Helper()
	t.Setenv("PATH", t.TempDir())
}

// useTempJobDB points SYNC_DB_PATH at a throwaway SQLite file carrying the
// backup_tasks table.
func useTempJobDB(t *testing.T) *sql.DB {
	t.Helper()

	isolateCrontab(t)

	path := filepath.Join(t.TempDir(), "sync.db")
	t.Setenv("SYNC_DB_PATH", path)

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open temp sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const schema = `
CREATE TABLE backup_tasks (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    enable           INTEGER NOT NULL DEFAULT 1,
    last_update_time DATETIME,
    last_backup_time DATETIME,
    next_backup_time DATETIME,
    config_json      TEXT NOT NULL
);`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	return db
}

// emptyJobDB points SYNC_DB_PATH at a file with no tables at all.
func emptyJobDB(t *testing.T) {
	t.Helper()
	isolateCrontab(t)
	t.Setenv("SYNC_DB_PATH", filepath.Join(t.TempDir(), "empty.db"))
}

// unopenableDB points SYNC_DB_PATH at a path whose parent is a regular file.
func unopenableDB(t *testing.T) {
	t.Helper()
	isolateCrontab(t)

	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatalf("write blocker: %v", err)
	}
	t.Setenv("SYNC_DB_PATH", filepath.Join(blocker, "sub", "sync.db"))
}

func insertJob(t *testing.T, db *sql.DB, enable int, cfg string) int64 {
	t.Helper()

	res, err := db.Exec(
		`INSERT INTO backup_tasks (enable, last_update_time, last_backup_time, next_backup_time, config_json)
		 VALUES (?, '2026-08-21 00:00:00', '2026-08-20 18:00:00', '2026-08-22 18:00:00', ?)`,
		enable, cfg)
	if err != nil {
		t.Fatalf("insert backup task: %v", err)
	}
	id, _ := res.LastInsertId()
	return id
}

func readConfig(t *testing.T, db *sql.DB, id int64) string {
	t.Helper()

	var cfg string
	if err := db.QueryRow(`SELECT config_json FROM backup_tasks WHERE id=?`, id).Scan(&cfg); err != nil {
		t.Fatalf("read config_json: %v", err)
	}
	return cfg
}

func contains(haystack, needle string) bool { return strings.Contains(haystack, needle) }

func itoa(id int64) string { return strconv.FormatInt(id, 10) }
