package infra

import (
	"database/sql"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// useTempJobDB points SYNC_DB_PATH at a throwaway SQLite file carrying the
// backup_tasks table, and returns a handle on it so a test can seed rows and
// read them back independently of the store under test.
func useTempJobDB(t *testing.T) *sql.DB {
	t.Helper()

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

// emptyJobDB points SYNC_DB_PATH at a file with no tables at all, so a store
// call fails on the query rather than on the connection.
func emptyJobDB(t *testing.T) {
	t.Helper()
	t.Setenv("SYNC_DB_PATH", filepath.Join(t.TempDir(), "empty.db"))
}

// insertJob seeds one backup_tasks row.
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

// readConfig returns one row's stored configuration document.
func readConfig(t *testing.T, db *sql.DB, id int64) string {
	t.Helper()

	var cfg string
	if err := db.QueryRow(`SELECT config_json FROM backup_tasks WHERE id=?`, id).Scan(&cfg); err != nil {
		t.Fatalf("read config_json: %v", err)
	}
	return cfg
}

// stageOf returns the stage a store failure was tagged with, or the empty
// string when the error is not a Fault.
func stageOf(err error) string {
	var fault *Fault
	if err == nil {
		return ""
	}
	if asFault(err, &fault) {
		return fault.Stage
	}
	return ""
}

// asFault is errors.As, wrapped so the helper above reads plainly.
func asFault(err error, target **Fault) bool {
	f, ok := err.(*Fault)
	if ok {
		*target = f
	}
	return ok
}

// contains is strings.Contains, named so the assertions above read plainly.
func contains(haystack, needle string) bool { return strings.Contains(haystack, needle) }

// itoa renders a row id the way the endpoints hand it to the store: as the
// string that arrived in the URL.
func itoa(id int64) string { return strconv.FormatInt(id, 10) }

// errNoRows is a stand-in cause for the Fault test.
func errNoRows() error { return sql.ErrNoRows }

// readTimestamp reads one of the DATETIME columns back as the string the store
// wrote. The go-sqlite3 driver converts a DATETIME to time.Time, so scanning
// straight into a string yields RFC 3339 rather than the value that went in;
// scanning into a time.Time and formatting back avoids asserting on the
// driver's rendering.
func readTimestamp(t *testing.T, db *sql.DB, column string, id int64) string {
	t.Helper()

	var ts time.Time
	if err := db.QueryRow(`SELECT `+column+` FROM backup_tasks WHERE id=?`, id).Scan(&ts); err != nil {
		t.Fatalf("read %s: %v", column, err)
	}
	return ts.UTC().Format("2006-01-02 15:04:05")
}
