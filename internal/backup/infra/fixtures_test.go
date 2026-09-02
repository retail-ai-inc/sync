package infra

import (
	"database/sql"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
	"github.com/retail-ai-inc/sync/internal/platform/sqlite/sqlitetest"

	_ "github.com/mattn/go-sqlite3"
)

// useTempJobDB points SYNC_DB_PATH at a throwaway SQLite file carrying the
// backup_tasks table, and returns a handle on it so a test can seed rows and
// read them back independently of the store under test.
func useTempJobDB(t *testing.T) *sql.DB {
	t.Helper()

	path := filepath.Join(t.TempDir(), "sync.db")
	t.Setenv("SYNC_DB_PATH", path)

	// Through the real opener, so the fixture carries the schema the program
	// creates rather than a copy of it that can drift.
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		t.Fatalf("open temp sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// emptyJobDB points SYNC_DB_PATH at a file with no tables at all, so a store
// call fails on the query rather than on the connection.
func emptyJobDB(t *testing.T) {
	t.Helper()
	sqlitetest.Tableless(t)
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

func asFault(err error, target **Fault) bool {
	f, ok := err.(*Fault)
	if ok {
		*target = f
	}
	return ok
}

func contains(haystack, needle string) bool { return strings.Contains(haystack, needle) }

// itoa renders a row id the way the endpoints hand it to the store: as the
// string that arrived in the URL.
func itoa(id int64) string { return strconv.FormatInt(id, 10) }

func errNoRows() error { return sql.ErrNoRows }

// readTimestamp reads one of the DATETIME columns back as the string the store
// wrote.
func readTimestamp(t *testing.T, db *sql.DB, column string, id int64) string {
	t.Helper()

	var ts time.Time
	if err := db.QueryRow(`SELECT `+column+` FROM backup_tasks WHERE id=?`, id).Scan(&ts); err != nil {
		t.Fatalf("read %s: %v", column, err)
	}
	return ts.UTC().Format("2006-01-02 15:04:05")
}
