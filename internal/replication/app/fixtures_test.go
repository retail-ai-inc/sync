package app

import (
	"database/sql"
	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
	"github.com/retail-ai-inc/sync/internal/platform/sqlite/sqlitetest"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// useTempTaskDB points SYNC_DB_PATH at a throwaway SQLite file carrying the two
// tables the store below reads.
func useTempTaskDB(t *testing.T) *sql.DB {
	t.Helper()

	path := filepath.Join(t.TempDir(), "sync.db")
	t.Setenv("SYNC_DB_PATH", path)

	// Through the real opener, which carries the whole schema and creates it
	// only when it is missing — rather than a copy kept here that can drift from
	// it, and that a background goroutine racing to the same path turns into
	// "table already exists".
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		t.Fatalf("open temp sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// emptyTaskDB points SYNC_DB_PATH at a file with no tables at all.
func emptyTaskDB(t *testing.T) {
	t.Helper()
	sqlitetest.Tableless(t)
}

// unopenableDB points SYNC_DB_PATH at a path whose parent is a regular file.
func unopenableDB(t *testing.T) {
	t.Helper()

	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatalf("write blocker: %v", err)
	}
	t.Setenv("SYNC_DB_PATH", filepath.Join(blocker, "sub", "sync.db"))
}

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

func insertMonitoringRow(t *testing.T, db *sql.DB, taskID int, loggedAt, table string, src, tgt int64) {
	t.Helper()

	// db_type is NOT NULL in the real schema, which the fixtures' own copies used
	// to leave off — so these rows could never have been written by the monitor
	// that writes them in production.
	if _, err := db.Exec(
		`INSERT INTO monitoring_log
		   (sync_task_id, logged_at, db_type, src_table, tgt_table, src_row_count, tgt_row_count)
		 VALUES (?, ?, 'MONGODB', ?, ?, ?, ?)`, taskID, loggedAt, table, table, src, tgt); err != nil {
		t.Fatalf("insert monitoring row: %v", err)
	}
}

func readConfig(t *testing.T, db *sql.DB, id int64) string {
	t.Helper()

	var cfg string
	if err := db.QueryRow(`SELECT config_json FROM sync_tasks WHERE id=?`, id).Scan(&cfg); err != nil {
		t.Fatalf("read config_json: %v", err)
	}
	return cfg
}

func itoa(id int64) string { return strconv.FormatInt(id, 10) }
