package app

import (
	"database/sql"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
	"github.com/retail-ai-inc/sync/internal/platform/sqlite/sqlitetest"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sirupsen/logrus"
)

// isolateCrontab empties PATH so the crontab command cannot be found.
func isolateCrontab(t *testing.T) {
	t.Helper()
	t.Setenv("PATH", t.TempDir())
}

// useTempJobDB points SYNC_DB_PATH at a throwaway SQLite file carrying the
// backup_tasks table.
func useTempJobDB(t *testing.T) *sql.DB {
	t.Helper()

	isolateCrontab(t)

	path := filepath.Join(jobDBDir(t), "sync.db")
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

// jobDBDir returns a throwaway directory that is removed on a best-effort
// basis rather than by t.TempDir.
func jobDBDir(t *testing.T) string {
	t.Helper()

	dir, err := os.MkdirTemp("", "backup-app-")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func emptyJobDB(t *testing.T) {
	t.Helper()
	isolateCrontab(t)
	sqlitetest.Tableless(t)
}

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

// quietBackupLogger keeps the scheduler's reports out of the test output.
func quietBackupLogger() logrus.FieldLogger {
	l := logrus.New()
	l.SetLevel(logrus.PanicLevel)
	return l
}
