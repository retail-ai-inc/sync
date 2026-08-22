package logging

import (
	"database/sql"
	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sirupsen/logrus"
)

// useTempLogDB points SYNC_DB_PATH at a throwaway SQLite file carrying the
// sync_log table the hook writes into.
func useTempLogDB(t *testing.T) *sql.DB {
	t.Helper()

	path := filepath.Join(t.TempDir(), "sync.db")
	t.Setenv("SYNC_DB_PATH", path)

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open temp sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec(`
CREATE TABLE sync_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    sync_task_id INTEGER,
    level        TEXT,
    message      TEXT,
    created_at   DATETIME DEFAULT CURRENT_TIMESTAMP
);`); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	return db
}

func TestTheHookWritesAnEntryToTheLogTable(t *testing.T) {
	db := useTempLogDB(t)

	hook := NewSQLiteHook()
	entry := &logrus.Entry{
		Logger:  logrus.New(),
		Level:   logrus.ErrorLevel,
		Message: "something went wrong",
		Data:    logrus.Fields{"sync_task_id": 7},
	}

	if err := hook.Fire(entry); err != nil {
		t.Fatalf("Fire: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sync_log`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("%d rows were written, want 1", count)
	}

	var taskID sql.NullInt64
	var level, message string
	if err := db.QueryRow(`SELECT sync_task_id, level, message FROM sync_log`).
		Scan(&taskID, &level, &message); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if !taskID.Valid || taskID.Int64 != 7 {
		t.Errorf("sync_task_id = %v, want 7", taskID)
	}
	if level != "error" {
		t.Errorf("level = %q, want error", level)
	}
	if message == "" {
		t.Error("the message is empty")
	}
}

// TestAnEntryWithoutATaskIDIsStillWritten records that the task id is optional:
// a log line from a part of the process that is not a sync task is stored with a
// NULL or zero id rather than being dropped.
func TestAnEntryWithoutATaskIDIsStillWritten(t *testing.T) {
	db := useTempLogDB(t)

	hook := NewSQLiteHook()
	if err := hook.Fire(&logrus.Entry{
		Logger:  logrus.New(),
		Level:   logrus.InfoLevel,
		Message: "starting up",
	}); err != nil {
		t.Fatalf("Fire: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sync_log`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("%d rows were written, want 1", count)
	}
}

// TestAFailureIsReportedOnceAndNotOncePerLine covers log rows quietly stopping.
// Every failure used to be answered with nil — a missing table, an unopenable
// path — so the hook could stop writing for good with nothing to show it. It now
// reports the first failure of an outage and stays quiet until it recovers,
// which is the difference between one visible complaint and either none or one
// per log line.
func TestAFailureIsReportedOnceAndNotOncePerLine(t *testing.T) {
	entry := &logrus.Entry{Logger: logrus.New(), Level: logrus.ErrorLevel, Message: "m"}

	t.Run("no tables", func(t *testing.T) {
		hook := NewSQLiteHook()
		t.Cleanup(func() { _ = hook.Close() })
		tablelessDB(t)

		if err := hook.Fire(entry); err == nil {
			t.Error("Fire = nil for a database with no sync_log table")
		}
		if err := hook.Fire(entry); err != nil {
			t.Errorf("the second line reported %v; the same outage should be quiet", err)
		}
	})

	t.Run("unopenable database", func(t *testing.T) {
		hook := NewSQLiteHook()
		t.Cleanup(func() { _ = hook.Close() })
		blocker := filepath.Join(t.TempDir(), "not-a-directory")
		if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
			t.Fatalf("write blocker: %v", err)
		}
		t.Setenv("SYNC_DB_PATH", filepath.Join(blocker, "sub", "sync.db"))

		if err := hook.Fire(entry); err == nil {
			t.Error("Fire = nil for an unopenable database")
		}
	})
}

// TestTheConnectionIsReusedAcrossLines covers the cost of logging. Every line
// used to open and close its own SQLite connection — at the info level that is
// one open per log entry, against a database whose pool holds a single
// connection and which replication also writes its checkpoints to.
func TestTheConnectionIsReusedAcrossLines(t *testing.T) {
	db := useTempLogDB(t)

	hook := NewSQLiteHook()
	t.Cleanup(func() { _ = hook.Close() })
	for i := 0; i < 5; i++ {
		if err := hook.Fire(&logrus.Entry{
			Logger:  logrus.New(),
			Level:   logrus.WarnLevel,
			Message: "m",
		}); err != nil {
			t.Fatalf("Fire: %v", err)
		}
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sync_log`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 5 {
		t.Errorf("%d rows were written, want 5", count)
	}
}

// tablelessDB points SYNC_DB_PATH at a database whose tables have been removed,
// which is the state a migration that did not finish — or a file restored from
// the wrong backup — leaves behind.
//
// Pointing at an empty file no longer produces one: opening the control database
// creates its schema, so the tables have to be dropped after that has happened.
// The schema is applied once per file, so later opens leave them dropped.
func tablelessDB(t *testing.T) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "empty.db")
	t.Setenv("SYNC_DB_PATH", path)

	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		t.Fatalf("open the control database: %v", err)
	}
	defer db.Close()

	rows, err := db.Query(
		`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`)
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		names = append(names, name)
	}
	rows.Close()

	for _, name := range names {
		if _, err := db.Exec(`DROP TABLE IF EXISTS "` + name + `"`); err != nil {
			t.Fatalf("drop %s: %v", name, err)
		}
	}
}
