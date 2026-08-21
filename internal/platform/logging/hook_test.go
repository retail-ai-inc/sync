package logging

import (
	"database/sql"
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

// TestTheHookSwallowsEveryFailure records that a log hook must never break the
// program that logs: a missing database, a missing table and an unopenable path
// are all answered with nil. The consequence is that log rows can stop being
// written with nothing to show it.
func TestTheHookSwallowsEveryFailure(t *testing.T) {
	hook := NewSQLiteHook()
	entry := &logrus.Entry{Logger: logrus.New(), Level: logrus.ErrorLevel, Message: "m"}

	t.Run("no tables", func(t *testing.T) {
		t.Setenv("SYNC_DB_PATH", filepath.Join(t.TempDir(), "empty.db"))
		if err := hook.Fire(entry); err != nil {
			t.Errorf("Fire = %v, want nil", err)
		}
	})

	t.Run("unopenable database", func(t *testing.T) {
		blocker := filepath.Join(t.TempDir(), "not-a-directory")
		if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
			t.Fatalf("write blocker: %v", err)
		}
		t.Setenv("SYNC_DB_PATH", filepath.Join(blocker, "sub", "sync.db"))

		if err := hook.Fire(entry); err != nil {
			t.Errorf("Fire = %v, want nil", err)
		}
	})
}

// TestTheHookOpensADatabasePerEntry records that every log line opens and closes
// its own connection. At the info level that is one SQLite open per line, which
// is why the hook is only attached at higher levels.
func TestTheHookOpensADatabasePerEntry(t *testing.T) {
	db := useTempLogDB(t)

	hook := NewSQLiteHook()
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
