package export

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func controlDatabase(t *testing.T, rows int) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "sync.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// WAL, because that is what a control database being written to while it is
	// backed up looks like -- and because it is what separates a snapshot from a
	// file copy: recent commits live in the -wal file, so copying the .db alone
	// silently loses them.
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("wal: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE sync_tasks (
		id INTEGER PRIMARY KEY, enable INTEGER NOT NULL, config_json TEXT NOT NULL)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 0; i < rows; i++ {
		if _, err := db.Exec("INSERT INTO sync_tasks (id, enable, config_json) VALUES (?, 1, ?)",
			i, `{"type":"mongodb"}`); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	return path
}

// The control database holds every task's configuration and stored position,
// and had no backup path of its own: the exporters covered the databases being
// replicated, not the one that says what to replicate. Losing it costs a full
// re-copy of every link.
func TestTheControlDatabaseIsSnapshotAndReadable(t *testing.T) {
	source := controlDatabase(t, 40)
	destination := filepath.Join(t.TempDir(), "snapshot.db")

	if err := vacuumInto(context.Background(), source, destination); err != nil {
		t.Fatalf("vacuumInto: %v", err)
	}

	info, err := os.Stat(destination)
	if err != nil || info.Size() == 0 {
		t.Fatalf("snapshot is missing or empty: %v", err)
	}

	// A snapshot nobody can open is not a backup.
	snapshot, err := sql.Open("sqlite3", destination)
	if err != nil {
		t.Fatalf("open the snapshot: %v", err)
	}
	defer snapshot.Close()

	var rows int
	if err := snapshot.QueryRow("SELECT COUNT(*) FROM sync_tasks").Scan(&rows); err != nil {
		t.Fatalf("read the snapshot: %v", err)
	}
	if rows != 40 {
		t.Errorf("the snapshot holds %d rows, want 40", rows)
	}
}

// A live database is what this has to work against: the tool is running while
// its own configuration is backed up.
func TestASnapshotIsTakenWhileTheDatabaseIsOpen(t *testing.T) {
	source := controlDatabase(t, 5)

	live, err := sql.Open("sqlite3", source)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer live.Close()
	if _, err := live.Exec("INSERT INTO sync_tasks (id, enable, config_json) VALUES (99, 1, '{}')"); err != nil {
		t.Fatalf("write: %v", err)
	}

	destination := filepath.Join(t.TempDir(), "snapshot.db")
	if err := vacuumInto(context.Background(), source, destination); err != nil {
		t.Fatalf("vacuumInto against an open database: %v", err)
	}

	snapshot, _ := sql.Open("sqlite3", destination)
	defer snapshot.Close()
	var rows int
	if err := snapshot.QueryRow("SELECT COUNT(*) FROM sync_tasks").Scan(&rows); err != nil {
		t.Fatalf("read: %v", err)
	}
	if rows != 6 {
		t.Errorf("the snapshot holds %d rows, want the 6 committed", rows)
	}
}

// A missing file is a configuration mistake, and saying so beats an empty
// archive that reads as a successful backup.
func TestAMissingDatabaseIsReported(t *testing.T) {
	err := vacuumInto(context.Background(), filepath.Join(t.TempDir(), "nope.db"),
		filepath.Join(t.TempDir(), "out.db"))
	if err == nil {
		t.Error("a database that does not exist produced no error")
	}
}

// A path with a quote in it must not end the statement early.
func TestAQuotedPathIsEscaped(t *testing.T) {
	if got := quoteSQLiteString("/tmp/it's/sync.db"); got != "'/tmp/it''s/sync.db'" {
		t.Errorf("quoted = %s, want the quote doubled", got)
	}
}
