package export

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// controlDatabase writes a small SQLite file with something in it, so a
// snapshot that produced an empty or unreadable file is distinguishable from
// one that worked.
func controlDatabase(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "sync.db")

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE tasks (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 0; i < 50; i++ {
		if _, err := db.Exec(`INSERT INTO tasks (name) VALUES (?)`, "task"); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	return path
}

func TestASnapshotIsAReadableDatabaseOfItsOwn(t *testing.T) {
	source := controlDatabase(t)
	destination := filepath.Join(t.TempDir(), "snapshot.db")

	if err := vacuumInto(context.Background(), source, destination); err != nil {
		t.Fatalf("vacuumInto: %v", err)
	}

	// Opened as a database rather than compared byte for byte, because a copy
	// that is not a database is the failure this exists to avoid.
	db, err := sql.Open("sqlite3", destination)
	if err != nil {
		t.Fatalf("open the snapshot: %v", err)
	}
	defer db.Close()
	var rows int
	if err := db.QueryRow(`SELECT COUNT(*) FROM tasks`).Scan(&rows); err != nil {
		t.Fatalf("read the snapshot: %v", err)
	}
	if rows != 50 {
		t.Errorf("the snapshot holds %d rows, want 50", rows)
	}
}

func TestASnapshotReplacesAnEarlierOne(t *testing.T) {
	source := controlDatabase(t)
	destination := filepath.Join(t.TempDir(), "snapshot.db")

	// VACUUM INTO refuses to overwrite, so a leftover from a previous run would
	// fail every backup from then on unless it is cleared first.
	if err := os.WriteFile(destination, []byte("leftover"), 0o600); err != nil {
		t.Fatalf("write a leftover: %v", err)
	}
	if err := vacuumInto(context.Background(), source, destination); err != nil {
		t.Fatalf("vacuumInto over a leftover: %v", err)
	}
}

func TestASnapshotOfSomethingThatIsNotADatabaseFails(t *testing.T) {
	source := filepath.Join(t.TempDir(), "not.db")
	if err := os.WriteFile(source, []byte("this is not a database"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := vacuumInto(context.Background(),
		source, filepath.Join(t.TempDir(), "out.db")); err == nil {
		t.Error("a file that is not a database was snapshotted")
	}
}

func TestTheSQLiteBackupLeavesNothingBehind(t *testing.T) {
	source := controlDatabase(t)
	tempDir := t.TempDir()

	var config ExecutorBackupConfig
	config.CompressionType = "none"

	if err := newExecutor().executeSQLiteBackup(
		context.Background(), source, tempDir, config); err != nil {
		t.Fatalf("executeSQLiteBackup: %v", err)
	}

	// Both the snapshot and the archive are removed however the backup returns,
	// so a destination that is unreachable for a while cannot fill the disk one
	// snapshot at a time.
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("read the temp directory: %v", err)
	}
	for _, entry := range entries {
		t.Errorf("%s was left in the temp directory", entry.Name())
	}
}

func TestTheSQLiteBackupSaysWhatItCannotFind(t *testing.T) {
	var config ExecutorBackupConfig
	config.CompressionType = "none"

	err := newExecutor().executeSQLiteBackup(
		context.Background(), "", t.TempDir(), config)
	if err == nil {
		t.Fatal("a backup with no database file reported success")
	}
	// The message names the setting, because the path is a configuration
	// mistake rather than a failure of the run.
	if !strings.Contains(err.Error(), "sync.db") {
		t.Errorf("the error does not say what to set: %v", err)
	}

	if err := newExecutor().executeSQLiteBackup(context.Background(),
		filepath.Join(t.TempDir(), "absent.db"), t.TempDir(), config); err == nil {
		t.Error("a backup of a file that is not there reported success")
	}
}

func TestAPathIsQuotedForSQLite(t *testing.T) {
	if got := quoteSQLiteString("/tmp/a.db"); got != "'/tmp/a.db'" {
		t.Errorf("quoteSQLiteString = %q", got)
	}
	// A quote in a path would otherwise close the literal and change the
	// statement.
	if got := quoteSQLiteString("/tmp/it's.db"); got != "'/tmp/it''s.db'" {
		t.Errorf("quoteSQLiteString of a path with a quote = %q", got)
	}
}

func TestTheBaseNameIsTheLastSegment(t *testing.T) {
	for path, want := range map[string]string{
		"/var/lib/sync/sync.db": "sync.db",
		`C:\data\sync.db`:       "sync.db",
		"sync.db":               "sync.db",
	} {
		if got := baseName(path); got != want {
			t.Errorf("baseName(%q) = %q, want %q", path, got, want)
		}
	}
}
