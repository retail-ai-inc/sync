package sqlite

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOpenSQLiteDBCreatesTheFileAndItsDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "deeper", "sync.db")
	t.Setenv("SYNC_DB_PATH", path)

	db, err := OpenSQLiteDB()
	if err != nil {
		t.Fatalf("OpenSQLiteDB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec(`CREATE TABLE t (id INTEGER)`); err != nil {
		t.Fatalf("the handle is not usable: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("the database file was not created: %v", err)
	}
}

// TestTheConnectionPoolIsCappedAtOne records that the pool is deliberately a
// single connection, because SQLite serialises writers anyway. Every store call
// in this repository opens its own pool and closes it again, so the cap applies
// per call rather than across the process.
func TestTheConnectionPoolIsCappedAtOne(t *testing.T) {
	t.Setenv("SYNC_DB_PATH", filepath.Join(t.TempDir(), "sync.db"))

	db, err := OpenSQLiteDB()
	if err != nil {
		t.Fatalf("OpenSQLiteDB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if got := db.Stats().MaxOpenConnections; got != 1 {
		t.Errorf("MaxOpenConnections = %d, want 1", got)
	}
}

// TestARelativePathIsMadeAbsolute records that a relative SYNC_DB_PATH is
// resolved against the working directory of whichever process opens it. Two
// processes started from different directories therefore use different
// databases, silently.
func TestARelativePathIsMadeAbsolute(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	t.Setenv("SYNC_DB_PATH", "relative.db")

	db, err := OpenSQLiteDB()
	if err != nil {
		t.Fatalf("OpenSQLiteDB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := os.Stat(filepath.Join(dir, "relative.db")); err != nil {
		t.Errorf("the database was not created next to the working directory: %v", err)
	}
}

// TestAnEmptyPathFallsBackToSyncDB records the fallback when the environment
// variable is unset: the file "sync.db" in the working directory.
func TestAnEmptyPathFallsBackToSyncDB(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	t.Setenv("SYNC_DB_PATH", "")

	db, err := OpenSQLiteDB()
	if err != nil {
		t.Fatalf("OpenSQLiteDB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := os.Stat(filepath.Join(dir, "sync.db")); err != nil {
		t.Errorf("the fallback file was not created: %v", err)
	}
}

// TestAnUncreatableDirectoryIsReportedImmediately records that a path whose
// parent is a regular file fails on the directory creation, before any of the
// five connection attempts. The retry loop is not entered, so the call returns
// at once rather than after five seconds.
func TestAnUncreatableDirectoryIsReportedImmediately(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatalf("write blocker: %v", err)
	}
	t.Setenv("SYNC_DB_PATH", filepath.Join(blocker, "sub", "sync.db"))

	db, err := OpenSQLiteDB()
	if err == nil {
		_ = db.Close()
		t.Fatal("OpenSQLiteDB succeeded for a path under a regular file")
	}
	if got := err.Error(); !contains(got, "failed to create database directory") {
		t.Errorf("error = %q, want the directory failure", got)
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
