package sqlite

import (
	"os"
	"path/filepath"
	"testing"
	"time"
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
// single connection, because SQLite serialises writers anyway.
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
// resolved against the working directory of whichever process opens it.
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
// five connection attempts.
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

// A control database that cannot be opened has to be reported quickly.
//
// The readiness probe opens it on every call. With a second between attempts
// this took four seconds to fail, which is longer than the probe that asked.
func TestAnUnopenableDatabaseIsReportedPromptly(t *testing.T) {
	// A directory where the file should be: openable by name, not by SQLite.
	path := filepath.Join(t.TempDir(), "sync.db")
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	t.Setenv("SYNC_DB_PATH", path)

	started := time.Now()
	db, err := OpenSQLiteDB()
	if err == nil {
		_ = db.Close()
		t.Fatal("opening a directory as a database reported success")
	}
	if taken := time.Since(started); taken > 2*time.Second {
		t.Errorf("failing took %v, which is longer than the readiness probe that "+
			"waits for it", taken)
	}
}
