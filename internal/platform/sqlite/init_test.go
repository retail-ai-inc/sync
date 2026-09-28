package sqlite

import (
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// The fallback used to be derived from runtime.Caller.
func TestAnUnsetPathDoesNotReachOutsideTheWorkingDirectory(t *testing.T) {
	if filepath.IsAbs(DefaultPath) {
		t.Errorf("DefaultPath = %q, want a path relative to the working directory", DefaultPath)
	}

	t.Setenv("SYNC_DB_PATH", "")
	dir := t.TempDir()
	t.Chdir(dir)

	db, err := OpenSQLiteDB()
	if err != nil {
		t.Fatalf("OpenSQLiteDB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := os.Stat(filepath.Join(dir, DefaultPath)); err != nil {
		t.Errorf("the database did not land in the working directory: %v", err)
	}
}

func TestAnExplicitPathIsUsedAsGiven(t *testing.T) {
	want := filepath.Join(t.TempDir(), "nested", "control.db")
	t.Setenv("SYNC_DB_PATH", want)

	db, err := OpenSQLiteDB()
	if err != nil {
		t.Fatalf("OpenSQLiteDB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := os.Stat(want); err != nil {
		t.Errorf("the database is not at %s: %v", want, err)
	}
}
