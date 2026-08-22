package sqlite

import (
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// TestAnUnsetPathDoesNotReachOutsideTheWorkingDirectory covers a deployment
// that forgets SYNC_DB_PATH.
//
// The fallback used to be derived from runtime.Caller — the path of the source
// file on the machine that compiled the binary. In a container built anywhere
// else that directory does not exist, so OpenSQLiteDB created it and put an
// empty database inside it, and the process came up with no sync tasks and
// nothing anywhere saying why.
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
