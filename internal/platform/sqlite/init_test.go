package sqlite

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestInitDBPathRespectsAnExistingEnvironmentVariable(t *testing.T) {
	t.Setenv("SYNC_DB_PATH", "/somewhere/else/sync.db")

	initDBPath()

	if got := os.Getenv("SYNC_DB_PATH"); got != "/somewhere/else/sync.db" {
		t.Errorf("SYNC_DB_PATH = %q, want the pre-set value to be left alone", got)
	}
}

// When SYNC_DB_PATH is unset, the fallback is derived from runtime.Caller,
// which yields the path of this source file **on the machine that compiled
// the binary**. In a container built elsewhere that directory does not exist,
// and OpenSQLiteDB then MkdirAll's it and creates an empty database there — so
// a deployment that forgets SYNC_DB_PATH starts with no tasks rather than
// failing loudly.
func TestTheDBPathFallbackIsABuildTimeSourcePath(t *testing.T) {
	t.Setenv("SYNC_DB_PATH", "")

	initDBPath()

	got := os.Getenv("SYNC_DB_PATH")
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Skip("runtime.Caller is unavailable")
	}
	// Resolve the repository root by walking up to the directory holding go.mod
	// rather than by counting "..", so that moving this package is caught here
	// instead of silently repointing the fallback at a subdirectory.
	root := filepath.Dir(thisFile)
	for {
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(root)
		if parent == root {
			t.Skip("no go.mod above the test file")
		}
		root = parent
	}
	want := filepath.Join(root, "sync.db")

	if got != want {
		t.Fatalf("SYNC_DB_PATH = %q, want the build-time source path %q — the fallback appears to have changed; assert the new one instead", got, want)
	}
	if !filepath.IsAbs(got) {
		t.Errorf("the fallback %q is not absolute", got)
	}
}
