package infra

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/retail-ai-inc/sync/internal/platform/sqlite/sqlitetest"

	_ "github.com/mattn/go-sqlite3"
)

// isolateCrontab empties PATH so the `crontab` command cannot be found. The
// backup handlers call CronManager.SyncCrontab unconditionally, which shells
// out to crontab and would otherwise rewrite the crontab of whoever runs the
// suite. With crontab unreachable the handlers log a warning and carry on,
// which is the behaviour under test.
func isolateCrontab(t *testing.T) {
	t.Helper()
	t.Setenv("PATH", t.TempDir())
}

// emptyIdentityDB points SYNC_DB_PATH at a file with no tables at all.
func emptyIdentityDB(t *testing.T) {
	t.Helper()
	isolateCrontab(t)
	sqlitetest.Tableless(t)
}

// unopenableIdentityDB points SYNC_DB_PATH at a path whose parent is a regular
// file, so opening the database fails outright.
func unopenableIdentityDB(t *testing.T) {
	t.Helper()
	isolateCrontab(t)

	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatalf("write blocker: %v", err)
	}
	t.Setenv("SYNC_DB_PATH", filepath.Join(blocker, "sub", "sync.db"))
}

// cheapPasswordHashing drops the key derivation cost for the duration of a test.
// The production figure is deliberately expensive — most of a second per login —
// and a suite that creates and authenticates users would otherwise spend all its
// time on it. What is being tested is the flow, not the work factor; the factor
// itself is covered in internal/identity/domain.
func cheapPasswordHashing(t *testing.T) {
	t.Helper()
	t.Setenv("SYNC_PASSWORD_ITERATIONS", "1")
}
