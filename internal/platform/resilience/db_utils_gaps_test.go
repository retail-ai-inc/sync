package resilience

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func TestCheckSQLConnection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "probe.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if err := CheckSQLConnection(t.Context(), db); err != nil {
		t.Errorf("CheckSQLConnection on an open database = %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := CheckSQLConnection(t.Context(), db); err == nil {
		t.Error("CheckSQLConnection on a closed database = nil, want an error")
	}
}

func TestReopenSQLConnection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reopen.db")

	db, err := ReopenSQLConnection(t.Context(), quietLogger(), path, "sqlite3")
	if err != nil {
		t.Fatalf("ReopenSQLConnection: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.PingContext(t.Context()); err != nil {
		t.Errorf("the returned handle does not ping: %v", err)
	}
}

// ReopenSQLConnection used to route every failure through Retry(5, 2s, 2.0)
// with no classification at all, so a misconfiguration that no attempt could
// survive — an unregistered driver, a malformed DSN — was retried five times
// over sixty-two seconds before the error surfaced. It is now reported at once.
func TestAnUnusableDriverIsReportedWithoutRetrying(t *testing.T) {
	start := time.Now()

	db, err := ReopenSQLConnection(t.Context(), quietLogger(), "whatever", "no-such-driver")

	if err == nil {
		t.Fatalf("an unregistered driver produced no error (db=%v)", db)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("took %v to report an unregistered driver; it is still being retried", elapsed)
	}
}
