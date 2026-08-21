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

// ReopenSQLConnection routes every failure through Retry(5, 2s, 2.0) with no
// error classification, so a permanent misconfiguration — an unregistered
// driver, a malformed DSN — is retried five times over 62 seconds (2+4+8+16+32)
// before the error surfaces. This test only proves the classification is
// absent; the timing is covered by TestRetrySleepsAfterTheFinalFailure.
func TestReopenSQLConnectionDoesNotClassifyErrors(t *testing.T) {
	attempts := 0
	err := Retry(2, time.Millisecond, 1.0, func() error {
		attempts++
		_, openErr := sql.Open("no-such-driver", "whatever")
		return openErr
	})

	if err == nil {
		t.Fatal("an unregistered driver did not produce an error")
	}
	if attempts != 2 {
		t.Fatalf("a permanent error was attempted %d times, want 2 — Retry appears to classify errors now", attempts)
	}
}
