package app

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/retail-ai-inc/sync/internal/platform/sqlite"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
)

// useMonitoringDB points the package at a throwaway SQLite file carrying the
// changestream_statistics schema, so the writers can be exercised without
// touching the database tracked in this repository.
func useMonitoringDB(t *testing.T) *sql.DB {
	t.Helper()

	path := filepath.Join(t.TempDir(), "sync.db")
	t.Setenv("SYNC_DB_PATH", path)

	// Through the real opener, which carries the whole schema and creates it
	// only when it is missing — rather than a copy kept here that can drift from
	// it, and that a background goroutine racing to the same path turns into
	// "table already exists".
	conn, err := sqlite.OpenSQLiteDB()
	if err != nil {
		t.Fatalf("open temp sqlite: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

type statsRow struct {
	Received, Executed, Pending, Errors int
	Inserted, Updated, Deleted          int
	LastUpdated                         string
}
