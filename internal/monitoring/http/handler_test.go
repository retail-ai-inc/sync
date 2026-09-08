package monitoringhttp

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
	"github.com/retail-ai-inc/sync/internal/platform/sqlite/sqlitetest"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
)

// useMonitorDB points the package at a throwaway SQLite file carrying the
// tables the monitoring handlers read.
func useMonitorDB(t *testing.T) *sql.DB {
	t.Helper()

	isolateCrontab(t)

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

func sqlNow(offset time.Duration) string {
	return time.Now().UTC().Add(offset).Format("2006-01-02 15:04:05")
}

func TestChangeStreamsStatusHandlerAggregates(t *testing.T) {
	conn := useMonitorDB(t)
	rows := []struct {
		taskID                            int
		coll                              string
		received, executed, pending, errs int
		inserted, updated, deleted        int
	}{
		{1, "source_db.orders", 100, 90, 10, 1, 50, 30, 10},
		{1, "source_db.users", 40, 40, 0, 0, 20, 15, 5},
		{2, "source_db.audit", 10, 5, 5, 2, 5, 0, 0},
	}
	for _, r := range rows {
		if _, err := conn.Exec(`
			INSERT INTO changestream_statistics
				(task_id, collection_name, received, executed, pending, errors, inserted, updated, deleted)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			r.taskID, r.coll, r.received, r.executed, r.pending, r.errs, r.inserted, r.updated, r.deleted); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	rec := httptest.NewRecorder()
	ChangeStreamsStatusHandler(rec, httptest.NewRequest(http.MethodGet, "/changestreams/status", nil))

	resp := decodeEnvelope(t, rec)
	if resp["success"] != true {
		t.Fatalf("success = %v (body: %s)", resp["success"], rec.Body.String())
	}
	body, err := json.Marshal(resp["data"])
	if err != nil {
		t.Fatalf("marshal data: %v", err)
	}
	// The aggregate must account for every seeded row.
	for _, want := range []string{"source_db.orders", "source_db.users", "source_db.audit"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("%q is missing from the response: %s", want, body)
		}
	}
}

func TestChangeStreamsStatusHandlerOnAnEmptyTable(t *testing.T) {
	useMonitorDB(t)

	rec := httptest.NewRecorder()
	ChangeStreamsStatusHandler(rec, httptest.NewRequest(http.MethodGet, "/changestreams/status", nil))

	if resp := decodeEnvelope(t, rec); resp["success"] != true {
		t.Errorf("success = %v, want true on an empty table", resp["success"])
	}
}

func TestChangeStreamsStatusHandlerReportsAMissingTable(t *testing.T) {
	sqlitetest.Tableless(t)

	rec := httptest.NewRecorder()
	ChangeStreamsStatusHandler(rec, httptest.NewRequest(http.MethodGet, "/changestreams/status", nil))

	if resp := decodeEnvelope(t, rec); resp["success"] != false {
		t.Errorf("success = %v, want false", resp["success"])
	}
}
