package replicationhttp

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/retail-ai-inc/sync/internal/platform/sqlite"

	"github.com/go-chi/chi/v5"
)

// Fixtures shared by this package's handler tests.

// isolateCrontab empties PATH so the `crontab` command cannot be found.
func isolateCrontab(t *testing.T) {
	t.Helper()
	t.Setenv("PATH", t.TempDir())
}

// useTempTaskDB points the package at a throwaway SQLite file carrying the
// sync_tasks and backup_tasks schema, so the list and mutate handlers can be
// exercised without touching the database tracked in this repository.
func useTempTaskDB(t *testing.T) *sql.DB {
	t.Helper()

	isolateCrontab(t)

	path := filepath.Join(t.TempDir(), "sync.db")
	t.Setenv("SYNC_DB_PATH", path)

	// Through the real opener, which carries the whole schema and creates it
	// only when it is missing — rather than a copy kept here that can drift from
	// it, and that a background goroutine racing to the same path turns into
	// "table already exists".
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		t.Fatalf("open temp sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func insertSyncTask(t *testing.T, db *sql.DB, enable int, cfg string) {
	t.Helper()

	if _, err := db.Exec(
		`INSERT INTO sync_tasks (enable, last_update_time, last_run_time, config_json)
		 VALUES (?, '2026-08-21 00:30:00', '2026-08-21 01:00:00', ?)`, enable, cfg); err != nil {
		t.Fatalf("insert sync task: %v", err)
	}
}

func decodeEnvelope(t *testing.T, rec *httptest.ResponseRecorder) map[string]interface{} {
	t.Helper()

	var resp map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("body is not JSON: %v (%q)", err, rec.Body.String())
	}
	return resp
}

// serveWithURLParams runs a handler with chi route parameters populated, which
// is how the handlers read {id} and {taskId}.
func serveWithURLParams(rec *httptest.ResponseRecorder, req *http.Request, h http.HandlerFunc, params map[string]string) {
	rctx := chi.NewRouteContext()
	for k, v := range params {
		rctx.URLParams.Add(k, v)
	}
	h(rec, req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx)))
}

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
