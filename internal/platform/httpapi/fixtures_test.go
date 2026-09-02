package httpapi

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/retail-ai-inc/sync/internal/platform/sqlite"

	"github.com/go-chi/chi/v5"
)

// scratchDir is a temporary directory removed on a best-effort basis.
// t.TempDir fails the test when the directory is not empty at cleanup, and the
// backup run handler starts a job that outlives the request: it writes into
// the database directory after the test has returned, which turned an
// unrelated assertion into a flake.
func scratchDir(t *testing.T) string {
	t.Helper()

	dir, err := os.MkdirTemp("", "httpapi-")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// Fixtures shared by this package's handler tests.

// useTempTaskDB points the package at a throwaway SQLite file carrying the
// sync_tasks and backup_tasks schema, so the list and mutate handlers can be
// exercised without touching the database tracked in this repository.
func useTempTaskDB(t *testing.T) *sql.DB {
	t.Helper()

	isolateCrontab(t)

	path := filepath.Join(scratchDir(t), "sync.db")
	t.Setenv("SYNC_DB_PATH", path)

	// Through the real opener, which carries the whole schema and creates it only
	// when it is missing.
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		t.Fatalf("open temp sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
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

	path := filepath.Join(scratchDir(t), "sync.db")
	t.Setenv("SYNC_DB_PATH", path)

	// Through the real opener, which carries the whole schema and creates it only
	// when it is missing.
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		t.Fatalf("open temp sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// isolateCrontab empties PATH so the `crontab` command cannot be found.
func isolateCrontab(t *testing.T) {
	t.Helper()
	t.Setenv("PATH", t.TempDir())
}

// useTempDB points the package at a throwaway SQLite file carrying the same
// schema as sync.db, so the user and auth-config helpers can be exercised
// without touching the database tracked in this repository.
func useTempDB(t *testing.T) *sql.DB {
	t.Helper()

	isolateCrontab(t)

	path := filepath.Join(scratchDir(t), "sync.db")
	t.Setenv("SYNC_DB_PATH", path)

	// Through the real opener, which carries the whole schema and creates it only
	// when it is missing.
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		t.Fatalf("open temp sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func insertUser(t *testing.T, db *sql.DB, username, password, name, access string) {
	t.Helper()

	if _, err := db.Exec(
		`INSERT INTO users (username, password, name, access) VALUES (?, ?, ?, ?)`,
		username, password, name, access); err != nil {
		t.Fatalf("insert user %q: %v", username, err)
	}
}

// resetTaskStatus clears the package-global task registry so tests do not see
// each other's entries.
