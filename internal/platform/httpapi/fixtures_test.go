package httpapi

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/go-chi/chi/v5"
	identityhttp "github.com/retail-ai-inc/sync/internal/identity/http"
)

// Fixtures shared by this package's handler tests. They are duplicated per
// package rather than shared through an importable helper package, because a
// non-test package holding test code compiles into every build.

// useTempTaskDB points the package at a throwaway SQLite file carrying the
// sync_tasks and backup_tasks schema, so the list and mutate handlers can be
// exercised without touching the database tracked in this repository.
func useTempTaskDB(t *testing.T) *sql.DB {
	t.Helper()

	isolateCrontab(t)

	path := filepath.Join(t.TempDir(), "sync.db")
	t.Setenv("SYNC_DB_PATH", path)

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open temp sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const schema = `
CREATE TABLE sync_tasks (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    enable           INTEGER NOT NULL DEFAULT 1,
    last_update_time DATETIME,
    last_run_time    DATETIME,
    config_json      TEXT NOT NULL
);
CREATE TABLE backup_tasks (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    enable           INTEGER NOT NULL DEFAULT 1,
    last_update_time DATETIME,
    last_backup_time DATETIME,
    next_backup_time DATETIME,
    config_json      TEXT NOT NULL
);`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
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

	path := filepath.Join(t.TempDir(), "sync.db")
	t.Setenv("SYNC_DB_PATH", path)

	conn, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open temp sqlite: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	const schema = `
CREATE TABLE sync_tasks (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    enable           INTEGER NOT NULL DEFAULT 1,
    last_update_time DATETIME,
    last_run_time    DATETIME,
    config_json      TEXT NOT NULL
);
CREATE TABLE monitoring_log (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    logged_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    db_type        TEXT NOT NULL,
    src_db         TEXT,
    src_table      TEXT,
    src_row_count  INTEGER,
    tgt_db         TEXT,
    tgt_table      TEXT,
    tgt_row_count  INTEGER,
    monitor_action TEXT,
    sync_task_id   INTEGER
);
CREATE TABLE sync_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    log_time     DATETIME DEFAULT CURRENT_TIMESTAMP,
    level        TEXT,
    message      TEXT,
    sync_task_id INTEGER
);
CREATE TABLE changestream_statistics (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id         INTEGER NOT NULL,
    collection_name VARCHAR(255) NOT NULL,
    received        INTEGER DEFAULT 0,
    executed        INTEGER DEFAULT 0,
    pending         INTEGER DEFAULT 0,
    errors          INTEGER DEFAULT 0,
    inserted        INTEGER DEFAULT 0,
    updated         INTEGER DEFAULT 0,
    deleted         INTEGER DEFAULT 0,
    last_updated    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(task_id, collection_name)
);`
	if _, err := conn.Exec(schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	return conn
}

// isolateCrontab empties PATH so the `crontab` command cannot be found. The
// backup handlers call CronManager.SyncCrontab unconditionally, which shells
// out to crontab and would otherwise rewrite the crontab of whoever runs the
// suite. With crontab unreachable the handlers log a warning and carry on,
// which is the behaviour under test.
func isolateCrontab(t *testing.T) {
	t.Helper()
	t.Setenv("PATH", t.TempDir())
}

// resetSessionGlobals clears the process-global session that identity's handlers
// read and write. Those variables are unexported, so from this package the only
// way to clear them is the logout handler, which is what it does. Unlike the
// in-package version this cannot restore the previous values on cleanup.
func resetSessionGlobals(t *testing.T) {
	t.Helper()
	rec := httptest.NewRecorder()
	identityhttp.AuthLogoutHandler(rec, httptest.NewRequest(http.MethodPost, "/logout", nil))
}

// useTempDB points the package at a throwaway SQLite file carrying the same
// schema as sync.db, so the user and auth-config helpers can be exercised
// without touching the database tracked in this repository.
func useTempDB(t *testing.T) *sql.DB {
	t.Helper()

	isolateCrontab(t)

	path := filepath.Join(t.TempDir(), "sync.db")
	t.Setenv("SYNC_DB_PATH", path)

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open temp sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const schema = `
CREATE TABLE users (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    username   TEXT NOT NULL UNIQUE,
    password   TEXT NOT NULL,
    name       TEXT NOT NULL,
    avatar     TEXT,
    userId     TEXT,
    email      TEXT,
    access     TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    status     TEXT DEFAULT 'active'
);
CREATE TABLE auth_configs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    provider    TEXT NOT NULL UNIQUE,
    config_json TEXT NOT NULL,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    enabled     BOOLEAN DEFAULT false
);`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
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
