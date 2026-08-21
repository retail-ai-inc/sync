package identityhttp

import (
	"database/sql"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// isolateCrontab empties PATH so any handler that shells out cannot reach the
// crontab of whoever runs the suite.
func isolateCrontab(t *testing.T) {
	t.Helper()
	t.Setenv("PATH", t.TempDir())
}

// useTempDB points SYNC_DB_PATH at a throwaway SQLite file carrying the two
// tables the identity store reads.
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

// emptyIdentityDB points SYNC_DB_PATH at a file with no tables at all.
func emptyIdentityDB(t *testing.T) {
	t.Helper()
	isolateCrontab(t)
	t.Setenv("SYNC_DB_PATH", filepath.Join(t.TempDir(), "empty.db"))
}

// unopenableDB points SYNC_DB_PATH at a path whose parent is a regular file.
func unopenableDB(t *testing.T) {
	t.Helper()
	isolateCrontab(t)

	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatalf("write blocker: %v", err)
	}
	t.Setenv("SYNC_DB_PATH", filepath.Join(blocker, "sub", "sync.db"))
}

// insertUser seeds one users row and gives it a userId, which the management
// endpoints address rows by.
func insertUser(t *testing.T, db *sql.DB, username, password, name, access string) {
	t.Helper()

	if _, err := db.Exec(
		`INSERT INTO users (username, password, name, avatar, userId, email, access)
		 VALUES (?, ?, ?, '', ?, ?, ?)`,
		username, password, name, "uid-"+username, username+"@example.test", access); err != nil {
		t.Fatalf("insert user: %v", err)
	}
}

// storeOAuthConfig seeds one auth_configs row.
func storeOAuthConfig(t *testing.T, db *sql.DB, provider, cfg string, enabled bool) {
	t.Helper()

	if _, err := db.Exec(
		`INSERT INTO auth_configs (provider, config_json, enabled) VALUES (?, ?, ?)`,
		provider, cfg, enabled); err != nil {
		t.Fatalf("insert auth config: %v", err)
	}
}

// envelope decodes a JSON response body.
func envelope(t *testing.T, rec *httptest.ResponseRecorder) map[string]interface{} {
	t.Helper()

	var resp map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("body is not JSON: %v (%q)", err, rec.Body.String())
	}
	return resp
}
