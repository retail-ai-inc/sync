package identityhttp

import (
	"database/sql"
	"encoding/json"
	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
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
	cheapPasswordHashing(t)

	path := filepath.Join(t.TempDir(), "sync.db")
	t.Setenv("SYNC_DB_PATH", path)

	// Through the real opener, so the fixture carries the schema the program
	// creates rather than a copy of it that can drift. That also settles the
	// schema for this file, so a table a test renames away stays away.
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		t.Fatalf("open temp sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// emptyIdentityDB points SYNC_DB_PATH at a file with no tables at all.
func emptyIdentityDB(t *testing.T) {
	t.Helper()
	isolateCrontab(t)
	tablelessDB(t)
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

// cheapPasswordHashing drops the key derivation cost for the duration of a test.
// The production figure is deliberately expensive — most of a second per login —
// and a suite that creates and authenticates users would otherwise spend all its
// time on it. What is being tested is the flow, not the work factor; the factor
// itself is covered in internal/identity/domain.
func cheapPasswordHashing(t *testing.T) {
	t.Helper()
	t.Setenv("SYNC_PASSWORD_ITERATIONS", "1")
}

// tablelessDB points SYNC_DB_PATH at a database whose tables have been removed,
// which is the state a migration that did not finish — or a file restored from
// the wrong backup — leaves behind.
//
// Pointing at an empty file no longer produces one: opening the control database
// creates its schema, so the tables have to be dropped after that has happened.
func tablelessDB(t *testing.T) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "empty.db")
	t.Setenv("SYNC_DB_PATH", path)

	// Opening it is what creates the schema.
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		t.Fatalf("open the control database: %v", err)
	}
	defer db.Close()

	rows, err := db.Query(
		`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`)
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		names = append(names, name)
	}
	rows.Close()

	for _, name := range names {
		if _, err := db.Exec(`DROP TABLE IF EXISTS "` + name + `"`); err != nil {
			t.Fatalf("drop %s: %v", name, err)
		}
	}
}
