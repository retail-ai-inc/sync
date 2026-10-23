package app

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
	"github.com/retail-ai-inc/sync/internal/platform/sqlite/sqlitetest"

	_ "github.com/mattn/go-sqlite3"
	"github.com/retail-ai-inc/sync/internal/identity/infra"
)

// useTempDB points the package at a throwaway SQLite file carrying the same
// schema as sync.db, so the user and auth-config helpers can be exercised
// without touching the database tracked in this repository.
func useTempDB(t *testing.T) *sql.DB {
	t.Helper()

	isolateCrontab(t)
	cheapPasswordHashing(t)

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

func insertUser(t *testing.T, db *sql.DB, username, password, name, access string) {
	t.Helper()

	if _, err := db.Exec(
		`INSERT INTO users (username, password, name, access) VALUES (?, ?, ?, ?)`,
		username, password, name, access); err != nil {
		t.Fatalf("insert user %q: %v", username, err)
	}
}

// isolateCrontab empties PATH so the crontab command cannot be found.
func isolateCrontab(t *testing.T) {
	t.Helper()
	t.Setenv("PATH", t.TempDir())
}

// emptyIdentityDB points SYNC_DB_PATH at a file with no tables at all, so a
// store call fails on the query rather than on the connection.
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

// infraValidateUser and infraUpdateUserPassword reach the store directly, so a
// test can check what a use case left behind without going through it again.
func infraValidateUser(username, password string) (bool, string, error) {
	return infra.ValidateUser(username, password)
}

func infraUpdateUserPassword(username, password string) error {
	return infra.UpdateUserPassword(username, password)
}

// currentDB opens the database SYNC_DB_PATH currently names, so a helper can
// seed a row into the file a fixture already created.
func currentDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", os.Getenv("SYNC_DB_PATH"))
	if err != nil {
		t.Fatalf("open current sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// infraSaveGoogleUser and infraGetUserByUsername reach the store directly, so a
// test can exercise the half of the Google flow that does not leave the machine.
func infraSaveGoogleUser(email, name string) (string, string, error) {
	return infra.SaveGoogleUser(email, name)
}

func infraGetUserByUsername(username string) (map[string]interface{}, error) {
	return infra.GetUserByUsername(username)
}

// cheapPasswordHashing drops the key derivation cost for the duration of a
// test.
func cheapPasswordHashing(t *testing.T) {
	t.Helper()
	t.Setenv("SYNC_PASSWORD_ITERATIONS", "1")
}
