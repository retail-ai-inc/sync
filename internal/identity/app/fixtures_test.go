package app

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/retail-ai-inc/sync/internal/identity/infra"
)

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

// isolateCrontab empties PATH so the crontab command cannot be found, keeping
// any handler that shells out from rewriting the crontab of whoever runs the
// suite.
func isolateCrontab(t *testing.T) {
	t.Helper()
	t.Setenv("PATH", t.TempDir())
}

// emptyIdentityDB points SYNC_DB_PATH at a file with no tables at all, so a
// store call fails on the query rather than on the connection.
func emptyIdentityDB(t *testing.T) {
	t.Helper()
	isolateCrontab(t)
	t.Setenv("SYNC_DB_PATH", filepath.Join(t.TempDir(), "empty.db"))
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
