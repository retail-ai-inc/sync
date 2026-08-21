package app

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
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
