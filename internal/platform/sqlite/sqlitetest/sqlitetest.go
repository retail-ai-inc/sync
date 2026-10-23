// Package sqlitetest builds control databases in the states a test needs.
//
// It is a normal package rather than a _test.go file because Go cannot share
// test helpers between packages, and thirteen packages needed the same one:
// every layer of every bounded context reads the control database, and every
// one of them has to be able to say "and now the table is not there".
package sqlitetest

import (
	"testing"

	"path/filepath"

	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
)

// Tableless points SYNC_DB_PATH at a database whose tables have been removed,
// which is the state a migration that did not finish — or a file restored from
// the wrong backup — leaves behind. Naming an empty file no longer produces
// one: opening the control database creates its schema, so the tables have to
// be dropped after that has happened.
func Tableless(t *testing.T) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "empty.db")
	t.Setenv("SYNC_DB_PATH", path)

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
			rows.Close()
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
