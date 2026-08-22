package sqlite

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// TestAnOlderDatabaseGainsTheAddedColumns records that a control database
// created before a column existed picks it up. CREATE TABLE IF NOT EXISTS does
// nothing to a table that is already there, so without the ALTER an upgraded
// binary would query a column the file does not have and every backup listing
// would fail.
func TestAnOlderDatabaseGainsTheAddedColumns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")
	t.Setenv("SYNC_DB_PATH", path)

	// The backup_tasks table as an earlier version wrote it.
	older, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := older.Exec(`
CREATE TABLE backup_tasks (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    enable           INTEGER NOT NULL DEFAULT 1,
    last_update_time DATETIME,
    last_backup_time DATETIME,
    next_backup_time DATETIME,
    config_json      TEXT NOT NULL
);
INSERT INTO backup_tasks (enable, config_json) VALUES (1, '{"name":"nightly"}');`); err != nil {
		t.Fatalf("create the older schema: %v", err)
	}
	older.Close()

	db, err := OpenSQLiteDB()
	if err != nil {
		t.Fatalf("OpenSQLiteDB: %v", err)
	}
	defer db.Close()

	for _, column := range []string{"last_run_time", "last_run_status", "last_run_message"} {
		present, err := hasColumn(db, "backup_tasks", column)
		if err != nil {
			t.Fatalf("read the columns: %v", err)
		}
		if !present {
			t.Errorf("backup_tasks has no %s after an upgrade", column)
		}
	}

	// The row that was already there is still there: adding a column must not
	// cost an operator their jobs.
	var jobs int
	if err := db.QueryRow(`SELECT COUNT(*) FROM backup_tasks`).Scan(&jobs); err != nil {
		t.Fatalf("count: %v", err)
	}
	if jobs != 1 {
		t.Errorf("got %d jobs after the upgrade, want the one that was there", jobs)
	}
}

// TestTheSettingsRowIsNotOverwritten records that an operator's global settings
// survive a restart. The row is part of the schema — the loader reads id = 1 and
// a database without it will not start — so it is seeded, and seeding it on
// every open would put the defaults back over whatever was configured.
func TestTheSettingsRowIsNotOverwritten(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")
	t.Setenv("SYNC_DB_PATH", path)

	db, err := OpenSQLiteDB()
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if _, err := db.Exec(`UPDATE config_global SET log_level='debug' WHERE id=1`); err != nil {
		t.Fatalf("change the settings: %v", err)
	}
	// Dropped so the second open has something to put back, which is what shows
	// the schema was applied again rather than skipped.
	if _, err := db.Exec(`DROP TABLE sync_tasks`); err != nil {
		t.Fatalf("drop sync_tasks: %v", err)
	}
	db.Close()

	// A different process, so the once-per-path guard is not what is being
	// relied on here.
	ensured.Delete(path)

	db, err = OpenSQLiteDB()
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	defer db.Close()

	// The schema really was applied a second time — otherwise the settings row
	// could not have been overwritten either way and this would prove nothing.
	if _, err := db.Exec(`SELECT 1 FROM sync_tasks LIMIT 1`); err != nil {
		t.Fatalf("the schema was not reapplied: %v", err)
	}

	var level string
	if err := db.QueryRow(`SELECT log_level FROM config_global WHERE id=1`).Scan(&level); err != nil {
		t.Fatalf("read the settings: %v", err)
	}
	if level != "debug" {
		t.Errorf("log_level = %q, want the configured value kept", level)
	}
}
