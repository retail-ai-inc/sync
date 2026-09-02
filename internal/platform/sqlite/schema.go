package sqlite

import (
	"database/sql"
	"fmt"
	"sync"
)

// The control database's schema lived in a SQLite file committed to the
// repository, and nothing in the program created it. A fresh checkout or a
// fresh deployment could not start at all unless somebody copied that file
// into place — and the committed copy carried whatever had accumulated in it,
// including an account called "admin" whose password was in the repository
// with it.

const schema = `
CREATE TABLE IF NOT EXISTS config_global (
    id                                INTEGER PRIMARY KEY,
    enable_table_row_count_monitoring INTEGER NOT NULL DEFAULT 0,
    log_level                         TEXT    NOT NULL DEFAULT 'info',
    monitor_interval                  INTEGER DEFAULT 60,
    slackWebhookURL                   TEXT,
    slackChannel                      TEXT
);

CREATE TABLE IF NOT EXISTS sync_tasks (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    enable           INTEGER NOT NULL DEFAULT 1,
    last_update_time DATETIME,
    last_run_time    DATETIME,
    config_json      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS backup_tasks (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    enable           INTEGER NOT NULL DEFAULT 1,
    last_update_time DATETIME,
    last_backup_time DATETIME,
    next_backup_time DATETIME,
    config_json      TEXT NOT NULL,
    last_run_time    DATETIME,
    last_run_status  TEXT,
    last_run_message TEXT
);

CREATE TABLE IF NOT EXISTS users (
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

CREATE TABLE IF NOT EXISTS auth_configs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    provider    TEXT NOT NULL UNIQUE,
    config_json TEXT NOT NULL,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    enabled     BOOLEAN DEFAULT false
);

CREATE TABLE IF NOT EXISTS monitoring_log (
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

CREATE TABLE IF NOT EXISTS sync_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    log_time     DATETIME DEFAULT CURRENT_TIMESTAMP,
    level        TEXT,
    message      TEXT,
    sync_task_id INTEGER
);

CREATE TABLE IF NOT EXISTS changestream_statistics (
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
);

CREATE INDEX IF NOT EXISTS idx_last_updated ON changestream_statistics(last_updated);
CREATE INDEX IF NOT EXISTS idx_monitoring_log_logged_at ON monitoring_log(logged_at);
CREATE INDEX IF NOT EXISTS idx_monitoring_log_task ON monitoring_log(sync_task_id);
`

// ensured records the database files this process has already applied the
// schema to. Once per file, not once per open.
var ensured sync.Map

// applySchema creates whatever the control database is missing, the first time
// this process opens a given file. The single row of config_global is part of
// the schema rather than data: the loader reads id = 1, and a database without
// it cannot be started.
func applySchema(db *sql.DB, path string) error {
	if _, done := ensured.Load(path); done {
		return nil
	}

	if err := createSchema(db); err != nil {
		return err
	}
	ensured.Store(path, true)
	return nil
}

func createSchema(db *sql.DB) error {
	if _, err := db.Exec(schema); err != nil {
		return fmt.Errorf("create the control database schema: %w", err)
	}

	if _, err := db.Exec(`
INSERT INTO config_global (id, enable_table_row_count_monitoring, log_level, monitor_interval)
SELECT 1, 0, 'info', 60
WHERE NOT EXISTS (SELECT 1 FROM config_global WHERE id = 1)`); err != nil {
		return fmt.Errorf("seed the global configuration: %w", err)
	}

	return addColumns(db)
}

// addedColumns are columns added to a table that already existed in databases
// created by an earlier version. CREATE TABLE IF NOT EXISTS does nothing to a
// table that is there, so a database from before these columns were introduced
// would otherwise never gain them.
var addedColumns = []struct{ table, column, definition string }{
	{"backup_tasks", "last_run_time", "DATETIME"},
	{"backup_tasks", "last_run_status", "TEXT"},
	{"backup_tasks", "last_run_message", "TEXT"},
}

// addColumns adds each of those columns if it is missing. SQLite has no ADD
// COLUMN IF NOT EXISTS, and the error for one that is already there is not
// distinguishable by a code — so the columns are read first.
func addColumns(db *sql.DB) error {
	for _, add := range addedColumns {
		present, err := hasColumn(db, add.table, add.column)
		if err != nil {
			return err
		}
		if present {
			continue
		}
		if _, err := db.Exec(fmt.Sprintf(
			`ALTER TABLE %s ADD COLUMN %s %s`, add.table, add.column, add.definition)); err != nil {
			return fmt.Errorf("add %s.%s: %w", add.table, add.column, err)
		}
	}
	return nil
}

func hasColumn(db *sql.DB, table, column string) (bool, error) {
	rows, err := db.Query(fmt.Sprintf(`PRAGMA table_info(%s)`, table))
	if err != nil {
		return false, fmt.Errorf("read the columns of %s: %w", table, err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid                 int
			name, declType      string
			notNull, primaryKey int
			defaultValue        sql.NullString
		)
		if err := rows.Scan(&cid, &name, &declType, &notNull, &defaultValue, &primaryKey); err != nil {
			return false, fmt.Errorf("read the columns of %s: %w", table, err)
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}
