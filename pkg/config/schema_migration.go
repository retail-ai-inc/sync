package config

import (
	"database/sql"
	"log"
)

// EnsureSchema creates or updates our table schema
func EnsureSchema(db *sql.DB) {
	_, err := db.Exec(`
CREATE TABLE IF NOT EXISTS config_global(
    id INTEGER PRIMARY KEY,
    enable_table_row_count_monitoring INTEGER NOT NULL DEFAULT 0,
    log_level TEXT NOT NULL DEFAULT 'info'
);

CREATE TABLE IF NOT EXISTS sync_tasks(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    enable INTEGER NOT NULL DEFAULT 1,
    last_update_time DATETIME,
    last_run_time DATETIME,
    config_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS monitoring_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    logged_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    db_type TEXT NOT NULL,
    src_db TEXT,
    src_table TEXT,
    src_row_count INTEGER,
    tgt_db TEXT,
    tgt_table TEXT,
    tgt_row_count INTEGER,
    monitor_action TEXT,
    sync_task_id INTEGER 
);

CREATE TABLE IF NOT EXISTS sync_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    log_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    level TEXT,
    message TEXT,
    sync_task_id INTEGER
);

INSERT INTO config_global(id, enable_table_row_count_monitoring, log_level)
    SELECT 1, 1, 'info'
   WHERE NOT EXISTS (SELECT 1 FROM config_global WHERE id=1);
`)
	if err != nil {
		log.Fatalf("Failed to create or update schema: %v", err)
	}
}
