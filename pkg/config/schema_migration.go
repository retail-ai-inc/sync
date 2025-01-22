package config

import (
	"database/sql"
	"log"
)

// EnsureSchema creates or updates any required tables if they do not exist
func EnsureSchema(db *sql.DB) {
	_, err := db.Exec(`
CREATE TABLE IF NOT EXISTS config_global(
    id INTEGER PRIMARY KEY,
    enable_table_row_count_monitoring INTEGER NOT NULL DEFAULT 0,
    log_level TEXT NOT NULL DEFAULT 'info'
);

CREATE TABLE IF NOT EXISTS sync_configs(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL,
    enable INTEGER NOT NULL DEFAULT 1,
    source_connection TEXT NOT NULL,
    target_connection TEXT NOT NULL,
    dump_execution_path TEXT,
    mysql_position_path TEXT,
    mongodb_resume_token_path TEXT,
    pg_replication_slot TEXT,
    pg_plugin TEXT,
    pg_position_path TEXT,
    pg_publication_names TEXT,
    redis_position_path TEXT
);

CREATE TABLE IF NOT EXISTS database_mappings(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sync_config_id INTEGER NOT NULL,
    source_database TEXT NOT NULL,
    source_schema TEXT,
    target_database TEXT NOT NULL,
    target_schema TEXT
);

CREATE TABLE IF NOT EXISTS table_mappings(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    database_mapping_id INTEGER NOT NULL,
    source_table TEXT NOT NULL,
    target_table TEXT NOT NULL
);

-- Used to store logs for each row count monitoring
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
    monitor_action TEXT
);

-- Ensure there is a default config_global record
INSERT INTO config_global (id, enable_table_row_count_monitoring, log_level)
SELECT 1, 0, 'info'
WHERE NOT EXISTS (SELECT 1 FROM config_global WHERE id=1);
`)
	if err != nil {
		log.Fatalf("Failed to create or update schema: %v", err)
	}
}
