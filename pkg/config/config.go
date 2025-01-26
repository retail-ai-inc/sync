package config

import (
	"database/sql"
	"log"
	"os"

	"github.com/sirupsen/logrus"
	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

type TableMapping struct {
	SourceTable string
	TargetTable string
}

type DatabaseMapping struct {
	SourceDatabase string
	SourceSchema   string
	TargetDatabase string
	TargetSchema   string
	Tables         []TableMapping
}

type SyncConfig struct {
	ID 					   int
	Type                   string
	Enable                 bool
	SourceConnection       string
	TargetConnection       string
	Mappings               []DatabaseMapping
	DumpExecutionPath      string
	MySQLPositionPath      string
	MongoDBResumeTokenPath string
	PGReplicationSlotName  string
	PGPluginName           string
	PGPositionPath         string
	PGPublicationNames     string
	RedisPositionPath      string
}

type Config struct {
	EnableTableRowCountMonitoring bool
	LogLevel                      string
	SyncConfigs                   []SyncConfig
	Logger                        *logrus.Logger
}

// NewConfig loads the configuration using SQLite
func NewConfig() *Config {
	// Preferably read the DB path from environment variables
	dbPath := os.Getenv("SYNC_DB_PATH")
	if dbPath == "" {
		dbPath = "sync.db"
	}
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Fatalf("Failed to open SQLite config DB: %v", err)
	}
	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping SQLite config DB: %v", err)
	}

	EnsureSchema(db)

	// Load global configuration
	globalCfg := loadGlobalConfig(db)

	// Load sync_configs
	syncConfigs := loadSyncConfigs(db)

	cfg := &Config{
		EnableTableRowCountMonitoring: globalCfg.EnableTableRowCountMonitoring,
		LogLevel:                      globalCfg.LogLevel,
		SyncConfigs:                   syncConfigs,
		Logger:                        logrus.New(),
	}

	_ = db.Close()
	return cfg
}

// globalConfig is used to store config_global
type globalConfig struct {
	EnableTableRowCountMonitoring bool
	LogLevel                      string
}

func loadGlobalConfig(db *sql.DB) globalConfig {
	var (
		enableMonitoring int
		logLevel         string
	)
	err := db.QueryRow(`
SELECT enable_table_row_count_monitoring, log_level
FROM config_global
WHERE id=1`).Scan(&enableMonitoring, &logLevel)
	if err != nil {
		log.Fatalf("Failed to load global config: %v", err)
	}
	return globalConfig{
		EnableTableRowCountMonitoring: (enableMonitoring != 0),
		LogLevel:                      logLevel,
	}
}

func loadSyncConfigs(db *sql.DB) []SyncConfig {
	rows, err := db.Query(`SELECT
       id,
       type,
       enable,
       source_connection,
       target_connection,
       dump_execution_path,
       mysql_position_path,
       mongodb_resume_token_path,
       pg_replication_slot,
       pg_plugin,
       pg_position_path,
       pg_publication_names,
       redis_position_path
     FROM sync_configs`)
	if err != nil {
		log.Fatalf("Failed to query sync_configs: %v", err)
	}
	defer rows.Close()

	var result []SyncConfig
	for rows.Next() {
		var (
			id                  int
			sType               string
			enableInt           int
			srcConn             string
			tgtConn             string
			dumpExecPath        sql.NullString
			mysqlPosPath        sql.NullString
			mongoResTokenPath   sql.NullString
			pgReplSlot          sql.NullString
			pgPlugin            sql.NullString
			pgPosPath           sql.NullString
			pgPubNames          sql.NullString
			redisPosPath        sql.NullString
		)
		if err2 := rows.Scan(
			&id,
			&sType,
			&enableInt,
			&srcConn,
			&tgtConn,
			&dumpExecPath,
			&mysqlPosPath,
			&mongoResTokenPath,
			&pgReplSlot,
			&pgPlugin,
			&pgPosPath,
			&pgPubNames,
			&redisPosPath,
		); err2 != nil {
			log.Fatalf("Failed scanning sync_config row: %v", err2)
		}

		newCfg := SyncConfig{
			ID:                     id,
			Type:                   sType,
			Enable:                 (enableInt != 0),
			SourceConnection:       srcConn,
			TargetConnection:       tgtConn,
			DumpExecutionPath:      dumpExecPath.String,
			MySQLPositionPath:      mysqlPosPath.String,
			MongoDBResumeTokenPath: mongoResTokenPath.String,
			PGReplicationSlotName:  pgReplSlot.String,
			PGPluginName:           pgPlugin.String,
			PGPositionPath:         pgPosPath.String,
			PGPublicationNames:     pgPubNames.String,
			RedisPositionPath:      redisPosPath.String,
		}
		newCfg.Mappings = loadDatabaseMappings(db, id)
		result = append(result, newCfg)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("sync_configs rows iteration error: %v", err)
	}
	return result
}

func loadDatabaseMappings(db *sql.DB, syncConfigID int) []DatabaseMapping {
	rows, err := db.Query(`
	  SELECT id, source_database, COALESCE(source_schema,''), target_database, COALESCE(target_schema,'')
	  FROM database_mappings
	  WHERE sync_config_id=?
	`, syncConfigID)
	if err != nil {
		log.Fatalf("Failed to query database_mappings for sync_config_id=%d: %v", syncConfigID, err)
	}
	defer rows.Close()

	var dbMaps []DatabaseMapping
	for rows.Next() {
		var (
			mID       int
			srcDB     string
			srcSchema string
			tgtDB     string
			tgtSchema string
		)
		if err2 := rows.Scan(&mID, &srcDB, &srcSchema, &tgtDB, &tgtSchema); err2 != nil {
			log.Fatalf("Failed scanning database_mappings row: %v", err2)
		}
		dbMap := DatabaseMapping{
			SourceDatabase: srcDB,
			SourceSchema:   srcSchema,
			TargetDatabase: tgtDB,
			TargetSchema:   tgtSchema,
		}
		dbMap.Tables = loadTableMappings(db, mID)
		dbMaps = append(dbMaps, dbMap)
	}
	return dbMaps
}

func loadTableMappings(db *sql.DB, dbMappingID int) []TableMapping {
	rows, err := db.Query(`
	  SELECT source_table, target_table
	  FROM table_mappings
	  WHERE database_mapping_id=?
	`, dbMappingID)
	if err != nil {
		log.Fatalf("Failed to query table_mappings for dbMappingID=%d: %v", dbMappingID, err)
	}
	defer rows.Close()

	var tblMaps []TableMapping
	for rows.Next() {
		var (
			srcTable string
			tgtTable string
		)
		if err2 := rows.Scan(&srcTable, &tgtTable); err2 != nil {
			log.Fatalf("Failed scanning table_mappings row: %v", err2)
		}
		tblMaps = append(tblMaps, TableMapping{
			SourceTable: srcTable,
			TargetTable: tgtTable,
		})
	}
	return tblMaps
}

// For PostgreSQL Syncer
func (s *SyncConfig) PGReplicationSlot() string {
	return s.PGReplicationSlotName
}

func (s *SyncConfig) PGPlugin() string {
	return s.PGPluginName
}
