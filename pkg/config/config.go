package config

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
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
	ID     int  // from sync_tasks.id
	Enable bool // from sync_tasks.enable

	LastUpdateTime string // from sync_tasks.last_update_time
	LastRunTime    string // from sync_tasks.last_run_time

	Type                   string
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
	Status                 string
	TaskName               string
}

func (s *SyncConfig) PGReplicationSlot() string {
	return s.PGReplicationSlotName
}
func (s *SyncConfig) PGPlugin() string {
	return s.PGPluginName
}

type Config struct {
	EnableTableRowCountMonitoring bool
	LogLevel                      string
	SyncConfigs                   []SyncConfig
	Logger                        *logrus.Logger
}

type globalConfig struct {
	EnableTableRowCountMonitoring bool
	LogLevel                      string
}

func NewConfig() *Config {
	dbPath := os.Getenv("SYNC_DB_PATH")
	if dbPath == "" {
		dbPath = "sync.db"
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Fatalf("Failed opening DB: %v", err)
	}
	if err := db.Ping(); err != nil {
		log.Fatalf("Failed pinging DB: %v", err)
	}

	EnsureSchema(db)

	gcfg := loadGlobalConfig(db)
	syncCfgs := loadSyncTasks(db)

	_ = db.Close()
	return &Config{
		EnableTableRowCountMonitoring: gcfg.EnableTableRowCountMonitoring,
		LogLevel:                      gcfg.LogLevel,
		SyncConfigs:                   syncCfgs,
		Logger:                        logrus.New(),
	}
}

func loadGlobalConfig(db *sql.DB) globalConfig {
	var em int
	var ll string
	err := db.QueryRow(`
SELECT enable_table_row_count_monitoring, log_level
FROM config_global
WHERE id=1
`).Scan(&em, &ll)
	if err != nil {
		log.Fatalf("Failed to load config_global: %v", err)
	}
	return globalConfig{
		EnableTableRowCountMonitoring: (em != 0),
		LogLevel:                      ll,
	}
}

func loadSyncTasks(db *sql.DB) []SyncConfig {
	rows, err := db.Query(`
SELECT
  id,
  enable,
  COALESCE(last_update_time,''),
  COALESCE(last_run_time,''),
  config_json
FROM sync_tasks
ORDER BY id ASC
`)
	if err != nil {
		log.Fatalf("Failed to query sync_tasks: %v", err)
	}
	defer rows.Close()

	var results []SyncConfig
	for rows.Next() {
		var (
			id        int
			enableInt int
			upTime    string
			runTime   string
			js        string
		)
		if err2 := rows.Scan(&id, &enableInt, &upTime, &runTime, &js); err2 != nil {
			log.Fatalf("scan row fail: %v", err2)
		}

		sc := SyncConfig{
			ID:             id,
			Enable:         (enableInt != 0),
			LastUpdateTime: upTime,
			LastRunTime:    runTime,
		}

		if js != "" {
			var extra struct {
				Type                   string            `json:"type"`
				TaskName               string            `json:"taskName"`
				Status                 string            `json:"status"`
				SourceConn             map[string]string `json:"sourceConn"`
				TargetConn             map[string]string `json:"targetConn"`
				Mappings               []DatabaseMapping `json:"mappings"`
				DumpExecutionPath      *string           `json:"dump_execution_path"`
				MySQLPositionPath      *string           `json:"mysql_position_path"`
				MongoDBResumeTokenPath *string           `json:"mongodb_resume_token_path"`
				PGReplicationSlot      *string           `json:"pg_replication_slot"`
				PGPlugin               *string           `json:"pg_plugin"`
				PGPositionPath         *string           `json:"pg_position_path"`
				PGPublicationNames     *string           `json:"pg_publication_names"`
				RedisPositionPath      *string           `json:"redis_position_path"`
			}
			if errJ := json.Unmarshal([]byte(js), &extra); errJ != nil {
				log.Printf("[WARN] parse config_json for id=%d => %v", id, errJ)
			} else {
				sc.Type = extra.Type
				sc.TaskName = extra.TaskName
				sc.Status = extra.Status

				if extra.DumpExecutionPath != nil {
					sc.DumpExecutionPath = *extra.DumpExecutionPath
				}
				if extra.MySQLPositionPath != nil {
					sc.MySQLPositionPath = *extra.MySQLPositionPath
				}
				if extra.MongoDBResumeTokenPath != nil {
					sc.MongoDBResumeTokenPath = *extra.MongoDBResumeTokenPath
				}
				if extra.PGReplicationSlot != nil {
					sc.PGReplicationSlotName = *extra.PGReplicationSlot
				}
				if extra.PGPlugin != nil {
					sc.PGPluginName = *extra.PGPlugin
				}
				if extra.PGPositionPath != nil {
					sc.PGPositionPath = *extra.PGPositionPath
				}
				if extra.PGPublicationNames != nil {
					sc.PGPublicationNames = *extra.PGPublicationNames
				}
				if extra.RedisPositionPath != nil {
					sc.RedisPositionPath = *extra.RedisPositionPath
				}
				sc.Mappings = extra.Mappings

				sc.SourceConnection = buildDSNByType(sc.Type, extra.SourceConn)
				sc.TargetConnection = buildDSNByType(sc.Type, extra.TargetConn)
			}
		}

		results = append(results, sc)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("rows iteration fail: %v", err)
	}
	return results
}

func buildDSNByType(dbType string, c map[string]string) string {
	if c == nil {
		return ""
	}
	switch strings.ToLower(dbType) {
	case "mysql", "mariadb":
		// MySQL => user:password@tcp(host:port)/database
		// e.g. root:root@tcp(localhost:3306)/source_db
		user := c["user"]
		pwd := c["password"]
		host := c["host"]
		port := c["port"]
		dbn := c["database"]
		return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, pwd, host, port, dbn)

	case "postgresql":
		// PostgreSQL => postgres://user:pwd@host:port/db?sslmode=disable
		// e.g. "postgres://root:root@localhost:5432/source_db?sslmode=disable"
		user := c["user"]
		pwd := c["password"]
		host := c["host"]
		port := c["port"]
		dbn := c["database"]
		// ?sslmode=disable
		return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, pwd, host, port, dbn)

	case "mongodb":
		// MongoDB => mongodb://host:port/database
		// e.g. "mongodb://localhost:27017/source_db"
		host := c["host"]
		port := c["port"]
		dbn := c["database"]
		return fmt.Sprintf("mongodb://%s:%s/%s?directConnection=true", host, port, dbn)

	case "redis":
		// Redis => redis://:pwd@host:port/db
		// e.g. "redis://:mypwd@localhost:6379/0"
		host := c["host"]
		port := c["port"]
		// user := c["user"]     // redis user rarely used
		pwd := c["password"] // e.g. :mypwd
		dbn := c["database"] // e.g. 0 or 1
		// user part in redis might not always exist, typically we do "redis://:pwd@host:port/db"
		// to keep it simple:
		if pwd != "" {
			return fmt.Sprintf("redis://:%s@%s:%s/%s", pwd, host, port, dbn)
		} else {
			return fmt.Sprintf("redis://%s:%s/%s", host, port, dbn)
		}

	default:
		// fallback => maybe user gave direct DSN
		return c["host"] // or something
	}
}
