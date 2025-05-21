package config

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/retail-ai-inc/sync/pkg/db"
	"github.com/sirupsen/logrus"
)

type TableMapping struct {
	SourceTable     string
	TargetTable     string
	SecurityEnabled bool
	FieldSecurity   []interface{}
	CountQuery      map[string]interface{} `json:"countQuery"`
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
	MonitorInterval               time.Duration
}

type globalConfig struct {
	EnableTableRowCountMonitoring bool
	LogLevel                      string
	MonitorInterval               time.Duration
}

type FieldSecurityItem struct {
	Field        string `json:"field"`
	SecurityType string `json:"securityType"`
}

type TableMappingJSON struct {
	SourceTable     string                 `json:"sourceTable"`
	TargetTable     string                 `json:"targetTable"`
	SecurityEnabled bool                   `json:"securityEnabled"`
	FieldSecurity   []FieldSecurityItem    `json:"fieldSecurity"`
	CountQuery      map[string]interface{} `json:"countQuery"`
}

type jsonMapping struct {
	SourceDatabase string `json:"sourceDatabase"`
	SourceSchema   string `json:"sourceSchema"`
	TargetDatabase string `json:"targetDatabase"`
	TargetSchema   string `json:"targetSchema"`
	Tables         []struct {
		SourceTable   string                 `json:"sourceTable"`
		TargetTable   string                 `json:"targetTable"`
		CountQuery    map[string]interface{} `json:"countQuery"`
		FieldSecurity []struct {
			Field        string `json:"field"`
			SecurityType string `json:"securityType"`
		} `json:"fieldSecurity"`
	} `json:"tables"`
}

func NewConfig() *Config {
	db, err := db.OpenSQLiteDB()
	if err != nil {
		log.Fatalf("Failed opening DB: %v", err)
	}

	gcfg := loadGlobalConfig(db)
	syncCfgs := loadSyncTasks(db)

	_ = db.Close()
	return &Config{
		EnableTableRowCountMonitoring: gcfg.EnableTableRowCountMonitoring,
		LogLevel:                      gcfg.LogLevel,
		SyncConfigs:                   syncCfgs,
		Logger:                        logrus.New(),
		MonitorInterval:               gcfg.MonitorInterval,
	}
}

func loadGlobalConfig(db *sql.DB) globalConfig {
	var em int
	var ll string
	var mi int
	err := db.QueryRow(`
SELECT enable_table_row_count_monitoring, log_level, monitor_interval
FROM config_global
WHERE id=1
`).Scan(&em, &ll, &mi)
	if err != nil {
		log.Fatalf("Failed to load config_global: %v", err)
	}
	return globalConfig{
		EnableTableRowCountMonitoring: (em != 0),
		LogLevel:                      ll,
		MonitorInterval:               time.Duration(mi) * time.Second,
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
				SecurityEnabled        *bool             `json:"securityEnabled"`
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

				securityEnabled := false
				if extra.SecurityEnabled != nil && *extra.SecurityEnabled {
					securityEnabled = true
				}

				for i := range sc.Mappings {
					for j := range sc.Mappings[i].Tables {
						sc.Mappings[i].Tables[j].SecurityEnabled = securityEnabled

						var rootData map[string]interface{}
						if err := json.Unmarshal([]byte(js), &rootData); err == nil {
							if mappings, ok := rootData["mappings"].([]interface{}); ok && i < len(mappings) {
								if mapping, ok := mappings[i].(map[string]interface{}); ok {
									if tables, ok := mapping["tables"].([]interface{}); ok && j < len(tables) {
										if table, ok := tables[j].(map[string]interface{}); ok {
											if fieldSecurity, ok := table["fieldSecurity"].([]interface{}); ok {
												sc.Mappings[i].Tables[j].FieldSecurity = fieldSecurity
											}

											if countQuery, ok := table["countQuery"].(map[string]interface{}); ok {
												sc.Mappings[i].Tables[j].CountQuery = countQuery
											}
										}
									}
								}
							}
						}
					}
				}

				sc.SourceConnection = buildDSNByType(sc.Type, extra.SourceConn)
				sc.TargetConnection = buildDSNByType(sc.Type, extra.TargetConn)

				if len(sc.Mappings) == 0 {
					sc.Mappings = []DatabaseMapping{
						{
							SourceDatabase: "",
							TargetDatabase: "",
							Tables:         []TableMapping{},
						},
					}
				}
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
		// MongoDB => mongodb://username:password@host:port/database
		host := c["host"]
		port := c["port"]
		dbn := c["database"]
		user := c["user"]
		pass := c["password"]

		var uri string
		if user != "" && pass != "" {
			uri = fmt.Sprintf("mongodb://%s:%s@%s:%s/%s", user, pass, host, port, dbn)
		} else {
			uri = fmt.Sprintf("mongodb://%s:%s/%s", host, port, dbn)
		}

		if strings.Contains(uri, "@") && !strings.Contains(uri, "authSource=") {
			uri += "?directConnection=true&authSource=admin"
		} else {
			uri += "?directConnection=true"
		}

		return uri

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

func BuildDSNByType(dbType string, c map[string]string) string {
	return buildDSNByType(dbType, c)
}
