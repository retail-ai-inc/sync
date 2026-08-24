package config

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/internal/platform/secret"
	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
	"github.com/sirupsen/logrus"
)

type AdvancedSettings struct {
	SyncIndexes     bool   `json:"syncIndexes"`
	IgnoreDeleteOps bool   `json:"ignoreDeleteOps"`
	UploadToGcs     bool   `json:"uploadToGcs"`
	GcsAddress      string `json:"gcsAddress"`
	// Retry settings for change stream recovery
	MaxRetries     int           `json:"maxRetries"`     // Maximum number of retry attempts (default: 10)
	BaseRetryDelay time.Duration `json:"baseRetryDelay"` // Base delay between retries (default: 5s)
	MaxRetryDelay  time.Duration `json:"maxRetryDelay"`  // Maximum delay between retries (default: 5m)
}

type TableMapping struct {
	SourceTable      string                 `json:"sourceTable"`
	TargetTable      string                 `json:"targetTable"`
	SecurityEnabled  bool                   `json:"securityEnabled"`
	FieldSecurity    []interface{}          `json:"fieldSecurity"`
	CountQuery       map[string]interface{} `json:"countQuery"`
	AdvancedSettings AdvancedSettings       `json:"advancedSettings"`
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
	// RedisReconcileInterval is how often the Redis keyspace is fully compared
	// against the source.
	//
	// It is no longer the mechanism that makes the target correct — the
	// replication stream is — but the backstop for that mechanism being wrong
	// somewhere nobody thought of. It also finds keys the target has and the
	// source does not, which nothing else looks for and which after a failover
	// are records nobody can account for.
	//
	// Zero means the default; a negative value turns it off.
	RedisReconcileInterval time.Duration
	Status                 string
	TaskName               string
	// Resync names the tables or collections to re-copy alongside the stream.
	//
	// A copy found not to match used to be repairable only by clearing the
	// checkpoint, which throws away the position for everything and re-copies
	// the lot — hours during which the target is further behind, not closer.
	// Listing one object here re-copies that one while the rest keeps streaming.
	//
	// Editing it changes the task's fingerprint, which is what the supervisor
	// already watches, so the edit is the trigger. Removing the name again stops
	// the re-copy from being started on the next restart.
	Resync []string
	// RetentionWindow is how far back the source's log reaches, for a source
	// that cannot be asked: a sharded MongoDB deployment, whose local database
	// is not addressable through mongos, or a managed MySQL whose real binlog
	// retention is set outside the server. It is what the headroom metric is
	// measured against; leaving it empty on such a source publishes no headroom
	// rather than a guess.
	RetentionWindow time.Duration

	// RedisBufferDir is where a Redis task writes the replication stream on its
	// way through. A master keeps its backlog in memory and one megabyte by
	// default, so without somewhere durable to put the stream, a target that is
	// briefly unavailable costs a full resync — and a full resync of a
	// disaster-recovery target is a window with no copy at all.
	RedisBufferDir string
	// RedisBufferBytes is roughly how much of the stream to keep. Zero means the
	// default.
	RedisBufferBytes int64
	// RedisBatchWindow is how long changes are gathered before being applied.
	// It trades the recovery point against how often the source is read for a
	// key that keeps changing. Zero means the default.
	RedisBatchWindow time.Duration
	// RedisSourceReadRate caps keys read from the source per second during a
	// first copy or a repair, so neither becomes a load test against a live
	// payment database. Zero means no limit.
	RedisSourceReadRate int
}

func (s *SyncConfig) PGReplicationSlot() string {
	return s.PGReplicationSlotName
}
func (s *SyncConfig) PGPlugin() string {
	return s.PGPluginName
}

// GetSlackWebhookURL returns the Slack webhook URL from config
func (c *Config) GetSlackWebhookURL() string {
	return c.SlackWebhookURL
}

// GetSlackChannel returns the Slack channel from config
func (c *Config) GetSlackChannel() string {
	return c.SlackChannel
}

type Config struct {
	EnableTableRowCountMonitoring bool
	LogLevel                      string
	SyncConfigs                   []SyncConfig
	Logger                        *logrus.Logger
	MonitorInterval               time.Duration
	SlackWebhookURL               string
	SlackChannel                  string
}

type globalConfig struct {
	EnableTableRowCountMonitoring bool
	LogLevel                      string
	MonitorInterval               time.Duration
	SlackWebhookURL               string
	SlackChannel                  string
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

// NewConfig reads the stored configuration.
//
// Every failure used to be log.Fatalf, and the supervisor re-reads the
// configuration every ten seconds: one unreadable read — a locked SQLite file,
// a moment of disk trouble — took the whole process down, replication and
// control plane together. A disaster-recovery component cannot be that brittle,
// so the failure is reported and the caller decides.
func NewConfig() (*Config, error) {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return nil, fmt.Errorf("open the configuration database: %w", err)
	}
	defer db.Close()

	gcfg, err := loadGlobalConfig(db)
	if err != nil {
		return nil, err
	}
	syncCfgs, err := loadSyncTasks(db)
	if err != nil {
		return nil, err
	}

	return &Config{
		EnableTableRowCountMonitoring: gcfg.EnableTableRowCountMonitoring,
		LogLevel:                      gcfg.LogLevel,
		SyncConfigs:                   syncCfgs,
		Logger:                        logrus.New(),
		MonitorInterval:               gcfg.MonitorInterval,
		SlackWebhookURL:               gcfg.SlackWebhookURL,
		SlackChannel:                  gcfg.SlackChannel,
	}, nil
}

func loadGlobalConfig(db *sql.DB) (globalConfig, error) {
	var em int
	var ll string
	var mi int
	var swu string
	var sc string
	err := db.QueryRow(`
SELECT enable_table_row_count_monitoring, log_level, monitor_interval, 
       COALESCE(slackWebhookURL, ''), COALESCE(slackChannel, '')
FROM config_global
WHERE id=1
`).Scan(&em, &ll, &mi, &swu, &sc)
	if err != nil {
		return globalConfig{}, fmt.Errorf("load config_global: %w", err)
	}
	return globalConfig{
		EnableTableRowCountMonitoring: (em != 0),
		LogLevel:                      ll,
		MonitorInterval:               time.Duration(mi) * time.Second,
		SlackWebhookURL:               swu,
		SlackChannel:                  sc,
	}, nil
}

func loadSyncTasks(db *sql.DB) ([]SyncConfig, error) {
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
		return nil, fmt.Errorf("query sync_tasks: %w", err)
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
			return nil, fmt.Errorf("scan a sync_tasks row: %w", err2)
		}

		sc := SyncConfig{
			ID:             id,
			Enable:         (enableInt != 0),
			LastUpdateTime: upTime,
			LastRunTime:    runTime,
		}

		if js != "" {
			// The stored credentials are encrypted when a key is configured. A
			// task whose credentials cannot be opened is skipped rather than
			// started with a password that is really ciphertext: it would fail
			// to authenticate with an error naming neither the task nor the
			// reason.
			opened, errS := secret.OpenTaskConfig(js)
			if errS != nil {
				log.Printf("[ERROR] task %d is not being replicated: %v", id, errS)
				continue
			}
			js = opened

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
				RedisReconcileInterval *string           `json:"redis_reconcile_interval"`
				SecurityEnabled        *bool             `json:"securityEnabled"`
				Resync                 []string          `json:"resync"`
				RetentionWindow        *string           `json:"retention_window"`
				RedisBufferDir         *string           `json:"redis_buffer_dir"`
				RedisBufferBytes       *int64            `json:"redis_buffer_bytes"`
				RedisBatchWindow       *string           `json:"redis_batch_window"`
				RedisSourceReadRate    *int              `json:"redis_source_read_rate"`
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
				if extra.RedisReconcileInterval != nil {
					if d, errD := time.ParseDuration(*extra.RedisReconcileInterval); errD == nil {
						sc.RedisReconcileInterval = d
					} else {
						log.Printf("[WARN] redis_reconcile_interval for id=%d is not a duration: %v", id, errD)
					}
				}

				if extra.RetentionWindow != nil {
					if d, errD := time.ParseDuration(*extra.RetentionWindow); errD == nil {
						sc.RetentionWindow = d
					} else {
						log.Printf("[WARN] retention_window for id=%d is not a duration: %v", id, errD)
					}
				}

				if extra.RedisBufferDir != nil {
					sc.RedisBufferDir = *extra.RedisBufferDir
				}
				if extra.RedisBufferBytes != nil {
					sc.RedisBufferBytes = *extra.RedisBufferBytes
				}
				if extra.RedisBatchWindow != nil {
					if d, errD := time.ParseDuration(*extra.RedisBatchWindow); errD == nil {
						sc.RedisBatchWindow = d
					} else {
						log.Printf("[WARN] redis_batch_window for id=%d is not a duration: %v", id, errD)
					}
				}
				if extra.RedisSourceReadRate != nil {
					sc.RedisSourceReadRate = *extra.RedisSourceReadRate
				}

				sc.Mappings = extra.Mappings
				sc.Resync = extra.Resync

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

											// Parse advancedSettings
											if advancedSettings, ok := table["advancedSettings"].(map[string]interface{}); ok {
												if syncIndexes, ok := advancedSettings["syncIndexes"].(bool); ok {
													sc.Mappings[i].Tables[j].AdvancedSettings.SyncIndexes = syncIndexes
												}
												if ignoreDeleteOps, ok := advancedSettings["ignoreDeleteOps"].(bool); ok {
													sc.Mappings[i].Tables[j].AdvancedSettings.IgnoreDeleteOps = ignoreDeleteOps
												}
												if uploadToGcs, ok := advancedSettings["uploadToGcs"].(bool); ok {
													sc.Mappings[i].Tables[j].AdvancedSettings.UploadToGcs = uploadToGcs
												}
												if gcsAddress, ok := advancedSettings["gcsAddress"].(string); ok {
													sc.Mappings[i].Tables[j].AdvancedSettings.GcsAddress = gcsAddress
												}
												// Parse retry settings
												if maxRetries, ok := advancedSettings["maxRetries"].(float64); ok {
													sc.Mappings[i].Tables[j].AdvancedSettings.MaxRetries = int(maxRetries)
												}
												if baseRetryDelay, ok := advancedSettings["baseRetryDelay"].(string); ok {
													if duration, err := time.ParseDuration(baseRetryDelay); err == nil {
														sc.Mappings[i].Tables[j].AdvancedSettings.BaseRetryDelay = duration
													}
												}
												if maxRetryDelay, ok := advancedSettings["maxRetryDelay"].(string); ok {
													if duration, err := time.ParseDuration(maxRetryDelay); err == nil {
														sc.Mappings[i].Tables[j].AdvancedSettings.MaxRetryDelay = duration
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}

				sc.SourceConnection = dsn.BuildDSNByType(sc.Type, extra.SourceConn)
				sc.TargetConnection = dsn.BuildDSNByType(sc.Type, extra.TargetConn)

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
		return nil, fmt.Errorf("read sync_tasks: %w", err)
	}
	return results, nil
}
