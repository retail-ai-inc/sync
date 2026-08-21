package backup

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/retail-ai-inc/sync/internal/platform/timex"
	"github.com/sirupsen/logrus"
)

// BackupExecutor Backup executor
type BackupExecutor struct {
	db *sql.DB
}

// ExecutorBackupConfig Configuration structure for backup operations
type ExecutorBackupConfig struct {
	Name       string `json:"name"`
	SourceType string `json:"sourceType"`
	Database   struct {
		URL      string              `json:"url"`
		Username string              `json:"username"`
		Password string              `json:"password"`
		Database string              `json:"database"`
		Tables   []string            `json:"tables"`
		Fields   map[string][]string `json:"fields"`
	} `json:"database"`
	Destination struct {
		GCSPath         string `json:"gcsPath"`
		Retention       int    `json:"retention"`
		ServiceAccount  string `json:"serviceAccount"`
		FileNamePattern string `json:"fileNamePattern"`
	} `json:"destination"`
	Format             string                            `json:"format"`
	BackupType         string                            `json:"backupType"`
	Query              map[string]map[string]interface{} `json:"query"`
	CompressionType    string                            `json:"compressionType"`
	TableSelectionMode string                            `json:"tableSelectionMode"`
	RegexPattern       string                            `json:"regexPattern"`
}

// NewBackupExecutor Create a new backup executor
func NewBackupExecutor(db *sql.DB) *BackupExecutor {
	return &BackupExecutor{db: db}
}

// Execute Execute backup task
func (e *BackupExecutor) Execute(ctx context.Context, taskID int) error {
	// Query backup task
	task, err := e.getBackupTask(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get backup task: %w", err)
	}

	// Parse configuration
	var config ExecutorBackupConfig

	if err := json.Unmarshal([]byte(task.ConfigJSON), &config); err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	logrus.Debugf("[BackupExecutor] Starting backup for task %d (%s, type: %s)",
		taskID, config.Name, config.SourceType)

	// Expand regex patterns and group tables for merging
	tableGroups, err := e.ExpandAndGroupTables(ctx, &config)
	if err != nil {
		return fmt.Errorf("failed to expand table patterns: %w", err)
	}

	// Process each table group separately
	for groupName, tables := range tableGroups {
		logrus.Debugf("[BackupExecutor] Processing table group: %s (%d tables)", groupName, len(tables))

		// Create temporary directory for this table group
		tempDir, err := os.MkdirTemp("", fmt.Sprintf("backup_%d_%s_", taskID, groupName))
		if err != nil {
			logrus.Errorf("[BackupExecutor] Failed to create temp directory for table group %s: %v", groupName, err)
			continue
		}

		// 🚀 Use external command mode directly for backup
		var exportErr error
		switch config.SourceType {
		case "mongodb":
			if len(tables) == 1 {
				// Single table export: use external command mode directly
				logrus.Infof("[BackupExecutor] 🚀 Starting external command backup for single table: %s", tables[0])
				connStr := buildMongoDBConnectionString(config.Database.URL, config.Database.Username, config.Database.Password)
				exportErr = e.executeExternalMongoExportSimple(ctx, connStr, config.Database.Database, tables[0], tempDir, config)
			} else {
				// Multi-table merged export: use external command mode
				logrus.Infof("[BackupExecutor] 🚀 Starting external command backup for %d merged tables: %v", len(tables), tables)
				connStr := buildMongoDBConnectionString(config.Database.URL, config.Database.Username, config.Database.Password)
				exportErr = e.exportMongoDBMergedTables(ctx, connStr, config.Database.Database, tables, tempDir, config)
			}
		case "mysql":
			if len(tables) == 1 {
				// Single table export: use external command mode directly
				logrus.Infof("[BackupExecutor] 🚀 Starting external MySQL backup for single table: %s", tables[0])
				exportErr = e.executeExternalMySQLBackupSimple(ctx, config.Database.URL, config.Database.Database, tables[0], tempDir, config)
			} else {
				// Multi-table merged export: use external command mode
				logrus.Infof("[BackupExecutor] 🚀 Starting external MySQL backup for %d merged tables: %v", len(tables), tables)
				exportErr = e.exportMySQLMergedTables(ctx, config.Database.URL, config.Database.Database, tables, tempDir, config)
			}
		default:
			exportErr = fmt.Errorf("unsupported database type: %s", config.SourceType)
		}

		if exportErr != nil {
			logrus.Errorf("[BackupExecutor] External command backup failed for table group %s: %v", groupName, exportErr)
			os.RemoveAll(tempDir) // Clean up failed export
			continue
		}

		// 🎉 External command mode has completed the full backup workflow (export + compression + upload)
		logrus.Infof("[BackupExecutor] ✅ External command backup completed successfully for table group: %s", groupName)

		// Clean up temporary directory
		if err := os.RemoveAll(tempDir); err != nil {
			logrus.Warnf("[BackupExecutor] Failed to remove temp directory %s: %v", tempDir, err)
		} else {
			logrus.Debugf("[BackupExecutor] 🗑️  Cleaned up temp directory: %s", tempDir)
		}
	}

	logrus.Debugf("[BackupExecutor] All table backups completed for task %d", taskID)
	return nil
}

// getBackupTask Get backup task information
func (e *BackupExecutor) getBackupTask(ctx context.Context, taskID int) (BackupTask, error) {
	var task BackupTask
	var lastUpdateTime, lastBackupTime, nextBackupTime sql.NullString

	query := `SELECT id, enable, last_update_time, last_backup_time, next_backup_time, config_json 
			 FROM backup_tasks WHERE id = ?`

	err := e.db.QueryRowContext(ctx, query, taskID).Scan(
		&task.ID, &task.Enable, &lastUpdateTime, &lastBackupTime, &nextBackupTime, &task.ConfigJSON)

	if err != nil {
		return task, fmt.Errorf("failed to query backup task: %w", err)
	}

	// Parse timestamps
	if lastUpdateTime.Valid {
		t, _ := timex.ParseDatabaseTimestamp(lastUpdateTime.String)
		task.LastUpdateTime = t
	}
	if lastBackupTime.Valid {
		t, _ := timex.ParseDatabaseTimestamp(lastBackupTime.String)
		task.LastBackupTime = t
	}
	if nextBackupTime.Valid {
		t, _ := timex.ParseDatabaseTimestamp(nextBackupTime.String)
		task.NextBackupTime = t
	}

	return task, nil
}
