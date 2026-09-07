package export

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/retail-ai-inc/sync/internal/backup/domain"
	"github.com/retail-ai-inc/sync/internal/platform/timex"
	"github.com/sirupsen/logrus"
)

type BackupExecutor struct {
	db *sql.DB
	// tally is what this run has backed up so far, read once it is over.
	tally tally
}

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

func NewBackupExecutor(db *sql.DB) *BackupExecutor {
	return &BackupExecutor{db: db}
}

func (e *BackupExecutor) Execute(ctx context.Context, taskID int) error {
	// Query backup task
	task, err := e.getBackupTask(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get backup task: %w", err)
	}

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

	// A job that selected nothing used to return nil, so an empty table list and
	// a completed backup were indistinguishable to the scheduler, which then
	// stamped last_backup_time and moved on.
	if len(tableGroups) == 0 {
		return fmt.Errorf("no table was selected for backup")
	}

	// Every group's failure is collected rather than logged and stepped over:
	// the caller used to be told the backup succeeded when mysqldump had died,
	// when the engine was one this does not support, and when the temporary
	// directory could not be created.
	var failures []error

	for groupName, tables := range tableGroups {
		logrus.Debugf("[BackupExecutor] Processing table group: %s (%d tables)", groupName, len(tables))

		tempDir, err := os.MkdirTemp("", fmt.Sprintf("backup_%d_%s_", taskID, groupName))
		if err != nil {
			failures = append(failures, fmt.Errorf("%s: create a temporary directory: %w", groupName, err))
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
		case "sqlite", "sqlite3":
			// The control database is one file, not a set of tables, so the table
			// grouping above does not apply to it. Its path arrives as the database.
			exportErr = e.executeSQLiteBackup(ctx, config.Database.Database, tempDir, config)
		default:
			exportErr = fmt.Errorf("unsupported database type: %s", config.SourceType)
		}

		if exportErr != nil {
			failures = append(failures, fmt.Errorf("%s: %w", groupName, exportErr))
			if err := os.RemoveAll(tempDir); err != nil { // Clean up failed export
				logrus.Warnf("[BackupExecutor] Failed to remove temp directory %s: %v", tempDir, err)
			}
			continue
		}

		// 🎉 External command mode has completed the full backup workflow (export + compression + upload)
		logrus.Infof("[BackupExecutor] ✅ External command backup completed successfully for table group: %s", groupName)

		if err := os.RemoveAll(tempDir); err != nil {
			logrus.Warnf("[BackupExecutor] Failed to remove temp directory %s: %v", tempDir, err)
		} else {
			logrus.Debugf("[BackupExecutor] 🗑️  Cleaned up temp directory: %s", tempDir)
		}
	}

	if len(failures) > 0 {
		return fmt.Errorf("back up task %d: %w", taskID, errors.Join(failures...))
	}

	logrus.Debugf("[BackupExecutor] All table backups completed for task %d", taskID)
	return nil
}

func parseStamp(taskID int, column string, stored sql.NullString) time.Time {
	if !stored.Valid || stored.String == "" {
		return time.Time{}
	}
	t, err := timex.ParseDatabaseTimestamp(stored.String)
	if err != nil {
		logrus.Warnf("[BackupExecutor] Task %d has %s = %q, which is not a timestamp; "+
			"it will read as never having happened: %v", taskID, column, stored.String, err)
		return time.Time{}
	}
	return t
}

func (e *BackupExecutor) getBackupTask(ctx context.Context, taskID int) (domain.BackupTask, error) {
	var task domain.BackupTask
	var lastUpdateTime, lastBackupTime, nextBackupTime sql.NullString

	query := `SELECT id, enable, last_update_time, last_backup_time, next_backup_time, config_json 
			 FROM backup_tasks WHERE id = ?`

	err := e.db.QueryRowContext(ctx, query, taskID).Scan(
		&task.ID, &task.Enable, &lastUpdateTime, &lastBackupTime, &nextBackupTime, &task.ConfigJSON)

	if err != nil {
		return task, fmt.Errorf("failed to query backup task: %w", err)
	}

	// Parse timestamps. A stored value that will not parse used to become the
	// zero time silently, which reads as "never backed up" — the one thing an
	// operator checks before a switchover.
	task.LastUpdateTime = parseStamp(taskID, "last_update_time", lastUpdateTime)
	task.LastBackupTime = parseStamp(taskID, "last_backup_time", lastBackupTime)
	task.NextBackupTime = parseStamp(taskID, "next_backup_time", nextBackupTime)

	return task, nil
}
