package backup

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

// BackupTask Backup task structure
type BackupTask struct {
	ID             int
	Enable         int
	LastUpdateTime time.Time
	LastBackupTime time.Time
	NextBackupTime time.Time
	ConfigJSON     string
}

type BackupConfig struct {
	Schedule string `json:"schedule"`
	Name     string `json:"name"`
	// Other fields omitted...
}

// CronManager Manages crontab entries for backup tasks
type CronManager struct {
	db        *sql.DB
	apiServer string // API server address
}

// NewCronManager Create a new crontab manager
func NewCronManager(db *sql.DB, apiServer string) *CronManager {
	return &CronManager{
		db:        db,
		apiServer: apiServer,
	}
}

// SyncCrontab Synchronize backup tasks from SQLite to system crontab
func (cm *CronManager) SyncCrontab(ctx context.Context) error {
	logrus.Info("[CronManager] Starting to sync backup tasks to crontab")

	// Query all enabled backup tasks
	rows, err := cm.db.QueryContext(ctx, "SELECT id, enable, last_update_time, last_backup_time, next_backup_time, config_json FROM backup_tasks WHERE enable = 1")
	if err != nil {
		logrus.Errorf("[CronManager] Failed to query backup tasks: %v", err)
		return fmt.Errorf("failed to query backup tasks: %w", err)
	}
	defer rows.Close()

	var tasks []BackupTask
	for rows.Next() {
		var task BackupTask
		var lastUpdateTime, lastBackupTime, nextBackupTime sql.NullString

		err := rows.Scan(&task.ID, &task.Enable, &lastUpdateTime, &lastBackupTime, &nextBackupTime, &task.ConfigJSON)
		if err != nil {
			logrus.Errorf("[CronManager] Failed to scan backup task: %v", err)
			continue
		}

		// Parse timestamps
		if lastUpdateTime.Valid {
			t, _ := time.Parse("2006-01-02 15:04:05", lastUpdateTime.String)
			task.LastUpdateTime = t
		}
		if lastBackupTime.Valid {
			t, _ := time.Parse("2006-01-02 15:04:05", lastBackupTime.String)
			task.LastBackupTime = t
		}
		if nextBackupTime.Valid {
			t, _ := time.Parse("2006-01-02 15:04:05", nextBackupTime.String)
			task.NextBackupTime = t
		}

		tasks = append(tasks, task)
	}

	// Generate crontab entries
	crontabEntries := generateCrontabEntries(tasks, cm.apiServer)

	// Update system crontab
	if err := updateSystemCrontab(crontabEntries); err != nil {
		logrus.Errorf("[CronManager] Failed to update system crontab: %v", err)
		return fmt.Errorf("failed to update system crontab: %w", err)
	}

	logrus.Infof("[CronManager] Successfully synced %d backup tasks to crontab", len(tasks))
	return nil
}

// generateCrontabEntries Generate crontab entries for tasks
func generateCrontabEntries(tasks []BackupTask, apiServer string) []string {
	var entries []string

	// Add comment marking the beginning
	entries = append(entries, "# BEGIN SYNC BACKUP TASKS - DO NOT EDIT THIS SECTION")

	for _, task := range tasks {
		var config BackupConfig
		if err := json.Unmarshal([]byte(task.ConfigJSON), &config); err != nil {
			logrus.Errorf("[CronManager] Failed to parse config_json for task %d: %v", task.ID, err)
			continue
		}

		// Generate crontab entry
		// Ensure the command format is correct, with complete path
		entry := fmt.Sprintf("%s /usr/bin/curl -s -X POST %s/backup/execute/%d > /dev/null 2>&1",
			config.Schedule, apiServer, task.ID)

		// Add comment for identification
		comment := fmt.Sprintf("# Backup task: %s (ID: %d)", config.Name, task.ID)
		entries = append(entries, comment, entry, "")
	}

	// Add comment marking the end
	entries = append(entries, "# END SYNC BACKUP TASKS")

	return entries
}

// updateSystemCrontab Update system crontab
func updateSystemCrontab(newEntries []string) error {
	// Get current crontab
	cmd := exec.Command("crontab", "-l")
	currentCrontab, err := cmd.Output()

	// Handle the case when crontab -l fails
	// If the user has no crontab or other errors, start with an empty crontab
	if err != nil {
		logrus.Warnf("[CronManager] crontab -l failed: %v, starting with empty crontab", err)
		currentCrontab = []byte("")
	}

	// Parse current crontab
	currentEntries := strings.Split(string(currentCrontab), "\n")
	var filteredEntries []string
	inBackupSection := false

	// Remove old backup tasks section
	for _, line := range currentEntries {
		if strings.Contains(line, "BEGIN SYNC BACKUP TASKS") {
			inBackupSection = true
			continue
		}

		if strings.Contains(line, "END SYNC BACKUP TASKS") {
			inBackupSection = false
			continue
		}

		if !inBackupSection && line != "" {
			filteredEntries = append(filteredEntries, line)
		}
	}

	// Merge old crontab with new backup tasks
	allEntries := append(filteredEntries, "")
	allEntries = append(allEntries, newEntries...)

	// Write new crontab
	tempFile, err := os.CreateTemp("", "crontab")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tempFile.Name())

	if _, err := tempFile.WriteString(strings.Join(allEntries, "\n")); err != nil {
		return fmt.Errorf("failed to write to temp file: %w", err)
	}

	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	// Update system crontab
	cmd = exec.Command("crontab", tempFile.Name())
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to update crontab: %w, output: %s", err, string(output))
	}

	logrus.Infof("[CronManager] Successfully updated crontab with %d entries", len(newEntries))
	return nil
}
