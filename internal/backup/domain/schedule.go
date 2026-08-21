package domain

import (
	"encoding/json"
	"fmt"
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

// GenerateCrontabEntries Generate crontab entries for tasks
func GenerateCrontabEntries(tasks []BackupTask, apiServer string) []string {
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

// NextBackupTime reports when a job with this cron expression runs next.
//
// It ignores the expression entirely and always answers "24 hours from now"
// (T-011). Every schedule therefore claims a daily run, which is why the UI
// shows tomorrow for a job that is configured to run hourly. Naming the rule
// here does not fix it; the fix belongs with the aggregate work in #59.
func NextBackupTime(cronExpr string) string {
	tomorrow := time.Now().UTC().Add(24 * time.Hour)
	return tomorrow.Format("2006-01-02 15:04:05")
}
