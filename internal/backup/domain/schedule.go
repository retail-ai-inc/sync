package domain

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

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

func GenerateCrontabEntries(tasks []BackupTask, apiServer string) []string {
	var entries []string

	// Add comment marking the beginning
	entries = append(entries, "# BEGIN SYNC BACKUP TASKS - DO NOT EDIT THIS SECTION")

	for _, task := range tasks {
		var config BackupConfig
		if err := json.Unmarshal([]byte(task.ConfigJSON), &config); err != nil {
			// The job's schedule is lost and nothing downstream can tell: no
			// entry is written, no alert is raised, and the API still shows it as
			// enabled. An error is the least this can do.
			logrus.Errorf("[CronManager] Task %d has a configuration that will not "+
				"parse, so it has NO SCHEDULED BACKUP: %v", task.ID, err)
			continue
		}

		// A schedule that is not a cron expression used to be written out
		// anyway, producing a line that begins with the curl command rather than
		// with five time fields — and crontab refuses the whole file when any
		// line is malformed, so one misconfigured job removed every backup
		// schedule on the machine.
		if _, err := ParseSchedule(config.Schedule); err != nil {
			logrus.Errorf("[CronManager] Task %d has the schedule %q, which is not a "+
				"cron expression, so it has NO SCHEDULED BACKUP: %v",
				task.ID, config.Schedule, err)
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
// It used to ignore the expression and answer "twenty-four hours from now"
// whatever it said, so a job running every five minutes and one running monthly
// showed the same time and neither was true. An expression that cannot be read
// answers the empty string: the column is then empty rather than carrying a
// confident wrong answer, and GenerateCrontabEntries reports the same job.
func NextBackupTime(cronExpr string) string {
	schedule, err := ParseSchedule(cronExpr)
	if err != nil {
		logrus.Warnf("[Backup] %q is not a cron expression, so there is no next run "+
			"time to show: %v", cronExpr, err)
		return ""
	}

	next := schedule.Next(time.Now().UTC())
	if next.IsZero() {
		logrus.Warnf("[Backup] %q names no time in the next four years", cronExpr)
		return ""
	}
	return next.Format("2006-01-02 15:04:05")
}
