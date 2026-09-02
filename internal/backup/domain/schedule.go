package domain

import (
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

// NextBackupTime reports when a job with this cron expression runs next. It
// used to ignore the expression and answer "twenty-four hours from now"
// whatever it said, so a job running every five minutes and one running
// monthly showed the same time and neither was true.
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
