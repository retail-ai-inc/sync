package domain

import (
	"fmt"
	"time"
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
//
// An expression that names no run time answers "" with the reason, rather than
// logging it here: what to do about a job whose schedule cannot be read is the
// caller's to decide, and the rule this package keeps is only what the next
// time is.
func NextBackupTime(cronExpr string) (string, error) {
	schedule, err := ParseSchedule(cronExpr)
	if err != nil {
		return "", fmt.Errorf("%q is not a cron expression: %w", cronExpr, err)
	}

	next := schedule.Next(time.Now().UTC())
	if next.IsZero() {
		return "", fmt.Errorf("%q names no time in the next four years", cronExpr)
	}
	return next.Format("2006-01-02 15:04:05"), nil
}
