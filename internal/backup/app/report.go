package app

import (
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/backup/domain"
	"github.com/retail-ai-inc/sync/internal/backup/infra"
	"github.com/retail-ai-inc/sync/internal/backup/infra/export"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/platform/timex"
)

// Publishing how the backup jobs went.
//
// A backup that does not run is silent. The job is a row in the control
// database and its outcome another column of that row, which nothing looks at
// until somebody needs the backup -- by which time the answer to "when did
// this last work" is either a date or a shrug.

// backupLabels identifies one job. The label is `backup` and not `task`: a
// backup job and a replication task are numbered separately, and a dashboard
// variable filtering both would mix them.
func backupLabels(id int) metrics.Labels {
	return metrics.Labels{"backup": fmt.Sprint(id)}
}

// reportOutcome publishes a finished run.
func reportOutcome(id int, ok bool, at time.Time, took time.Duration) {
	labels := backupLabels(id)
	result := domain.RunCompleted
	if !ok {
		result = domain.RunFailed
	}
	metrics.CountBackupRun(labels, result)
	metrics.SetBackupOutcome(labels, ok, at, took)
}

// reportContents publishes what a finished run wrote out.
func reportContents(id int, wrote export.Tally) {
	metrics.SetBackupContents(backupLabels(id), wrote.Records, wrote.Bytes, wrote.Files)
}

// describeContents is what the job's stored outcome says about a run that
// finished, so the interface shows the same thing the dashboard does.
func describeContents(wrote export.Tally) string {
	if wrote.Empty() {
		return fmt.Sprintf("%d file(s), nothing in them: no record was written in the "+
			"window this job asks for", wrote.Files)
	}
	return fmt.Sprintf("%d file(s), %d record(s), %.2f MB", wrote.Files, wrote.Records,
		float64(wrote.Bytes)/1024/1024)
}

// PublishStoredOutcomes reports what the control database already knows, so a
// restart does not leave every job looking as though it has never run. The
// process holds nothing across a restart and the panel that says how long ago
// a backup last worked would otherwise read "never" until the next run.
//
// The counter of runs is deliberately not seeded: it counts what this process
// saw, and a rate over a restart is what a counter is for.
func PublishStoredOutcomes(log logrus.FieldLogger) {
	jobs, err := infra.ListJobs()
	if err != nil {
		if log != nil {
			log.Warnf("[Backup] Could not read the backup jobs to report their last "+
				"outcome: %v", err)
		}
		return
	}

	for _, job := range jobs {
		labels := backupLabels(job.ID())

		config, err := job.Config()
		if err != nil && log != nil {
			log.Warnf("[Backup] Job %d has a configuration that will not parse, so it "+
				"is reported by id alone: %v", job.ID(), err)
		}
		metrics.SetBackupInfo(labels, job.DisplayName(config), config.SourceType, config.Schedule)

		outcome := job.LastRun()
		if outcome.Status == "" {
			continue
		}
		at, err := parseStoredTime(outcome.At)
		if err != nil {
			// A run with an unreadable time still says whether it worked.
			metrics.SetBackupOutcome(labels, outcome.Status == domain.RunCompleted,
				time.Time{}, 0)
			continue
		}
		metrics.SetBackupOutcome(labels, outcome.Status == domain.RunCompleted, at, 0)
	}
}

// parseStoredTime reads a time out of the control database, in either of the
// two formats that are in there.
func parseStoredTime(stored string) (time.Time, error) {
	return timex.ParseDatabaseTimestamp(stored)
}

// StoredJobPassword resolves the password a saved backup job holds for its
// source, so a probe from its edit form can test the connection the job
// actually uses.
//
// The job list masks passwords on the way out, so the form carries
// "********". A mask means "the one already stored", which is what saving an
// untouched field does; without this, testing the connection or listing the
// tables from an edit form made somebody retype a password to see either.
func StoredJobPassword(jobID string) (string, bool) {
	configJSON, _, err := infra.ReadJobRow(jobID)
	if err != nil {
		return "", false
	}
	stored := domain.ParseStoredConfig(configJSON)
	database, _ := stored["database"].(map[string]interface{})
	password, _ := database["password"].(string)
	return password, password != "" && password != domain.RedactedPassword
}
