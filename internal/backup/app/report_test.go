package app

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/backup/domain"
	"github.com/retail-ai-inc/sync/internal/backup/infra/export"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
)

// How a backup went, as something a dashboard can read. The outcome was a
// column of a row nobody looked at until they needed the backup.

func sampleFor(t *testing.T, name string, labels metrics.Labels) (float64, bool) {
	t.Helper()
	for _, sample := range metrics.Default.Snapshot(name) {
		if sample.Labels.Key() == labels.Key() {
			return sample.Value, true
		}
	}
	return 0, false
}

func TestAFinishedRunIsReported(t *testing.T) {
	labels := backupLabels(9101)
	t.Cleanup(func() { metrics.Default.Forget(labels) })

	at := time.Now()
	reportOutcome(9101, true, at, 42*time.Second)

	if got, ok := sampleFor(t, metrics.BackupLastStatus, labels); !ok || got != 1 {
		t.Errorf("%s = %v, want 1", metrics.BackupLastStatus, got)
	}
	if got, ok := sampleFor(t, metrics.BackupLastSuccessTimestamp, labels); !ok || got != float64(at.Unix()) {
		t.Errorf("%s = %v, want %d", metrics.BackupLastSuccessTimestamp, got, at.Unix())
	}
	if got, ok := sampleFor(t, metrics.BackupLastDurationSeconds, labels); !ok || got != 42 {
		t.Errorf("%s = %v, want 42", metrics.BackupLastDurationSeconds, got)
	}
}

// A job failing every night still has a last run. What matters is how long ago
// it last worked, so a failure leaves the last success where it was rather
// than stamping it with the failure's time.
func TestAFailedRunLeavesTheLastSuccessAlone(t *testing.T) {
	labels := backupLabels(9102)
	t.Cleanup(func() { metrics.Default.Forget(labels) })

	worked := time.Now().Add(-24 * time.Hour)
	reportOutcome(9102, true, worked, time.Minute)
	reportOutcome(9102, false, time.Now(), time.Second)

	if got, ok := sampleFor(t, metrics.BackupLastStatus, labels); !ok || got != 0 {
		t.Errorf("%s = %v, want 0 after a failure", metrics.BackupLastStatus, got)
	}
	if got, ok := sampleFor(t, metrics.BackupLastSuccessTimestamp, labels); !ok || got != float64(worked.Unix()) {
		t.Errorf("%s = %v, want the time it last worked, %d",
			metrics.BackupLastSuccessTimestamp, got, worked.Unix())
	}
	// And the run itself is the newer of the two.
	run, _ := sampleFor(t, metrics.BackupLastRunTimestamp, labels)
	success, _ := sampleFor(t, metrics.BackupLastSuccessTimestamp, labels)
	if run <= success {
		t.Errorf("the last run (%v) is not after the last success (%v)", run, success)
	}
}

func TestRunsAreCountedByResult(t *testing.T) {
	labels := backupLabels(9103)
	completed := metrics.Labels{"backup": "9103", "result": "completed"}
	failed := metrics.Labels{"backup": "9103", "result": "failed"}
	t.Cleanup(func() {
		metrics.Default.Forget(labels)
		metrics.Default.Forget(completed)
		metrics.Default.Forget(failed)
	})

	reportOutcome(9103, true, time.Now(), time.Second)
	reportOutcome(9103, false, time.Now(), time.Second)
	reportOutcome(9103, false, time.Now(), time.Second)

	if got, ok := sampleFor(t, metrics.BackupRunsTotal, completed); !ok || got != 1 {
		t.Errorf("completed runs = %v, want 1", got)
	}
	if got, ok := sampleFor(t, metrics.BackupRunsTotal, failed); !ok || got != 2 {
		t.Errorf("failed runs = %v, want 2", got)
	}
}

// The control database keeps times as UTC without a zone. Read as local they
// would be hours out, which on a panel measuring "how long since the last
// backup" is the difference between fine and alarming.
func TestAStoredTimeIsReadAsUTC(t *testing.T) {
	got, err := parseStoredTime("2026-09-07 01:30:00")
	if err != nil {
		t.Fatalf("parseStoredTime: %v", err)
	}
	if got.Location() != time.UTC {
		t.Errorf("read in %v, want UTC", got.Location())
	}
	if want := time.Date(2026, 9, 7, 1, 30, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("read %v, want %v", got, want)
	}
	if _, err := parseStoredTime(""); err == nil {
		t.Error("an empty time read as a time")
	}
}

// "Completed" alone made a backup of two hundred thousand records and a backup
// of nothing look the same. One job in staging was uploading an empty file
// every night and its outcome said the same word as every other job's.
func TestWhatARunWroteOutIsReported(t *testing.T) {
	labels := backupLabels(9104)
	t.Cleanup(func() { metrics.Default.Forget(labels) })

	reportContents(9104, export.Tally{Files: 3, Bytes: 19471297, Records: 289652})

	for name, want := range map[string]float64{
		metrics.BackupLastFiles:   3,
		metrics.BackupLastBytes:   19471297,
		metrics.BackupLastRecords: 289652,
	} {
		if got, ok := sampleFor(t, name, labels); !ok || got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
}

func TestAnEmptyRunSaysSoInItsOutcome(t *testing.T) {
	empty := describeContents(export.Tally{Files: 1})
	if !strings.Contains(empty, "nothing in them") {
		t.Errorf("an empty backup is described as %q", empty)
	}

	full := describeContents(export.Tally{Files: 2, Bytes: 2 << 20, Records: 1000})
	for _, want := range []string{"2 file(s)", "1000 record(s)", "2.00 MB"} {
		if !strings.Contains(full, want) {
			t.Errorf("the description %q does not carry %q", full, want)
		}
	}
}

// insertJobWithOutcome adds a job under a chosen id, carrying its last
// successful backup and its last run as the control database stores them.
func insertJobWithOutcome(t *testing.T, db *sql.DB, id int, lastBackup, runAt, runStatus string) {
	t.Helper()

	if _, err := db.Exec(
		`INSERT INTO backup_tasks (id, enable, last_update_time, last_backup_time, next_backup_time,
		   config_json, last_run_time, last_run_status, last_run_message)
		 VALUES (?, 1, '2026-08-21 00:00:00', NULLIF(?, ''), '', ?, ?, ?, '')`,
		id, lastBackup, `{"name":"nightly","sourceType":"mysql"}`, runAt, runStatus); err != nil {
		t.Fatalf("insert backup task %d: %v", id, err)
	}
}

// After a restart a failing job had no last success at all, so an alert on its age went silent for exactly those jobs.
func TestARestartKeepsTheLastSuccessOfAFailingJob(t *testing.T) {
	db := useTempJobDB(t)
	const failing, working, garbled, neverWorked = 9301, 9302, 9303, 9304
	for _, id := range []int{failing, working, garbled, neverWorked} {
		labels := backupLabels(id)
		metrics.Default.Forget(labels)
		t.Cleanup(func() { metrics.Default.Forget(labels) })
	}
	insertJobWithOutcome(t, db, failing, "2026-09-20 15:20:00", "2026-09-21 15:20:00", domain.RunFailed)
	insertJobWithOutcome(t, db, working, "2026-09-21 15:20:00", "2026-09-21 15:20:00", domain.RunCompleted)
	insertJobWithOutcome(t, db, garbled, "2026-09-20 15:20:00", "not a time", domain.RunFailed)
	insertJobWithOutcome(t, db, neverWorked, "", "2026-09-21 15:20:00", domain.RunFailed)

	PublishStoredOutcomes(quietBackupLogger())

	stamp := func(text string) float64 {
		at, err := time.Parse("2006-01-02 15:04:05", text)
		if err != nil {
			t.Fatalf("parse %q: %v", text, err)
		}
		return float64(at.Unix())
	}
	for _, want := range []struct {
		id     int
		metric string
		value  float64
	}{
		{failing, metrics.BackupLastStatus, 0},
		{failing, metrics.BackupLastRunTimestamp, stamp("2026-09-21 15:20:00")},
		{failing, metrics.BackupLastSuccessTimestamp, stamp("2026-09-20 15:20:00")},
		{working, metrics.BackupLastStatus, 1},
		{working, metrics.BackupLastRunTimestamp, stamp("2026-09-21 15:20:00")},
		{working, metrics.BackupLastSuccessTimestamp, stamp("2026-09-21 15:20:00")},
		{garbled, metrics.BackupLastStatus, 0},
		{garbled, metrics.BackupLastSuccessTimestamp, stamp("2026-09-20 15:20:00")},
		{neverWorked, metrics.BackupLastStatus, 0},
	} {
		if got, ok := sampleFor(t, want.metric, backupLabels(want.id)); !ok || got != want.value {
			t.Errorf("job %d: %s = %v (published %v), want %v", want.id, want.metric, got, ok, want.value)
		}
	}
	if got, ok := sampleFor(t, metrics.BackupLastSuccessTimestamp, backupLabels(neverWorked)); ok {
		t.Errorf("job %d never worked, yet %s = %v", neverWorked, metrics.BackupLastSuccessTimestamp, got)
	}
}
