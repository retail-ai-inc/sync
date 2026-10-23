package infra

import (
	"errors"
	"testing"

	"github.com/retail-ai-inc/sync/internal/backup/domain"
)

// Whether a backup had worked lived in an in-process map, so a job that failed
// overnight and a pod that restarted in the morning left the dashboard showing
// only the timestamp of the last run that succeeded — which reads as "a few
// days old" rather than "failing since Tuesday".
func TestARunOutcomeSurvivesTheProcess(t *testing.T) {
	db := useTempJobDB(t)
	id := int(insertJob(t, db, 1, `{"name":"nightly"}`))

	if err := RecordRunOutcome(id, "2026-08-22 03:00:00", domain.RunFailed,
		"mongodump: connection refused"); err != nil {
		t.Fatalf("RecordRunOutcome: %v", err)
	}

	// Read it back the way the list endpoint does, through a fresh handle.
	jobs, err := ListJobs()
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("got %d jobs, want 1", len(jobs))
	}

	outcome := jobs[0].LastRun()
	if outcome.Status != domain.RunFailed {
		t.Errorf("status = %q, want %q", outcome.Status, domain.RunFailed)
	}
	if outcome.At != "2026-08-22 03:00:00" {
		t.Errorf("at = %q, want the recorded time", outcome.At)
	}
	if outcome.Message != "mongodump: connection refused" {
		t.Errorf("message = %q, want the failure it was recorded with", outcome.Message)
	}

	// The successful-backup timestamp is a different question and must not have
	// been moved by a failure.
	if jobs[0].LastBackupTime() != "2026-08-20 18:00:00" {
		t.Errorf("lastBackupTime = %q, want it untouched by a failed run",
			jobs[0].LastBackupTime())
	}
}

// TestAnUnrunJobHasNoOutcome records that the three columns read as empty rather
// than as a status the job never had.
func TestAnUnrunJobHasNoOutcome(t *testing.T) {
	db := useTempJobDB(t)
	insertJob(t, db, 1, `{"name":"nightly"}`)

	jobs, err := ListJobs()
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if outcome := jobs[0].LastRun(); outcome != (domain.RunOutcome{}) {
		t.Errorf("outcome = %+v, want the zero value for a job that never ran", outcome)
	}
}

// TestRecordingAnOutcomeForAMissingJobIsReported records that a write against an
// id that names no row is a failure, not a silent success.
func TestRecordingAnOutcomeForAMissingJobIsReported(t *testing.T) {
	useTempJobDB(t)

	err := RecordRunOutcome(4321, "2026-08-22 03:00:00", domain.RunCompleted, "")
	if !errors.Is(err, ErrNoSuchJob) {
		t.Fatalf("err = %v, want ErrNoSuchJob", err)
	}
}
