package app

import (
	"strings"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/backup/domain"
)

func TestMarkRunStampsAnExistingJob(t *testing.T) {
	db := useTempJobDB(t)
	id := insertJob(t, db, 1, `{"name":"nightly"}`)

	if err := MarkRun(itoa(id)); err != nil {
		t.Fatalf("MarkRun: %v", err)
	}

	var lastBackup time.Time
	if err := db.QueryRow(`SELECT last_backup_time FROM backup_tasks WHERE id=?`, id).
		Scan(&lastBackup); err != nil {
		t.Fatalf("read last_backup_time: %v", err)
	}
	if time.Since(lastBackup) > time.Minute {
		t.Errorf("last_backup_time = %v, want roughly now", lastBackup)
	}
}

// TestMarkRunRunsNothing records the second of the two ways to "run a backup":
// this one stamps the timestamp and returns. No executor is built, no export
// happens, and no run is registered — yet the endpoint that calls it answers
// "Backup job started successfully".
func TestMarkRunRunsNothing(t *testing.T) {
	db := useTempJobDB(t)
	id := insertJob(t, db, 1, `{"name":"nightly","sourceType":"mongodb"}`)
	ForgetRuns()
	t.Cleanup(ForgetRuns)

	if err := MarkRun(itoa(id)); err != nil {
		t.Fatalf("MarkRun: %v", err)
	}

	if n := RunCount(); n != 0 {
		t.Fatalf("MarkRun registered %d run(s); it appears to execute now, so assert "+
			"that instead", n)
	}
}

func TestMarkRunOnAnUnknownJob(t *testing.T) {
	useTempJobDB(t)

	if err := MarkRun("999"); err != ErrJobNotFound {
		t.Errorf("MarkRun on an unknown id = %v, want ErrJobNotFound", err)
	}
}

func TestMarkRunReportsAMissingTable(t *testing.T) {
	emptyJobDB(t)

	if err := MarkRun("1"); err == nil {
		t.Error("MarkRun on a database with no tables returned no error")
	}
}

func TestSubmitRunRegistersARunImmediately(t *testing.T) {
	useTempJobDB(t)
	ForgetRuns()
	t.Cleanup(ForgetRuns)

	taskID := SubmitRun(7)

	if !strings.HasPrefix(taskID, "backup_7_") {
		t.Errorf("taskID = %q, want a backup_7_ prefix", taskID)
	}
	run, ok := snapshotRun(t, taskID)
	if !ok {
		t.Fatal("the run was not registered")
	}
	if run.BackupID != 7 {
		t.Errorf("BackupID = %d, want 7", run.BackupID)
	}
	// The registration happens before the goroutine starts, so the status is
	// either the initial one or one the background run has already reached.
	switch run.Status {
	case domain.RunPending, domain.RunRunning, domain.RunFailed, domain.RunCompleted:
	default:
		t.Errorf("Status = %q, want one of the four", run.Status)
	}
}

// TestSubmitRunReturnsBeforeTheExportFinishes records that the submission is
// asynchronous: the caller is handed a task id to poll and the export runs in a
// goroutine with a two-hour timeout.
func TestSubmitRunReturnsBeforeTheExportFinishes(t *testing.T) {
	useTempJobDB(t)
	ForgetRuns()
	t.Cleanup(ForgetRuns)

	start := time.Now()
	SubmitRun(7)

	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("SubmitRun took %v; it appears to run the export inline now", elapsed)
	}
}

// TestABackgroundRunOnAMissingJobFails records what the goroutine does with a
// job that is not there: the executor reports the missing row and the run is
// marked failed, with the message the executor produced.
func TestABackgroundRunOnAMissingJobFails(t *testing.T) {
	useTempJobDB(t)
	ForgetRuns()
	t.Cleanup(ForgetRuns)

	taskID := SubmitRun(999)

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if run, ok := snapshotRun(t, taskID); ok && domain.IsTerminal(run.Status) {
			if run.Status != domain.RunFailed {
				t.Fatalf("Status = %q for a job that does not exist, want %q",
					run.Status, domain.RunFailed)
			}
			if run.CompletedAt == nil {
				t.Error("CompletedAt was not stamped")
			}
			if run.Error == "" {
				t.Error("Error is empty for a failed run")
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the run never reached a terminal status")
}

// TestABackgroundRunFailsWhenTheDatabaseCannotBeOpened covers the other exit
// from the goroutine.
func TestABackgroundRunFailsWhenTheDatabaseCannotBeOpened(t *testing.T) {
	unopenableDB(t)
	ForgetRuns()
	t.Cleanup(ForgetRuns)

	taskID := SubmitRun(1)

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if run, ok := snapshotRun(t, taskID); ok && run.Status == domain.RunFailed {
			if run.Message != "Failed to open database" {
				t.Errorf("Message = %q, want %q", run.Message, "Failed to open database")
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the run never failed")
}

// TestTwoSubmissionsInTheSameSecondCollide records T-114: the task id is the job
// id and the current Unix second, so two submissions of the same job inside one
// second produce the same id and the second overwrites the first's record. The
// caller that submitted the first then polls a status belonging to a different
// execution.
func TestTwoSubmissionsInTheSameSecondCollide(t *testing.T) {
	useTempJobDB(t)
	ForgetRuns()
	t.Cleanup(ForgetRuns)

	first := SubmitRun(7)
	second := SubmitRun(7)

	if first != second {
		t.Skip("the two submissions straddled a second boundary")
	}
	if n := RunCount(); n != 1 {
		t.Fatalf("the register holds %d runs for two submissions with the same id, "+
			"want 1 — collisions appear to be handled now, so assert that instead", n)
	}
}

// snapshotRun copies a recorded run while holding the register's own lock.
//
// LookupRun hands back the live *domain.Run that the background goroutine keeps
// mutating, and Run has no synchronisation of its own, so reading its fields
// after LookupRun returns is a data race — one production shares (T-214). This
// helper reaches for runsLock directly, which a test in this package can do and
// an HTTP handler cannot.
func snapshotRun(t *testing.T, taskID string) (domain.Run, bool) {
	t.Helper()

	runsLock.Lock()
	defer runsLock.Unlock()
	if run, ok := runs[taskID]; ok {
		return *run, true
	}
	return domain.Run{}, false
}
