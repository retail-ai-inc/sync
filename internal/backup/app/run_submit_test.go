package app

import (
	"strings"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/backup/domain"
)

// settled waits for a run to reach an outcome, so a test does not depend on how
// fast the background goroutine gets there.
func settled(t *testing.T, taskID string) domain.Run {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for {
		run, ok := snapshotRun(t, taskID)
		if !ok {
			t.Fatalf("the run %s is not registered", taskID)
		}
		if run.Status != domain.RunPending && run.Status != domain.RunRunning {
			return run
		}
		if time.Now().After(deadline) {
			t.Fatalf("the run %s is still %s", taskID, run.Status)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestStartRunActuallyRunsTheJob covers "back this up now". It used to stamp
// last_backup_time and answer "started successfully" without running anything:
// no executor was built, no command ran, nothing was written anywhere.
func TestStartRunActuallyRunsTheJob(t *testing.T) {
	db := useTempJobDB(t)
	id := insertJob(t, db, 1, `{"name":"nightly","sourceType":"mongodb"}`)
	ForgetRuns()
	t.Cleanup(ForgetRuns)

	taskID, err := StartRun(itoa(id))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if !strings.HasPrefix(taskID, "backup_") {
		t.Errorf("taskID = %q, want one to poll", taskID)
	}

	run, ok := snapshotRun(t, taskID)
	if !ok {
		t.Fatal("no run was registered, so nothing is running and nothing can be polled")
	}
	if run.BackupID != int(id) {
		t.Errorf("BackupID = %d, want %d", run.BackupID, id)
	}

	// The job names no tables, so the export refuses it — which is the point:
	// the run reaches a real outcome instead of being reported as a success that
	// never happened.
	if final := settled(t, taskID); final.Status != domain.RunFailed {
		t.Errorf("Status = %q, want the export's refusal to have been recorded", final.Status)
	}
}

func TestStartRunOnAnUnknownJob(t *testing.T) {
	useTempJobDB(t)

	if _, err := StartRun("999"); err != ErrJobNotFound {
		t.Errorf("StartRun on an unknown id = %v, want ErrJobNotFound", err)
	}
}

func TestStartRunReportsAMissingTable(t *testing.T) {
	emptyJobDB(t)

	if _, err := StartRun("1"); err == nil {
		t.Error("StartRun on a database with no tables returned no error")
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

// TestTwoSubmissionsInTheSameSecondCollide records T-114: the task id is the
// job id and the current Unix second, so two submissions of the same job
// inside one second produce the same id and the second overwrites the first's
// record.
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
func snapshotRun(t *testing.T, taskID string) (domain.Run, bool) {
	t.Helper()

	runsLock.Lock()
	defer runsLock.Unlock()
	if run, ok := runs[taskID]; ok {
		return *run, true
	}
	return domain.Run{}, false
}

// TestAFailedRunIsRecordedInTheDatabase records that the outcome outlives the
// process. It used to live only in the in-memory register, so a job that failed
// overnight and a restart in the morning left nothing anywhere saying so — the
// dashboard showed the timestamp of the last run that had worked, and "the
// backup is a few days old" and "the backup has been failing since Tuesday"
// looked the same.
func TestAFailedRunIsRecordedInTheDatabase(t *testing.T) {
	db := useTempJobDB(t)
	id := insertJob(t, db, 1, `{"name":"nightly","sourceType":"mongodb"}`)
	ForgetRuns()
	t.Cleanup(ForgetRuns)

	taskID, err := StartRun(itoa(id))
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if final := settled(t, taskID); final.Status != domain.RunFailed {
		t.Fatalf("Status = %q, want a failure to record", final.Status)
	}

	var status, message, at string
	if err := db.QueryRow(
		`SELECT COALESCE(last_run_status,''), COALESCE(last_run_message,''), COALESCE(last_run_time,'')
		 FROM backup_tasks WHERE id = ?`, id).Scan(&status, &message, &at); err != nil {
		t.Fatalf("read the recorded outcome: %v", err)
	}
	if status != domain.RunFailed {
		t.Errorf("last_run_status = %q, want %q", status, domain.RunFailed)
	}
	if message == "" {
		t.Error("last_run_message is empty, so the failure says nothing about itself")
	}
	if at == "" {
		t.Error("last_run_time is empty, so there is no telling when it failed")
	}

	// The successful-backup timestamp answers a different question and must not
	// have moved.
	var lastBackup string
	if err := db.QueryRow(
		`SELECT COALESCE(last_backup_time,'') FROM backup_tasks WHERE id = ?`, id).
		Scan(&lastBackup); err != nil {
		t.Fatalf("read last_backup_time: %v", err)
	}
	if lastBackup != "2026-08-20 18:00:00" {
		t.Errorf("last_backup_time = %q, want it untouched by a failed run", lastBackup)
	}
}
