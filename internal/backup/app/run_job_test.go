package app

import (
	"errors"
	"fmt"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/retail-ai-inc/sync/internal/backup/domain"
)

// resetTaskStatus clears the package-global task registry so tests do not see
// each other's entries.
func resetTaskStatus(t *testing.T) {
	t.Helper()

	ForgetRuns()
	t.Cleanup(ForgetRuns)
}

func TestTaskStatusRoundTrip(t *testing.T) {
	resetTaskStatus(t)

	stored := &domain.Run{TaskID: "t1", BackupID: 7, Status: "pending", Message: "queued"}
	RecordRun("t1", stored)

	got, ok := LookupRun("t1")
	if !ok {
		t.Fatal("getTaskStatus reported the task as missing")
	}
	if *got != *stored {
		t.Errorf("getTaskStatus returned %#v, want %#v", got, stored)
	}
}

// The lock guards the map, not the Run behind it, and the background goroutine
// writes to that Run as the export proceeds — so the status endpoint used to
// read a struct that was being changed underneath it, which is a data race and
// shows up as a status and a message from two different moments.
func TestLookupRunHandsBackACopy(t *testing.T) {
	resetTaskStatus(t)

	stored := &domain.Run{TaskID: "t1", BackupID: 7, Status: "pending", Message: "queued"}
	RecordRun("t1", stored)

	got, _ := LookupRun("t1")
	if got == stored {
		t.Fatal("LookupRun returned the pointer the background run writes through")
	}

	AdvanceRun("t1", "completed", "done", nil)
	if got.Status != "pending" {
		t.Errorf("the copy changed underneath the caller: %q", got.Status)
	}
}

func TestGetTaskStatusMissing(t *testing.T) {
	resetTaskStatus(t)

	if _, ok := LookupRun("nope"); ok {
		t.Error("getTaskStatus reported an unknown task as present")
	}
}

func TestUpdateBackupTaskStatusRunning(t *testing.T) {
	resetTaskStatus(t)
	RecordRun("t1", &domain.Run{TaskID: "t1", Status: "pending"})

	AdvanceRun("t1", "running", "started", nil)

	got, _ := LookupRun("t1")
	if got.Status != "running" || got.Message != "started" {
		t.Errorf("status = %q, message = %q", got.Status, got.Message)
	}
	if got.Error != "" {
		t.Errorf("Error = %q, want empty", got.Error)
	}
	if got.CompletedAt != nil {
		t.Errorf("CompletedAt = %v, want nil while running", got.CompletedAt)
	}
}

func TestUpdateBackupTaskStatusTerminalStatesStampCompletedAt(t *testing.T) {
	for _, status := range []string{"completed", "failed"} {
		t.Run(status, func(t *testing.T) {
			resetTaskStatus(t)
			RecordRun("t1", &domain.Run{TaskID: "t1", Status: "running"})

			AdvanceRun("t1", status, "done", nil)

			got, _ := LookupRun("t1")
			if got.CompletedAt == nil {
				t.Fatalf("CompletedAt is nil after reaching %q", status)
			}
			if d := time.Since(*got.CompletedAt); d < 0 || d > 2*time.Second {
				t.Errorf("CompletedAt = %v, %v away from now", got.CompletedAt, d)
			}
		})
	}
}

func TestUpdateBackupTaskStatusRecordsTheError(t *testing.T) {
	resetTaskStatus(t)
	RecordRun("t1", &domain.Run{TaskID: "t1", Status: "running"})

	AdvanceRun("t1", "failed", "backup failed", errors.New("disk full"))

	got, _ := LookupRun("t1")
	if got.Error != "disk full" {
		t.Errorf("Error = %q, want %q", got.Error, "disk full")
	}
}

func TestUpdateBackupTaskStatusUnknownTaskIsSilent(t *testing.T) {
	resetTaskStatus(t)

	AdvanceRun("never-submitted", "failed", "boom", errors.New("x"))

	if _, ok := LookupRun("never-submitted"); ok {
		t.Error("updating an unknown task created an entry")
	}
}

// A later update never cleared an error a previous one had recorded, so the
// run came back as "completed" carrying the text of the failure — which reads
// as a backup that both worked and did not.
func TestARecoveredTaskDropsItsStaleError(t *testing.T) {
	resetTaskStatus(t)
	RecordRun("t1", &domain.Run{TaskID: "t1", Status: "running"})

	AdvanceRun("t1", "failed", "attempt 1 failed", errors.New("timeout"))
	AdvanceRun("t1", "completed", "attempt 2 succeeded", nil)

	got, _ := LookupRun("t1")
	if got.Status != "completed" {
		t.Fatalf("status = %q, want completed", got.Status)
	}
	if got.Error != "" {
		t.Errorf("Error = %q, want it cleared with the status", got.Error)
	}
}

// TestFinishedRunsAreEventuallyForgotten covers a register that only a restart
// used to reclaim: every backup ever submitted left a record in memory for the
// life of the process.
func TestFinishedRunsAreEventuallyForgotten(t *testing.T) {
	resetTaskStatus(t)

	old := time.Now().Add(-30 * 24 * time.Hour)
	for i := 0; i < 500; i++ {
		id := fmt.Sprintf("task_%d", i)
		RecordRun(id, &domain.Run{
			TaskID:      id,
			Status:      "completed",
			CreatedAt:   old,
			CompletedAt: &old,
		})
	}

	// Recording anything is what sweeps the finished ones out.
	RecordRun("recent", &domain.Run{TaskID: "recent", Status: "running"})

	if n := RunCount(); n != 1 {
		t.Errorf("the register holds %d entries, want just the running one", n)
	}
}

// TestARunningRunIsNotForgotten is the other half: a run still in flight, and a
// finished one somebody may still be polling, both stay.
func TestARunningRunIsNotForgotten(t *testing.T) {
	resetTaskStatus(t)

	justFinished := time.Now()
	RecordRun("running", &domain.Run{TaskID: "running", Status: "running", CreatedAt: time.Now()})
	RecordRun("finished", &domain.Run{
		TaskID:      "finished",
		Status:      "completed",
		CompletedAt: &justFinished,
	})

	RecordRun("another", &domain.Run{TaskID: "another", Status: "pending"})

	if n := RunCount(); n != 3 {
		t.Errorf("the register holds %d entries, want all three", n)
	}
}
