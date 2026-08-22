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

// TestLookupRunHandsBackACopy covers a live pointer being serialised. The lock
// guards the map, not the Run behind it, and the background goroutine writes to
// that Run as the export proceeds — so the status endpoint used to read a struct
// that was being changed underneath it, which is a data race and shows up as a
// status and a message from two different moments.
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

// A later update never clears an error a previous one recorded, so a task that
// fails and is then retried into "completed" reports success while still
// carrying the failure text.
func TestARecoveredTaskKeepsItsStaleError(t *testing.T) {
	resetTaskStatus(t)
	RecordRun("t1", &domain.Run{TaskID: "t1", Status: "running"})

	AdvanceRun("t1", "failed", "attempt 1 failed", errors.New("timeout"))
	AdvanceRun("t1", "completed", "attempt 2 succeeded", nil)

	got, _ := LookupRun("t1")
	if got.Status != "completed" {
		t.Fatalf("status = %q, want completed", got.Status)
	}
	if got.Error != "timeout" {
		t.Fatalf("Error = %q, no longer stale — it appears to be cleared now; assert the empty error instead", got.Error)
	}
}

// Nothing ever removes an entry from taskStatusMap: every backup run leaves a
// domain.Run in memory for the lifetime of the process, and the only way
// to reclaim it is a restart.
func TestTaskStatusEntriesAreNeverEvicted(t *testing.T) {
	resetTaskStatus(t)

	for i := 0; i < 500; i++ {
		id := fmt.Sprintf("task_%d", i)
		RecordRun(id, &domain.Run{
			TaskID:    id,
			Status:    "completed",
			CreatedAt: time.Now().Add(-30 * 24 * time.Hour),
		})
		AdvanceRun(id, "completed", "done", nil)
	}

	n := RunCount()

	if n != 500 {
		t.Fatalf("the run register holds %d of 500 month-old completed tasks — eviction appears to have been added; assert the retention policy instead", n)
	}
}
