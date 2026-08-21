package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
)

// resetTaskStatus clears the package-global task registry so tests do not see
// each other's entries.
func resetTaskStatus(t *testing.T) {
	t.Helper()

	taskStatusMutex.Lock()
	taskStatusMap = make(map[string]*BackupTaskStatus)
	taskStatusMutex.Unlock()

	t.Cleanup(func() {
		taskStatusMutex.Lock()
		taskStatusMap = make(map[string]*BackupTaskStatus)
		taskStatusMutex.Unlock()
	})
}

func TestTaskStatusRoundTrip(t *testing.T) {
	resetTaskStatus(t)

	want := &BackupTaskStatus{TaskID: "t1", BackupID: 7, Status: "pending", Message: "queued"}
	setTaskStatus("t1", want)

	got, ok := getTaskStatus("t1")
	if !ok {
		t.Fatal("getTaskStatus reported the task as missing")
	}
	if got != want {
		t.Errorf("getTaskStatus returned %#v, want the stored pointer", got)
	}
}

func TestGetTaskStatusMissing(t *testing.T) {
	resetTaskStatus(t)

	if _, ok := getTaskStatus("nope"); ok {
		t.Error("getTaskStatus reported an unknown task as present")
	}
}

func TestUpdateBackupTaskStatusRunning(t *testing.T) {
	resetTaskStatus(t)
	setTaskStatus("t1", &BackupTaskStatus{TaskID: "t1", Status: "pending"})

	updateBackupTaskStatus("t1", "running", "started", nil)

	got, _ := getTaskStatus("t1")
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
			setTaskStatus("t1", &BackupTaskStatus{TaskID: "t1", Status: "running"})

			updateBackupTaskStatus("t1", status, "done", nil)

			got, _ := getTaskStatus("t1")
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
	setTaskStatus("t1", &BackupTaskStatus{TaskID: "t1", Status: "running"})

	updateBackupTaskStatus("t1", "failed", "backup failed", errors.New("disk full"))

	got, _ := getTaskStatus("t1")
	if got.Error != "disk full" {
		t.Errorf("Error = %q, want %q", got.Error, "disk full")
	}
}

func TestUpdateBackupTaskStatusUnknownTaskIsSilent(t *testing.T) {
	resetTaskStatus(t)

	updateBackupTaskStatus("never-submitted", "failed", "boom", errors.New("x"))

	if _, ok := getTaskStatus("never-submitted"); ok {
		t.Error("updating an unknown task created an entry")
	}
}

// A later update never clears an error a previous one recorded, so a task that
// fails and is then retried into "completed" reports success while still
// carrying the failure text.
func TestARecoveredTaskKeepsItsStaleError(t *testing.T) {
	resetTaskStatus(t)
	setTaskStatus("t1", &BackupTaskStatus{TaskID: "t1", Status: "running"})

	updateBackupTaskStatus("t1", "failed", "attempt 1 failed", errors.New("timeout"))
	updateBackupTaskStatus("t1", "completed", "attempt 2 succeeded", nil)

	got, _ := getTaskStatus("t1")
	if got.Status != "completed" {
		t.Fatalf("status = %q, want completed", got.Status)
	}
	if got.Error != "timeout" {
		t.Fatalf("Error = %q, no longer stale — it appears to be cleared now; assert the empty error instead", got.Error)
	}
}

// Nothing ever removes an entry from taskStatusMap: every backup run leaves a
// BackupTaskStatus in memory for the lifetime of the process, and the only way
// to reclaim it is a restart.
func TestTaskStatusEntriesAreNeverEvicted(t *testing.T) {
	resetTaskStatus(t)

	for i := 0; i < 500; i++ {
		id := fmt.Sprintf("task_%d", i)
		setTaskStatus(id, &BackupTaskStatus{
			TaskID:    id,
			Status:    "completed",
			CreatedAt: time.Now().Add(-30 * 24 * time.Hour),
		})
		updateBackupTaskStatus(id, "completed", "done", nil)
	}

	taskStatusMutex.RLock()
	n := len(taskStatusMap)
	taskStatusMutex.RUnlock()

	if n != 500 {
		t.Fatalf("taskStatusMap holds %d of 500 month-old completed tasks — eviction appears to have been added; assert the retention policy instead", n)
	}
}

func TestBackupStatusHandlerReturnsTheStoredStatus(t *testing.T) {
	resetTaskStatus(t)
	setTaskStatus("backup_7_1", &BackupTaskStatus{
		TaskID: "backup_7_1", BackupID: 7, Status: "running", Message: "in progress",
	})

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodGet, "/backup/status/backup_7_1", nil),
		BackupStatusHandler, map[string]string{"taskId": "backup_7_1"})

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var got BackupTaskStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("body is not a BackupTaskStatus: %v (%q)", err, rec.Body.String())
	}
	if got.TaskID != "backup_7_1" || got.BackupID != 7 || got.Status != "running" {
		t.Errorf("got %#v", got)
	}
	if got.CompletedAt != nil {
		t.Errorf("completedAt is present on a running task: %v", got.CompletedAt)
	}
}

func TestBackupStatusHandlerUnknownTaskIs404(t *testing.T) {
	resetTaskStatus(t)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodGet, "/backup/status/nope", nil),
		BackupStatusHandler, map[string]string{"taskId": "nope"})

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}

func TestBackupStatusHandlerEmptyTaskIDIs400(t *testing.T) {
	resetTaskStatus(t)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodGet, "/backup/status/", nil),
		BackupStatusHandler, map[string]string{"taskId": ""})

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestBackupExecuteHandlerRejectsANonNumericID(t *testing.T) {
	resetTaskStatus(t)

	for _, id := range []string{"abc", "", "1.5", "7x"} {
		rec := httptest.NewRecorder()
		serveWithURLParams(rec, httptest.NewRequest(http.MethodPost, "/backup/execute/"+id, nil),
			BackupExecuteHandler, map[string]string{"id": id})

		if rec.Code != http.StatusBadRequest {
			t.Errorf("id %q: status = %d, want 400", id, rec.Code)
		}
	}

	taskStatusMutex.RLock()
	n := len(taskStatusMap)
	taskStatusMutex.RUnlock()
	if n != 0 {
		t.Errorf("a rejected request registered %d task(s)", n)
	}
}

// The task ID is derived from the backup ID and a one-second timestamp, so two
// runs of the same backup inside the same second produce the same ID and the
// second silently replaces the first's status entry. The caller that submitted
// the first run then polls a status that belongs to a different execution.
func TestTaskIDsCollideWithinTheSameSecond(t *testing.T) {
	resetTaskStatus(t)

	now := time.Now().Unix()
	first := fmt.Sprintf("backup_%d_%d", 7, now)
	second := fmt.Sprintf("backup_%d_%d", 7, now)

	if first != second {
		t.Fatal("the ID format no longer collides within a second — it appears to include a unique component now")
	}

	setTaskStatus(first, &BackupTaskStatus{TaskID: first, BackupID: 7, Status: "running", Message: "run 1"})
	setTaskStatus(second, &BackupTaskStatus{TaskID: second, BackupID: 7, Status: "pending", Message: "run 2"})

	got, _ := getTaskStatus(first)
	if got.Message != "run 2" {
		t.Fatalf("message = %q — the collision no longer overwrites; assert the distinct entries instead", got.Message)
	}

	taskStatusMutex.RLock()
	n := len(taskStatusMap)
	taskStatusMutex.RUnlock()
	if n != 1 {
		t.Fatalf("taskStatusMap holds %d entries for two runs — the IDs appear to be unique now", n)
	}
}

// serveWithURLParams runs a handler with chi route parameters populated, which
// is how the handlers read {id} and {taskId}.
func serveWithURLParams(rec *httptest.ResponseRecorder, req *http.Request, h http.HandlerFunc, params map[string]string) {
	rctx := chi.NewRouteContext()
	for k, v := range params {
		rctx.URLParams.Add(k, v)
	}
	h(rec, req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx)))
}
