package backuphttp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	_ "github.com/mattn/go-sqlite3"
	"github.com/retail-ai-inc/sync/internal/backup/app"
	"github.com/retail-ai-inc/sync/internal/backup/domain"
)

// serveWithURLParams runs a handler with chi route parameters populated, which
// is how the handlers read {id} and {taskId}.
func serveWithURLParams(rec *httptest.ResponseRecorder, req *http.Request, h http.HandlerFunc, params map[string]string) {
	rctx := chi.NewRouteContext()
	for k, v := range params {
		rctx.URLParams.Add(k, v)
	}
	h(rec, req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx)))
}

func TestBackupStatusHandlerReturnsTheStoredStatus(t *testing.T) {
	app.ForgetRuns()
	t.Cleanup(app.ForgetRuns)
	app.RecordRun("backup_7_1", &domain.Run{
		TaskID: "backup_7_1", BackupID: 7, Status: "running", Message: "in progress",
	})

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodGet, "/backup/status/backup_7_1", nil),
		BackupStatusHandler, map[string]string{"taskId": "backup_7_1"})

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var got domain.Run
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("body is not a domain.Run: %v (%q)", err, rec.Body.String())
	}
	if got.TaskID != "backup_7_1" || got.BackupID != 7 || got.Status != "running" {
		t.Errorf("got %#v", got)
	}
	if got.CompletedAt != nil {
		t.Errorf("completedAt is present on a running task: %v", got.CompletedAt)
	}
}

func TestBackupStatusHandlerUnknownTaskIs404(t *testing.T) {
	app.ForgetRuns()
	t.Cleanup(app.ForgetRuns)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodGet, "/backup/status/nope", nil),
		BackupStatusHandler, map[string]string{"taskId": "nope"})

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}

func TestBackupStatusHandlerEmptyTaskIDIs400(t *testing.T) {
	app.ForgetRuns()
	t.Cleanup(app.ForgetRuns)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodGet, "/backup/status/", nil),
		BackupStatusHandler, map[string]string{"taskId": ""})

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestBackupExecuteHandlerRejectsANonNumericID(t *testing.T) {
	app.ForgetRuns()
	t.Cleanup(app.ForgetRuns)

	for _, id := range []string{"abc", "", "1.5", "7x"} {
		rec := httptest.NewRecorder()
		serveWithURLParams(rec, httptest.NewRequest(http.MethodPost, "/backup/execute/"+id, nil),
			BackupExecuteHandler, map[string]string{"id": id})

		if rec.Code != http.StatusBadRequest {
			t.Errorf("id %q: status = %d, want 400", id, rec.Code)
		}
	}

	n := app.RunCount()
	if n != 0 {
		t.Errorf("a rejected request registered %d task(s)", n)
	}
}

// TestTwoRunsOfOneJobGetDistinctIDs covers two submissions of the same job
// inside one second. The id was the backup id and a one-second timestamp, so
// both produced the same one and the second silently replaced the first's
// record — the caller that submitted first then polled somebody else's run.
func TestTwoRunsOfOneJobGetDistinctIDs(t *testing.T) {
	app.ForgetRuns()
	t.Cleanup(app.ForgetRuns)
	useTempTaskDB(t)

	first := app.SubmitRun(7)
	second := app.SubmitRun(7)

	if first == second {
		t.Fatalf("both runs were given the id %q", first)
	}
	if n := app.RunCount(); n != 2 {
		t.Errorf("the register holds %d entries for two runs", n)
	}
}
