package backup

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestBackupListHandlerReturnsTheTaskTable(t *testing.T) {
	db := useTempTaskDB(t)
	if _, err := db.Exec(
		`INSERT INTO backup_tasks (enable, last_update_time, last_backup_time, next_backup_time, config_json)
		 VALUES (1, '2026-08-21 00:00:00', '2026-08-20 18:00:00', '2026-08-22 18:00:00', ?)`,
		`{"name":"nightly","sourceType":"mysql"}`); err != nil {
		t.Fatalf("insert backup task: %v", err)
	}

	rec := httptest.NewRecorder()
	BackupListHandler(rec, httptest.NewRequest(http.MethodGet, "/backup", nil))

	resp := decodeEnvelope(t, rec)
	if resp["success"] != true {
		t.Fatalf("success = %v (body: %s)", resp["success"], rec.Body.String())
	}
	data, ok := resp["data"].([]interface{})
	if !ok || len(data) != 1 {
		t.Fatalf("data = %#v, want 1 task", resp["data"])
	}
	if got := data[0].(map[string]interface{})["status"]; got != "enabled" {
		t.Errorf("status = %v, want enabled", got)
	}
}

func TestBackupListHandlerReportsAMissingTable(t *testing.T) {
	t.Setenv("SYNC_DB_PATH", filepath.Join(t.TempDir(), "empty.db"))

	rec := httptest.NewRecorder()
	BackupListHandler(rec, httptest.NewRequest(http.MethodGet, "/backup", nil))

	resp := decodeEnvelope(t, rec)
	if resp["success"] != false {
		t.Errorf("success = %v, want false", resp["success"])
	}
}

func TestBackupDeleteHandlerRejectsANonNumericID(t *testing.T) {
	useTempTaskDB(t)

	for _, id := range []string{"abc", "", "1.5"} {
		rec := httptest.NewRecorder()
		serveWithURLParams(rec, httptest.NewRequest(http.MethodDelete, "/backup/{id}", nil),
			BackupDeleteHandler, map[string]string{"id": id})

		if rec.Code == http.StatusOK {
			resp := decodeEnvelope(t, rec)
			if resp["success"] == true {
				t.Errorf("id %q: success = true, want a rejection", id)
			}
		}
	}
}

func TestBackupPauseAndResumeFlipTheEnableColumn(t *testing.T) {
	db := useTempTaskDB(t)
	if _, err := db.Exec(
		`INSERT INTO backup_tasks (enable, config_json) VALUES (1, ?)`,
		`{"name":"nightly","cronExpression":"0 3 * * *"}`); err != nil {
		t.Fatalf("insert backup task: %v", err)
	}

	enable := func() int {
		t.Helper()
		var n int
		if err := db.QueryRow("SELECT enable FROM backup_tasks WHERE id=1").Scan(&n); err != nil {
			t.Fatalf("read enable: %v", err)
		}
		return n
	}

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodPut, "/backup/1/pause", nil),
		BackupPauseHandler, map[string]string{"id": "1"})
	if got := enable(); got != 0 {
		t.Errorf("after pause enable = %d, want 0 (body: %s)", got, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodPut, "/backup/1/resume", nil),
		BackupResumeHandler, map[string]string{"id": "1"})
	if got := enable(); got != 1 {
		t.Errorf("after resume enable = %d, want 1 (body: %s)", got, rec.Body.String())
	}
}
