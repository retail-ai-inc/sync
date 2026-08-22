package backuphttp

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/retail-ai-inc/sync/internal/backup/app"
)

func insertBackupTask(t *testing.T, conn *sql.DB, enable int, cfg string) {
	t.Helper()

	if _, err := conn.Exec(
		`INSERT INTO backup_tasks (enable, last_update_time, last_backup_time, next_backup_time, config_json)
		 VALUES (?, '2026-08-20 00:00:00', '2026-08-20 18:00:00', '2026-08-21 18:00:00', ?)`,
		enable, cfg); err != nil {
		t.Fatalf("insert backup task: %v", err)
	}
}

func backupConfig(t *testing.T, conn *sql.DB, id int) map[string]interface{} {
	t.Helper()

	var raw string
	if err := conn.QueryRow("SELECT config_json FROM backup_tasks WHERE id=?", id).Scan(&raw); err != nil {
		t.Fatalf("read config_json: %v", err)
	}
	var cfg map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatalf("parse config_json %q: %v", raw, err)
	}
	return cfg
}

// ------------------------------------------------------------- BackupRun

// emptyTaskDB points the package at a SQLite file with no tables.
func emptyTaskDB(t *testing.T) {
	t.Helper()
	isolateCrontab(t)
	t.Setenv("SYNC_DB_PATH", filepath.Join(t.TempDir(), "empty.db"))
}

func TestBackupRunHandlerRejectsAnUnknownTask(t *testing.T) {
	useTempTaskDB(t)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodPost, "/backup/{id}/run", nil),
		BackupRunHandler, map[string]string{"id": "999"})

	resp := decodeEnvelope(t, rec)
	if resp["success"] != false {
		t.Errorf("success = %v, want false", resp["success"])
	}
	if resp["error"] != "backup task not found" {
		t.Errorf("error = %v", resp["error"])
	}
}

func TestBackupRunHandlerReportsAMissingTable(t *testing.T) {
	emptyTaskDB(t)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodPost, "/backup/{id}/run", nil),
		BackupRunHandler, map[string]string{"id": "1"})

	if resp := decodeEnvelope(t, rec); resp["success"] != false {
		t.Errorf("success = %v, want false", resp["success"])
	}
}

// TestBackupRunHandlerActuallyRunsTheJob covers "back this up now". It used to
// stamp last_backup_time and answer "Backup job started successfully" without
// running anything at all — no executor, no command, nothing written anywhere —
// so the dashboard showed a fresh, successful backup that did not exist. That is
// worse than showing "never backed up": an operator checking before a switchover
// that the data was recoverable saw exactly what they were hoping for.
func TestBackupRunHandlerActuallyRunsTheJob(t *testing.T) {
	conn := useTempTaskDB(t)
	insertBackupTask(t, conn, 1, `{"name":"nightly","sourceType":"mongodb","schedule":"0 3 * * *"}`)
	app.ForgetRuns()
	t.Cleanup(app.ForgetRuns)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodPost, "/backup/{id}/run", nil),
		BackupRunHandler, map[string]string{"id": "1"})

	resp := decodeEnvelope(t, rec)
	if resp["success"] != true {
		t.Fatalf("success = %v (body: %s)", resp["success"], rec.Body.String())
	}
	taskID, _ := resp["taskId"].(string)
	if taskID == "" {
		t.Fatal("no task id came back, so there is nothing to poll and nothing running")
	}
	if n := app.RunCount(); n != 1 {
		t.Errorf("%d runs were registered, want 1", n)
	}
	if _, found := app.LookupRun(taskID); !found {
		t.Errorf("the run %q is not registered", taskID)
	}

	// It runs in the background against the temporary database this test owns,
	// so it has to finish before that is taken away.
	settle(t, taskID)
}

// ---------------------------------------------------------- BackupUpdate

func TestBackupUpdateHandlerReplacesTheConfig(t *testing.T) {
	conn := useTempTaskDB(t)
	insertBackupTask(t, conn, 1, `{"name":"nightly","sourceType":"mongodb","schedule":"0 3 * * *","status":"enabled"}`)

	body := `{"name":"renamed","sourceType":"mysql","schedule":"*/5 * * * *",
	          "database":{"host":"h","port":"3306"},"destination":{"bucket":"b"},
	          "format":"json","backupType":"full","compressionType":"gzip",
	          "tableSelectionMode":"regex","regexPattern":"^orders_"}`

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodPut, "/backup/{id}", strings.NewReader(body)),
		BackupUpdateHandler, map[string]string{"id": "1"})

	resp := decodeEnvelope(t, rec)
	if resp["success"] != true {
		t.Fatalf("success = %v (body: %s)", resp["success"], rec.Body.String())
	}

	cfg := backupConfig(t, conn, 1)
	for field, want := range map[string]interface{}{
		"name": "renamed", "sourceType": "mysql", "schedule": "*/5 * * * *",
		"format": "json", "backupType": "full", "compressionType": "gzip",
		"tableSelectionMode": "regex", "regexPattern": "^orders_",
	} {
		if cfg[field] != want {
			t.Errorf("%s = %v, want %v", field, cfg[field], want)
		}
	}
	// status is carried over from the stored config, not the request.
	if cfg["status"] != "enabled" {
		t.Errorf("status = %v, want the preserved enabled", cfg["status"])
	}
}

func TestBackupUpdateHandlerDerivesStatusFromEnable(t *testing.T) {
	conn := useTempTaskDB(t)
	// No "status" key in the stored config, enable = 1.
	insertBackupTask(t, conn, 1, `{"name":"nightly"}`)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodPut, "/backup/{id}", strings.NewReader(`{"name":"n"}`)),
		BackupUpdateHandler, map[string]string{"id": "1"})
	if resp := decodeEnvelope(t, rec); resp["success"] != true {
		t.Fatalf("body: %s", rec.Body.String())
	}

	if got := backupConfig(t, conn, 1)["status"]; got != "enabled" {
		t.Errorf("status = %v, want enabled (derived from enable=1)", got)
	}
}

func TestBackupUpdateHandlerKeepsTheStoredNameWhenOmitted(t *testing.T) {
	conn := useTempTaskDB(t)
	insertBackupTask(t, conn, 1, `{"name":"keep me","status":"enabled"}`)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodPut, "/backup/{id}", strings.NewReader(`{"sourceType":"mysql"}`)),
		BackupUpdateHandler, map[string]string{"id": "1"})
	if resp := decodeEnvelope(t, rec); resp["success"] != true {
		t.Fatalf("body: %s", rec.Body.String())
	}

	if got := backupConfig(t, conn, 1)["name"]; got != "keep me" {
		t.Errorf("name = %v, want the stored name", got)
	}
}

func TestBackupUpdateHandlerGeneratesANameWhenThereIsNone(t *testing.T) {
	conn := useTempTaskDB(t)
	insertBackupTask(t, conn, 1, `{"status":"enabled"}`)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodPut, "/backup/{id}", strings.NewReader(`{}`)),
		BackupUpdateHandler, map[string]string{"id": "1"})
	if resp := decodeEnvelope(t, rec); resp["success"] != true {
		t.Fatalf("body: %s", rec.Body.String())
	}

	if got := backupConfig(t, conn, 1)["name"]; got != "Backup Task 1" {
		t.Errorf("name = %v, want the generated default", got)
	}
}

func TestBackupUpdateHandlerRejectsMalformedJSON(t *testing.T) {
	conn := useTempTaskDB(t)
	insertBackupTask(t, conn, 1, `{"name":"n"}`)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodPut, "/backup/{id}", strings.NewReader(`{not json`)),
		BackupUpdateHandler, map[string]string{"id": "1"})

	if resp := decodeEnvelope(t, rec); resp["success"] != false {
		t.Errorf("success = %v, want false", resp["success"])
	}
}

func TestBackupUpdateHandlerRejectsAnUnknownTask(t *testing.T) {
	useTempTaskDB(t)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodPut, "/backup/{id}", strings.NewReader(`{"name":"n"}`)),
		BackupUpdateHandler, map[string]string{"id": "999"})

	resp := decodeEnvelope(t, rec)
	if resp["success"] != false {
		t.Errorf("success = %v, want false", resp["success"])
	}
	if resp["error"] != "fetch existing config fail" {
		t.Errorf("error = %v", resp["error"])
	}
}

func TestBackupUpdateHandlerToleratesACorruptStoredConfig(t *testing.T) {
	conn := useTempTaskDB(t)
	insertBackupTask(t, conn, 1, `{not json`)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodPut, "/backup/{id}", strings.NewReader(`{"name":"n"}`)),
		BackupUpdateHandler, map[string]string{"id": "1"})

	if resp := decodeEnvelope(t, rec); resp["success"] != true {
		t.Fatalf("a corrupt stored config was not recovered from: %s", rec.Body.String())
	}
	if got := backupConfig(t, conn, 1)["name"]; got != "n" {
		t.Errorf("name = %v, want n", got)
	}
}

// The update is a full replacement, not a merge: only name and status are read
// back from the stored config. A client that PUTs a partial body — changing
// just the schedule, say — silently blanks the database connection, the
// destination, the format and everything else, and the backup stops working.
func TestAPartialUpdateWipesTheRestOfTheConfig(t *testing.T) {
	conn := useTempTaskDB(t)
	insertBackupTask(t, conn, 1, `{
		"name":"nightly","sourceType":"mongodb","status":"enabled",
		"database":{"host":"tokyo","port":"27017","database":"app"},
		"destination":{"bucket":"gs://backups"},
		"format":"json","compressionType":"gzip","regexPattern":"^orders_"
	}`)

	// Change only the schedule.
	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodPut, "/backup/{id}",
		strings.NewReader(`{"schedule":"0 4 * * *"}`)),
		BackupUpdateHandler, map[string]string{"id": "1"})
	if resp := decodeEnvelope(t, rec); resp["success"] != true {
		t.Fatalf("body: %s", rec.Body.String())
	}

	cfg := backupConfig(t, conn, 1)
	if cfg["schedule"] != "0 4 * * *" {
		t.Fatalf("schedule = %v, want the new value", cfg["schedule"])
	}
	if cfg["sourceType"] != "" || cfg["database"] != nil || cfg["destination"] != nil ||
		cfg["format"] != "" || cfg["compressionType"] != "" || cfg["regexPattern"] != "" {
		t.Fatalf("the untouched fields survived — the update appears to merge now; assert the merge instead: %#v", cfg)
	}
	// name and status are the only two that are carried over.
	if cfg["name"] != "nightly" || cfg["status"] != "enabled" {
		t.Errorf("name/status = %v/%v, want them preserved", cfg["name"], cfg["status"])
	}
}

// TestAStoredValueOfTheWrongTypeIsAnswered covers the stored status and name,
// which were read with unchecked type assertions. A value of any other JSON type
// panicked, and with no recovery in the router that aborted the connection
// rather than returning anything at all.
func TestAStoredValueOfTheWrongTypeIsAnswered(t *testing.T) {
	for name, stored := range map[string]string{
		"a numeric status": `{"name":"n","status":123}`,
		"a numeric name":   `{"name":42,"status":"enabled"}`,
	} {
		t.Run(name, func(t *testing.T) {
			conn := useTempTaskDB(t)
			insertBackupTask(t, conn, 1, stored)

			rec := httptest.NewRecorder()
			serveWithURLParams(rec,
				httptest.NewRequest(http.MethodPut, "/backup/{id}", strings.NewReader(`{}`)),
				BackupUpdateHandler, map[string]string{"id": "1"})

			if rec.Code == 0 {
				t.Fatal("nothing was written to the response")
			}
			if rec.Body.Len() == 0 {
				t.Error("the response has no body")
			}
		})
	}
}

// TestTheStoredNextBackupTimeFollowsTheSchedule covers what an update writes
// into next_backup_time. It used to be "twenty-four hours from now" whatever the
// caller had just set the schedule to.
func TestTheStoredNextBackupTimeFollowsTheSchedule(t *testing.T) {
	conn := useTempTaskDB(t)
	insertBackupTask(t, conn, 1, `{"name":"n","status":"enabled"}`)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodPut, "/backup/{id}",
		strings.NewReader(`{"name":"n","schedule":"*/5 * * * *"}`)),
		BackupUpdateHandler, map[string]string{"id": "1"})
	if resp := decodeEnvelope(t, rec); resp["success"] != true {
		t.Fatalf("body: %s", rec.Body.String())
	}

	var next time.Time
	if err := conn.QueryRow("SELECT next_backup_time FROM backup_tasks WHERE id=1").Scan(&next); err != nil {
		t.Fatalf("read next_backup_time: %v", err)
	}
	// A five-minute schedule puts the next run minutes away, not a day.
	if d := time.Until(next); d > 10*time.Minute {
		t.Errorf("next_backup_time is %v away for a five-minute schedule", d)
	}
}
