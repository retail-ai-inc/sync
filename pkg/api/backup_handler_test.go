package api

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

// "Manually triggered backup" runs no backup. The handler checks the task
// exists, stamps last_backup_time with the current time, and answers "Backup
// job started successfully" — no executor is constructed, no command is run,
// nothing is written anywhere. The dashboard then shows a backup that happened
// seconds ago and does not exist, which is worse than showing none: an operator
// checking recoverability before a failover sees a fresh successful backup.
func TestBackupRunHandlerRecordsASuccessWithoutRunningAnything(t *testing.T) {
	conn := useTempTaskDB(t)
	insertBackupTask(t, conn, 1, `{"name":"nightly","sourceType":"mongodb","schedule":"0 3 * * *"}`)

	before := time.Now().UTC().Add(-time.Second)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodPost, "/backup/{id}/run", nil),
		BackupRunHandler, map[string]string{"id": "1"})

	resp := decodeEnvelope(t, rec)
	if resp["success"] != true {
		t.Fatalf("success = %v (body: %s)", resp["success"], rec.Body.String())
	}
	if resp["message"] != "Backup job started successfully" {
		t.Errorf("message = %v", resp["message"])
	}

	// The driver converts DATETIME columns to time.Time.
	var lastBackup time.Time
	if err := conn.QueryRow("SELECT last_backup_time FROM backup_tasks WHERE id=1").Scan(&lastBackup); err != nil {
		t.Fatalf("read last_backup_time: %v", err)
	}
	if !lastBackup.After(before) {
		t.Fatalf("last_backup_time = %v, expected it to be stamped with now — the handler appears to run a real backup now; assert the executed backup instead", lastBackup)
	}

	// Nothing else changed: no task status was registered, which is what a real
	// asynchronous run does (see BackupExecuteHandler).
	resetTaskStatus(t)
	taskStatusMutex.RLock()
	n := len(taskStatusMap)
	taskStatusMutex.RUnlock()
	if n != 0 {
		t.Fatalf("%d background tasks were registered — the handler appears to execute now", n)
	}
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

// The stored config's status and name are read with unchecked type assertions.
// A value of any other JSON type panics, and with no Recoverer in the router
// (T-072) that aborts the connection rather than returning a 500.
func TestANonStringStatusPanics(t *testing.T) {
	conn := useTempTaskDB(t)
	insertBackupTask(t, conn, 1, `{"name":"n","status":123}`)

	defer func() {
		if recover() == nil {
			t.Fatal("a numeric status no longer panics — the assertion appears to be checked now; assert the error response instead")
		}
	}()

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodPut, "/backup/{id}", strings.NewReader(`{"name":"x"}`)),
		BackupUpdateHandler, map[string]string{"id": "1"})
	_ = rec
}

func TestANonStringNamePanics(t *testing.T) {
	conn := useTempTaskDB(t)
	insertBackupTask(t, conn, 1, `{"name":42,"status":"enabled"}`)

	defer func() {
		if recover() == nil {
			t.Fatal("a numeric name no longer panics — the assertion appears to be checked now; assert the error response instead")
		}
	}()

	rec := httptest.NewRecorder()
	// An empty name in the request makes the handler fall back to the stored one.
	serveWithURLParams(rec, httptest.NewRequest(http.MethodPut, "/backup/{id}", strings.NewReader(`{}`)),
		BackupUpdateHandler, map[string]string{"id": "1"})
	_ = rec
}

// next_backup_time is written from calculateNextBackupTime, which ignores the
// cron expression entirely (T-074), so every update stamps the same now+24h
// regardless of the schedule the caller just set.
func TestTheStoredNextBackupTimeIgnoresTheSchedule(t *testing.T) {
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
	// A five-minute schedule should put the next run minutes away, not a day.
	if d := time.Until(next); d < 23*time.Hour {
		t.Fatalf("next_backup_time is %v away — the schedule appears to be parsed now; assert the real next run instead", d)
	}
}

// emptyTaskDB points the package at a SQLite file with no tables.
func emptyTaskDB(t *testing.T) {
	t.Helper()
	isolateCrontab(t)
	t.Setenv("SYNC_DB_PATH", filepath.Join(t.TempDir(), "empty.db"))
}
