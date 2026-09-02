package replicationhttp

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/retail-ai-inc/sync/internal/platform/sqlite/sqlitetest"

	_ "github.com/mattn/go-sqlite3"
)

func TestSyncListHandlerReturnsTheTaskTable(t *testing.T) {
	db := useTempTaskDB(t)
	insertSyncTask(t, db, 1, `{"type":"mysql","taskName":"tokyo to osaka","securityEnabled":true}`)
	insertSyncTask(t, db, 0, `{"type":"mongodb"}`)

	rec := httptest.NewRecorder()
	SyncListHandler(rec, httptest.NewRequest(http.MethodGet, "/sync", nil))

	resp := decodeEnvelope(t, rec)
	if resp["success"] != true {
		t.Fatalf("success = %v (body: %s)", resp["success"], rec.Body.String())
	}
	data, ok := resp["data"].([]interface{})
	if !ok || len(data) != 2 {
		t.Fatalf("data = %#v, want 2 tasks", resp["data"])
	}

	first := data[0].(map[string]interface{})
	if first["taskName"] != "tokyo to osaka" {
		t.Errorf("taskName = %v", first["taskName"])
	}
	if first["status"] != "Running" || first["enable"] != true {
		t.Errorf("status = %v, enable = %v, want Running/true", first["status"], first["enable"])
	}
	if first["securityEnabled"] != true {
		t.Errorf("securityEnabled = %v, want true", first["securityEnabled"])
	}
	if first["lastUpdateTime"] != "2026-08-21 09:30:00" {
		t.Errorf("lastUpdateTime = %v, want the JST-shifted value", first["lastUpdateTime"])
	}

	second := data[1].(map[string]interface{})
	if second["status"] != "Stopped" || second["enable"] != false {
		t.Errorf("status = %v, enable = %v, want Stopped/false", second["status"], second["enable"])
	}
	if second["taskName"] != "Sync Task 2" {
		t.Errorf("taskName = %v, want the generated default", second["taskName"])
	}
}

func TestSyncListHandlerOnAnEmptyTable(t *testing.T) {
	useTempTaskDB(t)

	rec := httptest.NewRecorder()
	SyncListHandler(rec, httptest.NewRequest(http.MethodGet, "/sync", nil))

	resp := decodeEnvelope(t, rec)
	if resp["success"] != true {
		t.Fatalf("success = %v", resp["success"])
	}
	// A nil slice marshals to null, not [], so an empty list and a failure are
	// distinguishable only by the success flag.
	if resp["data"] != nil {
		t.Fatalf("data = %#v — an empty list now marshals as a JSON array; assert that instead", resp["data"])
	}
}

// A row whose config_json cannot be parsed is logged at warn level and then
// returned as a task with every field blank, so a corrupt configuration
// surfaces as an apparently valid but empty entry rather than an error.
func TestAnUnparseableTaskConfigIsReturnedAsBlank(t *testing.T) {
	db := useTempTaskDB(t)
	insertSyncTask(t, db, 1, `{not json`)

	rec := httptest.NewRecorder()
	SyncListHandler(rec, httptest.NewRequest(http.MethodGet, "/sync", nil))

	resp := decodeEnvelope(t, rec)
	if resp["success"] != true {
		t.Fatalf("success = %v — a corrupt config appears to be reported now; assert the error instead", resp["success"])
	}
	data := resp["data"].([]interface{})
	if len(data) != 1 {
		t.Fatalf("data = %#v, want the corrupt row", resp["data"])
	}
	row := data[0].(map[string]interface{})
	if row["sourceType"] != "" || row["sourceConn"] != nil {
		t.Fatalf("row = %#v — the corrupt row is no longer returned blank", row)
	}
}

// TestSyncListHandlerMasksConnectionPasswords is the fix for a response that
// used to be a credential dump: the stored connection blocks were echoed
// verbatim, passwords included, to any caller that reached the endpoint.
func TestSyncListHandlerMasksConnectionPasswords(t *testing.T) {
	db := useTempTaskDB(t)
	insertSyncTask(t, db, 1, `{
		"type":"mysql",
		"sourceConn":{"host":"tokyo","port":"3306","user":"repl","password":"tokyo-secret","database":"app"},
		"targetConn":{"host":"osaka","port":"3306","user":"repl","password":"osaka-secret","database":"app"}
	}`)

	rec := httptest.NewRecorder()
	SyncListHandler(rec, httptest.NewRequest(http.MethodGet, "/sync", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	body := rec.Body.String()
	for _, secret := range []string{"tokyo-secret", "osaka-secret"} {
		if strings.Contains(body, secret) {
			t.Errorf("the response still carries %q: %s", secret, body)
		}
	}
	// The rest of the connection stays, because the UI shows where a task points.
	for _, kept := range []string{"tokyo", "osaka", "repl", "3306"} {
		if !strings.Contains(body, kept) {
			t.Errorf("the response no longer names %q: %s", kept, body)
		}
	}
}

// TestMaskingDoesNotChangeTheStoredTask pins that the redaction happens on the
// way out: the copy the syncer authenticates with must keep its password.
func TestMaskingDoesNotChangeTheStoredTask(t *testing.T) {
	conn := map[string]string{"host": "tokyo", "password": "tokyo-secret"}

	masked := withoutCredentials(conn)

	if masked["password"] != redactedPassword {
		t.Errorf("masked password = %q", masked["password"])
	}
	if conn["password"] != "tokyo-secret" {
		t.Errorf("the source map was modified: %q", conn["password"])
	}
	if withoutCredentials(nil) != nil {
		t.Error("a nil connection produced a map")
	}
	if got := withoutCredentials(map[string]string{"host": "h"}); got["password"] != "" {
		t.Errorf("a connection with no password gained one: %q", got["password"])
	}
}

func TestSyncListHandlerReportsAMissingTable(t *testing.T) {
	sqlitetest.Tableless(t)

	rec := httptest.NewRecorder()
	SyncListHandler(rec, httptest.NewRequest(http.MethodGet, "/sync", nil))

	resp := decodeEnvelope(t, rec)
	if resp["success"] != false {
		t.Errorf("success = %v, want false", resp["success"])
	}
	if resp["error"] != "query sync_tasks fail" {
		t.Errorf("error = %v", resp["error"])
	}
}

func TestSyncDeleteHandlerRejectsANonNumericID(t *testing.T) {
	useTempTaskDB(t)

	for _, id := range []string{"abc", "", "1;DROP TABLE sync_tasks", "1.5"} {
		rec := httptest.NewRecorder()
		serveWithURLParams(rec, httptest.NewRequest(http.MethodDelete, "/sync/{id}", nil),
			SyncDeleteHandler, map[string]string{"id": id})

		resp := decodeEnvelope(t, rec)
		if resp["success"] == true {
			t.Errorf("id %q: success = true, want a rejection (body: %s)", id, rec.Body.String())
		}
	}
}

func TestSyncStartAndStopFlipTheEnableColumn(t *testing.T) {
	db := useTempTaskDB(t)
	insertSyncTask(t, db, 0, `{"type":"mysql","taskName":"t"}`)

	enable := func() int {
		t.Helper()
		var n int
		if err := db.QueryRow("SELECT enable FROM sync_tasks WHERE id=1").Scan(&n); err != nil {
			t.Fatalf("read enable: %v", err)
		}
		return n
	}

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodPut, "/sync/1/start", nil),
		SyncStartHandler, map[string]string{"id": "1"})
	if resp := decodeEnvelope(t, rec); resp["success"] != true {
		t.Fatalf("start: %s", rec.Body.String())
	}
	if got := enable(); got != 1 {
		t.Errorf("after start enable = %d, want 1", got)
	}

	rec = httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodPut, "/sync/1/stop", nil),
		SyncStopHandler, map[string]string{"id": "1"})
	if resp := decodeEnvelope(t, rec); resp["success"] != true {
		t.Fatalf("stop: %s", rec.Body.String())
	}
	if got := enable(); got != 0 {
		t.Errorf("after stop enable = %d, want 0", got)
	}
}

func TestStartingAMissingTaskIsRejected(t *testing.T) {
	useTempTaskDB(t)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodPut, "/sync/999/start", nil),
		SyncStartHandler, map[string]string{"id": "999"})

	resp := decodeEnvelope(t, rec)
	if resp["success"] != false {
		t.Errorf("success = %v, want false for a task that does not exist", resp["success"])
	}
}
