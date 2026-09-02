package backuphttp

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/retail-ai-inc/sync/internal/platform/httpx"
	"github.com/retail-ai-inc/sync/internal/platform/sqlite/sqlitetest"

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
	sqlitetest.Tableless(t)

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

// The list endpoint answers with the connection settings of every job, and
// those carry the passwords the backup authenticates with. The replication
// side masked them from the start; this one did not, so GET /api/backup handed
// out the source and destination database passwords to anybody with a token.
func TestTheListDoesNotHandOutPasswords(t *testing.T) {
	db := useTempTaskDB(t)
	if _, err := db.Exec(
		`INSERT INTO backup_tasks (enable, last_update_time, last_backup_time, next_backup_time, config_json)
		 VALUES (1, '2026-08-21 00:00:00', '2026-08-20 18:00:00', '2026-08-22 18:00:00', ?)`,
		`{"name":"nightly","schedule":"0 2 * * *","sourceType":"mongodb",
		 "database":{"url":"mongos:27017","username":"root","password":"s3cret-source"},
		 "destination":{"gcsPath":"gs://bucket/x","password":"s3cret-destination"}}`); err != nil {
		t.Fatalf("insert backup task: %v", err)
	}

	rec := httptest.NewRecorder()
	BackupListHandler(rec, httptest.NewRequest(http.MethodGet, "/backup", nil))
	body := rec.Body.String()

	for _, leaked := range []string{"s3cret-source", "s3cret-destination"} {
		if strings.Contains(body, leaked) {
			t.Errorf("the response carries the stored password %q:\n%s", leaked, body)
		}
	}
	if !strings.Contains(body, httpx.RedactedPassword) {
		t.Errorf("no password was masked, so the fields were dropped rather than "+
			"redacted — the UI needs to know one is set:\n%s", body)
	}
	// The rest of the connection has to survive: the UI shows the host and user.
	for _, kept := range []string{"mongos:27017", "root", "gs://bucket/x"} {
		if !strings.Contains(body, kept) {
			t.Errorf("the response lost %q, which is not a credential:\n%s", kept, body)
		}
	}
}
