package httpapi

import (
	"database/sql"
	"net/http"
	"strings"
	"testing"

	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
)

// currentDB opens the control database the test is already pointed at, which
// withUsers has created and populated.
func currentDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		t.Fatalf("open the test control database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// The audit trail is applied to the group of routes that change something, so
// these check the wiring rather than the recording, which audit's own tests
// cover.

func auditedPaths(t *testing.T, db *sql.DB) []string {
	t.Helper()

	rows, err := db.Query(`SELECT method || " " || path || " " || username ||
		" " || status FROM audit_log ORDER BY id`)
	if err != nil {
		t.Fatalf("read audit_log: %v", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var entry string
		if err := rows.Scan(&entry); err != nil {
			t.Fatalf("scan audit_log: %v", err)
		}
		out = append(out, entry)
	}
	return out
}

func TestAnAdministrativeWriteIsRecorded(t *testing.T) {
	adminToken, _ := withUsers(t)
	db := currentDB(t)

	call(t, http.MethodDelete, "/sync/9999", "", adminToken)

	entries := auditedPaths(t, db)
	if len(entries) != 1 {
		t.Fatalf("recorded %v, want one entry", entries)
	}
	if want := "DELETE /sync/9999 admin 200"; entries[0] != want {
		t.Errorf("recorded %q, want %q", entries[0], want)
	}
}

// TestAReadIsNotRecorded keeps the trail to what changed something. Recording
// every list and every poll of the monitor endpoint would bury the writes.
func TestAReadIsNotRecorded(t *testing.T) {
	adminToken, _ := withUsers(t)
	db := currentDB(t)

	call(t, http.MethodGet, "/sync", "", adminToken)
	call(t, http.MethodGet, "/changestreams/status", "", adminToken)

	if entries := auditedPaths(t, db); len(entries) != 0 {
		t.Errorf("reads were recorded: %v", entries)
	}
}

// TestARefusedWriteIsRecorded covers the attempt: a non-administrator who tried
// to delete a task is in the trail with the 403.
func TestARefusedWriteIsRecorded(t *testing.T) {
	_, guestToken := withUsers(t)
	db := currentDB(t)

	call(t, http.MethodDelete, "/sync/9999", "", guestToken)

	entries := auditedPaths(t, db)
	if len(entries) != 1 {
		t.Fatalf("recorded %v, want the refused attempt", entries)
	}
	if want := "DELETE /sync/9999 guest 403"; entries[0] != want {
		t.Errorf("recorded %q, want %q", entries[0], want)
	}
}

// TestAnUnauthenticatedWriteIsNotRecorded pins the near edge of the trail. The
// audit middleware sits after RequireAuth, so a caller with no token cannot
// fill the control database from outside.
func TestAnUnauthenticatedWriteIsNotRecorded(t *testing.T) {
	withUsers(t)
	db := currentDB(t)

	call(t, http.MethodDelete, "/sync/9999", "", "")

	if entries := auditedPaths(t, db); len(entries) != 0 {
		t.Errorf("an unauthenticated request was recorded: %v", entries)
	}
}

// An acknowledgement recorded with no author, or by the wrong one, cannot be traced back.
func TestADDLAcknowledgementNamesTheAdminWhoMadeIt(t *testing.T) {
	adminToken, _ := withUsers(t)
	db := currentDB(t)
	if _, err := db.Exec(`INSERT INTO sync_tasks (id, enable, config_json) VALUES (1, 0, ?)`,
		`{"type":"mysql","taskName":"trial",`+
			`"sourceConn":{"host":"h","port":"3306","user":"u","password":"p","database":"shop"},`+
			`"targetConn":{"host":"h","port":"3306","user":"u","password":"p","database":"shop_bk"}}`); err != nil {
		t.Fatalf("seed sync_tasks: %v", err)
	}

	rec := call(t, http.MethodPost, "/sync/1/ddl-acknowledgements",
		`{"statement":"ALTER TABLE t DROP COLUMN c"}`, adminToken)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"success":true`) {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	var createdBy string
	if err := db.QueryRow(`SELECT created_by FROM ddl_acknowledgements WHERE task_id = 1`).
		Scan(&createdBy); err != nil {
		t.Fatalf("read the acknowledgement: %v", err)
	}
	if createdBy != "admin" {
		t.Errorf("created_by = %q, want admin", createdBy)
	}
	entries := auditedPaths(t, db)
	if want := "POST /sync/1/ddl-acknowledgements admin 200"; len(entries) != 1 || entries[0] != want {
		t.Errorf("recorded %v, want [%s]", entries, want)
	}
}
