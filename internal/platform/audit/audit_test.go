package audit

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
	"github.com/sirupsen/logrus"

	_ "github.com/mattn/go-sqlite3"
)

func init() { logrus.SetOutput(nopWriter{}) }

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }

// useAuditDB points the package at a throwaway control database, through the
// real opener so the schema is the one the deployment gets.
func useAuditDB(t *testing.T) *sql.DB {
	t.Helper()
	t.Setenv("SYNC_DB_PATH", filepath.Join(t.TempDir(), "sync.db"))

	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		t.Fatalf("open temp sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

type row struct {
	At, Username, Access, Method, Path, RemoteAddr string
	Status                                         int
}

func auditRows(t *testing.T, db *sql.DB) []row {
	t.Helper()
	rows, err := db.Query(`SELECT at, username, access, method, path, status,
		COALESCE(remote_addr, '') FROM audit_log ORDER BY id`)
	if err != nil {
		t.Fatalf("read audit_log: %v", err)
	}
	defer rows.Close()

	var out []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.At, &r.Username, &r.Access, &r.Method, &r.Path,
			&r.Status, &r.RemoteAddr); err != nil {
			t.Fatalf("scan audit_log: %v", err)
		}
		out = append(out, r)
	}
	return out
}

func TestRecordStoresTheChange(t *testing.T) {
	db := useAuditDB(t)

	Record(Entry{Username: "jack", Access: "admin", Method: "DELETE",
		Path: "/api/sync/42", Status: 200, RemoteAddr: "10.0.0.9"})

	rows := auditRows(t, db)
	if len(rows) != 1 {
		t.Fatalf("recorded %d rows, want 1", len(rows))
	}
	got := rows[0]
	if got.Username != "jack" || got.Access != "admin" ||
		got.Method != "DELETE" || got.Path != "/api/sync/42" ||
		got.Status != 200 || got.RemoteAddr != "10.0.0.9" {
		t.Errorf("recorded %+v", got)
	}
	if got.At == "" {
		t.Error("the entry has no time")
	}
}

// TestRecordDoesNotStopTheOperation covers the rule that an audit trail must not
// be able to take the system down: a control database that cannot be written
// must not make a task undeletable.
func TestRecordDoesNotStopTheOperation(t *testing.T) {
	t.Setenv("SYNC_DB_PATH", filepath.Join(t.TempDir(), "no-such-dir", "sync.db"))
	Record(Entry{Username: "jack", Method: "DELETE", Path: "/api/sync/42"})
}

func TestMiddlewareRecordsAWrite(t *testing.T) {
	db := useAuditDB(t)

	handler := Middleware(func(*http.Request) (string, string) {
		return "jack", "admin"
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	request := httptest.NewRequest(http.MethodDelete, "/api/sync/42", nil)
	request.RemoteAddr = "10.0.0.9:54321"
	handler.ServeHTTP(httptest.NewRecorder(), request)

	rows := auditRows(t, db)
	if len(rows) != 1 {
		t.Fatalf("recorded %d rows, want 1", len(rows))
	}
	if rows[0].Method != "DELETE" || rows[0].Path != "/api/sync/42" ||
		rows[0].Username != "jack" || rows[0].Status != 200 {
		t.Errorf("recorded %+v", rows[0])
	}
}

// TestARefusedAttemptIsRecorded covers why the status is stored: somebody who
// tried to delete a task and was denied is worth as much in the trail as
// somebody who succeeded.
func TestARefusedAttemptIsRecorded(t *testing.T) {
	db := useAuditDB(t)

	handler := Middleware(func(*http.Request) (string, string) {
		return "someone", "user"
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	handler.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest(http.MethodDelete, "/api/sync/42", nil))

	rows := auditRows(t, db)
	if len(rows) != 1 || rows[0].Status != http.StatusForbidden {
		t.Fatalf("recorded %+v, want one row with status 403", rows)
	}
}

// TestTheTrailNeverHoldsACredential is the reason the body is not recorded: an
// audit trail of task edits would otherwise carry the source and target
// password of every task ever saved.
func TestTheTrailNeverHoldsACredential(t *testing.T) {
	db := useAuditDB(t)

	const password = "cpZEUF9sw9apGErA"
	body := strings.NewReader(`{"sourceConn":{"password":"` + password + `"}}`)
	handler := Middleware(func(*http.Request) (string, string) {
		return "jack", "admin"
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	handler.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest(http.MethodPut, "/api/sync/42", body))

	var dump string
	if err := db.QueryRow(`SELECT COALESCE(GROUP_CONCAT(
		at || username || access || method || path || COALESCE(remote_addr,'')), '')
		FROM audit_log`).Scan(&dump); err != nil {
		t.Fatalf("read audit_log: %v", err)
	}
	if strings.Contains(dump, password) {
		t.Error("the recorded entry carries the password from the request body")
	}
}

func TestAHandlerThatWritesNothingIsATwoHundred(t *testing.T) {
	db := useAuditDB(t)

	handler := Middleware(nil)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	handler.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest(http.MethodPost, "/api/sync", nil))

	rows := auditRows(t, db)
	if len(rows) != 1 || rows[0].Status != http.StatusOK {
		t.Fatalf("recorded %+v, want one row with status 200", rows)
	}
}

func TestAnUnknownCallerIsStillRecorded(t *testing.T) {
	db := useAuditDB(t)

	handler := Middleware(func(*http.Request) (string, string) { return "", "" })(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
		}))
	handler.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest(http.MethodDelete, "/api/sync/42", nil))

	rows := auditRows(t, db)
	if len(rows) != 1 {
		t.Fatalf("recorded %d rows, want the attempt recorded anyway", len(rows))
	}
	if rows[0].Username != "" || rows[0].Status != http.StatusUnauthorized {
		t.Errorf("recorded %+v", rows[0])
	}
}

func TestCallerAddressPrefersTheForwardedFor(t *testing.T) {
	for _, c := range []struct {
		name, forwarded, remote, want string
	}{
		{"behind an ingress", "203.0.113.7, 10.0.0.1", "10.0.0.1:443", "203.0.113.7"},
		{"one hop", "203.0.113.7", "10.0.0.1:443", "203.0.113.7"},
		{"no header", "", "10.0.0.9:54321", "10.0.0.9"},
		{"no port", "", "10.0.0.9", "10.0.0.9"},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.RemoteAddr = c.remote
			if c.forwarded != "" {
				r.Header.Set("X-Forwarded-For", c.forwarded)
			}
			if got := callerAddress(r); got != c.want {
				t.Errorf("callerAddress() = %q, want %q", got, c.want)
			}
		})
	}
}
