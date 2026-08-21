package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The router installs no middleware (see TestTheRouterInstallsNoMiddleware)
// and most handlers perform no credential check of their own, so those
// endpoints are reachable by anyone who can reach the port.
//
// The tests below record that state. They are NOT a specification: the
// permission model for this API is undecided, and every one of them must be
// replaced with a real authorization assertion once it is settled. Each asserts
// only that no 401 or 403 is returned — deliberately not a specific success
// code, so it pins "no credentials required" and nothing else.
//
// See T-072 in docs/TEST_FINDINGS.md.

// unauthenticated runs a handler with no Authorization header and no session,
// and fails if the handler rejected the caller on credentials.
func unauthenticated(t *testing.T, name string, h http.HandlerFunc, req *http.Request, params map[string]string) *httptest.ResponseRecorder {
	t.Helper()

	resetSessionGlobals(t)

	rec := httptest.NewRecorder()
	if params != nil {
		serveWithURLParams(rec, req, h, params)
	} else {
		h(rec, req)
	}

	if rec.Code == http.StatusUnauthorized || rec.Code == http.StatusForbidden {
		t.Fatalf("%s now returns %d without credentials — the permission model appears to have been implemented; replace this with the intended authorization assertion", name, rec.Code)
	}
	return rec
}

func jsonRequest(method, path, body string) *http.Request {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return req
}

// ------------------------------------------------- task management is open

func TestTaskManagementIsReachableWithoutCredentials(t *testing.T) {
	cases := []struct {
		name    string
		handler http.HandlerFunc
		req     *http.Request
		params  map[string]string
	}{
		{"GET /api/sync", SyncListHandler, jsonRequest(http.MethodGet, "/sync", ""), nil},
		{"POST /api/sync", SyncCreateHandler, jsonRequest(http.MethodPost, "/sync",
			`{"taskName":"injected","type":"mysql","sourceConn":{"host":"h"},"targetConn":{"host":"h"}}`), nil},
		{"PUT /api/sync/{id}", SyncUpdateHandler, jsonRequest(http.MethodPut, "/sync/{id}",
			`{"taskName":"renamed"}`), map[string]string{"id": "1"}},
		{"DELETE /api/sync/{id}", SyncDeleteHandler, jsonRequest(http.MethodDelete, "/sync/{id}", ""),
			map[string]string{"id": "1"}},
		{"PUT /api/sync/{id}/start", SyncStartHandler, jsonRequest(http.MethodPut, "/sync/{id}/start", ""),
			map[string]string{"id": "1"}},
		{"PUT /api/sync/{id}/stop", SyncStopHandler, jsonRequest(http.MethodPut, "/sync/{id}/stop", ""),
			map[string]string{"id": "1"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			useTempTaskDB(t)
			unauthenticated(t, tc.name, tc.handler, tc.req, tc.params)
		})
	}
}

// An unauthenticated caller can create a sync task, and the task is persisted.
// Task configuration is what getRowCountWithContext interpolates into SQL
// (T-098), so this is also the entry point for that.
func TestAnUnauthenticatedCallerCanPersistASyncTask(t *testing.T) {
	conn := useTempTaskDB(t)
	resetSessionGlobals(t)

	rec := httptest.NewRecorder()
	SyncCreateHandler(rec, jsonRequest(http.MethodPost, "/sync",
		`{"taskName":"created without credentials","type":"mysql",
		  "sourceConn":{"host":"h","port":"3306","user":"u","password":"p","database":"d"},
		  "targetConn":{"host":"h","port":"3306","user":"u","password":"p","database":"d"},
		  "mappings":[]}`))

	if rec.Code == http.StatusUnauthorized || rec.Code == http.StatusForbidden {
		t.Fatalf("POST /api/sync now returns %d — replace this with the intended authorization assertion", rec.Code)
	}

	var n int
	if err := conn.QueryRow("SELECT COUNT(*) FROM sync_tasks").Scan(&n); err != nil {
		t.Fatalf("count sync_tasks: %v", err)
	}
	if n == 0 {
		t.Fatalf("the task was not persisted (body: %s) — creation appears to be gated now", rec.Body.String())
	}
}

// ---------------------------------------------- user management is open

func TestUserManagementIsReachableWithoutCredentials(t *testing.T) {
	cases := []struct {
		name    string
		handler http.HandlerFunc
		req     *http.Request
	}{
		{"GET /api/users", GetUsersHandler, jsonRequest(http.MethodGet, "/users", "")},
		{"PUT /api/users/access", UpdateUserAccessHandler, jsonRequest(http.MethodPut, "/users/access",
			`{"userId":"u1","access":"admin"}`)},
		{"DELETE /api/users", DeleteUserHandler, jsonRequest(http.MethodDelete, "/users", `{"userId":"u1"}`)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			useTempDB(t)
			unauthenticated(t, tc.name, tc.handler, tc.req, nil)
		})
	}
}

// GET /api/users needs no credentials and returns every account's identity and
// privilege level. It does at least strip the password (see
// TestGetUsersHandlerDoesNotLeakPasswords).
func TestTheUserDirectoryIsPubliclyReadable(t *testing.T) {
	conn := useTempDB(t)
	insertUser(t, conn, "admin", "s3cret", "Administrator", "admin")
	insertUser(t, conn, "bob", "hunter2", "Bob", "guest")
	resetSessionGlobals(t)

	rec := httptest.NewRecorder()
	GetUsersHandler(rec, jsonRequest(http.MethodGet, "/users", ""))

	if rec.Code == http.StatusUnauthorized || rec.Code == http.StatusForbidden {
		t.Fatalf("GET /api/users now returns %d — replace this with the intended authorization assertion", rec.Code)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "Administrator") || !strings.Contains(body, "admin") {
		t.Fatalf("the directory no longer lists accounts — assert the new response instead: %s", body)
	}
	// Passwords must not be there regardless of the permission model.
	for _, secret := range []string{"s3cret", "hunter2"} {
		if strings.Contains(body, secret) {
			t.Errorf("a password leaked into the response: %s", body)
		}
	}
}

// An unauthenticated caller can grant itself admin.
func TestAnUnauthenticatedCallerCanGrantAdmin(t *testing.T) {
	conn := useTempDB(t)
	insertUser(t, conn, "bob", "pw", "Bob", "guest")
	// UpdateUserAccessHandler looks users up by the userId column.
	const userID = "google_20260821000000"
	if _, err := conn.Exec("UPDATE users SET userId = ? WHERE username = 'bob'", userID); err != nil {
		t.Fatalf("set userId: %v", err)
	}
	resetSessionGlobals(t)

	rec := httptest.NewRecorder()
	UpdateUserAccessHandler(rec, jsonRequest(http.MethodPut, "/users/access",
		`{"userId":"`+userID+`","access":"admin"}`))

	if rec.Code == http.StatusUnauthorized || rec.Code == http.StatusForbidden {
		t.Fatalf("PUT /api/users/access now returns %d — replace this with the intended authorization assertion", rec.Code)
	}

	var access string
	if err := conn.QueryRow("SELECT access FROM users WHERE username='bob'").Scan(&access); err != nil {
		t.Fatalf("read access: %v", err)
	}
	if access != "admin" {
		t.Fatalf("access = %q — the privilege change appears to be gated now; assert the rejection instead (body: %s)", access, rec.Body.String())
	}
}

// ------------------------------------------------ backup control is open

func TestBackupControlIsReachableWithoutCredentials(t *testing.T) {
	cases := []struct {
		name    string
		handler http.HandlerFunc
		req     *http.Request
		params  map[string]string
	}{
		{"GET /api/backup", BackupListHandler, jsonRequest(http.MethodGet, "/backup", ""), nil},
		{"POST /api/backup", BackupCreateHandler, jsonRequest(http.MethodPost, "/backup",
			`{"name":"injected","sourceType":"mysql","schedule":"0 3 * * *"}`), nil},
		{"DELETE /api/backup/{id}", BackupDeleteHandler, jsonRequest(http.MethodDelete, "/backup/{id}", ""),
			map[string]string{"id": "1"}},
		{"PUT /api/backup/{id}/pause", BackupPauseHandler, jsonRequest(http.MethodPut, "/backup/{id}/pause", ""),
			map[string]string{"id": "1"}},
		{"PUT /api/backup/{id}/resume", BackupResumeHandler, jsonRequest(http.MethodPut, "/backup/{id}/resume", ""),
			map[string]string{"id": "1"}},
		{"GET /api/backup/status/{taskId}", BackupStatusHandler,
			jsonRequest(http.MethodGet, "/backup/status/{taskId}", ""), map[string]string{"taskId": "unknown"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			useTempTaskDB(t)
			resetTaskStatus(t)
			unauthenticated(t, tc.name, tc.handler, tc.req, tc.params)
		})
	}
}

// ------------------------------------------ database access endpoints

// POST /api/test-connection probes an arbitrary host and port supplied by the
// caller and reports whether it answered, with no credentials required.
func TestConnectionProbeIsReachableWithoutCredentials(t *testing.T) {
	rec := unauthenticated(t, "POST /api/test-connection", TestConnectionHandler,
		jsonRequest(http.MethodPost, "/test-connection",
			`{"dbType":"mysql","host":"127.0.0.1","port":"1","user":"u","password":"p","database":"d"}`), nil)

	if rec.Code == http.StatusOK {
		t.Logf("the probe reported success: %s", rec.Body.String())
	}
}

// POST /api/tables/schema reads a schema from any reachable database using
// credentials the caller supplies, with no authentication of the caller.
func TestSchemaReadIsReachableWithoutCredentials(t *testing.T) {
	unauthenticated(t, "POST /api/tables/schema", GetTableSchemaHandler,
		jsonRequest(http.MethodPost, "/tables/schema",
			`{"sourceType":"mysql","connection":{"host":"127.0.0.1","port":"1","user":"u","password":"p","database":"d"},"tableName":"t"}`), nil)
}

// ------------------------------------------------ monitoring is open

func TestMonitoringIsReachableWithoutCredentials(t *testing.T) {
	cases := []struct {
		name    string
		handler http.HandlerFunc
		req     *http.Request
		params  map[string]string
	}{
		{"GET /api/sync/{id}/monitor", SyncMonitorHandler,
			jsonRequest(http.MethodGet, "/sync/{id}/monitor", ""), map[string]string{"id": "1"}},
		{"GET /api/sync/{id}/metrics", SyncMetricsHandler,
			jsonRequest(http.MethodGet, "/sync/{id}/metrics", ""), map[string]string{"id": "1"}},
		{"GET /api/sync/{id}/logs", SyncLogsHandler,
			jsonRequest(http.MethodGet, "/sync/{id}/logs", ""), map[string]string{"id": "1"}},
		{"GET /api/sync/{id}/tables", SyncTablesHandler,
			jsonRequest(http.MethodGet, "/sync/{id}/tables", ""), map[string]string{"id": "1"}},
		{"GET /api/changestreams/status", ChangeStreamsStatusHandler,
			jsonRequest(http.MethodGet, "/changestreams/status", ""), nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			useMonitorDB(t)
			unauthenticated(t, tc.name, tc.handler, tc.req, tc.params)
		})
	}
}

// GET /api/oauth/{provider}/config is readable without credentials while its
// PUT counterpart requires an admin token — the two halves of the same
// resource disagree.
func TestOAuthConfigReadIsOpenWhileWriteIsGated(t *testing.T) {
	conn := useTempDB(t)
	if _, err := conn.Exec(
		`INSERT INTO auth_configs (provider, config_json, enabled) VALUES ('google', ?, 1)`,
		`{"clientId":"cid","clientSecret":"csecret"}`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Read: no credentials needed. The provider must come from the query
	// string, not the path — see TestTheOAuthPathParameterIsIgnored.
	rec := unauthenticated(t, "GET /api/oauth/{provider}/config", GetOAuthConfigHandler,
		jsonRequest(http.MethodGet, "/oauth/google/config?provider=google", ""), nil)
	if !strings.Contains(rec.Body.String(), "cid") {
		t.Errorf("the config was not returned: %s", rec.Body.String())
	}

	// Write: admin token required, so the asymmetry is real.
	resetSessionGlobals(t)
	wrec := httptest.NewRecorder()
	UpdateOAuthConfigHandler(wrec, jsonRequest(http.MethodPut, "/oauth/google/config?provider=google", `{}`))
	if wrec.Code != http.StatusUnauthorized {
		t.Fatalf("PUT returned %d, want 401 — the two halves appear to agree now; assert the shared policy instead", wrec.Code)
	}
}

// The client secret is served to unauthenticated callers along with the rest of
// the OAuth configuration.
func TestTheOAuthClientSecretIsPubliclyReadable(t *testing.T) {
	conn := useTempDB(t)
	if _, err := conn.Exec(
		`INSERT INTO auth_configs (provider, config_json, enabled) VALUES ('google', ?, 1)`,
		`{"clientId":"cid","clientSecret":"the-client-secret"}`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	resetSessionGlobals(t)

	rec := httptest.NewRecorder()
	GetOAuthConfigHandler(rec, jsonRequest(http.MethodGet, "/oauth/google/config?provider=google", ""))

	if !strings.Contains(rec.Body.String(), "the-client-secret") {
		t.Fatalf("the client secret is no longer served — it appears to be redacted; assert the redaction instead (body: %s)", rec.Body.String())
	}
}

// auth_handler.go imports github.com/go-chi/chi (v1) while router.go registers
// its routes with github.com/go-chi/chi/v5. chi v1's URLParam reads a route
// context key that v5 never populates, so every path parameter read in
// auth_handler.go is empty. Both OAuth handlers happen to fall back to a query
// string, so the route as documented — /api/oauth/{provider}/config — answers
// 400 unless the caller also repeats the provider as ?provider=.
func TestTheOAuthPathParameterIsIgnored(t *testing.T) {
	conn := useTempDB(t)
	if _, err := conn.Exec(
		`INSERT INTO auth_configs (provider, config_json, enabled) VALUES ('google', ?, 1)`,
		`{"clientId":"cid"}`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	resetSessionGlobals(t)

	router := NewRouter()

	// The documented route shape.
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/oauth/google/config", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("GET /oauth/google/config returned %d — the path parameter appears to work now (chi versions unified?); assert the config instead", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "missing provider parameter") {
		t.Errorf("body = %s, want the missing-provider error", rec.Body.String())
	}

	// The same request with the provider repeated in the query string.
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/oauth/google/config?provider=google", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("the query-string form returned %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "cid") {
		t.Errorf("the config was not returned: %s", rec.Body.String())
	}
}
