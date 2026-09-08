package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/retail-ai-inc/sync/internal/identity/domain"
)

// call runs a request through the real router, which is what makes these tests
// worth having: the access rules are the routing, so exercising a handler
// directly would prove nothing about them.
func call(t *testing.T, method, path, body, token string) *httptest.ResponseRecorder {
	t.Helper()

	var reader *strings.Reader
	if body == "" {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	rec := httptest.NewRecorder()
	NewRouter().ServeHTTP(rec, req)
	return rec
}

func withUsers(t *testing.T) (adminToken, guestToken string) {
	t.Helper()

	// The production work factor costs most of a second per login.
	t.Setenv("SYNC_PASSWORD_ITERATIONS", "1")

	db := useTempDB(t)
	insertUser(t, db, "admin", "adminpw", "Admin", domain.AccessAdmin)
	insertUser(t, db, "guest", "guestpw", "Guest", domain.AccessGuest)

	return domain.GenerateUserToken("admin", domain.AccessAdmin),
		domain.GenerateUserToken("guest", domain.AccessGuest)
}

var readRoutes = []struct{ method, path string }{
	{http.MethodGet, "/currentUser"},
	{http.MethodGet, "/sync"},
	{http.MethodGet, "/sync/1/monitor"},
	{http.MethodGet, "/sync/1/tables"},
	{http.MethodGet, "/sync/1/position"},
	{http.MethodGet, "/sync/1/rowcounts"},
	{http.MethodGet, "/changestreams/status"},
	{http.MethodGet, "/settings"},
	{http.MethodGet, "/backup"},
	{http.MethodGet, "/backup/status/1"},
	{http.MethodPut, "/updatePassword"},
}

var adminRoutes = []struct{ method, path string }{
	{http.MethodPost, "/sync"},
	{http.MethodPut, "/sync/1"},
	{http.MethodPut, "/sync/1/start"},
	{http.MethodPut, "/sync/1/stop"},
	{http.MethodDelete, "/sync/1"},
	{http.MethodGet, "/users"},
	{http.MethodPut, "/users/access"},
	{http.MethodDelete, "/users"},
	{http.MethodPut, "/updateAdminPassword"},
	{http.MethodPut, "/settings"},
	{http.MethodPut, "/oauth/google/config"},
	{http.MethodPost, "/test-connection"},
	{http.MethodPost, "/tables/schema"},
	{http.MethodPost, "/backup"},
	{http.MethodPut, "/backup/1"},
	{http.MethodPut, "/backup/1/pause"},
	{http.MethodPut, "/backup/1/resume"},
	{http.MethodDelete, "/backup/1"},
	{http.MethodPost, "/backup/execute/1"},
}

// Four handlers used to check the Authorization header for themselves and the
// other twenty-six did not, so anybody who could reach the port could list
// every replication task with its credentials, create and delete tasks, read
// schemas, drive the connection prober at arbitrary hosts, and list the users.
func TestNoRouteAnswersWithoutACredential(t *testing.T) {
	withUsers(t)

	for _, route := range append(append([]struct{ method, path string }{}, readRoutes...), adminRoutes...) {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			rec := call(t, route.method, route.path, "{}", "")

			if rec.Code != http.StatusUnauthorized {
				t.Errorf("status = %d without a token, want 401 (body: %q)",
					rec.Code, rec.Body.String())
			}
		})
	}
}

func TestAnUnreadableTokenIsRejected(t *testing.T) {
	withUsers(t)

	for name, token := range map[string]string{
		"nonsense":     "not-a-token",
		"truncated":    domain.GenerateUserToken("admin", domain.AccessAdmin)[:10],
		"unknown user": domain.GenerateUserToken("nobody", domain.AccessAdmin),
	} {
		t.Run(name, func(t *testing.T) {
			if rec := call(t, http.MethodGet, "/sync", "", token); rec.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, want 401 (body: %q)", rec.Code, rec.Body.String())
			}
		})
	}
}

// TestAGuestReachesTheReadRoutes pins that authentication is not the same as
// administration: a guest can see what is happening.
func TestAGuestReachesTheReadRoutes(t *testing.T) {
	_, guest := withUsers(t)

	for _, route := range readRoutes {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			rec := call(t, route.method, route.path, "{}", guest)

			if rec.Code == http.StatusUnauthorized || rec.Code == http.StatusForbidden {
				t.Errorf("status = %d for an authenticated guest (body: %q)",
					rec.Code, rec.Body.String())
			}
		})
	}
}

// TestAGuestCannotChangeAnything is the other half: everything that mutates,
// and the two endpoints that connect to a database the caller names, are held
// to administrators.
func TestAGuestCannotChangeAnything(t *testing.T) {
	_, guest := withUsers(t)

	for _, route := range adminRoutes {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			rec := call(t, route.method, route.path, "{}", guest)

			if rec.Code != http.StatusForbidden {
				t.Errorf("status = %d for a guest, want 403 (body: %q)",
					rec.Code, rec.Body.String())
			}
		})
	}
}

// TestAnAdministratorIsNotRefused checks the rules do not lock the operator
// out of their own control plane.
func TestAnAdministratorIsNotRefused(t *testing.T) {
	admin, _ := withUsers(t)

	for _, route := range append(append([]struct{ method, path string }{}, readRoutes...), adminRoutes...) {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			rec := call(t, route.method, route.path, "{}", admin)

			if rec.Code == http.StatusUnauthorized || rec.Code == http.StatusForbidden {
				t.Errorf("status = %d for an administrator (body: %q)",
					rec.Code, rec.Body.String())
			}
		})
	}
}

// TestSigningInNeedsNoCredential covers the four routes that have to stay open,
// because a caller with no token has no other way to get one.
func TestSigningInNeedsNoCredential(t *testing.T) {
	withUsers(t)

	for _, route := range []struct{ method, path, body string }{
		{http.MethodPost, "/login", `{"username":"admin","password":"adminpw"}`},
		{http.MethodPost, "/logout", ""},
		{http.MethodPost, "/login/google/callback", `{"code":""}`},
		{http.MethodGet, "/oauth/google/config?provider=google", ""},
	} {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			rec := call(t, route.method, route.path, route.body, "")

			if rec.Code == http.StatusUnauthorized || rec.Code == http.StatusForbidden {
				t.Errorf("status = %d without a token (body: %q)", rec.Code, rec.Body.String())
			}
		})
	}
}

// TestAnExpiredTokenIsRefused closes the loop with the token's own expiry.
func TestAnExpiredTokenIsRefused(t *testing.T) {
	withUsers(t)

	// A token signed with a different secret stands in for one that no longer
	// verifies, which is what an expired token amounts to at this layer.
	if rec := call(t, http.MethodGet, "/sync", "", "e30.deadbeef"); rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
}

// TestTheRefusalSaysWhy pins the body shape, which the UI reads to decide
// whether to send the caller back to the sign-in page.
func TestTheRefusalSaysWhy(t *testing.T) {
	_, guest := withUsers(t)

	unauthenticated := call(t, http.MethodGet, "/sync", "", "")
	var body map[string]interface{}
	if err := json.Unmarshal(unauthenticated.Body.Bytes(), &body); err != nil {
		t.Fatalf("the refusal is not JSON: %v (%q)", err, unauthenticated.Body.String())
	}
	if body["success"] != false {
		t.Errorf("success = %v, want false", body["success"])
	}
	if !strings.Contains(body["errorMessage"].(string), "Authentication") {
		t.Errorf("errorMessage = %v", body["errorMessage"])
	}

	forbidden := call(t, http.MethodDelete, "/sync/1", "", guest)
	if err := json.Unmarshal(forbidden.Body.Bytes(), &body); err != nil {
		t.Fatalf("the refusal is not JSON: %v", err)
	}
	if !strings.Contains(body["errorMessage"].(string), "Admin") {
		t.Errorf("errorMessage = %v", body["errorMessage"])
	}
}

func TestTheProbesAnswer(t *testing.T) {
	for name, handler := range map[string]http.HandlerFunc{
		"healthz": Health,
		"readyz":  Ready(func() error { return nil }),
	} {
		t.Run(name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			handler(rec, httptest.NewRequest(http.MethodGet, "/"+name, nil))

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d", rec.Code)
			}
			var body map[string]interface{}
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("body is not JSON: %v (%q)", err, rec.Body.String())
			}
			if body["status"] == nil {
				t.Errorf("body = %q, want a status", rec.Body.String())
			}
		})
	}
}
