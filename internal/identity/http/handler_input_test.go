package identityhttp

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/retail-ai-inc/sync/internal/identity/domain"
)

// postJSON runs a handler over a request body and returns the recorder.
func postJSON(h http.HandlerFunc, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h(rec, req)
	return rec
}

// resetSessionGlobals restores the process-wide session, which several
// handlers both read and write.
func resetSessionGlobals(t *testing.T) {
	t.Helper()

	prevAccess, prevUser := domain.Current().Access(), domain.Current().Username()
	t.Cleanup(func() {
		domain.Current().Authenticate(prevUser, prevAccess)
	})
	domain.Current().Clear()
}

func TestHandlersRejectMalformedJSON(t *testing.T) {
	cases := []struct {
		name    string
		handler http.HandlerFunc
		method  string
		path    string
	}{
		{"login", AuthLoginHandler, http.MethodPost, "/login"},
		{"google callback", AuthGoogleCallbackHandler, http.MethodPost, "/login/google/callback"},
		{"user access", UpdateUserAccessHandler, http.MethodPut, "/users/access"},
		{"delete user", DeleteUserHandler, http.MethodDelete, "/users"},
	}

	for _, tc := range cases {
		for _, body := range []string{"", "{", "not json", `{"a":}`, `[1,2,3`} {
			t.Run(fmt.Sprintf("%s/%q", tc.name, body), func(t *testing.T) {
				resetSessionGlobals(t)

				rec := postJSON(tc.handler, tc.method, tc.path, body)
				if rec.Code != http.StatusBadRequest {
					t.Errorf("status = %d, want 400 (body: %q)", rec.Code, rec.Body.String())
				}
			})
		}
	}
}

func TestUpdateUserAccessRejectsAnEmptyUserID(t *testing.T) {
	resetSessionGlobals(t)

	rec := postJSON(UpdateUserAccessHandler, http.MethodPut, "/users/access", `{"access":"admin"}`)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestUpdateUserAccessRejectsAnUnknownAccessLevel(t *testing.T) {
	resetSessionGlobals(t)

	for _, level := range []string{"root", "superuser", "Admin", "GUEST", "owner"} {
		body := fmt.Sprintf(`{"userId":"u1","access":%q}`, level)
		rec := postJSON(UpdateUserAccessHandler, http.MethodPut, "/users/access", body)

		var resp map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("access %q: body is not JSON: %v (%q)", level, err, rec.Body.String())
		}
		if resp["success"] != false {
			t.Errorf("access %q: success = %v, want false", level, resp["success"])
		}
	}
}

// An invalid access level is reported in the body but served as 200 OK, so a
// client that branches on the status code treats a rejected privilege change
// as applied.
func TestARejectedAccessLevelStillReturnsHTTP200(t *testing.T) {
	resetSessionGlobals(t)

	rec := postJSON(UpdateUserAccessHandler, http.MethodPut, "/users/access", `{"userId":"u1","access":"root"}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d — the handler appears to return a real status now; assert it instead", rec.Code)
	}
}

func TestDeleteUserRejectsAnEmptyUserID(t *testing.T) {
	resetSessionGlobals(t)

	rec := postJSON(DeleteUserHandler, http.MethodDelete, "/users", `{"userId":""}`)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestAuthCurrentUserRequiresAToken(t *testing.T) {
	resetSessionGlobals(t)

	for _, header := range []string{"", "Bearer", "Bearer nonsense", "nonsense", "Basic dXNlcjpwYXNz"} {
		req := httptest.NewRequest(http.MethodGet, "/currentUser", nil)
		if header != "" {
			req.Header.Set("Authorization", header)
		}
		rec := httptest.NewRecorder()

		AuthCurrentUserHandler(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("Authorization %q: status = %d, want 401", header, rec.Code)
		}
	}
}

// The 401 body carries "success": true alongside errorCode 401, so a client
// keying off that field reads an authentication failure as a success.
func TestTheUnauthorizedBodyClaimsSuccess(t *testing.T) {
	resetSessionGlobals(t)

	rec := httptest.NewRecorder()
	AuthCurrentUserHandler(rec, httptest.NewRequest(http.MethodGet, "/currentUser", nil))

	var resp map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("body is not JSON: %v (%q)", err, rec.Body.String())
	}
	if resp["success"] != true {
		t.Fatalf("success = %v — the 401 body appears to be fixed; assert false instead", resp["success"])
	}
	if resp["errorCode"] != "401" {
		t.Errorf("errorCode = %v, want \"401\"", resp["errorCode"])
	}
}

func TestUpdateAdminPasswordRequiresAnAuthorizationHeader(t *testing.T) {
	resetSessionGlobals(t)

	rec := postJSON(UpdateAdminPasswordHandler, http.MethodPut, "/updateAdminPassword", `{"newPassword":"x"}`)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
}

func TestUpdateAdminPasswordRejectsANonAdminToken(t *testing.T) {
	resetSessionGlobals(t)

	req := httptest.NewRequest(http.MethodPut, "/updateAdminPassword", strings.NewReader(`{"newPassword":"x"}`))
	req.Header.Set("Authorization", "Bearer "+domain.GenerateUserToken("bob", "guest"))
	rec := httptest.NewRecorder()

	UpdateAdminPasswordHandler(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
}

func TestUpdatePasswordRequiresAnIdentity(t *testing.T) {
	resetSessionGlobals(t)

	rec := postJSON(UpdatePasswordHandler, http.MethodPut, "/updatePassword", `{"newPassword":"x"}`)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
}

func TestGetOAuthConfigRequiresAProvider(t *testing.T) {
	resetSessionGlobals(t)

	rec := httptest.NewRecorder()
	GetOAuthConfigHandler(rec, httptest.NewRequest(http.MethodGet, "/oauth//config", nil))

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestUpdateOAuthConfigRequiresAnAdminToken(t *testing.T) {
	resetSessionGlobals(t)

	t.Run("no header", func(t *testing.T) {
		rec := postJSON(UpdateOAuthConfigHandler, http.MethodPut, "/oauth/google/config", `{}`)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want 401", rec.Code)
		}
	})

	t.Run("guest token", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/oauth/google/config", strings.NewReader(`{}`))
		req.Header.Set("Authorization", "Bearer "+domain.GenerateUserToken("bob", "guest"))
		rec := httptest.NewRecorder()

		UpdateOAuthConfigHandler(rec, req)

		if rec.Code != http.StatusForbidden {
			t.Errorf("status = %d, want 403", rec.Code)
		}
	})
}

// The session is a pair of package-level variables rather than per-request
// state, so there is exactly one "current user" for the whole process. A
// second client logging in silently reassigns who every session-reading
// handler thinks it is talking to.
func TestTheSessionIsProcessGlobal(t *testing.T) {
	resetSessionGlobals(t)

	domain.Current().Authenticate("admin", "admin")

	// Nobody presents a credential, yet the admin-only handler answers.
	rec := httptest.NewRecorder()
	GetAdminTokenHandler(rec, httptest.NewRequest(http.MethodGet, "/getAdminToken", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d — the handler appears to read per-request state now; assert that instead", rec.Code)
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("body is not JSON: %v (%q)", err, rec.Body.String())
	}
	data, _ := resp["data"].(map[string]interface{})
	token, _ := data["accessToken"].(string)
	if !domain.ValidateAdminToken(token) {
		t.Fatalf("no admin token was issued to an unauthenticated caller (body: %s)", rec.Body.String())
	}
}

// Logging out is a global operation: it clears the one session pair, so one
// client's logout revokes the session every other client is riding on.
func TestLogoutClearsTheSessionForEveryone(t *testing.T) {
	resetSessionGlobals(t)

	domain.Current().Authenticate("admin", "admin")

	rec := httptest.NewRecorder()
	AuthLogoutHandler(rec, httptest.NewRequest(http.MethodPost, "/logout", nil))

	if domain.Current().Access() != "" || domain.Current().Username() != "" {
		t.Fatalf("access = %q, username = %q — logout appears to be per-session now",
			domain.Current().Access(), domain.Current().Username())
	}

	rec = httptest.NewRecorder()
	GetAdminTokenHandler(rec, httptest.NewRequest(http.MethodGet, "/getAdminToken", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("after logout the admin handler returned %d, want 401", rec.Code)
	}
}

// The session variables are written from handler goroutines with no
// synchronisation, so concurrent requests race on them. Demonstrating that
// costs the whole package: the race detector aborts it, and CI runs
// `go test -race ./...`. The test is therefore skipped unless asked for.
//
//	SYNC_RACE_PROOF=1 go test -race -run TestConcurrentLoginsRace ./pkg/api/
//
// See T-070 in docs/TEST_FINDINGS.md.
func TestConcurrentLoginsRaceOnTheSessionGlobals(t *testing.T) {
	if os.Getenv("SYNC_RACE_PROOF") == "" {
		t.Skip("set SYNC_RACE_PROOF=1 together with -race to demonstrate T-070")
	}

	resetSessionGlobals(t)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := httptest.NewRecorder()
			GetAdminTokenHandler(rec, httptest.NewRequest(http.MethodGet, "/getAdminToken", nil))
		}()
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := httptest.NewRecorder()
			AuthLogoutHandler(rec, httptest.NewRequest(http.MethodPost, "/logout", nil))
		}()
	}
	wg.Wait()
}
