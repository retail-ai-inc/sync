package identityhttp

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func postJSON(h http.HandlerFunc, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h(rec, req)
	return rec
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

				rec := postJSON(tc.handler, tc.method, tc.path, body)
				if rec.Code != http.StatusBadRequest {
					t.Errorf("status = %d, want 400 (body: %q)", rec.Code, rec.Body.String())
				}
			})
		}
	}
}

func TestUpdateUserAccessRejectsAnEmptyUserID(t *testing.T) {

	rec := postJSON(UpdateUserAccessHandler, http.MethodPut, "/users/access", `{"access":"admin"}`)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestUpdateUserAccessRejectsAnUnknownAccessLevel(t *testing.T) {

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

	rec := postJSON(UpdateUserAccessHandler, http.MethodPut, "/users/access", `{"userId":"u1","access":"root"}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d — the handler appears to return a real status now; assert it instead", rec.Code)
	}
}

func TestDeleteUserRejectsAnEmptyUserID(t *testing.T) {

	rec := postJSON(DeleteUserHandler, http.MethodDelete, "/users", `{"userId":""}`)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestAuthCurrentUserRequiresAToken(t *testing.T) {

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

	rec := postJSON(UpdateAdminPasswordHandler, http.MethodPut, "/updateAdminPassword", `{"newPassword":"x"}`)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
}

func TestUpdatePasswordRequiresAnIdentity(t *testing.T) {

	rec := postJSON(UpdatePasswordHandler, http.MethodPut, "/updatePassword", `{"newPassword":"x"}`)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
}

func TestGetOAuthConfigRequiresAProvider(t *testing.T) {

	rec := httptest.NewRecorder()
	GetOAuthConfigHandler(rec, httptest.NewRequest(http.MethodGet, "/oauth//config", nil))

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}
