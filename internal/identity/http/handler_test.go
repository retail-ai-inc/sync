package identityhttp

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/retail-ai-inc/sync/internal/identity/domain"
)

func TestAuthLoginHandlerIssuesATokenForACorrectPassword(t *testing.T) {
	db := useTempDB(t)
	resetSessionGlobals(t)
	insertUser(t, db, "alice", "secret", "Alice", domain.AccessAdmin)

	rec := postJSON(AuthLoginHandler, http.MethodPost, "/login",
		`{"username":"alice","password":"secret","type":"account"}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (body: %q)", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q", got)
	}
	resp := envelope(t, rec)
	if resp["status"] != "ok" {
		t.Errorf("status = %v", resp["status"])
	}
	if resp["currentAuthority"] != domain.AccessAdmin {
		t.Errorf("currentAuthority = %v", resp["currentAuthority"])
	}
	if resp["type"] != "account" {
		t.Errorf("type = %v, want the request's", resp["type"])
	}
	if resp["accessToken"] == "" || resp["accessToken"] == nil {
		t.Error("no token was returned")
	}
}

// TestARejectedLoginAnswersHTTP200 records that a wrong password is reported
// inside the body rather than by the status code, so a caller that only looks at
// the status treats a failed login as a success.
func TestARejectedLoginAnswersHTTP200(t *testing.T) {
	db := useTempDB(t)
	resetSessionGlobals(t)
	insertUser(t, db, "alice", "secret", "Alice", domain.AccessAdmin)

	rec := postJSON(AuthLoginHandler, http.MethodPost, "/login",
		`{"username":"alice","password":"wrong","type":"account"}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; a rejected login appears to answer with a status code "+
			"now, so assert that instead", rec.Code)
	}
	resp := envelope(t, rec)
	if resp["status"] != "error" {
		t.Errorf("status = %v, want error", resp["status"])
	}
	if resp["currentAuthority"] != domain.AccessGuest {
		t.Errorf("currentAuthority = %v, want guest", resp["currentAuthority"])
	}
	if _, ok := resp["accessToken"]; ok {
		t.Error("a token was returned for a rejected login")
	}
}

func TestAuthLoginHandlerReportsAStoreFailure(t *testing.T) {
	emptyIdentityDB(t)
	resetSessionGlobals(t)

	rec := postJSON(AuthLoginHandler, http.MethodPost, "/login",
		`{"username":"alice","password":"secret"}`)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500 (body: %q)", rec.Code, rec.Body.String())
	}
}

func TestAuthCurrentUserHandlerReturnsTheProfile(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", domain.AccessAdmin)
	token := domain.GenerateUserToken("alice", domain.AccessAdmin)

	req := httptest.NewRequest(http.MethodGet, "/currentUser", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	AuthCurrentUserHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (body: %q)", rec.Code, rec.Body.String())
	}
	resp := envelope(t, rec)
	if resp["success"] != true {
		t.Errorf("success = %v", resp["success"])
	}
	data, _ := resp["data"].(map[string]interface{})
	if data["name"] != "Alice" || data["access"] != domain.AccessAdmin {
		t.Errorf("data = %v", data)
	}
}

func TestAuthCurrentUserHandlerReportsAStoreFailure(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", domain.AccessAdmin)
	token := domain.GenerateUserToken("alice", domain.AccessAdmin)

	// The token validates against the table, then the profile read is made to
	// fail by dropping the table underneath it.
	if _, err := db.Exec(`ALTER TABLE users RENAME TO users_gone`); err != nil {
		t.Fatalf("rename table: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/currentUser", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	AuthCurrentUserHandler(rec, req)

	// With no users table the token cannot validate either, so the answer is the
	// 401 shape rather than the 500 one.
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401 (body: %q)", rec.Code, rec.Body.String())
	}
}

func TestAuthLogoutHandlerClearsTheSession(t *testing.T) {
	resetSessionGlobals(t)
	domain.Current().Authenticate("alice", domain.AccessAdmin)

	rec := postJSON(AuthLogoutHandler, http.MethodPost, "/logout", "")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if envelope(t, rec)["success"] != true {
		t.Error("success is not true")
	}
	if domain.Current().Username() != "" {
		t.Errorf("the session still holds %q", domain.Current().Username())
	}
}

func TestUpdatePasswordHandlerChangesThePassword(t *testing.T) {
	db := useTempDB(t)
	resetSessionGlobals(t)
	insertUser(t, db, "alice", "secret", "Alice", domain.AccessAdmin)
	token := domain.GenerateUserToken("alice", domain.AccessAdmin)

	req := httptest.NewRequest(http.MethodPut, "/updatePassword",
		strings.NewReader(`{"oldPassword":"secret","newPassword":"newsecret"}`))
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	UpdatePasswordHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (body: %q)", rec.Code, rec.Body.String())
	}
	if envelope(t, rec)["success"] != true {
		t.Errorf("success is not true: %q", rec.Body.String())
	}

	var stored string
	if err := db.QueryRow(`SELECT password FROM users WHERE username='alice'`).Scan(&stored); err != nil {
		t.Fatalf("read password: %v", err)
	}
	if stored != "newsecret" {
		t.Errorf("the stored password is %q", stored)
	}
}

func TestUpdatePasswordHandlerRejectsAWrongOldPassword(t *testing.T) {
	db := useTempDB(t)
	resetSessionGlobals(t)
	insertUser(t, db, "alice", "secret", "Alice", domain.AccessAdmin)
	token := domain.GenerateUserToken("alice", domain.AccessAdmin)

	req := httptest.NewRequest(http.MethodPut, "/updatePassword",
		strings.NewReader(`{"oldPassword":"wrong","newPassword":"newsecret"}`))
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	UpdatePasswordHandler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body: %q)", rec.Code, rec.Body.String())
	}
	resp := envelope(t, rec)
	if resp["errorMessage"] != "Old password is incorrect" {
		t.Errorf("errorMessage = %v", resp["errorMessage"])
	}
}

func TestUpdatePasswordHandlerReportsALookupFailure(t *testing.T) {
	emptyIdentityDB(t)
	resetSessionGlobals(t)
	domain.Current().Authenticate("alice", domain.AccessAdmin)

	rec := postJSON(UpdatePasswordHandler, http.MethodPut, "/updatePassword",
		`{"oldPassword":"secret","newPassword":"new"}`)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (body: %q)", rec.Code, rec.Body.String())
	}
	if envelope(t, rec)["errorMessage"] != "Internal server error" {
		t.Errorf("errorMessage = %v", envelope(t, rec)["errorMessage"])
	}
}

// TestTheUnauthorizedPasswordBodyHasNoContentType records that the failure
// helper writes the status before setting the header, so the Set never reaches
// the wire. That ordering is preserved from the original handlers because it is
// observable: the response a client receives carries no Content-Type, and Go
// sniffs one from the body instead.
//
// It has to be read off Result(), which is the snapshot taken at WriteHeader.
// ResponseRecorder.Header() hands back the live map, where the later Set is
// still visible — so a test that reads it sees a header the client never gets.
func TestTheUnauthorizedPasswordBodyHasNoContentType(t *testing.T) {
	useTempDB(t)
	resetSessionGlobals(t)

	rec := postJSON(UpdatePasswordHandler, http.MethodPut, "/updatePassword", `{}`)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	if got := rec.Result().Header.Get("Content-Type"); got == "application/json" {
		t.Fatalf("Content-Type = %q; the header is set before the status now, so "+
			"assert that instead", got)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("the live header map holds %q; the handler no longer sets it after "+
			"the status", got)
	}
}

// TestASuccessfulPasswordChangeDoesCarryContentType is the other side of that:
// the success path sets the header before writing anything, so it reaches the
// client.
func TestASuccessfulPasswordChangeDoesCarryContentType(t *testing.T) {
	db := useTempDB(t)
	resetSessionGlobals(t)
	insertUser(t, db, "alice", "secret", "Alice", domain.AccessAdmin)
	token := domain.GenerateUserToken("alice", domain.AccessAdmin)

	req := httptest.NewRequest(http.MethodPut, "/updatePassword",
		strings.NewReader(`{"oldPassword":"secret","newPassword":"new"}`))
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	UpdatePasswordHandler(rec, req)

	if got := rec.Result().Header.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q on the success path, want application/json", got)
	}
}

func TestUpdateAdminPasswordHandlerChangesThePassword(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "admin", "secret", "Admin", domain.AccessAdmin)

	req := httptest.NewRequest(http.MethodPut, "/updateAdminPassword",
		strings.NewReader(`{"oldPassword":"secret","newPassword":"newsecret"}`))
	req.Header.Set("Authorization", "Bearer "+domain.GenerateAdminToken())
	rec := httptest.NewRecorder()
	UpdateAdminPasswordHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (body: %q)", rec.Code, rec.Body.String())
	}

	var stored string
	if err := db.QueryRow(`SELECT password FROM users WHERE username='admin'`).Scan(&stored); err != nil {
		t.Fatalf("read password: %v", err)
	}
	if stored != "newsecret" {
		t.Errorf("the stored password is %q", stored)
	}
}

func TestUpdateAdminPasswordHandlerRejectsAWrongOldPassword(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "admin", "secret", "Admin", domain.AccessAdmin)

	req := httptest.NewRequest(http.MethodPut, "/updateAdminPassword",
		strings.NewReader(`{"oldPassword":"wrong","newPassword":"x"}`))
	req.Header.Set("Authorization", "Bearer "+domain.GenerateAdminToken())
	rec := httptest.NewRecorder()
	UpdateAdminPasswordHandler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 (body: %q)", rec.Code, rec.Body.String())
	}
}

// TestTheAdminPasswordEndpointAlwaysTargetsTheAdminRow records that the username
// is hardcoded: the endpoint changes the password of the row called "admin",
// whoever presented the token. The admin token is derived from that same row, so
// the two agree — but the endpoint cannot be used for anyone else.
func TestTheAdminPasswordEndpointAlwaysTargetsTheAdminRow(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "admin", "adminpw", "Admin", domain.AccessAdmin)
	insertUser(t, db, "alice", "alicepw", "Alice", domain.AccessAdmin)

	req := httptest.NewRequest(http.MethodPut, "/updateAdminPassword",
		strings.NewReader(`{"oldPassword":"alicepw","newPassword":"x"}`))
	req.Header.Set("Authorization", "Bearer "+domain.GenerateAdminToken())
	rec := httptest.NewRecorder()
	UpdateAdminPasswordHandler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d; alice's password was accepted, so the endpoint appears "+
			"to read the token's identity now", rec.Code)
	}
}

func TestGetUsersHandlerReturnsThePage(t *testing.T) {
	db := useTempDB(t)
	for _, name := range []string{"a", "b", "c"} {
		insertUser(t, db, name, "secret", "User "+name, domain.AccessGuest)
	}

	req := httptest.NewRequest(http.MethodGet, "/users?current=1&pageSize=2", nil)
	rec := httptest.NewRecorder()
	GetUsersHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (body: %q)", rec.Code, rec.Body.String())
	}
	resp := envelope(t, rec)
	if resp["total"] != float64(3) {
		t.Errorf("total = %v, want 3", resp["total"])
	}
	data, _ := resp["data"].([]interface{})
	if len(data) != 2 {
		t.Errorf("data holds %d users, want 2", len(data))
	}
}

// TestGetUsersHandlerRefusesANonPositivePage records that the endpoint's own
// parsing is what keeps the pagination arithmetic away from a negative index: a
// page number that is not greater than zero falls back to one rather than
// reaching the domain function, which would panic (T-130).
func TestGetUsersHandlerRefusesANonPositivePage(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "a", "secret", "User", domain.AccessGuest)

	for _, query := range []string{"?current=0", "?current=-1", "?current=abc", "?pageSize=0", "?pageSize=-5"} {
		t.Run(query, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/users"+query, nil)
			rec := httptest.NewRecorder()
			GetUsersHandler(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d for %q (body: %q)", rec.Code, query, rec.Body.String())
			}
			if envelope(t, rec)["success"] != true {
				t.Errorf("success is not true for %q", query)
			}
		})
	}
}

// TestGetUsersHandlerAnswersHTTP200ForAStoreFailure records that a failure to
// read the directory is reported inside the body with an empty list, at HTTP
// 200. A caller that only checks the status shows an empty user table.
func TestGetUsersHandlerAnswersHTTP200ForAStoreFailure(t *testing.T) {
	emptyIdentityDB(t)

	req := httptest.NewRequest(http.MethodGet, "/users", nil)
	rec := httptest.NewRecorder()
	GetUsersHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; the failure appears to be reported by status now, so "+
			"assert that instead", rec.Code)
	}
	resp := envelope(t, rec)
	if resp["success"] != false {
		t.Errorf("success = %v, want false", resp["success"])
	}
	if resp["message"] != "Failed to get user list" {
		t.Errorf("message = %v", resp["message"])
	}
}

func TestUpdateUserAccessHandlerAppliesTheChange(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", domain.AccessGuest)

	rec := postJSON(UpdateUserAccessHandler, http.MethodPut, "/users/access",
		`{"userId":"uid-alice","access":"admin","status":"inactive"}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (body: %q)", rec.Code, rec.Body.String())
	}
	resp := envelope(t, rec)
	if resp["success"] != true {
		t.Fatalf("success = %v (body: %q)", resp["success"], rec.Body.String())
	}
	data, _ := resp["data"].(map[string]interface{})
	if data["access"] != domain.AccessAdmin || data["status"] != domain.StatusInactive {
		t.Errorf("data = %v", data)
	}
}

func TestUpdateUserAccessHandlerRefusesAnUnknownUser(t *testing.T) {
	useTempDB(t)

	rec := postJSON(UpdateUserAccessHandler, http.MethodPut, "/users/access",
		`{"userId":"nobody","access":"admin"}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	resp := envelope(t, rec)
	if resp["success"] != false || resp["message"] != "User does not exist" {
		t.Errorf("resp = %v", resp)
	}
}

func TestUpdateUserAccessHandlerReportsAStoreFailureAsPlainText(t *testing.T) {
	emptyIdentityDB(t)

	rec := postJSON(UpdateUserAccessHandler, http.MethodPut, "/users/access",
		`{"userId":"uid","access":"admin"}`)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (body: %q)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "Query user failed") {
		t.Errorf("body = %q, want the query-stage message", rec.Body.String())
	}
}

func TestUpdateUserAccessHandlerReportsAnUnopenableDatabase(t *testing.T) {
	unopenableDB(t)

	rec := postJSON(UpdateUserAccessHandler, http.MethodPut, "/users/access",
		`{"userId":"uid","access":"admin"}`)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "Failed to connect to database") {
		t.Errorf("body = %q, want the connect-stage message", rec.Body.String())
	}
}

func TestDeleteUserHandlerRemovesTheUser(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", domain.AccessGuest)

	rec := postJSON(DeleteUserHandler, http.MethodDelete, "/users", `{"userId":"uid-alice"}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (body: %q)", rec.Code, rec.Body.String())
	}
	if envelope(t, rec)["success"] != true {
		t.Errorf("success is not true: %q", rec.Body.String())
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Errorf("%d users survived", count)
	}
}

func TestDeleteUserHandlerRefusesAnUnknownUser(t *testing.T) {
	useTempDB(t)

	rec := postJSON(DeleteUserHandler, http.MethodDelete, "/users", `{"userId":"nobody"}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if envelope(t, rec)["message"] != "User does not exist" {
		t.Errorf("message = %v", envelope(t, rec)["message"])
	}
}

func TestDeleteUserHandlerReportsAStoreFailureAsPlainText(t *testing.T) {
	emptyIdentityDB(t)

	rec := postJSON(DeleteUserHandler, http.MethodDelete, "/users", `{"userId":"uid"}`)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (body: %q)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "Failed to check user existence") {
		t.Errorf("body = %q, want the check-stage message", rec.Body.String())
	}
}

func TestGetOAuthConfigHandlerReturnsTheConfiguration(t *testing.T) {
	db := useTempDB(t)
	storeOAuthConfig(t, db, "google",
		`{"clientId":"id","clientSecret":"secret","redirectUri":"uri"}`, true)

	req := httptest.NewRequest(http.MethodGet, "/oauth/google/config?provider=google", nil)
	rec := httptest.NewRecorder()
	GetOAuthConfigHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (body: %q)", rec.Code, rec.Body.String())
	}
	resp := envelope(t, rec)
	if resp["success"] != true {
		t.Fatalf("success = %v", resp["success"])
	}
	data, _ := resp["data"].(map[string]interface{})
	if data["clientId"] != "id" {
		t.Errorf("clientId = %v", data["clientId"])
	}
}

// TestTheOAuthConfigIsReadableWithoutCredentials records that the read endpoint
// asks for nothing and returns the client secret, while the write endpoint next
// to it demands an admin token.
func TestTheOAuthConfigIsReadableWithoutCredentials(t *testing.T) {
	db := useTempDB(t)
	storeOAuthConfig(t, db, "google",
		`{"clientId":"id","clientSecret":"top-secret","redirectUri":"uri"}`, true)

	req := httptest.NewRequest(http.MethodGet, "/oauth/google/config?provider=google", nil)
	rec := httptest.NewRecorder()
	GetOAuthConfigHandler(rec, req)

	if !strings.Contains(rec.Body.String(), "top-secret") {
		t.Fatalf("the secret is no longer returned; assert the redaction instead "+
			"(body: %q)", rec.Body.String())
	}
}

// TestAMissingOAuthConfigurationAnswersHTTP200 records that a provider with
// nothing stored is reported inside the body at HTTP 200 rather than as a 404.
func TestAMissingOAuthConfigurationAnswersHTTP200(t *testing.T) {
	useTempDB(t)

	req := httptest.NewRequest(http.MethodGet, "/oauth/google/config?provider=google", nil)
	rec := httptest.NewRecorder()
	GetOAuthConfigHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	resp := envelope(t, rec)
	if resp["success"] != false || resp["error"] != "No specified OAuth configuration found" {
		t.Errorf("resp = %v", resp)
	}
}

func TestGetOAuthConfigHandlerReportsAStoreFailure(t *testing.T) {
	emptyIdentityDB(t)

	req := httptest.NewRequest(http.MethodGet, "/oauth/google/config?provider=google", nil)
	rec := httptest.NewRecorder()
	GetOAuthConfigHandler(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (body: %q)", rec.Code, rec.Body.String())
	}
}

func TestUpdateOAuthConfigHandlerStoresTheConfiguration(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", domain.AccessAdmin)

	req := httptest.NewRequest(http.MethodPut, "/oauth/google/config?provider=google",
		strings.NewReader(`{"clientId":"id","clientSecret":"secret","redirectUri":"uri","enabled":true}`))
	req.Header.Set("Authorization", "Bearer "+domain.GenerateUserToken("alice", domain.AccessAdmin))
	rec := httptest.NewRecorder()
	UpdateOAuthConfigHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (body: %q)", rec.Code, rec.Body.String())
	}
	if envelope(t, rec)["message"] != "Configuration updated successfully" {
		t.Errorf("message = %v", envelope(t, rec)["message"])
	}
}

func TestUpdateOAuthConfigHandlerRefusesAnIncompleteConfiguration(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", domain.AccessAdmin)

	req := httptest.NewRequest(http.MethodPut, "/oauth/google/config?provider=google",
		strings.NewReader(`{"enabled":true}`))
	req.Header.Set("Authorization", "Bearer "+domain.GenerateUserToken("alice", domain.AccessAdmin))
	rec := httptest.NewRecorder()
	UpdateOAuthConfigHandler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body: %q)", rec.Code, rec.Body.String())
	}
	if envelope(t, rec)["error"] != "Missing necessary configuration field: clientId" {
		t.Errorf("error = %v", envelope(t, rec)["error"])
	}
}

func TestUpdateOAuthConfigHandlerRefusesANonAdmin(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "bob", "secret", "Bob", domain.AccessGuest)

	req := httptest.NewRequest(http.MethodPut, "/oauth/google/config?provider=google",
		strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer "+domain.GenerateUserToken("bob", domain.AccessGuest))
	rec := httptest.NewRecorder()
	UpdateOAuthConfigHandler(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403 (body: %q)", rec.Code, rec.Body.String())
	}
}

func TestUpdateOAuthConfigHandlerRefusesAMissingProvider(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", domain.AccessAdmin)

	req := httptest.NewRequest(http.MethodPut, "/oauth/config", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer "+domain.GenerateUserToken("alice", domain.AccessAdmin))
	rec := httptest.NewRecorder()
	UpdateOAuthConfigHandler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if envelope(t, rec)["error"] != "missing provider parameter" {
		t.Errorf("error = %v", envelope(t, rec)["error"])
	}
}

func TestUpdateOAuthConfigHandlerReportsAStoreFailure(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", domain.AccessAdmin)
	token := domain.GenerateUserToken("alice", domain.AccessAdmin)
	if _, err := db.Exec(`ALTER TABLE auth_configs RENAME TO gone`); err != nil {
		t.Fatalf("rename table: %v", err)
	}

	req := httptest.NewRequest(http.MethodPut, "/oauth/google/config?provider=google",
		strings.NewReader(`{"enabled":false}`))
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	UpdateOAuthConfigHandler(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (body: %q)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "Failed to update configuration") {
		t.Errorf("body = %q", rec.Body.String())
	}
}

func TestAuthGoogleCallbackHandlerRefusesAnEmptyCode(t *testing.T) {
	useTempDB(t)
	resetSessionGlobals(t)

	rec := postJSON(AuthGoogleCallbackHandler, http.MethodPost, "/login/google/callback",
		`{"code":""}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	resp := envelope(t, rec)
	if resp["status"] != "error" || resp["errorMessage"] != "Invalid Google authentication data" {
		t.Errorf("resp = %v", resp)
	}
	if resp["currentAuthority"] != domain.AccessGuest {
		t.Errorf("currentAuthority = %v", resp["currentAuthority"])
	}
}

func TestAuthGoogleCallbackHandlerReportsAMissingConfiguration(t *testing.T) {
	useTempDB(t)
	resetSessionGlobals(t)

	rec := postJSON(AuthGoogleCallbackHandler, http.MethodPost, "/login/google/callback",
		`{"code":"a-code"}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if envelope(t, rec)["errorMessage"] != "Please configure Google OAuth information first" {
		t.Errorf("errorMessage = %v", envelope(t, rec)["errorMessage"])
	}
}

// TestEveryGoogleFailureAnswersHTTP200WithTheGuestShape records that the
// callback never uses a status code: every one of its failures is an HTTP 200
// carrying status "error" and currentAuthority "guest".
func TestEveryGoogleFailureAnswersHTTP200WithTheGuestShape(t *testing.T) {
	db := useTempDB(t)
	resetSessionGlobals(t)
	storeOAuthConfig(t, db, "google", `{"clientId":"id"}`, true)

	rec := postJSON(AuthGoogleCallbackHandler, http.MethodPost, "/login/google/callback",
		`{"code":"a-code"}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; the callback appears to use status codes now, so "+
			"assert that instead", rec.Code)
	}
	resp := envelope(t, rec)
	if resp["currentAuthority"] != domain.AccessGuest || resp["type"] != "google" {
		t.Errorf("resp = %v", resp)
	}
}
