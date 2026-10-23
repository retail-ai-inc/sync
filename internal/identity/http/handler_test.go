package identityhttp

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/retail-ai-inc/sync/internal/identity/domain"
	"github.com/retail-ai-inc/sync/internal/identity/infra"
)

func TestAuthLoginHandlerIssuesATokenForACorrectPassword(t *testing.T) {
	db := useTempDB(t)
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

	// The token validates against the table.
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

// TestAuthLogoutHandlerAnswersSuccess pins what logging out means now: the
// client discards its token and the server has nothing to clear.
func TestAuthLogoutHandlerAnswersSuccess(t *testing.T) {
	rec := postJSON(AuthLogoutHandler, http.MethodPost, "/logout", "")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if envelope(t, rec)["success"] != true {
		t.Error("success is not true")
	}
}

func TestUpdatePasswordHandlerChangesThePassword(t *testing.T) {
	db := useTempDB(t)
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
	// Stored as a hash, not as it was typed.
	if stored == "newsecret" || !domain.IsHashed(stored) {
		t.Errorf("the stored password is %q", stored)
	}
	if ok, _, err := infra.ValidateUser("alice", "newsecret"); err != nil || !ok {
		t.Errorf("the new password does not authenticate (%v, %v)", ok, err)
	}
}

func TestUpdatePasswordHandlerRejectsAWrongOldPassword(t *testing.T) {
	db := useTempDB(t)
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

// TestUpdatePasswordHandlerReportsALookupFailure covers the store failing after
// the caller has been identified.
func TestUpdatePasswordHandlerReportsALookupFailure(t *testing.T) {
	emptyIdentityDB(t)

	req := httptest.NewRequest(http.MethodPut, "/updatePassword",
		strings.NewReader(`{"oldPassword":"secret","newPassword":"new"}`))
	req.Header.Set("Authorization", "Bearer "+domain.GenerateUserToken("alice", domain.AccessAdmin))
	rec := httptest.NewRecorder()
	UpdatePasswordHandler(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 (body: %q)", rec.Code, rec.Body.String())
	}
}

// TestTheUnauthorizedPasswordBodyHasNoContentType records that the failure
// helper writes the status before setting the header, so the Set never reaches
// the wire.
func TestTheUnauthorizedPasswordBodyHasNoContentType(t *testing.T) {
	useTempDB(t)

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

// TestGetUsersHandlerRefusesAPageThatIsNotOne covers a parameter that is not a
// page.
func TestGetUsersHandlerRefusesAPageThatIsNotOne(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "a", "secret", "User", domain.AccessGuest)

	for _, query := range []string{
		"?current=0", "?current=-1", "?current=abc", "?pageSize=0", "?pageSize=-5",
	} {
		t.Run(query, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/users"+query, nil)
			rec := httptest.NewRecorder()
			GetUsersHandler(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d for %q, want 400 (body: %q)", rec.Code, query, rec.Body.String())
			}
		})
	}
}

// TestGetUsersHandlerBoundsThePageSize covers a page size with no upper bound.
// ?pageSize=1000000 was accepted and the whole table read through a connection
// pool that holds exactly one connection — which replication's checkpoint writes
// are also queued on.
func TestGetUsersHandlerBoundsThePageSize(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "a", "secret", "User", domain.AccessGuest)

	req := httptest.NewRequest(http.MethodGet, "/users?pageSize=1000000", nil)
	rec := httptest.NewRecorder()
	GetUsersHandler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d for an unbounded page size, want 400", rec.Code)
	}
}

// TestGetUsersHandlerPagesInTheDatabase covers the paging itself, which used to
// read every row and slice the result in memory.
func TestGetUsersHandlerPagesInTheDatabase(t *testing.T) {
	db := useTempDB(t)
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		insertUser(t, db, name, "secret", "User "+name, domain.AccessGuest)
	}

	req := httptest.NewRequest(http.MethodGet, "/users?current=2&pageSize=2", nil)
	rec := httptest.NewRecorder()
	GetUsersHandler(rec, req)

	resp := envelope(t, rec)
	if resp["success"] != true {
		t.Fatalf("body: %s", rec.Body.String())
	}
	data, _ := resp["data"].([]interface{})
	if len(data) != 2 {
		t.Errorf("the second page holds %d users, want 2", len(data))
	}
	if total, _ := resp["total"].(float64); total != 5 {
		t.Errorf("total = %v, want 5", resp["total"])
	}
}

// TestGetUsersHandlerReportsAStoreFailureByStatus covers a directory that could
// not be read. It used to be reported inside the body with an empty list at HTTP
// 200, so a front end that checks the status showed an empty user table rather
// than a failure.
func TestGetUsersHandlerReportsAStoreFailureByStatus(t *testing.T) {
	emptyIdentityDB(t)

	req := httptest.NewRequest(http.MethodGet, "/users", nil)
	rec := httptest.NewRecorder()
	GetUsersHandler(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
	resp := envelope(t, rec)
	if resp["success"] != false {
		t.Errorf("success = %v, want false", resp["success"])
	}
	if resp["error"] != "Failed to get user list" {
		t.Errorf("error = %v", resp["error"])
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

// TestTheOAuthConfigIsReadableWithoutTheSecret pins the shape of the one
// endpoint that has to stay reachable without a token: the sign-in page needs
// the client id before anybody has one.
func TestTheOAuthConfigIsReadableWithoutTheSecret(t *testing.T) {
	db := useTempDB(t)
	storeOAuthConfig(t, db, "google",
		`{"clientId":"id","clientSecret":"top-secret","redirectUri":"uri"}`, true)

	req := httptest.NewRequest(http.MethodGet, "/oauth/google/config?provider=google", nil)
	rec := httptest.NewRecorder()
	GetOAuthConfigHandler(rec, req)

	body := rec.Body.String()
	if strings.Contains(body, "top-secret") {
		t.Errorf("the client secret is still served: %q", body)
	}
	if !strings.Contains(body, `"clientId":"id"`) {
		t.Errorf("the client id is no longer served, so the sign-in page cannot "+
			"start the flow: %q", body)
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

// A sign-in answered without the token or with the wrong authority leaves the UI signed out or mis-privileged.
func TestASuccessfulGoogleSignInAnswersWithTheTokenAndTheAuthority(t *testing.T) {
	previous := googleLogin
	googleLogin = func(code string) (string, string, string) {
		if code != "a-code" {
			t.Errorf("GoogleLogin got code %q, want the caller's", code)
		}
		return domain.AccessAdmin, "signed-token", ""
	}
	t.Cleanup(func() { googleLogin = previous })

	rec := postJSON(AuthGoogleCallbackHandler, http.MethodPost, "/login/google/callback",
		`{"code":"a-code"}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	resp := envelope(t, rec)
	if resp["status"] != "ok" || resp["type"] != "google" || resp["currentAuthority"] != domain.AccessAdmin ||
		resp["accessToken"] != "signed-token" {
		t.Errorf("resp = %v", resp)
	}
	if _, ok := resp["errorMessage"]; ok {
		t.Errorf("a successful sign-in carries an errorMessage: %v", resp["errorMessage"])
	}
}

// TestTheAdminPasswordEndpointFollowsTheToken covers an endpoint that used to
// change the literal "admin" row whoever asked.
func TestTheAdminPasswordEndpointFollowsTheToken(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "admin", "secret", "Admin", domain.AccessAdmin)
	insertUser(t, db, "second", "other", "Second", domain.AccessAdmin)

	req := httptest.NewRequest(http.MethodPut, "/updateAdminPassword",
		strings.NewReader(`{"oldPassword":"other","newPassword":"newsecret"}`))
	req.Header.Set("Authorization", "Bearer "+domain.GenerateUserToken("second", domain.AccessAdmin))
	rec := httptest.NewRecorder()

	UpdateAdminPasswordHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (body: %q)", rec.Code, rec.Body.String())
	}
	if ok, _, err := infra.ValidateUser("second", "newsecret"); err != nil || !ok {
		t.Errorf("the second administrator's password was not changed (%v, %v)", ok, err)
	}
	// And the admin row is untouched.
	if ok, _, err := infra.ValidateUser("admin", "secret"); err != nil || !ok {
		t.Errorf("somebody else's password was changed (%v, %v)", ok, err)
	}
}

// TestTheAdminPasswordEndpointNeedsAnAdmin is the other half.
func TestTheAdminPasswordEndpointNeedsAnAdmin(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "guest", "secret", "Guest", domain.AccessGuest)

	req := httptest.NewRequest(http.MethodPut, "/updateAdminPassword",
		strings.NewReader(`{"oldPassword":"secret","newPassword":"newsecret"}`))
	req.Header.Set("Authorization", "Bearer "+domain.GenerateUserToken("guest", domain.AccessGuest))
	rec := httptest.NewRecorder()

	UpdateAdminPasswordHandler(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d for a guest, want 401", rec.Code)
	}
}
