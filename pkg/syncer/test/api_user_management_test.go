package test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/retail-ai-inc/sync/pkg/api"
)

func testUserManagementApi(t *testing.T) {
	r := api.NewRouter()

	// Test getting users
	getUsers(t, r)

	// Test updating user access
	updateUserAccess(t, r)

	// Test updating password
	updatePassword(t, r)

	// Test updating admin password
	updateAdminPassword(t, r)

	// Test getting admin token
	getAdminToken(t, r)

	// Test deleting a user
	deleteUser(t, r)
}

func getUsers(t *testing.T, r http.Handler) *httptest.ResponseRecorder {
	req, _ := http.NewRequest("GET", "/users?current=1&pageSize=10", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	t.Logf("GET users response: %d", resp.Code)
	return resp
}

func updateUserAccess(t *testing.T, r http.Handler) *httptest.ResponseRecorder {
	reqBody := map[string]string{
		"userId": "test-user-id",
		"access": "guest",
		"status": "active",
	}

	bodyBytes, _ := json.Marshal(reqBody)
	body := bytes.NewBuffer(bodyBytes)

	req, _ := http.NewRequest("PUT", "/users/access", body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer test-token")

	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	t.Logf("UPDATE user access response: %d", resp.Code)
	return resp
}

func updatePassword(t *testing.T, r http.Handler) *httptest.ResponseRecorder {
	reqBody := map[string]string{
		"oldPassword": "old-password",
		"newPassword": "new-password",
	}

	bodyBytes, _ := json.Marshal(reqBody)
	body := bytes.NewBuffer(bodyBytes)

	req, _ := http.NewRequest("PUT", "/updatePassword", body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer test-token")

	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	t.Logf("UPDATE password response: %d", resp.Code)
	return resp
}

func updateAdminPassword(t *testing.T, r http.Handler) *httptest.ResponseRecorder {
	reqBody := map[string]string{
		"oldPassword": "admin",
		"newPassword": "new-admin-password",
	}

	bodyBytes, _ := json.Marshal(reqBody)
	body := bytes.NewBuffer(bodyBytes)

	req, _ := http.NewRequest("PUT", "/updateAdminPassword", body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer test-admin-token")

	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	t.Logf("UPDATE admin password response: %d", resp.Code)
	return resp
}

func getAdminToken(t *testing.T, r http.Handler) *httptest.ResponseRecorder {
	req, _ := http.NewRequest("GET", "/getAdminToken", nil)

	// Set current session state to admin (normally would be done in login)
	// This is just for testing purposes

	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	t.Logf("GET admin token response: %d", resp.Code)
	return resp
}

func deleteUser(t *testing.T, r http.Handler) *httptest.ResponseRecorder {
	reqBody := map[string]string{
		"userId": "test-user-id",
	}

	bodyBytes, _ := json.Marshal(reqBody)
	body := bytes.NewBuffer(bodyBytes)

	req, _ := http.NewRequest("DELETE", "/users", body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer test-admin-token")

	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	t.Logf("DELETE user response: %d", resp.Code)
	return resp
}
