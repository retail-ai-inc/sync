package test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/retail-ai-inc/sync/pkg/api"
)

func testOAuthConfig(t *testing.T) {
	r := api.NewRouter()

	// Test getting OAuth config
	getOAuthConfig(t, r, "google")

	// Test updating OAuth config
	// updateOAuthConfig(t, r, "google")
}

func getOAuthConfig(t *testing.T, r http.Handler, provider string) *httptest.ResponseRecorder {
	req, _ := http.NewRequest("GET", "/oauth/"+provider+"/config", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	t.Logf("GET OAuth config response: %d", resp.Code)
	return resp
}

func updateOAuthConfig(t *testing.T, r http.Handler, provider string) *httptest.ResponseRecorder {
	config := map[string]interface{}{
		"clientId":     "test-client-id",
		"clientSecret": "test-client-secret",
		"redirectUri":  "http://localhost:8000/callback",
		"scopes":       []string{"email", "profile"},
	}

	configBytes, _ := json.Marshal(config)
	body := bytes.NewBuffer(configBytes)

	req, _ := http.NewRequest("PUT", "/oauth/"+provider+"/config", body)
	req.Header.Set("Content-Type", "application/json")

	// Add mock auth header for admin
	req.Header.Set("Authorization", "Bearer test-token")

	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	t.Logf("UPDATE OAuth config response: %d", resp.Code)
	return resp
}
