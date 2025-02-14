package test

import (
	"bytes"
	"encoding/json"
	"github.com/retail-ai-inc/sync/pkg/api"
	"net/http"
	"net/http/httptest"
	"testing"
)

func testTC14AuthApi(t *testing.T) {
	r := api.NewRouter()

	login(t, r)
	currentUser(t, r)
	logout(t, r)
	testConnection(t, r, "mysql", "localhost", "3306", "root", "root", "source_db")
	testConnection(t, r, "mongodb", "localhost", "27017", "", "", "source_db")
	testConnection(t, r, "postgresql", "localhost", "5432", "root", "root", "source_db")
	testConnection(t, r, "redis", "localhost", "6379", "", "", "source_db")

}

func login(t *testing.T, r http.Handler) *httptest.ResponseRecorder {
	body := bytes.NewBufferString(`{"username":"admin","password":"admin"}`)
	req, _ := http.NewRequest("POST", "/login", body)
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	body = bytes.NewBufferString(`{"username":"test","password":"test"}`)
	req, _ = http.NewRequest("POST", "/login", body)
	req.Header.Set("Content-Type", "application/json")
	resp = httptest.NewRecorder()
	r.ServeHTTP(resp, req)
	return resp
}

func currentUser(t *testing.T, r http.Handler) *httptest.ResponseRecorder {
	req, _ := http.NewRequest("GET", "/currentUser", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)
	return resp
}

func logout(t *testing.T, r http.Handler) *httptest.ResponseRecorder {
	req, _ := http.NewRequest("POST", "/logout", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)
	return resp
}

func testConnection(t *testing.T, r http.Handler, dbType, host, port, user, password, database string) *httptest.ResponseRecorder {
	bodyContent := map[string]string{
		"dbType":   dbType,
		"host":     host,
		"port":     port,
		"user":     user,
		"password": password,
		"database": database,
	}

	bodyBytes, err := json.Marshal(bodyContent)
	if err != nil {
		t.Fatalf("Error marshalling request body: %v", err)
	}

	body := bytes.NewBuffer(bodyBytes)
	req, _ := http.NewRequest("POST", "/test-connection", body)
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)
	t.Logf("INFO decoding response: %v", resp)

	return resp
}
