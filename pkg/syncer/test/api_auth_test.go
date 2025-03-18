package test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/retail-ai-inc/sync/pkg/api"
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
	executeSql(t, r, 1, "db.users.find({}).sort({_id: -1}).limit(10)", false)
	executeSql(t, r, 2, "SELECT * FROM users LIMIT 10;", false)
	executeSql(t, r, 4, "SELECT * FROM users LIMIT 10;", false)
	getTableSchema(t, r, "mongodb", "127.0.0.1", "27017", "", "", "source_db", "users")
	getTableSchema(t, r, "mysql", "127.0.0.1", "3306", "root", "root", "source_db", "users")
	getTableSchema(t, r, "postgresql", "127.0.0.1", "5432", "root", "root", "source_db", "users")
	testGoogleCallback(t, r)
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

	t.Logf("login response: %d", resp.Code)
	return resp
}

func currentUser(t *testing.T, r http.Handler) *httptest.ResponseRecorder {
	req, _ := http.NewRequest("GET", "/currentUser", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	t.Logf("currentUser response: %d", resp.Code)
	return resp
}

func logout(t *testing.T, r http.Handler) *httptest.ResponseRecorder {
	req, _ := http.NewRequest("POST", "/logout", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	t.Logf("logout response: %d", resp.Code)
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
	t.Logf("testConnection response: %d", resp.Code)
	return resp
}

func executeSql(t *testing.T, r http.Handler, taskId int, sql string, target bool) *httptest.ResponseRecorder {
	bodyContent := map[string]interface{}{
		"taskId": taskId,
		"sql":    sql,
		"target": target,
	}

	bodyBytes, err := json.Marshal(bodyContent)
	if err != nil {
		t.Fatalf("Failed to serialize the request body: %v", err)
	}

	body := bytes.NewBuffer(bodyBytes)
	req, _ := http.NewRequest("POST", "/sql/execute", body)
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)
	t.Logf("executeSql response: %d", resp.Code)
	return resp
}

func getTableSchema(t *testing.T, r http.Handler, sourceType, host, port, user, password, database, tableName string) *httptest.ResponseRecorder {
	connection := map[string]string{
		"host":     host,
		"port":     port,
		"user":     user,
		"password": password,
		"database": database,
	}

	bodyContent := map[string]interface{}{
		"sourceType": sourceType,
		"connection": connection,
		"tableName":  tableName,
	}

	bodyBytes, err := json.Marshal(bodyContent)
	if err != nil {
		t.Fatalf("Failed to serialize the request body: %v", err)
	}

	body := bytes.NewBuffer(bodyBytes)
	req, _ := http.NewRequest("POST", "/tables/schema", body)
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)
	t.Logf("getTableSchema response: %d", resp.Code)
	return resp
}

func testGoogleCallback(t *testing.T, r http.Handler) *httptest.ResponseRecorder {
	bodyContent := map[string]string{
		"code": "4/0AQSTgQEIZp9_oJnehA1zx6UBAXnAQ5n4zKzkIbBL9O6QkNuGw4OEnZNwHTa9ACJdUq7Bmw",
	}

	bodyBytes, err := json.Marshal(bodyContent)
	if err != nil {
		t.Fatalf("Failed to serialize the request body: %v", err)
	}

	body := bytes.NewBuffer(bodyBytes)
	req, _ := http.NewRequest("POST", "/login/google/callback", body)
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	t.Logf("testGoogleCallback response: %d", resp.Code)
	return resp
}
