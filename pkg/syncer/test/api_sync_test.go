package test

import (
	"bytes"
	"encoding/json"
	"github.com/retail-ai-inc/sync/pkg/api"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

type ResponseType struct {
	Data struct {
		FormData struct {
			ID int `json:"id"`
		} `json:"formData"`
	} `json:"data"`
}

func testTC15SyncApi(t *testing.T) {
	r := api.NewRouter()

	listSyncs(t, r)
	stopSync(t, r)
	startSync(t, r)
	respBody, body := createSync(t, r)
	taskID := respBody.Data.FormData.ID
	t.Logf("Task ID: %v, Response Body: %v", taskID, body.Body.String())

	updateSync(t, r, taskID)
	deleteSync(t, r, taskID)
}

func listSyncs(t *testing.T, r http.Handler) *httptest.ResponseRecorder {
	req, _ := http.NewRequest("GET", "/sync", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)
	return resp
}

func stopSync(t *testing.T, r http.Handler) *httptest.ResponseRecorder {
	req, _ := http.NewRequest("PUT", "/sync/1/stop", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)
	return resp
}

func startSync(t *testing.T, r http.Handler) *httptest.ResponseRecorder {
	req, _ := http.NewRequest("PUT", "/sync/1/start", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)
	return resp
}

func createSync(t *testing.T, r http.Handler) (ResponseType, *httptest.ResponseRecorder) {
	body := bytes.NewBufferString(`{"sourceType":"mysql","sourceConn":{"database":"db1"},"targetConn":{"database":"db2"},"mappings":[{"sourceTable":"t1","targetTable":"t2"}],"taskName":"Demo","status":"running"}`)
	req, _ := http.NewRequest("POST", "/sync", body)
	req.Header.Set("Content-Type", "application/json")

	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)

	var respBody ResponseType
	if err := json.NewDecoder(resp.Body).Decode(&respBody); err != nil {
		t.Fatalf("Error decoding response: %v", err)
	}

	return respBody, resp
}

func updateSync(t *testing.T, r http.Handler, taskID int) *httptest.ResponseRecorder {
	body := bytes.NewBufferString(`{"sourceType":"mysql","sourceConn":{"database":"db1"},"targetConn":{"database":"db2"},"mappings":[{"sourceTable":"t1","targetTable":"t2"}],"taskName":"Demo","status":"running"}`)
	req, _ := http.NewRequest("PUT", "/sync/"+strconv.Itoa(taskID), body)
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)
	return resp
}

func deleteSync(t *testing.T, r http.Handler, taskID int) *httptest.ResponseRecorder {
	req, _ := http.NewRequest("DELETE", "/sync/"+strconv.Itoa(taskID), nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)
	return resp
}
