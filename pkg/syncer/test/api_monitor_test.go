package test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/retail-ai-inc/sync/pkg/api"
)

func testTC16MonitorApi(t *testing.T) {
	r := api.NewRouter()

	monitor(t, r)
	metrics(t, r)
	logs(t, r)
}

func monitor(t *testing.T, r http.Handler) *httptest.ResponseRecorder {
	req, _ := http.NewRequest("GET", "/sync/1/monitor", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)
	t.Logf("monitor response: %d", resp.Code)
	return resp
}
func metrics(t *testing.T, r http.Handler) *httptest.ResponseRecorder {
	req, _ := http.NewRequest("GET", "/sync/1/metrics", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)
	t.Logf("metrics response: %d", resp.Code)
	return resp
}
func logs(t *testing.T, r http.Handler) *httptest.ResponseRecorder {
	req, _ := http.NewRequest("GET", "/sync/1/logs", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)
	t.Logf("logs response: %d", resp.Code)
	return resp
}
