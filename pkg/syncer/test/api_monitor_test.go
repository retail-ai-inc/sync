package test

import (
	"github.com/retail-ai-inc/sync/pkg/api"
	"net/http"
	"net/http/httptest"
	"testing"
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
	return resp
}
func metrics(t *testing.T, r http.Handler) *httptest.ResponseRecorder {
	req, _ := http.NewRequest("GET", "/sync/1/metrics", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)
	return resp
}
func logs(t *testing.T, r http.Handler) *httptest.ResponseRecorder {
	req, _ := http.NewRequest("GET", "/sync/1/logs", nil)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)
	return resp
}
