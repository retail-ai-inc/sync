package dbinspect

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// postJSON runs a handler over a request body and returns the recorder.
func postJSON(h http.HandlerFunc, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h(rec, req)
	return rec
}

// TestHandlersRejectMalformedJSON records that both inspection endpoints answer
// a body they cannot parse with 400 rather than a panic or a 200.
func TestHandlersRejectMalformedJSON(t *testing.T) {
	cases := []struct {
		name    string
		handler http.HandlerFunc
		method  string
		path    string
	}{
		{"test connection", TestConnectionHandler, http.MethodPost, "/test-connection"},
		{"table schema", GetTableSchemaHandler, http.MethodPost, "/tables/schema"},
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
