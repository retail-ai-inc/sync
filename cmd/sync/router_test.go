package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func serve(t *testing.T, method, target string) *httptest.ResponseRecorder {
	t.Helper()

	rec := httptest.NewRecorder()
	newRouter().ServeHTTP(rec, httptest.NewRequest(method, target, nil))
	return rec
}

// TestTheProbesAnswerWithoutACredential covers what a rolling deployment
// depends on: the readiness and liveness probes are outside /api, so they
// answer before anything is configured and without a token.
func TestTheProbesAnswerWithoutACredential(t *testing.T) {
	for _, path := range []string{"/healthz", "/readyz"} {
		rec := serve(t, http.MethodGet, path)
		if rec.Code != http.StatusOK {
			t.Errorf("GET %s = %d, want 200 (%q)", path, rec.Code, rec.Body.String())
		}
	}
}

// TestTheMetricsAreServedWithoutACredential records the deliberate decision
// that the exposition takes no token, which is what every scraper expects.
func TestTheMetricsAreServedWithoutACredential(t *testing.T) {
	rec := serve(t, http.MethodGet, "/metrics")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics = %d, want 200", rec.Code)
	}
	if body := rec.Body.String(); !strings.Contains(body, "# HELP") && body != "" {
		t.Errorf("body = %q, want the Prometheus text format", body)
	}
}

// TestTheAPIRequiresACredential records that mounting the API under /api keeps
// it behind the authentication middleware — the probes and the exposition are
// the exceptions, and they are exceptions because they sit outside it.
func TestTheAPIRequiresACredential(t *testing.T) {
	rec := serve(t, http.MethodGet, "/api/users")
	if rec.Code == http.StatusOK {
		t.Errorf("GET /api/users = 200 with no token: %q", rec.Body.String())
	}
}

// TestAnUnknownPathServesTheApplication covers the single-page application's
// routes: they belong to the browser, not to this server, so a path with no file
// behind it has to answer with the entry point rather than with a 404.
func TestAnUnknownPathServesTheApplication(t *testing.T) {
	inWorkingDirectory(t)
	writeUIFile(t, "index.html", "<html>the application</html>")

	rec := serve(t, http.MethodGet, "/tasks/17/edit")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET a client-side route = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "the application") {
		t.Errorf("body = %q, want the application's entry point", rec.Body.String())
	}
}

// TestABuiltFileIsServedAsItself is the other half: a path that does name a
// built file must serve that file, not the entry point.
func TestABuiltFileIsServedAsItself(t *testing.T) {
	inWorkingDirectory(t)
	writeUIFile(t, "index.html", "<html>the application</html>")
	writeUIFile(t, "app.js", "console.log('built')")

	rec := serve(t, http.MethodGet, "/app.js")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /app.js = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "built") {
		t.Errorf("body = %q, want the built file", rec.Body.String())
	}
}

// inWorkingDirectory moves the process into a throwaway directory, because the
// UI is served from the relative path "ui/dist".
func inWorkingDirectory(t *testing.T) {
	t.Helper()

	previous, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	dir := t.TempDir()
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(previous) })
}

func writeUIFile(t *testing.T, name, content string) {
	t.Helper()

	if err := os.MkdirAll("ui/dist", 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join("ui/dist", name), []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}
