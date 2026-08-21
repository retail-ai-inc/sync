package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
)

// routeTable walks the router and returns "METHOD /path" for every registered
// endpoint.
func routeTable(t *testing.T) []string {
	t.Helper()

	r, ok := NewRouter().(chi.Routes)
	if !ok {
		t.Fatal("NewRouter did not return a chi.Routes")
	}

	var routes []string
	err := chi.Walk(r, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		routes = append(routes, method+" "+route)
		return nil
	})
	if err != nil {
		t.Fatalf("walk routes: %v", err)
	}
	sort.Strings(routes)
	return routes
}

func TestRouterRegistersEveryEndpoint(t *testing.T) {
	want := []string{
		"DELETE /backup/{id}",
		"DELETE /sync/{id}",
		"DELETE /users",
		"GET /backup",
		"GET /backup/status/{taskId}",
		"GET /changestreams/status",
		"GET /currentUser",
		"GET /getAdminToken",
		"GET /oauth/{provider}/config",
		"GET /sync",
		"GET /sync/{id}/logs",
		"GET /sync/{id}/metrics",
		"GET /sync/{id}/monitor",
		"GET /sync/{id}/tables",
		"GET /users",
		"POST /backup",
		"POST /backup/execute/{id}",
		"POST /backup/{id}/run",
		"POST /login",
		"POST /login/google/callback",
		"POST /logout",
		"POST /sync",
		"POST /tables/schema",
		"POST /test-connection",
		"PUT /backup/{id}",
		"PUT /backup/{id}/pause",
		"PUT /backup/{id}/resume",
		"PUT /oauth/{provider}/config",
		"PUT /sync/{id}",
		"PUT /sync/{id}/start",
		"PUT /sync/{id}/stop",
		"PUT /updateAdminPassword",
		"PUT /updatePassword",
		"PUT /users/access",
	}

	got := routeTable(t)

	if len(got) != len(want) {
		t.Errorf("router exposes %d routes, want %d", len(got), len(want))
	}
	inGot := map[string]bool{}
	for _, r := range got {
		inGot[r] = true
	}
	for _, r := range want {
		if !inGot[r] {
			t.Errorf("route %q is not registered", r)
		}
	}
	inWant := map[string]bool{}
	for _, r := range want {
		inWant[r] = true
	}
	for _, r := range got {
		if !inWant[r] {
			t.Errorf("route %q is registered but not expected — update this table deliberately", r)
		}
	}
}

func TestRouterHasNoDuplicateRoutes(t *testing.T) {
	seen := map[string]bool{}
	for _, r := range routeTable(t) {
		if seen[r] {
			t.Errorf("route %q is registered twice", r)
		}
		seen[r] = true
	}
}

func TestRouterReturns404ForUnknownPaths(t *testing.T) {
	for _, path := range []string{"/", "/nope", "/api/sync", "/sync/1/unknown"} {
		rec := httptest.NewRecorder()
		NewRouter().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))

		if rec.Code != http.StatusNotFound {
			t.Errorf("GET %s: status = %d, want 404", path, rec.Code)
		}
	}
}

func TestRouterReturns405ForTheWrongMethod(t *testing.T) {
	cases := []struct{ method, path string }{
		{http.MethodGet, "/login"},
		{http.MethodPost, "/users"},
		{http.MethodDelete, "/sync"},
		{http.MethodPut, "/backup"},
	}
	for _, tc := range cases {
		rec := httptest.NewRecorder()
		NewRouter().ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, nil))

		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s %s: status = %d, want 405", tc.method, tc.path, rec.Code)
		}
	}
}

// NewRouter registers no middleware at all: there is no authentication,
// authorisation, rate limiting, request logging, panic recovery or body-size
// limit in front of any handler. Every check is left to each handler to
// perform for itself, so a handler that forgets one is reachable
// unauthenticated, and a panic in any handler kills the connection rather than
// returning a 500.
func TestTheRouterInstallsNoMiddleware(t *testing.T) {
	r, ok := NewRouter().(chi.Routes)
	if !ok {
		t.Fatal("NewRouter did not return a chi.Routes")
	}

	if n := len(r.Middlewares()); n != 0 {
		t.Fatalf("the router now installs %d middleware(s) — assert what they enforce instead", n)
	}

	var withMiddleware []string
	err := chi.Walk(r, func(method, route string, _ http.Handler, mw ...func(http.Handler) http.Handler) error {
		if len(mw) > 0 {
			withMiddleware = append(withMiddleware, fmt.Sprintf("%s %s (%d)", method, route, len(mw)))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk routes: %v", err)
	}
	if len(withMiddleware) > 0 {
		t.Fatalf("routes now carry middleware: %s — assert what it enforces instead", strings.Join(withMiddleware, ", "))
	}
}

// Without chi's Recoverer, a panic in any handler propagates into
// net/http, which aborts the connection. The client sees a dropped request
// rather than a 500, and no other request on that connection completes.
func TestAPanickingHandlerIsNotRecovered(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/boom", func(http.ResponseWriter, *http.Request) { panic("handler bug") })

	defer func() {
		if recover() == nil {
			t.Fatal("the panic was recovered — a Recoverer appears to be installed; assert the 500 response instead")
		}
	}()

	r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/boom", nil))
}
