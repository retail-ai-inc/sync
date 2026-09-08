package httpapi

import (
	"errors"
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
		"GET /settings",
		"GET /currentUser",
		"GET /oauth/{provider}/config",
		"GET /sync",
		"GET /sync/{id}/monitor",
		"GET /sync/{id}/tables",
		"GET /sync/{id}/position",
		"GET /sync/{id}/rowcounts",
		"GET /users",
		"POST /backup",
		"POST /backup/execute/{id}",
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
		"PUT /settings",
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

// NewRouter registers no middleware at all: there is no authentication.
func TestEveryRouteIsCoveredByTheAccessRules(t *testing.T) {
	r, ok := NewRouter().(chi.Routes)
	if !ok {
		t.Fatal("NewRouter did not return a chi.Routes")
	}

	public := map[string]bool{
		"POST /login":                  true,
		"POST /logout":                 true,
		"POST /login/google/callback":  true,
		"GET /oauth/{provider}/config": true,
	}

	var uncovered []string
	err := chi.Walk(r, func(method, route string, _ http.Handler, mw ...func(http.Handler) http.Handler) error {
		name := method + " " + route
		if public[name] {
			if len(mw) > 0 {
				uncovered = append(uncovered, fmt.Sprintf("%s is public but carries %d middleware", name, len(mw)))
			}
			return nil
		}
		if len(mw) == 0 {
			uncovered = append(uncovered, fmt.Sprintf("%s carries no middleware", name))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk routes: %v", err)
	}
	if len(uncovered) > 0 {
		t.Errorf("routes outside the access rules: %s", strings.Join(uncovered, ", "))
	}
}

// Without chi's Recoverer, a panic in any handler propagates into net/http,
// which aborts the connection.
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

// The readiness probe used to answer ready before anything had been checked.
func TestReadinessReportsAControlPlaneThatCannotAnswer(t *testing.T) {
	handler := Ready(func() error { return errors.New("the control database is not there") })

	recorder := httptest.NewRecorder()
	handler(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if recorder.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 so the rollout stops", recorder.Code)
	}
	if !strings.Contains(recorder.Body.String(), "not there") {
		t.Errorf("the body does not say why: %s", recorder.Body.String())
	}
}

func TestReadinessAnswersReadyWhenTheCheckPasses(t *testing.T) {
	handler := Ready(func() error { return nil })

	recorder := httptest.NewRecorder()
	handler(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if recorder.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", recorder.Code)
	}
}
