package webui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTheSettingsPageIsServedAsHTML(t *testing.T) {
	recorder := httptest.NewRecorder()
	SettingsPage(recorder, httptest.NewRequest(http.MethodGet, "/settings.html", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d", recorder.Code)
	}
	if kind := recorder.Header().Get("Content-Type"); !strings.HasPrefix(kind, "text/html") {
		t.Errorf("Content-Type = %q", kind)
	}
	// Not cached: it is read once and then edited, and a stale copy would show
	// somebody a value that is no longer stored.
	if recorder.Header().Get("Cache-Control") != "no-store" {
		t.Errorf("Cache-Control = %q", recorder.Header().Get("Cache-Control"))
	}
}

// The page carries no credential of its own. It reads the one the application
// stored and sends the same bearer header, so it reaches the same endpoints
// under the same rules.
func TestTheSettingsPageUsesTheApplicationsOwnToken(t *testing.T) {
	body := string(settingsPage)
	for _, want := range []string{
		`localStorage.getItem("accessToken")`,
		`"Bearer "`,
		"/api/settings",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the page does not contain %q", want)
		}
	}
	// Nothing that would make it a second way in.
	for _, absent := range []string{"password", "/api/login"} {
		if strings.Contains(strings.ToLower(body), absent) {
			t.Errorf("the page mentions %q, which would make it a second way in", absent)
		}
	}
}

// Every field the API accepts is on the page, or a setting exists that nobody
// can reach.
func TestThePageCoversEverySetting(t *testing.T) {
	body := string(settingsPage)
	for _, field := range []string{
		"verifyIntervalSeconds", "verifyRepair", "lagAlertSeconds",
		"monitoringRetentionDays", "batchMaxEvents", "batchMaxBytes",
		"mongoNoTransaction",
	} {
		if !strings.Contains(body, field) {
			t.Errorf("the page has no field for %s", field)
		}
	}
}
