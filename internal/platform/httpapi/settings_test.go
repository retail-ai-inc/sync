package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/retail-ai-inc/sync/internal/platform/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

// useSettingsDB points the package at a throwaway control database carrying
// the schema, so the settings can be written without touching the one tracked
// in this repository.
func useSettingsDB(t *testing.T) {
	t.Helper()
	t.Setenv("SYNC_DB_PATH", filepath.Join(t.TempDir(), "sync.db"))
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		t.Fatalf("open the control database: %v", err)
	}
	_ = db.Close()
}

func readSettings(t *testing.T) map[string]interface{} {
	t.Helper()
	recorder := httptest.NewRecorder()
	SettingsHandler(recorder, httptest.NewRequest(http.MethodGet, "/settings", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET /settings = %d: %s", recorder.Code, recorder.Body.String())
	}
	var body map[string]interface{}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return body
}

func writeSettings(t *testing.T, payload map[string]interface{}) *httptest.ResponseRecorder {
	t.Helper()
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	recorder := httptest.NewRecorder()
	UpdateSettingsHandler(recorder,
		httptest.NewRequest(http.MethodPut, "/settings", bytes.NewReader(encoded)))
	return recorder
}

// A database that has never had a setting written reads as every default,
// which is what a deployment upgrading into these columns holds.
func TestSettingsStartAtTheBuiltInDefaults(t *testing.T) {
	useSettingsDB(t)

	data := readSettings(t)["data"].(map[string]interface{})
	for field, want := range map[string]float64{
		"verifyIntervalSeconds":   0,
		"lagAlertSeconds":         0,
		"monitoringRetentionDays": 0,
		"batchMaxEvents":          0,
		"batchMaxBytes":           0,
	} {
		if got := data[field].(float64); got != want {
			t.Errorf("%s = %v, want %v", field, got, want)
		}
	}
	if data["verifyRepair"].(bool) || data["mongoNoTransaction"].(bool) {
		t.Error("a switch defaults to on")
	}
}

func TestSettingsSurviveTheRoundTrip(t *testing.T) {
	useSettingsDB(t)

	recorder := writeSettings(t, map[string]interface{}{
		"verifyIntervalSeconds":   3600,
		"verifyRepair":            true,
		"lagAlertSeconds":         120,
		"monitoringRetentionDays": 30,
		"batchMaxEvents":          250,
		"batchMaxBytes":           1 << 20,
		"mongoNoTransaction":      false,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("PUT /settings = %d: %s", recorder.Code, recorder.Body.String())
	}

	data := readSettings(t)["data"].(map[string]interface{})
	if data["verifyIntervalSeconds"].(float64) != 3600 {
		t.Errorf("verifyIntervalSeconds = %v", data["verifyIntervalSeconds"])
	}
	if !data["verifyRepair"].(bool) {
		t.Error("verifyRepair did not survive")
	}
	if data["batchMaxBytes"].(float64) != float64(1<<20) {
		t.Errorf("batchMaxBytes = %v", data["batchMaxBytes"])
	}
}

// Zero already means "the built-in default", so a negative number is somebody
// meaning something else. Guessing which is how a setting ends up doing the
// opposite of what its author intended.
func TestANegativeSettingIsRefused(t *testing.T) {
	useSettingsDB(t)

	for field, value := range map[string]interface{}{
		"verifyIntervalSeconds":   -1,
		"lagAlertSeconds":         -1,
		"monitoringRetentionDays": -1,
		"batchMaxEvents":          -1,
		"batchMaxBytes":           -1,
	} {
		recorder := writeSettings(t, map[string]interface{}{field: value})
		if recorder.Code != http.StatusBadRequest {
			t.Errorf("%s = %v was accepted with %d", field, value, recorder.Code)
		}
		if !bytes.Contains(recorder.Body.Bytes(), []byte(field)) {
			t.Errorf("the refusal does not name %s: %s", field, recorder.Body.String())
		}
	}
}

func TestABodyThatIsNotSettingsIsRefused(t *testing.T) {
	useSettingsDB(t)
	recorder := httptest.NewRecorder()
	UpdateSettingsHandler(recorder,
		httptest.NewRequest(http.MethodPut, "/settings", bytes.NewReader([]byte("not json"))))
	if recorder.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", recorder.Code)
	}
}

// A page that showed only the stored value would tell somebody their change
// had taken effect while a variable overruled it.
func TestSettingsReportTheVariablesInForceOverThem(t *testing.T) {
	useSettingsDB(t)
	t.Setenv("SYNC_VERIFY_INTERVAL", "5m")

	over := readSettings(t)["overridden"].(map[string]interface{})
	value, named := over["verifyIntervalSeconds"]
	if !named {
		t.Fatalf("the override was not reported: %v", over)
	}
	if !bytes.Contains([]byte(value.(string)), []byte("SYNC_VERIFY_INTERVAL")) {
		t.Errorf("the override does not name the variable: %v", value)
	}
}
