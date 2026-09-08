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
		"verifyIntervalSeconds": 0,
		"lagAlertSeconds":       0,
		"batchMaxEvents":        0,
		"batchMaxBytes":         0,
	} {
		if got := data[field].(float64); got != want {
			t.Errorf("%s = %v, want %v", field, got, want)
		}
	}
	if data["verifyRepair"].(bool) || data["mongoNoTransaction"].(bool) {
		t.Error("a switch defaults to on")
	}
	// Except this one, which is on unless it is turned off: a standby that
	// cannot resume is rebuilt rather than left standing still.
	if !data["recopyOnUnusablePosition"].(bool) {
		t.Error("re-copying after a position that cannot be used defaults to off")
	}
}

// A body that leaves the switch out means "leave it as it is". A plain bool
// would decode to false, so a script that sent any other setting would quietly
// turn this one off.
func TestASettingLeftOutOfTheBodyIsLeftAlone(t *testing.T) {
	useSettingsDB(t)

	off := writeSettings(t, map[string]interface{}{"recopyOnUnusablePosition": false})
	if off.Code != http.StatusOK {
		t.Fatalf("PUT = %d: %s", off.Code, off.Body.String())
	}
	if readSettings(t)["data"].(map[string]interface{})["recopyOnUnusablePosition"].(bool) {
		t.Fatal("turning it off did not take")
	}

	if got := writeSettings(t, map[string]interface{}{"batchMaxEvents": 10}); got.Code != http.StatusOK {
		t.Fatalf("PUT = %d: %s", got.Code, got.Body.String())
	}
	if readSettings(t)["data"].(map[string]interface{})["recopyOnUnusablePosition"].(bool) {
		t.Error("a body that left the switch out turned it back on")
	}

	if got := writeSettings(t, map[string]interface{}{"recopyOnUnusablePosition": true}); got.Code != http.StatusOK {
		t.Fatalf("PUT = %d: %s", got.Code, got.Body.String())
	}
	if !readSettings(t)["data"].(map[string]interface{})["recopyOnUnusablePosition"].(bool) {
		t.Error("turning it back on did not take")
	}
}

func TestSettingsSurviveTheRoundTrip(t *testing.T) {
	useSettingsDB(t)

	recorder := writeSettings(t, map[string]interface{}{
		"verifyIntervalSeconds":  3600,
		"verifyRepair":           true,
		"lagAlertSeconds":        120,
		"batchMaxEvents":         250,
		"batchMaxBytes":          1 << 20,
		"mongoNoTransaction":     false,
		"queueMaxEvents":         4096,
		"queueMaxBytes":          128 << 20,
		"snapshotQueueMaxEvents": 1024,
		"flushIntervalMs":        250,
		"copyBatchRows":          750,
		"mongoStreamAwaitMs":     150,
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
	// The tuning settings are stored as milliseconds and read back as
	// milliseconds: a duration that went in as 250 and came back as 250
	// nanoseconds would be a setting that silently did nothing.
	for field, want := range map[string]float64{
		"queueMaxEvents":         4096,
		"queueMaxBytes":          128 << 20,
		"snapshotQueueMaxEvents": 1024,
		"flushIntervalMs":        250,
		"copyBatchRows":          750,
		"mongoStreamAwaitMs":     150,
	} {
		if data[field].(float64) != want {
			t.Errorf("%s = %v, want %v", field, data[field], want)
		}
	}
}

// Zero already means "the built-in default", so a negative number is somebody
// meaning something else. Guessing which is how a setting ends up doing the
// opposite of what its author intended.
func TestANegativeSettingIsRefused(t *testing.T) {
	useSettingsDB(t)

	for field, value := range map[string]interface{}{
		"verifyIntervalSeconds":  -1,
		"lagAlertSeconds":        -1,
		"batchMaxEvents":         -1,
		"batchMaxBytes":          -1,
		"queueMaxEvents":         -1,
		"queueMaxBytes":          -1,
		"snapshotQueueMaxEvents": -1,
		"flushIntervalMs":        -1,
		"copyBatchRows":          -1,
		"mongoStreamAwaitMs":     -1,
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
