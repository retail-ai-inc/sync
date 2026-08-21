package replicationhttp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func TestSyncTablesHandlerSummarisesToday(t *testing.T) {
	conn := useMonitorDB(t)
	today := time.Now().UTC().Format("2006-01-02")
	insertMonitoringRow(t, conn, 1, today+" 01:00:00", "orders", 100, 100)
	insertMonitoringRow(t, conn, 1, today+" 02:00:00", "orders", 180, 175)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodGet, "/sync/{id}/tables", nil),
		SyncTablesHandler, map[string]string{"id": "1"})

	resp := decodeEnvelope(t, rec)
	if resp["success"] != true {
		t.Fatalf("success = %v (body: %s)", resp["success"], rec.Body.String())
	}
	body, _ := json.Marshal(resp["data"])
	if !strings.Contains(string(body), "orders") {
		t.Errorf("orders is missing: %s", body)
	}
	// synced_today = MAX(tgt) - MIN(tgt) = 175 - 100
	if !strings.Contains(string(body), "75") {
		t.Errorf("the daily delta of 75 is missing: %s", body)
	}
}

func TestSyncTablesHandlerIgnoresOtherDays(t *testing.T) {
	conn := useMonitorDB(t)
	yesterday := time.Now().UTC().AddDate(0, 0, -1).Format("2006-01-02")
	insertMonitoringRow(t, conn, 1, yesterday+" 12:00:00", "orders", 999, 999)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodGet, "/sync/{id}/tables", nil),
		SyncTablesHandler, map[string]string{"id": "1"})

	body, _ := json.Marshal(decodeEnvelope(t, rec)["data"])
	if strings.Contains(string(body), "999") {
		t.Errorf("yesterday's row leaked into today's summary: %s", body)
	}
}

// The window is built from UTC calendar days while the rest of the API reports
// JST. Between 00:00 and 09:00 JST the "today" summary therefore covers the
// previous JST day, and rows from the current JST morning are excluded.
func TestTheDailySummaryWindowIsUTCNotJST(t *testing.T) {
	conn := useMonitorDB(t)

	nowUTC := time.Now().UTC()
	jst := nowUTC.In(time.FixedZone("JST", 9*60*60))
	if nowUTC.Format("2006-01-02") == jst.Format("2006-01-02") {
		t.Skip("UTC and JST are on the same calendar day right now")
	}

	// A row stamped for the current JST day but the next UTC day.
	insertMonitoringRow(t, conn, 1, jst.Format("2006-01-02")+" 00:30:00", "orders", 5, 5)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodGet, "/sync/{id}/tables", nil),
		SyncTablesHandler, map[string]string{"id": "1"})

	body, _ := json.Marshal(decodeEnvelope(t, rec)["data"])
	if strings.Contains(string(body), "orders") {
		t.Fatalf("the window appears to use JST now — assert the JST day instead: %s", body)
	}
}

func TestSyncTablesHandlerReportsAMissingTable(t *testing.T) {
	t.Setenv("SYNC_DB_PATH", filepath.Join(t.TempDir(), "empty.db"))

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodGet, "/sync/{id}/tables", nil),
		SyncTablesHandler, map[string]string{"id": "1"})

	if resp := decodeEnvelope(t, rec); resp["success"] != false {
		t.Errorf("success = %v, want false", resp["success"])
	}
}
