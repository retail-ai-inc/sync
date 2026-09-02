package replicationhttp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/sqlite/sqlitetest"

	_ "github.com/mattn/go-sqlite3"
)

// jstDay names a calendar day in Tokyo, which is the day the handler's window
// is built from.
func jstDay(offset int) string {
	return time.Now().In(time.FixedZone("JST", 9*60*60)).AddDate(0, 0, offset).Format("2006-01-02")
}

func TestSyncTablesHandlerSummarisesToday(t *testing.T) {
	conn := useMonitorDB(t)
	today := jstDay(0)
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
	yesterday := jstDay(-1)
	insertMonitoringRow(t, conn, 1, yesterday+" 12:00:00", "orders", 999, 999)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodGet, "/sync/{id}/tables", nil),
		SyncTablesHandler, map[string]string{"id": "1"})

	body, _ := json.Marshal(decodeEnvelope(t, rec)["data"])
	if strings.Contains(string(body), "999") {
		t.Errorf("yesterday's row leaked into today's summary: %s", body)
	}
}

// The window was built from the UTC calendar day while the answer was labelled
// with the JST date — and everything else this API reports is converted to JST
// — so during those hours the figures belonged to the day before the label
// said, and that morning's traffic was left out.
func TestTheDailyWindowIsTheJSTDay(t *testing.T) {
	conn := useMonitorDB(t)

	nowUTC := time.Now().UTC()
	jst := nowUTC.In(time.FixedZone("JST", 9*60*60))
	if nowUTC.Format("2006-01-02") == jst.Format("2006-01-02") {
		t.Skip("UTC and JST are on the same calendar day right now")
	}

	// A row stamped for the current JST day, which is the next UTC day.
	insertMonitoringRow(t, conn, 1, jst.Format("2006-01-02")+" 00:30:00", "orders", 5, 5)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodGet, "/sync/{id}/tables", nil),
		SyncTablesHandler, map[string]string{"id": "1"})

	body, _ := json.Marshal(decodeEnvelope(t, rec)["data"])
	if !strings.Contains(string(body), "orders") {
		t.Errorf("this JST morning's traffic is missing from today's figures: %s", body)
	}
}

func TestSyncTablesHandlerReportsAMissingTable(t *testing.T) {
	sqlitetest.Tableless(t)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodGet, "/sync/{id}/tables", nil),
		SyncTablesHandler, map[string]string{"id": "1"})

	if resp := decodeEnvelope(t, rec); resp["success"] != false {
		t.Errorf("success = %v, want false", resp["success"])
	}
}
