package monitoringhttp

import (
	"database/sql"
	"encoding/json"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
)

// useMonitorDB points the package at a throwaway SQLite file carrying the
// tables the monitoring handlers read.
func useMonitorDB(t *testing.T) *sql.DB {
	t.Helper()

	isolateCrontab(t)

	path := filepath.Join(t.TempDir(), "sync.db")
	t.Setenv("SYNC_DB_PATH", path)

	conn, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open temp sqlite: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	const schema = `
CREATE TABLE sync_tasks (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    enable           INTEGER NOT NULL DEFAULT 1,
    last_update_time DATETIME,
    last_run_time    DATETIME,
    config_json      TEXT NOT NULL
);
CREATE TABLE monitoring_log (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    logged_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    db_type        TEXT NOT NULL,
    src_db         TEXT,
    src_table      TEXT,
    src_row_count  INTEGER,
    tgt_db         TEXT,
    tgt_table      TEXT,
    tgt_row_count  INTEGER,
    monitor_action TEXT,
    sync_task_id   INTEGER
);
CREATE TABLE sync_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    log_time     DATETIME DEFAULT CURRENT_TIMESTAMP,
    level        TEXT,
    message      TEXT,
    sync_task_id INTEGER
);
CREATE TABLE changestream_statistics (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id         INTEGER NOT NULL,
    collection_name VARCHAR(255) NOT NULL,
    received        INTEGER DEFAULT 0,
    executed        INTEGER DEFAULT 0,
    pending         INTEGER DEFAULT 0,
    errors          INTEGER DEFAULT 0,
    inserted        INTEGER DEFAULT 0,
    updated         INTEGER DEFAULT 0,
    deleted         INTEGER DEFAULT 0,
    last_updated    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(task_id, collection_name)
);`
	if _, err := conn.Exec(schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	return conn
}

func insertMonitoringRow(t *testing.T, conn *sql.DB, taskID int, loggedAt, table string, src, tgt int64) {
	t.Helper()

	if _, err := conn.Exec(`
		INSERT INTO monitoring_log
			(logged_at, db_type, src_db, src_table, src_row_count, tgt_db, tgt_table, tgt_row_count, monitor_action, sync_task_id)
		VALUES (?, 'mysql', 'source_db', ?, ?, 'target_db', ?, ?, 'row_count_minutely', ?)`,
		loggedAt, table, src, table, tgt, taskID); err != nil {
		t.Fatalf("insert monitoring_log: %v", err)
	}
}

func sqlNow(offset time.Duration) string {
	return time.Now().UTC().Add(offset).Format("2006-01-02 15:04:05")
}

func metricsFor(t *testing.T, id, rangeStr string) map[string]interface{} {
	t.Helper()

	url := "/sync/{id}/metrics"
	if rangeStr != "" {
		url += "?range=" + rangeStr
	}
	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodGet, url, nil),
		SyncMetricsHandler, map[string]string{"id": id})

	resp := decodeEnvelope(t, rec)
	if resp["success"] != true {
		t.Fatalf("success = %v (body: %s)", resp["success"], rec.Body.String())
	}
	return resp["data"].(map[string]interface{})
}

func TestSyncMonitorHandlerReportsTaskStatus(t *testing.T) {
	conn := useMonitorDB(t)
	if _, err := conn.Exec(`INSERT INTO sync_tasks (id, enable, config_json) VALUES (1, 1, '{}'), (2, 0, '{}')`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	for _, tc := range []struct{ id, want string }{{"1", "Running"}, {"2", "Stopped"}} {
		rec := httptest.NewRecorder()
		serveWithURLParams(rec, httptest.NewRequest(http.MethodGet, "/sync/{id}/monitor", nil),
			SyncMonitorHandler, map[string]string{"id": tc.id})

		resp := decodeEnvelope(t, rec)
		if resp["success"] != true {
			t.Fatalf("task %s: %s", tc.id, rec.Body.String())
		}
		data := resp["data"].(map[string]interface{})
		if data["status"] != tc.want {
			t.Errorf("task %s: status = %v, want %v", tc.id, data["status"], tc.want)
		}
	}
}

func TestSyncMonitorHandlerOnAnUnknownTask(t *testing.T) {
	useMonitorDB(t)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodGet, "/sync/{id}/monitor", nil),
		SyncMonitorHandler, map[string]string{"id": "999"})

	resp := decodeEnvelope(t, rec)
	if resp["success"] != false {
		t.Errorf("success = %v, want false", resp["success"])
	}
}

// TestTheMonitorReportsWhatItKnows covers three numbers that were constants
// compiled into the handler: 85% progress, 500 tps and 0.2s delay, for every
// task in every state — including one that was stopped, and one that had never
// run at all. They now come from the counters the syncers keep, and a task with
// no counters yet reports nothing rather than a figure nobody measured.
func TestTheMonitorReportsWhatItKnows(t *testing.T) {
	conn := useMonitorDB(t)
	if _, err := conn.Exec(`INSERT INTO sync_tasks (id, enable, config_json) VALUES (1, 0, '{}')`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodGet, "/sync/{id}/monitor", nil),
		SyncMonitorHandler, map[string]string{"id": "1"})

	data := decodeEnvelope(t, rec)["data"].(map[string]interface{})
	if data["progress"] != nil {
		t.Errorf("progress = %v; nothing measures it", data["progress"])
	}
	if data["applied"] != nil || data["delay"] != nil {
		t.Errorf("applied/delay = %v/%v for a task that has never run", data["applied"], data["delay"])
	}
	if data["status"] != "Stopped" {
		t.Errorf("status = %v, want Stopped", data["status"])
	}

	// Once the syncer has recorded something, that is what comes back.
	labels := metrics.Labels{"task": "1", "engine": "mongodb", "collection": "orders"}
	t.Cleanup(func() { metrics.Default.Forget(labels) })
	metrics.Applied(labels, 7)
	metrics.SetLag(labels, 1.5)

	rec = httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodGet, "/sync/{id}/monitor", nil),
		SyncMonitorHandler, map[string]string{"id": "1"})

	data = decodeEnvelope(t, rec)["data"].(map[string]interface{})
	if data["applied"] != float64(7) {
		t.Errorf("applied = %v, want 7", data["applied"])
	}
	if data["delay"] != 1.5 {
		t.Errorf("delay = %v, want 1.5", data["delay"])
	}
}

func TestSyncMetricsBuildsThreeSeriesPerRow(t *testing.T) {
	conn := useMonitorDB(t)
	insertMonitoringRow(t, conn, 1, sqlNow(-30*time.Minute), "orders", 100, 90)

	data := metricsFor(t, "1", "1h")
	trend := data["rowCountTrend"].([]interface{})

	if len(trend) != 3 {
		t.Fatalf("rowCountTrend has %d points, want 3 (source/target/diff)", len(trend))
	}
	types := map[string]float64{}
	for _, p := range trend {
		m := p.(map[string]interface{})
		types[m["type"].(string)] = m["value"].(float64)
		if m["table"] != "orders" {
			t.Errorf("table = %v, want orders", m["table"])
		}
	}
	if types["source"] != 100 || types["target"] != 90 || types["diff"] != 10 {
		t.Errorf("source/target/diff = %v/%v/%v, want 100/90/10", types["source"], types["target"], types["diff"])
	}
	if data["syncEventStats"] == nil {
		t.Error("syncEventStats is missing")
	}
}

func TestSyncMetricsDiffIsAbsolute(t *testing.T) {
	conn := useMonitorDB(t)
	// Target ahead of source, which happens when the target holds extra rows.
	insertMonitoringRow(t, conn, 1, sqlNow(-10*time.Minute), "orders", 50, 80)

	for _, p := range metricsFor(t, "1", "1h")["rowCountTrend"].([]interface{}) {
		m := p.(map[string]interface{})
		if m["type"] == "diff" && m["value"] != float64(30) {
			t.Errorf("diff = %v, want the absolute value 30", m["value"])
		}
	}
}

func TestSyncMetricsFiltersByTask(t *testing.T) {
	conn := useMonitorDB(t)
	insertMonitoringRow(t, conn, 1, sqlNow(-10*time.Minute), "orders", 10, 10)
	insertMonitoringRow(t, conn, 2, sqlNow(-10*time.Minute), "users", 20, 20)

	for _, p := range metricsFor(t, "1", "1h")["rowCountTrend"].([]interface{}) {
		if tbl := p.(map[string]interface{})["table"]; tbl != "orders" {
			t.Errorf("task 1 returned a row for %v", tbl)
		}
	}
}

// Task id 0 is the "all tasks" view and prefixes the table with its task id.
func TestSyncMetricsTaskZeroAggregatesEveryTask(t *testing.T) {
	conn := useMonitorDB(t)
	insertMonitoringRow(t, conn, 1, sqlNow(-10*time.Minute), "orders", 10, 10)
	insertMonitoringRow(t, conn, 2, sqlNow(-10*time.Minute), "users", 20, 20)

	seen := map[string]bool{}
	for _, p := range metricsFor(t, "0", "1h")["rowCountTrend"].([]interface{}) {
		seen[p.(map[string]interface{})["table"].(string)] = true
	}
	for _, want := range []string{"taskID:1_orders", "taskID:2_users"} {
		if !seen[want] {
			t.Errorf("%q is missing from the aggregate view: %v", want, seen)
		}
	}
}

func TestSyncMetricsWithNoRange(t *testing.T) {
	conn := useMonitorDB(t)
	insertMonitoringRow(t, conn, 1, sqlNow(-40*24*time.Hour), "orders", 1, 1)

	// An empty range means no lower bound, so even a 40-day-old row is returned.
	if n := len(metricsFor(t, "1", "")["rowCountTrend"].([]interface{})); n != 3 {
		t.Errorf("rowCountTrend has %d points, want 3", n)
	}
}

// TestAnEmptyWindowIsAnEmptyAnswer covers a request for a window with nothing
// in it. The handler used to re-run the query with the time filter removed and
// return the entire history — up to a thousand rows — with nothing in the
// response to say the window had been abandoned, so a client asking what
// happened in the last hour got last month and could not tell.
func TestAnEmptyWindowIsAnEmptyAnswer(t *testing.T) {
	conn := useMonitorDB(t)
	// Nothing recent; one row far outside the requested window.
	old := time.Now().UTC().AddDate(0, 0, -40).Format("2006-01-02 15:04:05")
	insertMonitoringRow(t, conn, 1, old, "orders", 7, 7)

	trend := metricsFor(t, "1", "1h")["rowCountTrend"].([]interface{})

	if len(trend) != 0 {
		t.Errorf("a one-hour window returned %d points from outside it: %v", len(trend), trend)
	}
}

func TestSyncLogsHandlerReturnsRows(t *testing.T) {
	conn := useMonitorDB(t)
	for i, lvl := range []string{"info", "warn", "error"} {
		if _, err := conn.Exec(
			`INSERT INTO sync_log (log_time, level, message, sync_task_id) VALUES (?, ?, ?, 1)`,
			sqlNow(-time.Duration(i+1)*time.Minute), lvl, "message "+lvl); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodGet, "/sync/{id}/logs?range=1h", nil),
		SyncLogsHandler, map[string]string{"id": "1"})

	resp := decodeEnvelope(t, rec)
	if resp["success"] != true {
		t.Fatalf("success = %v (body: %s)", resp["success"], rec.Body.String())
	}
	if resp["data"] == nil {
		t.Fatal("data is nil")
	}
	if n := len(resp["data"].([]interface{})); n != 3 {
		t.Errorf("data has %d entries, want 3", n)
	}
}

func TestSyncLogsHandlerFiltersByLevel(t *testing.T) {
	conn := useMonitorDB(t)
	for _, lvl := range []string{"info", "warn", "error", "error"} {
		if _, err := conn.Exec(
			`INSERT INTO sync_log (log_time, level, message, sync_task_id) VALUES (?, ?, 'm', 1)`,
			sqlNow(-time.Minute), lvl); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodGet, "/sync/{id}/logs?level=error&range=1h", nil),
		SyncLogsHandler, map[string]string{"id": "1"})

	resp := decodeEnvelope(t, rec)
	data, _ := resp["data"].([]interface{})
	for _, e := range data {
		if lvl := e.(map[string]interface{})["level"]; lvl != "error" {
			t.Errorf("level filter returned %v", lvl)
		}
	}
	if len(data) != 2 {
		t.Errorf("data has %d entries, want 2", len(data))
	}
}

func TestSyncLogsHandlerSearches(t *testing.T) {
	conn := useMonitorDB(t)
	for _, msg := range []string{"connection refused", "sync completed", "connection reset"} {
		if _, err := conn.Exec(
			`INSERT INTO sync_log (log_time, level, message, sync_task_id) VALUES (?, 'info', ?, 1)`,
			sqlNow(-time.Minute), msg); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodGet, "/sync/{id}/logs?search=connection&range=1h", nil),
		SyncLogsHandler, map[string]string{"id": "1"})

	data, _ := decodeEnvelope(t, rec)["data"].([]interface{})
	if len(data) != 2 {
		t.Errorf("search returned %d entries, want 2", len(data))
	}
}

func TestSyncLogsHandlerReportsAMissingTable(t *testing.T) {
	tablelessDB(t)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodGet, "/sync/{id}/logs", nil),
		SyncLogsHandler, map[string]string{"id": "1"})

	if resp := decodeEnvelope(t, rec); resp["success"] != false {
		t.Errorf("success = %v, want false", resp["success"])
	}
}

func TestChangeStreamsStatusHandlerAggregates(t *testing.T) {
	conn := useMonitorDB(t)
	rows := []struct {
		taskID                            int
		coll                              string
		received, executed, pending, errs int
		inserted, updated, deleted        int
	}{
		{1, "source_db.orders", 100, 90, 10, 1, 50, 30, 10},
		{1, "source_db.users", 40, 40, 0, 0, 20, 15, 5},
		{2, "source_db.audit", 10, 5, 5, 2, 5, 0, 0},
	}
	for _, r := range rows {
		if _, err := conn.Exec(`
			INSERT INTO changestream_statistics
				(task_id, collection_name, received, executed, pending, errors, inserted, updated, deleted)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			r.taskID, r.coll, r.received, r.executed, r.pending, r.errs, r.inserted, r.updated, r.deleted); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	rec := httptest.NewRecorder()
	ChangeStreamsStatusHandler(rec, httptest.NewRequest(http.MethodGet, "/changestreams/status", nil))

	resp := decodeEnvelope(t, rec)
	if resp["success"] != true {
		t.Fatalf("success = %v (body: %s)", resp["success"], rec.Body.String())
	}
	body, err := json.Marshal(resp["data"])
	if err != nil {
		t.Fatalf("marshal data: %v", err)
	}
	// The aggregate must account for every seeded row.
	for _, want := range []string{"source_db.orders", "source_db.users", "source_db.audit"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("%q is missing from the response: %s", want, body)
		}
	}
}

func TestChangeStreamsStatusHandlerOnAnEmptyTable(t *testing.T) {
	useMonitorDB(t)

	rec := httptest.NewRecorder()
	ChangeStreamsStatusHandler(rec, httptest.NewRequest(http.MethodGet, "/changestreams/status", nil))

	if resp := decodeEnvelope(t, rec); resp["success"] != true {
		t.Errorf("success = %v, want true on an empty table", resp["success"])
	}
}

func TestChangeStreamsStatusHandlerReportsAMissingTable(t *testing.T) {
	tablelessDB(t)

	rec := httptest.NewRecorder()
	ChangeStreamsStatusHandler(rec, httptest.NewRequest(http.MethodGet, "/changestreams/status", nil))

	if resp := decodeEnvelope(t, rec); resp["success"] != false {
		t.Errorf("success = %v, want false", resp["success"])
	}
}

// tablelessDB points SYNC_DB_PATH at a database whose tables have been removed,
// which is the state a migration that did not finish — or a file restored from
// the wrong backup — leaves behind.
//
// Pointing at an empty file no longer produces one: opening the control database
// creates its schema, so the tables have to be dropped after that has happened.
// The schema is applied once per file, so later opens leave them dropped.
func tablelessDB(t *testing.T) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "empty.db")
	t.Setenv("SYNC_DB_PATH", path)

	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		t.Fatalf("open the control database: %v", err)
	}
	defer db.Close()

	rows, err := db.Query(
		`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`)
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		names = append(names, name)
	}
	rows.Close()

	for _, name := range names {
		if _, err := db.Exec(`DROP TABLE IF EXISTS "` + name + `"`); err != nil {
			t.Fatalf("drop %s: %v", name, err)
		}
	}
}
