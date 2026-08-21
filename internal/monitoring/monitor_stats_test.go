package monitoring

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// useMonitoringDB points the package at a throwaway SQLite file carrying the
// monitoring_log and changestream_statistics schemas, so the writers can be
// exercised without touching the database tracked in this repository.
func useMonitoringDB(t *testing.T) *sql.DB {
	t.Helper()

	path := filepath.Join(t.TempDir(), "sync.db")
	t.Setenv("SYNC_DB_PATH", path)

	conn, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open temp sqlite: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	const schema = `
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

// emptyDB points the package at a SQLite file with no tables, so the writers
// hit a missing-table error.
func emptyDB(t *testing.T) {
	t.Helper()
	t.Setenv("SYNC_DB_PATH", filepath.Join(t.TempDir(), "empty.db"))
}

type statsRow struct {
	Received, Executed, Pending, Errors int
	Inserted, Updated, Deleted          int
	LastUpdated                         string
}

func readStats(t *testing.T, conn *sql.DB, taskID int, collection string) (statsRow, bool) {
	t.Helper()

	var r statsRow
	err := conn.QueryRow(`
		SELECT received, executed, pending, errors, inserted, updated, deleted, last_updated
		FROM changestream_statistics WHERE task_id = ? AND collection_name = ?`,
		taskID, collection).Scan(&r.Received, &r.Executed, &r.Pending, &r.Errors,
		&r.Inserted, &r.Updated, &r.Deleted, &r.LastUpdated)
	if err == sql.ErrNoRows {
		return r, false
	}
	if err != nil {
		t.Fatalf("read stats: %v", err)
	}
	return r, true
}

func countRows(t *testing.T, conn *sql.DB, table string) int {
	t.Helper()

	var n int
	if err := conn.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

// ---------------------------------------------------------------- row counting

func TestGetRowCountWithContext(t *testing.T) {
	conn := useMonitoringDB(t)
	if _, err := conn.Exec(`INSERT INTO monitoring_log (db_type) VALUES ('mysql'), ('mongodb')`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if got := getRowCountWithContext(t.Context(), conn, "monitoring_log"); got != 2 {
		t.Errorf("getRowCountWithContext = %d, want 2", got)
	}
	if got := getRowCountWithContext(t.Context(), conn, "changestream_statistics"); got != 0 {
		t.Errorf("getRowCountWithContext on an empty table = %d, want 0", got)
	}
}

// Every failure is flattened into -1 and returned as if it were a count: a
// missing table, a permission error, a dropped connection and a cancelled
// context are indistinguishable from one another and, once stored, from real
// data. storeMonitoringLog then writes -1 into src_row_count / tgt_row_count,
// so monitoring_log accumulates rows that look like measurements but are not.
func TestARowCountFailureIsIndistinguishableFromData(t *testing.T) {
	conn := useMonitoringDB(t)

	if got := getRowCountWithContext(t.Context(), conn, "no_such_table"); got != -1 {
		t.Fatalf("getRowCountWithContext on a missing table = %d, want -1 — errors appear to be reported now; assert the new signal instead", got)
	}

	cancelled, cancelNow := context.WithCancel(t.Context())
	cancelNow()
	if got := getRowCountWithContext(cancelled, conn, "monitoring_log"); got != -1 {
		t.Errorf("getRowCountWithContext with a cancelled context = %d, want -1", got)
	}

	// And -1 is persisted as though it were a row count.
	storeMonitoringLog(7, "mysql", "src", "orders", -1, "tgt", "orders", -1, "row_count_minutely")

	var srcCount, tgtCount int64
	if err := conn.QueryRow(
		`SELECT src_row_count, tgt_row_count FROM monitoring_log WHERE sync_task_id = 7`,
	).Scan(&srcCount, &tgtCount); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if srcCount != -1 || tgtCount != -1 {
		t.Fatalf("stored counts = %d/%d — the sentinel appears to be filtered now", srcCount, tgtCount)
	}
}

// The table name is interpolated straight into the SQL text. Table names come
// from the sync task configuration, so anyone who can create or edit a task
// through the API controls a fragment of a query that runs against the source
// and target databases.
func TestTheTableNameIsInterpolatedIntoTheQuery(t *testing.T) {
	conn := useMonitoringDB(t)
	if _, err := conn.Exec(`INSERT INTO monitoring_log (db_type) VALUES ('a'), ('b'), ('c')`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// A "table name" that is really a query fragment is accepted and changes
	// what the count means.
	if got := getRowCountWithContext(t.Context(), conn, "monitoring_log WHERE db_type = 'a'"); got != 1 {
		t.Fatalf("a WHERE clause smuggled through the table name returned %d — the name appears to be validated or quoted now; assert the rejection instead", got)
	}
}

// ------------------------------------------------------------ monitoring_log

func TestStoreMonitoringLogWritesEveryColumn(t *testing.T) {
	conn := useMonitoringDB(t)

	storeMonitoringLog(42, "mysql", "source_db", "orders", 1200, "target_db", "orders", 1199, "row_count_minutely")

	var (
		taskID                int
		dbType, srcDB, srcTbl string
		srcCount              int64
		tgtDB, tgtTbl, action string
		tgtCount              int64
		loggedAt              time.Time
	)
	err := conn.QueryRow(`
		SELECT sync_task_id, db_type, src_db, src_table, src_row_count,
		       tgt_db, tgt_table, tgt_row_count, monitor_action, logged_at
		FROM monitoring_log`).Scan(&taskID, &dbType, &srcDB, &srcTbl, &srcCount,
		&tgtDB, &tgtTbl, &tgtCount, &action, &loggedAt)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}

	if taskID != 42 || dbType != "mysql" || srcDB != "source_db" || srcTbl != "orders" ||
		srcCount != 1200 || tgtDB != "target_db" || tgtTbl != "orders" || tgtCount != 1199 ||
		action != "row_count_minutely" {
		t.Errorf("row = %d %q %q.%q(%d) -> %q.%q(%d) %q",
			taskID, dbType, srcDB, srcTbl, srcCount, tgtDB, tgtTbl, tgtCount, action)
	}
	// The driver converts DATETIME columns to time.Time, so this is what a
	// reader actually gets back.
	if d := time.Since(loggedAt); d < -2*time.Second || d > 2*time.Second {
		t.Errorf("logged_at = %v, %v away from now", loggedAt, d)
	}
}

func TestStoreMonitoringLogAppends(t *testing.T) {
	conn := useMonitoringDB(t)

	for i := 0; i < 3; i++ {
		storeMonitoringLog(1, "mongodb", "s", "c", int64(i), "t", "c", int64(i), "row_count_minutely")
	}

	if got := countRows(t, conn, "monitoring_log"); got != 3 {
		t.Errorf("monitoring_log holds %d rows, want 3", got)
	}
}

// storeMonitoringLog returns nothing and swallows every failure into a log
// line. If monitoring_log is missing — a fresh database, a failed migration —
// every monitoring cycle silently records nothing while the caller carries on
// as if the measurement had been persisted.
func TestStoreMonitoringLogSwallowsAMissingTable(t *testing.T) {
	emptyDB(t)

	// No panic, no error, no way for the caller to notice.
	storeMonitoringLog(1, "mysql", "s", "orders", 10, "t", "orders", 10, "row_count_minutely")
}

// --------------------------------------------------- changestream_statistics

func TestStoreChangeStreamStatisticsUpserts(t *testing.T) {
	conn := useMonitoringDB(t)

	streams := map[string]*ChangeStreamInfo{
		"source_db.orders": {
			SyncTaskID: 1, Active: true,
			ReceivedEvents: 100, ExecutedEvents: 90, ErrorCount: 2,
			InsertedCount: 50, UpdatedCount: 30, DeletedCount: 10,
		},
	}

	if err := StoreChangeStreamStatistics(1, streams); err != nil {
		t.Fatalf("StoreChangeStreamStatistics: %v", err)
	}

	got, ok := readStats(t, conn, 1, "source_db.orders")
	if !ok {
		t.Fatal("no row was written")
	}
	want := statsRow{Received: 100, Executed: 90, Pending: 10, Errors: 2, Inserted: 50, Updated: 30, Deleted: 10}
	if got.Received != want.Received || got.Executed != want.Executed || got.Pending != want.Pending ||
		got.Errors != want.Errors || got.Inserted != want.Inserted || got.Updated != want.Updated ||
		got.Deleted != want.Deleted {
		t.Errorf("row = %+v, want %+v", got, want)
	}

	// A second call for the same collection updates in place.
	streams["source_db.orders"].ReceivedEvents = 200
	streams["source_db.orders"].ExecutedEvents = 200
	if err := StoreChangeStreamStatistics(1, streams); err != nil {
		t.Fatalf("second store: %v", err)
	}

	if n := countRows(t, conn, "changestream_statistics"); n != 1 {
		t.Errorf("changestream_statistics holds %d rows, want 1 after an upsert", n)
	}
	got, _ = readStats(t, conn, 1, "source_db.orders")
	if got.Received != 200 || got.Executed != 200 || got.Pending != 0 {
		t.Errorf("after the upsert row = %+v, want received/executed 200 and pending 0", got)
	}
}

func TestStoreChangeStreamStatisticsClampsPendingAtZero(t *testing.T) {
	conn := useMonitoringDB(t)

	// More executed than received: the syncer counts these independently, so
	// the difference can go negative.
	streams := map[string]*ChangeStreamInfo{
		"db.coll": {SyncTaskID: 1, Active: true, ReceivedEvents: 5, ExecutedEvents: 9},
	}
	if err := StoreChangeStreamStatistics(1, streams); err != nil {
		t.Fatalf("StoreChangeStreamStatistics: %v", err)
	}

	got, _ := readStats(t, conn, 1, "db.coll")
	if got.Pending != 0 {
		t.Errorf("pending = %d, want 0", got.Pending)
	}
}

// Inactive streams are skipped rather than marked, so a collection whose change
// stream has died keeps its last row forever. The table cannot distinguish "no
// events since the last cycle" from "this stream stopped and nobody noticed".
func TestAnInactiveStreamKeepsItsLastRow(t *testing.T) {
	conn := useMonitoringDB(t)

	stream := &ChangeStreamInfo{SyncTaskID: 1, Active: true, ReceivedEvents: 100, ExecutedEvents: 100}
	streams := map[string]*ChangeStreamInfo{"db.coll": stream}

	if err := StoreChangeStreamStatistics(1, streams); err != nil {
		t.Fatalf("first store: %v", err)
	}

	stream.Active = false
	stream.ReceivedEvents = 500 // would be written if inactive streams were stored
	if err := StoreChangeStreamStatistics(1, streams); err != nil {
		t.Fatalf("second store: %v", err)
	}

	got, ok := readStats(t, conn, 1, "db.coll")
	if !ok {
		t.Fatal("the row was removed")
	}
	if got.Received != 100 {
		t.Fatalf("received = %d — inactive streams appear to be recorded now; assert the new signal instead", got.Received)
	}
}

// The whole registry that feeds this table is disconnected: RegisterChangeStream,
// UpdateChangeStreamActivity, UpdateChangeStreamDetailedActivity,
// AccumulateChangeStreamActivity, RecordChangeStreamError and
// DeactivateChangeStream have no callers anywhere outside their own
// definitions, so changeStreamTracker is permanently empty. The monitoring loop
// calls GetActiveChangeStreamsByTaskID, gets an empty map, and hands it here —
// where the upsert loop has nothing to iterate. StoreChangeStreamStatistics
// returns nil, the caller logs a success, and not one row is ever written or
// updated. This is why the production table holds 33 rows created in 2025 whose
// counters are all still zero (T-052).
func TestAnEmptyRegistryWritesNothingAndReportsSuccess(t *testing.T) {
	conn := useMonitoringDB(t)

	// What GetActiveChangeStreamsByTaskID returns in production.
	resetTracker(t)
	empty := GetActiveChangeStreamsByTaskID(1)
	if len(empty) != 0 {
		t.Fatalf("the tracker is not empty: %d entries", len(empty))
	}

	if err := StoreChangeStreamStatistics(1, empty); err != nil {
		t.Fatalf("StoreChangeStreamStatistics(empty) = %v — an empty registry appears to be reported now; assert the error instead", err)
	}
	if n := countRows(t, conn, "changestream_statistics"); n != 0 {
		t.Fatalf("%d rows were written from an empty registry", n)
	}
}

func TestStoreChangeStreamStatisticsReportsAMissingTable(t *testing.T) {
	emptyDB(t)

	streams := map[string]*ChangeStreamInfo{
		"db.coll": {SyncTaskID: 1, Active: true, ReceivedEvents: 1, ExecutedEvents: 1},
	}
	err := StoreChangeStreamStatistics(1, streams)

	if err == nil {
		t.Fatal("StoreChangeStreamStatistics() = nil with no changestream_statistics table")
	}
}

func TestStoreChangeStreamStatisticsSeparatesTasks(t *testing.T) {
	conn := useMonitoringDB(t)

	for _, taskID := range []int{1, 2} {
		streams := map[string]*ChangeStreamInfo{
			"db.coll": {SyncTaskID: taskID, Active: true, ReceivedEvents: taskID * 10, ExecutedEvents: taskID * 10},
		}
		if err := StoreChangeStreamStatistics(taskID, streams); err != nil {
			t.Fatalf("store for task %d: %v", taskID, err)
		}
	}

	if n := countRows(t, conn, "changestream_statistics"); n != 2 {
		t.Errorf("changestream_statistics holds %d rows, want one per task", n)
	}
	for _, taskID := range []int{1, 2} {
		got, ok := readStats(t, conn, taskID, "db.coll")
		if !ok {
			t.Fatalf("no row for task %d", taskID)
		}
		if got.Received != taskID*10 {
			t.Errorf("task %d received = %d, want %d", taskID, got.Received, taskID*10)
		}
	}
}

// ------------------------------------------------------------- daily reset

func TestResetDailyStatisticsIsANoOpWithNoRecords(t *testing.T) {
	conn := useMonitoringDB(t)

	tx, err := conn.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := resetDailyStatisticsIfNeeded(tx, 1); err != nil {
		t.Errorf("resetDailyStatisticsIfNeeded on an empty table = %v", err)
	}
}

func TestResetDailyStatisticsClearsYesterdaysCounters(t *testing.T) {
	conn := useMonitoringDB(t)

	// A row last updated two days ago, in the CURRENT_TIMESTAMP (UTC) format
	// the writer uses.
	twoDaysAgo := time.Now().UTC().AddDate(0, 0, -2).Format("2006-01-02 15:04:05")
	if _, err := conn.Exec(`
		INSERT INTO changestream_statistics
			(task_id, collection_name, received, executed, pending, errors, inserted, updated, deleted, last_updated)
		VALUES (1, 'db.coll', 500, 480, 20, 3, 200, 200, 80, ?)`, twoDaysAgo); err != nil {
		t.Fatalf("seed: %v", err)
	}

	tx, err := conn.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := resetDailyStatisticsIfNeeded(tx, 1); err != nil {
		t.Fatalf("resetDailyStatisticsIfNeeded: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	got, ok := readStats(t, conn, 1, "db.coll")
	if !ok {
		t.Fatal("the row was deleted rather than reset")
	}
	if got.Received != 0 || got.Executed != 0 || got.Pending != 0 || got.Errors != 0 ||
		got.Inserted != 0 || got.Updated != 0 || got.Deleted != 0 {
		t.Errorf("row = %+v, want every counter zeroed", got)
	}
}

func TestResetDailyStatisticsLeavesTodaysCountersAlone(t *testing.T) {
	conn := useMonitoringDB(t)

	now := time.Now().UTC().Format("2006-01-02 15:04:05")
	if _, err := conn.Exec(`
		INSERT INTO changestream_statistics
			(task_id, collection_name, received, executed, last_updated)
		VALUES (1, 'db.coll', 77, 70, ?)`, now); err != nil {
		t.Fatalf("seed: %v", err)
	}

	tx, err := conn.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := resetDailyStatisticsIfNeeded(tx, 1); err != nil {
		t.Fatalf("resetDailyStatisticsIfNeeded: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	got, _ := readStats(t, conn, 1, "db.coll")
	if got.Received != 77 || got.Executed != 70 {
		t.Errorf("row = %+v, want the counters untouched", got)
	}
}

func TestResetDailyStatisticsOnlyTouchesItsOwnTask(t *testing.T) {
	conn := useMonitoringDB(t)

	twoDaysAgo := time.Now().UTC().AddDate(0, 0, -2).Format("2006-01-02 15:04:05")
	for _, taskID := range []int{1, 2} {
		if _, err := conn.Exec(`
			INSERT INTO changestream_statistics
				(task_id, collection_name, received, executed, last_updated)
			VALUES (?, 'db.coll', 100, 100, ?)`, taskID, twoDaysAgo); err != nil {
			t.Fatalf("seed task %d: %v", taskID, err)
		}
	}

	tx, err := conn.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := resetDailyStatisticsIfNeeded(tx, 1); err != nil {
		t.Fatalf("resetDailyStatisticsIfNeeded: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if got, _ := readStats(t, conn, 1, "db.coll"); got.Received != 0 {
		t.Errorf("task 1 received = %d, want 0", got.Received)
	}
	if got, _ := readStats(t, conn, 2, "db.coll"); got.Received != 100 {
		t.Errorf("task 2 received = %d, want it left alone", got.Received)
	}
}

// A last_updated value the writer never produces aborts the whole cycle:
// resetDailyStatisticsIfNeeded parses it with a single fixed layout and returns
// an error, which StoreChangeStreamStatistics propagates before writing
// anything. One malformed timestamp — from a manual edit or an older schema —
// stops statistics for that task permanently.
func TestAnUnparseableTimestampStopsStatisticsForTheTask(t *testing.T) {
	conn := useMonitoringDB(t)

	if _, err := conn.Exec(`
		INSERT INTO changestream_statistics
			(task_id, collection_name, received, last_updated)
		VALUES (1, 'db.coll', 5, '2026-08-19T00:00:00Z')`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	streams := map[string]*ChangeStreamInfo{
		"db.coll": {SyncTaskID: 1, Active: true, ReceivedEvents: 999, ExecutedEvents: 999},
	}
	err := StoreChangeStreamStatistics(1, streams)

	if err == nil {
		t.Fatalf("StoreChangeStreamStatistics() = nil — the timestamp appears to be tolerated now; assert the write instead")
	}

	got, _ := readStats(t, conn, 1, "db.coll")
	if got.Received != 5 {
		t.Errorf("received = %d, want the pre-existing 5 (nothing should have been written)", got.Received)
	}
}

// ------------------------------------------------------ in-memory reset

func TestResetInMemoryStatistics(t *testing.T) {
	resetTracker(t)

	RegisterChangeStream(1, "db", "a")
	RegisterChangeStream(1, "db", "b")
	RegisterChangeStream(2, "db", "c")
	AccumulateChangeStreamActivity("db", "a", 10, 10, 8, 5, 3, 2)
	AccumulateChangeStreamActivity("db", "c", 20, 20, 20, 10, 5, 5)
	RecordChangeStreamError("db", "a", "boom")

	before := GetActiveChangeStreamsByTaskID(1)["db.a"]
	created, lastActivity, active := before.Created, before.LastActivity, before.Active

	resetInMemoryStatistics(1)

	a := GetActiveChangeStreamsByTaskID(1)["db.a"]
	if a.ReceivedEvents != 0 || a.ExecutedEvents != 0 || a.EventCount != 0 ||
		a.InsertedCount != 0 || a.UpdatedCount != 0 || a.DeletedCount != 0 || a.ErrorCount != 0 {
		t.Errorf("task 1 counters were not cleared: %+v", a)
	}
	if !a.Created.Equal(created) || !a.LastActivity.Equal(lastActivity) || a.Active != active {
		t.Error("resetInMemoryStatistics changed a field other than the counters")
	}

	c := GetActiveChangeStreamsByTaskID(2)["db.c"]
	if c.ReceivedEvents != 20 {
		t.Errorf("task 2 received = %d, want it left alone", c.ReceivedEvents)
	}
}

// The error message is kept while the count that justified it is cleared, so
// after a daily reset a stream reports zero errors alongside a stale
// LastErrorMsg from a previous day.
func TestResetInMemoryStatisticsKeepsTheStaleErrorMessage(t *testing.T) {
	resetTracker(t)

	RegisterChangeStream(1, "db", "a")
	RecordChangeStreamError("db", "a", "yesterday's failure")

	resetInMemoryStatistics(1)

	a := GetActiveChangeStreamsByTaskID(1)["db.a"]
	if a.ErrorCount != 0 {
		t.Fatalf("ErrorCount = %d, want 0", a.ErrorCount)
	}
	if a.LastErrorMsg != "yesterday's failure" {
		t.Fatalf("LastErrorMsg = %q — it appears to be cleared now; assert the empty value instead", a.LastErrorMsg)
	}
}

func TestResetInMemoryStatisticsOnAnUnknownTask(t *testing.T) {
	resetTracker(t)
	RegisterChangeStream(1, "db", "a")

	resetInMemoryStatistics(99) // must not panic or touch task 1

	if n := len(GetActiveChangeStreamsByTaskID(1)); n != 1 {
		t.Errorf("task 1 has %d streams, want 1", n)
	}
}
