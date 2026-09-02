package infra

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
	"github.com/retail-ai-inc/sync/internal/platform/sqlite/sqlitetest"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/retail-ai-inc/sync/internal/monitoring/domain"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
)

// useMonitoringDB points the package at a throwaway SQLite file carrying the
// monitoring_log and changestream_statistics schemas, so the writers can be
// exercised without touching the database tracked in this repository.
func useMonitoringDB(t *testing.T) *sql.DB {
	t.Helper()

	path := filepath.Join(t.TempDir(), "sync.db")
	t.Setenv("SYNC_DB_PATH", path)

	// Through the real opener, which carries the whole schema and creates it
	// only when it is missing — rather than a copy kept here that can drift from
	// it, and that a background goroutine racing to the same path turns into
	// "table already exists".
	conn, err := sqlite.OpenSQLiteDB()
	if err != nil {
		t.Fatalf("open temp sqlite: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// emptyDB points the package at a SQLite file with no tables, so the writers
// hit a missing-table error.
func emptyDB(t *testing.T) {
	t.Helper()
	sqlitetest.Tableless(t)
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

	got, err := getRowCountWithContext(t.Context(), conn, "monitoring_log")
	if err != nil {
		t.Fatalf("getRowCountWithContext: %v", err)
	}
	if got != 2 {
		t.Errorf("getRowCountWithContext = %d, want 2", got)
	}

	got, err = getRowCountWithContext(t.Context(), conn, "changestream_statistics")
	if err != nil {
		t.Fatalf("getRowCountWithContext: %v", err)
	}
	if got != 0 {
		t.Errorf("getRowCountWithContext on an empty table = %d, want 0", got)
	}
}

// Every failure used to be flattened into -1 and returned as a count, so a
// missing table, a permission that was not granted, a dropped connection and a
// cancelled context were indistinguishable from each other and, once stored,
// from real data.
func TestAFailureToCountIsNotACount(t *testing.T) {
	conn := useMonitoringDB(t)

	if _, err := getRowCountWithContext(t.Context(), conn, "no_such_table"); err == nil {
		t.Error("counting a missing table reported no error")
	}

	cancelled, cancelNow := context.WithCancel(t.Context())
	cancelNow()
	if _, err := getRowCountWithContext(cancelled, conn, "monitoring_log"); err == nil {
		t.Error("counting with a cancelled context reported no error")
	}
}

// A row is still written — writing nothing would leave the last good numbers
// looking current — but under an action that says the counts were not taken,
// so -1 is no longer something a reader has to guess about.
func TestAFailedMeasurementIsRecordedAsOne(t *testing.T) {
	conn := useMonitoringDB(t)

	if got := rowCountAction(true, true); got != actionRowCount {
		t.Errorf("action for two good counts = %q, want %q", got, actionRowCount)
	}
	for name, args := range map[string][2]bool{
		"source failed": {false, true},
		"target failed": {true, false},
		"both failed":   {false, false},
	} {
		if got := rowCountAction(args[0], args[1]); got != actionCountFailed {
			t.Errorf("action when the %s = %q, want %q", name, got, actionCountFailed)
		}
	}

	storeMonitoringLog(7, "mysql", "src", "orders", -1, "tgt", "orders", -1, actionCountFailed)

	var action string
	if err := conn.QueryRow(
		`SELECT monitor_action FROM monitoring_log WHERE sync_task_id = 7`,
	).Scan(&action); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if action != actionCountFailed {
		t.Errorf("stored action = %q, want %q", action, actionCountFailed)
	}
}

// The name was interpolated straight into the SQL text, unquoted and
// unchecked, and it runs against both the source and the target.
func TestATableNameThatIsNotOneIsRefused(t *testing.T) {
	conn := useMonitoringDB(t)
	if _, err := conn.Exec(`INSERT INTO monitoring_log (db_type) VALUES ('a'), ('b'), ('c')`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	for _, name := range []string{
		"monitoring_log WHERE db_type = 'a'",
		"monitoring_log; DROP TABLE monitoring_log",
		`monitoring_log"`,
		"",
		"a.b.c",
	} {
		if got, err := getRowCountWithContext(t.Context(), conn, name); err == nil {
			t.Errorf("the table name %q was accepted and counted %d", name, got)
		}
	}

	// A qualified name is still a name.
	if _, err := getRowCountWithContext(t.Context(), conn, "main.monitoring_log"); err != nil {
		t.Errorf("a schema-qualified name was refused: %v", err)
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

// storeMonitoringLog returns nothing and swallows every failure into a log line,
// so a missing table is invisible to the caller.
func TestStoreMonitoringLogSwallowsAMissingTable(t *testing.T) {
	emptyDB(t)

	// No panic, no error, no way for the caller to notice.
	storeMonitoringLog(1, "mysql", "s", "orders", 10, "t", "orders", 10, "row_count_minutely")
}

// --------------------------------------------------- changestream_statistics

func TestStoreChangeStreamStatisticsUpserts(t *testing.T) {
	conn := useMonitoringDB(t)

	streams := map[string]*domain.ChangeStreamInfo{
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
	streams := map[string]*domain.ChangeStreamInfo{
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

// An inactive stream was skipped rather than written, so a collection whose
// change stream had died kept its last numbers forever and the table could not
// distinguish "no events since the last cycle" from "this stopped and nobody
// noticed".
func TestAStreamThatStoppedIsStillRecorded(t *testing.T) {
	conn := useMonitoringDB(t)

	stream := &domain.ChangeStreamInfo{SyncTaskID: 1, Active: true, ReceivedEvents: 100, ExecutedEvents: 100}
	streams := map[string]*domain.ChangeStreamInfo{"db.coll": stream}

	if err := StoreChangeStreamStatistics(1, streams); err != nil {
		t.Fatalf("first store: %v", err)
	}

	stream.Active = false
	stream.ReceivedEvents = 500
	if err := StoreChangeStreamStatistics(1, streams); err != nil {
		t.Fatalf("second store: %v", err)
	}

	got, ok := readStats(t, conn, 1, "db.coll")
	if !ok {
		t.Fatal("the row was removed")
	}
	if got.Received != 500 {
		t.Errorf("received = %d, want the figures the stream last reported", got.Received)
	}
}

// They used to come from a registry — RegisterChangeStream and six functions
// that updated it — which nothing in the tree ever called, so the map was
// permanently empty: the collector asked a task for its streams, got nothing,
// wrote nothing, and logged that it had stored them.
func TestTheFiguresComeFromTheReplicationCounters(t *testing.T) {
	conn := useMonitoringDB(t)

	labels := metrics.Labels{
		"task": "1", "engine": "mongodb", "collection": "orders", "source": "tokyo:27017",
	}
	t.Cleanup(func() { metrics.Default.Forget(labels) })
	metrics.Applied(labels, 12)
	metrics.Failed(labels, 3)

	streams := changeStreamActivity(1)
	if len(streams) != 1 {
		t.Fatalf("%d streams were reported, want the one the counters name: %v", len(streams), streams)
	}

	if err := StoreChangeStreamStatistics(1, streams); err != nil {
		t.Fatalf("StoreChangeStreamStatistics: %v", err)
	}

	got, ok := readStats(t, conn, 1, "tokyo:27017.orders")
	if !ok {
		t.Fatalf("no row was written for the collection: %v", streams)
	}
	if got.Executed != 12 {
		t.Errorf("executed = %d, want 12", got.Executed)
	}
	if got.Errors != 3 {
		t.Errorf("errors = %d, want 3", got.Errors)
	}
}

// TestATaskWithNoActivityWritesNothing is the other half: a task whose syncers
// have recorded nothing has no streams to report, and inventing rows of zeroes
// for it is what made the old table so misleading.
func TestATaskWithNoActivityWritesNothing(t *testing.T) {
	conn := useMonitoringDB(t)

	empty := changeStreamActivity(4242)
	if len(empty) != 0 {
		t.Fatalf("%d streams were reported for a task with no counters", len(empty))
	}
	if err := StoreChangeStreamStatistics(4242, empty); err != nil {
		t.Fatalf("StoreChangeStreamStatistics: %v", err)
	}
	if n := countRows(t, conn, "changestream_statistics"); n != 0 {
		t.Errorf("%d rows were written for a task with no activity", n)
	}
}

func TestStoreChangeStreamStatisticsReportsAMissingTable(t *testing.T) {
	emptyDB(t)

	streams := map[string]*domain.ChangeStreamInfo{
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
		streams := map[string]*domain.ChangeStreamInfo{
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

// TestATimestampInAnotherLayoutDoesNotStopTheStatistics covers a stored value
// the writer does not produce — a hand edit, or a row from an older schema.
func TestATimestampInAnotherLayoutDoesNotStopTheStatistics(t *testing.T) {
	conn := useMonitoringDB(t)

	if _, err := conn.Exec(`
		INSERT INTO changestream_statistics
			(task_id, collection_name, received, last_updated)
		VALUES (1, 'db.coll', 5, '2026-08-19T00:00:00Z')`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	streams := map[string]*domain.ChangeStreamInfo{
		"db.coll": {SyncTaskID: 1, Active: true, ReceivedEvents: 999, ExecutedEvents: 999},
	}
	if err := StoreChangeStreamStatistics(1, streams); err != nil {
		t.Fatalf("StoreChangeStreamStatistics: %v", err)
	}

	got, _ := readStats(t, conn, 1, "db.coll")
	if got.Received != 999 {
		t.Errorf("received = %d, want the figures just reported", got.Received)
	}
}

// TestAnUnreadableTimestampIsStillNotSilent is the other half: it is written
// down, because a timestamp nothing here wrote means somebody or something has
// been editing the table.
func TestAnUnreadableTimestampIsStillNotSilent(t *testing.T) {
	if _, err := parseStoredTime("not a time at all"); err == nil {
		t.Error("parseStoredTime accepted a value that is not a timestamp")
	}
	for _, layout := range []string{
		"2026-08-19 00:00:00",
		"2026-08-19T00:00:00Z",
		"2026-08-19 00:00:00.123456",
	} {
		if _, err := parseStoredTime(layout); err != nil {
			t.Errorf("parseStoredTime(%q) = %v", layout, err)
		}
	}
}

// ------------------------------------------------------ in-memory reset
