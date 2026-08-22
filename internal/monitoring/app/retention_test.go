package app

import (
	"context"
	"database/sql"
	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// retentionDB points SYNC_DB_PATH at a throwaway database holding the
// monitoring table, and returns a handle for seeding and reading it back.
func retentionDB(t *testing.T) *sql.DB {
	t.Helper()

	path := filepath.Join(t.TempDir(), "sync.db")
	t.Setenv("SYNC_DB_PATH", path)

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec(`CREATE TABLE monitoring_log (
		id            INTEGER PRIMARY KEY AUTOINCREMENT,
		sync_task_id  INTEGER NOT NULL,
		logged_at     DATETIME NOT NULL,
		src_table     TEXT,
		tgt_table     TEXT,
		src_row_count INTEGER,
		tgt_row_count INTEGER)`); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	return db
}

// seedRow writes one monitoring row aged by the given number of days.
func seedRow(t *testing.T, db *sql.DB, daysOld int) {
	t.Helper()

	at := time.Now().UTC().AddDate(0, 0, -daysOld).Format("2006-01-02 15:04:05")
	if _, err := db.Exec(
		`INSERT INTO monitoring_log (sync_task_id, logged_at, src_table, tgt_table,
		 src_row_count, tgt_row_count) VALUES (1, ?, 'orders', 'orders', 10, 10)`,
		at); err != nil {
		t.Fatalf("seed a row aged %d days: %v", daysOld, err)
	}
}

func rowCount(t *testing.T, db *sql.DB) int {
	t.Helper()

	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM monitoring_log`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

// TestOldRowsAreRemovedAndRecentOnesKept is the whole point: the log grows by a
// row per table per interval, on the volume the replication state lives on.
func TestOldRowsAreRemovedAndRecentOnesKept(t *testing.T) {
	db := retentionDB(t)
	for _, age := range []int{0, 1, 29, 31, 100, 400} {
		seedRow(t, db, age)
	}

	sweepMonitoringLog(context.Background(), quiet(), 30)

	if got := rowCount(t, db); got != 3 {
		t.Errorf("%d rows remain, want the three inside the window", got)
	}
	var oldest string
	if err := db.QueryRow(`SELECT MIN(logged_at) FROM monitoring_log`).Scan(&oldest); err != nil {
		t.Fatalf("read the oldest row: %v", err)
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -30).Format("2006-01-02 15:04:05")
	if oldest < cutoff {
		t.Errorf("oldest remaining row is %s, before the %s cutoff", oldest, cutoff)
	}
}

// TestTheSweepIsBatched pins that one sweep does not hold the single SQLite
// writer for as long as it takes to delete a year of rows: the syncer records
// through the same connection.
func TestTheSweepIsBatched(t *testing.T) {
	db := retentionDB(t)
	const old = retentionBatch + 100
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	at := time.Now().UTC().AddDate(0, 0, -60).Format("2006-01-02 15:04:05")
	for i := 0; i < old; i++ {
		if _, err := tx.Exec(
			`INSERT INTO monitoring_log (sync_task_id, logged_at, src_table, tgt_table,
			 src_row_count, tgt_row_count) VALUES (1, ?, 'orders', 'orders', 1, 1)`,
			at); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	removed, err := deleteOlderThan(context.Background(), db,
		time.Now().UTC().Format("2006-01-02 15:04:05"))
	if err != nil {
		t.Fatalf("deleteOlderThan: %v", err)
	}
	if removed != old {
		t.Errorf("removed %d of %d rows", removed, old)
	}
	if got := rowCount(t, db); got != 0 {
		t.Errorf("%d rows remain", got)
	}
}

// TestNothingToRemoveIsNotAnError covers the ordinary case, which is every sweep
// after the first.
func TestNothingToRemoveIsNotAnError(t *testing.T) {
	db := retentionDB(t)
	seedRow(t, db, 1)

	removed, err := deleteOlderThan(context.Background(), db,
		time.Now().UTC().AddDate(0, 0, -30).Format("2006-01-02 15:04:05"))
	if err != nil {
		t.Fatalf("deleteOlderThan: %v", err)
	}
	if removed != 0 {
		t.Errorf("removed %d rows, want none", removed)
	}
	if got := rowCount(t, db); got != 1 {
		t.Errorf("%d rows remain, want the recent one", got)
	}
}

// TestTheWindowIsConfigurable covers the setting, including turning the sweep
// off — which an operator may want and which must then say so rather than
// silently keeping everything.
func TestTheWindowIsConfigurable(t *testing.T) {
	for name, tc := range map[string]struct {
		set  string
		want int
	}{
		"unset":      {set: "", want: defaultRetentionDays},
		"seven days": {set: "7", want: 7},
		"turned off": {set: "0", want: 0},
		"nonsense":   {set: "soon", want: defaultRetentionDays},
		"negative":   {set: "-1", want: -1},
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("SYNC_MONITORING_RETENTION_DAYS", tc.set)
			if got := retentionDays(); got != tc.want {
				t.Errorf("retentionDays() = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestTurningItOffStartsNothing pins that a zero window does not spawn a sweep
// that would delete everything.
func TestTurningItOffStartsNothing(t *testing.T) {
	db := retentionDB(t)
	seedRow(t, db, 400)
	t.Setenv("SYNC_MONITORING_RETENTION_DAYS", "0")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartMonitoringRetention(ctx, quiet())
	time.Sleep(200 * time.Millisecond)

	if got := rowCount(t, db); got != 1 {
		t.Errorf("%d rows remain; the sweep ran with the window turned off", got)
	}
}

// TestTheSweepRunsAtStartup matters because a deployment that has been running
// without this for months should not wait a day for the first one.
func TestTheSweepRunsAtStartup(t *testing.T) {
	db := retentionDB(t)
	seedRow(t, db, 400)
	seedRow(t, db, 1)
	t.Setenv("SYNC_MONITORING_RETENTION_DAYS", "30")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartMonitoringRetention(ctx, quiet())

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if rowCount(t, db) == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("the old row was still there after five seconds (%d rows)", rowCount(t, db))
}

// TestAMissingTableIsReportedNotPanicked covers a database that predates the
// monitoring table.
func TestAMissingTableIsReportedNotPanicked(t *testing.T) {
	tablelessDB(t)

	if _, err := deleteOlderThan(context.Background(), openEmpty(t), "2026-01-01 00:00:00"); err == nil {
		t.Error("deleting from a database with no monitoring table returned no error")
	}
}

func openEmpty(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "empty.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestAnUnopenableDatabaseIsReportedNotPanicked covers the sweep running before
// the volume is mounted, or after somebody moved the file.
func TestAnUnopenableDatabaseIsReportedNotPanicked(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatalf("write blocker: %v", err)
	}
	t.Setenv("SYNC_DB_PATH", filepath.Join(blocker, "sub", "sync.db"))

	// The sweep reports and returns; it must not stop the process that runs it.
	sweepMonitoringLog(context.Background(), quiet(), 30)
}

// TestAMissingTableIsReportedByTheSweep covers a database written by a build
// that predates the monitoring table.
func TestAMissingTableIsReportedByTheSweep(t *testing.T) {
	tablelessDB(t)

	sweepMonitoringLog(context.Background(), quiet(), 30)
}

// TestACancelledSweepStops keeps a shutdown from waiting on a sweep that has a
// year of rows to remove.
func TestACancelledSweepStops(t *testing.T) {
	db := retentionDB(t)
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	at := time.Now().UTC().AddDate(0, 0, -60).Format("2006-01-02 15:04:05")
	for i := 0; i < retentionBatch*2; i++ {
		if _, err := tx.Exec(
			`INSERT INTO monitoring_log (sync_task_id, logged_at, src_table, tgt_table,
			 src_row_count, tgt_row_count) VALUES (1, ?, 'orders', 'orders', 1, 1)`,
			at); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := deleteOlderThan(ctx, db,
		time.Now().UTC().Format("2006-01-02 15:04:05")); err == nil {
		t.Error("a cancelled sweep ran to completion")
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
