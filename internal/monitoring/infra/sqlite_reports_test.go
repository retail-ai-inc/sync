package infra

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func TestTaskEnabledReadsTheSwitch(t *testing.T) {
	db := useMonitoringDB(t)

	if _, err := db.Exec(
		`INSERT INTO sync_tasks (id, enable, config_json) VALUES (1, 1, '{}'), (2, 0, '{}')`); err != nil {
		t.Fatalf("seed sync_tasks: %v", err)
	}

	for _, c := range []struct {
		taskID string
		want   bool
	}{{"1", true}, {"2", false}} {
		got, err := TaskEnabled(c.taskID)
		if err != nil {
			t.Fatalf("TaskEnabled(%s): %v", c.taskID, err)
		}
		if got != c.want {
			t.Errorf("TaskEnabled(%s) = %v, want %v", c.taskID, got, c.want)
		}
	}

	// A task that is not there is not the same as a task that is switched off,
	// and the endpoint answers them differently.
	if _, err := TaskEnabled("404"); !errors.Is(err, ErrNoTask) {
		t.Errorf("TaskEnabled of an unknown id returned %v, want ErrNoTask", err)
	}
}

func TestTaskEnabledCannotReadATablelessDatabase(t *testing.T) {
	emptyDB(t)
	if _, err := TaskEnabled("1"); err == nil {
		t.Error("a database with no tables reported a task's state")
	}
}

func insertSample(t *testing.T, db *sql.DB, taskID int, table string,
	at time.Time, source, target int) {

	t.Helper()
	if _, err := db.Exec(
		`INSERT INTO monitoring_log
		   (sync_task_id, db_type, tgt_table, src_row_count, tgt_row_count, logged_at)
		 VALUES (?, 'mysql', ?, ?, ?, ?)`,
		taskID, table, source, target, at.UTC().Format(storedTimeFormat)); err != nil {
		t.Fatalf("seed monitoring_log: %v", err)
	}
}

func TestRowCountHistoryFiltersByTaskAndWindow(t *testing.T) {
	db := useMonitoringDB(t)

	now := time.Now().UTC().Truncate(time.Second)
	insertSample(t, db, 1, "orders", now.Add(-3*time.Hour), 10, 10)
	insertSample(t, db, 1, "orders", now.Add(-1*time.Hour), 20, 19)
	insertSample(t, db, 2, "customers", now.Add(-1*time.Hour), 5, 5)

	all, err := RowCountHistory("0", time.Time{})
	if err != nil {
		t.Fatalf("RowCountHistory: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("every task over no window read %d samples, want 3", len(all))
	}
	// Ascending, because the chart draws them in order.
	if all[0].LoggedAt > all[len(all)-1].LoggedAt {
		t.Errorf("the samples came back newest first: %q then %q",
			all[0].LoggedAt, all[len(all)-1].LoggedAt)
	}

	one, err := RowCountHistory("1", time.Time{})
	if err != nil {
		t.Fatalf("RowCountHistory: %v", err)
	}
	if len(one) != 2 {
		t.Errorf("task 1 read %d samples, want 2", len(one))
	}

	recent, err := RowCountHistory("1", now.Add(-2*time.Hour))
	if err != nil {
		t.Fatalf("RowCountHistory: %v", err)
	}
	if len(recent) != 1 {
		t.Fatalf("a two-hour window read %d samples, want 1", len(recent))
	}
	if recent[0].Source != 20 || recent[0].Target != 19 {
		t.Errorf("the sample read back as %d/%d, want 20/19", recent[0].Source, recent[0].Target)
	}
	if recent[0].Table != "orders" {
		t.Errorf("table = %q, want orders", recent[0].Table)
	}
}

func TestRowCountHistoryCannotReadATablelessDatabase(t *testing.T) {
	emptyDB(t)
	if _, err := RowCountHistory("0", time.Time{}); err == nil {
		t.Error("a database with no tables reported row counts")
	}
}

func TestTaskLogsComeBackNewestFirstAndWindowed(t *testing.T) {
	db := useMonitoringDB(t)

	now := time.Now().UTC().Truncate(time.Second)
	for _, entry := range []struct {
		at      time.Time
		level   string
		message string
	}{
		{now.Add(-3 * time.Hour), "info", "older"},
		{now.Add(-1 * time.Hour), "error", "newer"},
	} {
		if _, err := db.Exec(
			`INSERT INTO sync_log (sync_task_id, log_time, level, message) VALUES (?, ?, ?, ?)`,
			7, entry.at.Format(storedTimeFormat), entry.level, entry.message); err != nil {
			t.Fatalf("seed sync_log: %v", err)
		}
	}
	if _, err := db.Exec(
		`INSERT INTO sync_log (sync_task_id, log_time, level, message) VALUES (?, ?, ?, ?)`,
		8, now.Format(storedTimeFormat), "info", "another task"); err != nil {
		t.Fatalf("seed sync_log: %v", err)
	}

	entries, err := TaskLogs("7", time.Time{})
	if err != nil {
		t.Fatalf("TaskLogs: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("task 7 read %d lines, want 2", len(entries))
	}
	if entries[0].Message != "newer" {
		t.Errorf("the first line is %q, want the newest", entries[0].Message)
	}

	windowed, err := TaskLogs("7", now.Add(-2*time.Hour))
	if err != nil {
		t.Fatalf("TaskLogs: %v", err)
	}
	if len(windowed) != 1 || windowed[0].Level != "error" {
		t.Errorf("a two-hour window read %+v, want the one error line", windowed)
	}
}

func TestTaskLogsCannotReadATablelessDatabase(t *testing.T) {
	emptyDB(t)
	if _, err := TaskLogs("1", time.Time{}); err == nil {
		t.Error("a database with no tables reported log lines")
	}
}

func TestChangeStreamStatisticsReadsEveryCounter(t *testing.T) {
	db := useMonitoringDB(t)

	if _, err := db.Exec(`
INSERT INTO changestream_statistics
  (task_id, collection_name, received, executed, pending, errors,
   inserted, updated, deleted, last_updated)
VALUES (2, 'beta', 5, 4, 1, 0, 2, 1, 1, '2026-01-01 00:00:00'),
       (1, 'alpha', 9, 9, 0, 1, 3, 3, 3, '2026-01-02 00:00:00')`); err != nil {
		t.Fatalf("seed changestream_statistics: %v", err)
	}

	stats, err := ChangeStreamStatistics()
	if err != nil {
		t.Fatalf("ChangeStreamStatistics: %v", err)
	}
	if len(stats) != 2 {
		t.Fatalf("read %d rows, want 2", len(stats))
	}
	// Ordered by task then collection, so the page does not reshuffle between
	// refreshes.
	if stats[0].Collection != "alpha" || stats[1].Collection != "beta" {
		t.Errorf("the rows came back as %q then %q", stats[0].Collection, stats[1].Collection)
	}
	if stats[0].Received != 9 || stats[0].Errors != 1 {
		t.Errorf("alpha read back as received=%d errors=%d, want 9 and 1",
			stats[0].Received, stats[0].Errors)
	}
}

func TestChangeStreamStatisticsCannotReadATablelessDatabase(t *testing.T) {
	emptyDB(t)
	if _, err := ChangeStreamStatistics(); err == nil {
		t.Error("a database with no tables reported change stream counters")
	}
}
