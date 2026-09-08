package app

import (
	"errors"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/platform/sqlite/sqlitetest"
)

func TestTaskStatusIsTheWordTheUIShows(t *testing.T) {
	db := useMonitoringDB(t)
	if _, err := db.Exec(
		`INSERT INTO sync_tasks (id, enable, config_json) VALUES (1, 1, '{}'), (2, 0, '{}')`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	for id, want := range map[string]string{"1": "Running", "2": "Stopped"} {
		got, err := TaskStatus(id)
		if err != nil {
			t.Fatalf("TaskStatus(%s): %v", id, err)
		}
		if got != want {
			t.Errorf("TaskStatus(%s) = %q, want %q", id, got, want)
		}
	}

	if _, err := TaskStatus("404"); !errors.Is(err, ErrNoTask) {
		t.Errorf("TaskStatus of an unknown id returned %v, want ErrNoTask", err)
	}
}

// Nothing recorded is not the same as zero: these used to be the constants 85,
// 500 and 0.2, so a task that had never run showed the same healthy figures as
// one carrying payments.
func TestATaskWithNoMeasurementsReportsNothingRatherThanZero(t *testing.T) {
	applied, lag := TaskActivity("no-such-task")
	if applied != nil {
		t.Errorf("applied = %v for a task that never ran, want nil", applied)
	}
	if lag != nil {
		t.Errorf("lag = %v for a task that never ran, want nil", lag)
	}
}

func TestTaskActivityAddsUpAndTakesTheWorstLag(t *testing.T) {
	const task = "reports-activity"
	metrics.Applied(metrics.Labels{"task": task, "table": "orders"}, 3)
	metrics.Applied(metrics.Labels{"task": task, "table": "customers"}, 4)
	metrics.SetLag(metrics.Labels{"task": task, "table": "orders"}, 1.5)
	metrics.SetLag(metrics.Labels{"task": task, "table": "customers"}, 4.25)
	// Another task's figures share the register and must not be counted.
	metrics.Applied(metrics.Labels{"task": "someone-else"}, 99)
	metrics.SetLag(metrics.Labels{"task": "someone-else"}, 99)

	applied, lag := TaskActivity(task)
	if applied != float64(7) {
		t.Errorf("applied = %v, want 7 across the task's tables", applied)
	}
	// The worst of a task's collections is the one that matters.
	if lag != 4.25 {
		t.Errorf("lag = %v, want the worst of the two", lag)
	}
}

func TestTheReportsCannotReadATablelessDatabase(t *testing.T) {
	sqlitetest.Tableless(t)

	if _, err := TaskStatus("1"); err == nil {
		t.Error("a database with no tables reported a task's status")
	}
	if _, err := RowCountTrend("0", time.Time{}); err == nil {
		t.Error("a database with no tables reported a row count trend")
	}
	if _, err := ChangeStreamStatus(); err == nil {
		t.Error("a database with no tables reported change stream counters")
	}
}

func TestChangeStreamStatusSummarisesWhatIsStored(t *testing.T) {
	db := useMonitoringDB(t)
	if _, err := db.Exec(`
INSERT INTO changestream_statistics
  (task_id, collection_name, received, executed, pending, errors,
   inserted, updated, deleted, last_updated)
VALUES (1, 'orders', 10, 9, 1, 2, 4, 3, 2, '2026-01-01 00:00:00'),
       (1, 'customers', 5, 5, 0, 0, 5, 0, 0, '2026-01-01 00:00:00')`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	report, err := ChangeStreamStatus()
	if err != nil {
		t.Fatalf("ChangeStreamStatus: %v", err)
	}
	if len(report.Streams) != 2 {
		t.Fatalf("the report covers %d streams, want 2", len(report.Streams))
	}
	if report.TotalReceived != 15 || report.TotalErrors != 2 {
		t.Errorf("the totals read %d received and %d errors, want 15 and 2",
			report.TotalReceived, report.TotalErrors)
	}
	if report.TasksCount != 1 {
		t.Errorf("two collections of one task counted as %d tasks", report.TasksCount)
	}
}

func TestRowCountTrendReadsWhatWasStored(t *testing.T) {
	db := useMonitoringDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	if _, err := db.Exec(
		`INSERT INTO monitoring_log
		   (sync_task_id, db_type, tgt_table, src_row_count, tgt_row_count, logged_at)
		 VALUES (5, 'mysql', 'orders', 12, 11, ?)`,
		now.Format("2006-01-02 15:04:05")); err != nil {
		t.Fatalf("seed: %v", err)
	}

	samples, err := RowCountTrend("5", time.Time{})
	if err != nil {
		t.Fatalf("RowCountTrend: %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("read %d samples, want 1", len(samples))
	}
	if samples[0].Source != 12 || samples[0].Target != 11 {
		t.Errorf("the sample reads %d/%d, want 12/11", samples[0].Source, samples[0].Target)
	}
}
