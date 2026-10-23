package app

import (
	"testing"

	"github.com/retail-ai-inc/sync/internal/platform/sqlite/sqlitetest"
)

func TestTheReportsCannotReadATablelessDatabase(t *testing.T) {
	sqlitetest.Tableless(t)

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
