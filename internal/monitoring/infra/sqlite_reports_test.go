package infra

import (
	"errors"
	"testing"

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
