package config

import (
	"strings"
	"testing"
)

// LoadSyncTask reads one task by id. It exists for the paths that run with no
// configuration loaded: deleting a task has to read its target before the row
// goes, and the switch-over endpoint has to read a task that may not be
// running.

func TestLoadSyncTaskReadsTheTaskAsked(t *testing.T) {
	db := useTempConfigDB(t)
	for _, cfg := range []string{
		`{"type":"mysql","taskName":"first"}`,
		`{"type":"redis","taskName":"second"}`,
	} {
		if _, err := db.Exec(`INSERT INTO sync_tasks (enable, config_json) VALUES (1, ?)`,
			cfg); err != nil {
			t.Fatalf("insert task: %v", err)
		}
	}

	task, err := LoadSyncTask(2)
	if err != nil {
		t.Fatalf("LoadSyncTask(2): %v", err)
	}
	if task.ID != 2 {
		t.Errorf("read task %d, want 2", task.ID)
	}
	if task.Type != "redis" {
		t.Errorf("read a %s task, want the redis one", task.Type)
	}
}

// TestLoadSyncTaskOfATaskThatIsNotThereIsAnError, and names the id. The two
// callers both do something destructive next -- purging a target's positions,
// or reporting how far a switch-over has got -- so an empty task returned as a
// success would have them act on a zero value.
func TestLoadSyncTaskOfATaskThatIsNotThereIsAnError(t *testing.T) {
	useTempConfigDB(t)

	task, err := LoadSyncTask(9999)
	if err == nil {
		t.Fatal("a task that does not exist was returned as a success")
	}
	if !strings.Contains(err.Error(), "9999") {
		t.Errorf("the error does not name the id: %v", err)
	}
	if task.ID != 0 || task.Type != "" {
		t.Errorf("a partially filled task was returned: %+v", task)
	}
}

// TestLoadSyncTaskFindsADisabledTask. Both callers ask about tasks that are
// stopped -- one is deleting it, and the other is deciding whether the region
// it wrote to is safe to promote, which is asked precisely when it has stopped.
func TestLoadSyncTaskFindsADisabledTask(t *testing.T) {
	db := useTempConfigDB(t)
	if _, err := db.Exec(`INSERT INTO sync_tasks (enable, config_json) VALUES (0, ?)`,
		`{"type":"mongodb","taskName":"stopped"}`); err != nil {
		t.Fatalf("insert task: %v", err)
	}

	task, err := LoadSyncTask(1)
	if err != nil {
		t.Fatalf("LoadSyncTask on a stopped task: %v", err)
	}
	if task.Enable {
		t.Error("the task reported itself enabled")
	}
	if task.Type != "mongodb" {
		t.Errorf("read a %s task", task.Type)
	}
}

func TestLoadSyncTaskWithNoDatabaseIsReported(t *testing.T) {
	t.Setenv("SYNC_DB_PATH", t.TempDir()) // a directory, which cannot be opened as a file

	if _, err := LoadSyncTask(1); err == nil {
		t.Error("an unopenable control database was reported as a success")
	}
}
