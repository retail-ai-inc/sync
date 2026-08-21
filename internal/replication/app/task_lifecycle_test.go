package app

import (
	"strings"
	"testing"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra"
)

func TestCreateTaskStoresAndEchoesTheRequest(t *testing.T) {
	db := useTempTaskDB(t)

	id, stored, now, enable, err := CreateTask(domain.Request{
		TaskName: "orders", SourceType: "mongodb", Status: "Running",
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if id == 0 {
		t.Fatal("CreateTask returned id 0")
	}
	if enable != 1 {
		t.Errorf("enable = %d for a Running task, want 1", enable)
	}
	if stored.TaskName != "orders" || stored.Status != "Running" {
		t.Errorf("stored = %q/%q", stored.TaskName, stored.Status)
	}
	if !strings.HasPrefix(now, "20") || len(now) != 19 {
		t.Errorf("now = %q, want a SQL timestamp", now)
	}

	cfg := readConfig(t, db, id)
	if !strings.Contains(cfg, `"taskName":"orders"`) || !strings.Contains(cfg, `"type":"mongodb"`) {
		t.Errorf("stored document = %s", cfg)
	}
}

func TestCreateTaskAppliesTheDefaults(t *testing.T) {
	db := useTempTaskDB(t)

	id, stored, _, enable, err := CreateTask(domain.Request{})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if stored.TaskName != "Sync Task" {
		t.Errorf("TaskName = %q, want the default", stored.TaskName)
	}
	if stored.Status != domain.StatusStopped {
		t.Errorf("Status = %q, want %q", stored.Status, domain.StatusStopped)
	}
	if enable != 0 {
		t.Errorf("enable = %d for a defaulted task, want 0", enable)
	}
	if !strings.Contains(readConfig(t, db, id), `"taskName":"Sync Task"`) {
		t.Error("the default name was not stored")
	}
}

// TestTheEchoedRequestIsTheNormalisedOne records that the caller is handed the
// request after the defaults were applied, not the one it sent. The endpoint
// therefore echoes the stored task rather than the submitted body.
func TestTheEchoedRequestIsTheNormalisedOne(t *testing.T) {
	useTempTaskDB(t)

	sent := domain.Request{}
	_, stored, _, _, err := CreateTask(sent)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if stored.TaskName == sent.TaskName {
		t.Fatal("the echoed request is the submitted one now; assert that instead")
	}
}

func TestCreateTaskPropagatesAStoreFailure(t *testing.T) {
	emptyTaskDB(t)

	if _, _, _, _, err := CreateTask(domain.Request{}); err == nil {
		t.Error("CreateTask on a database with no tables returned no error")
	}
}

func TestUpdateTaskReplacesTheStoredConfiguration(t *testing.T) {
	db := useTempTaskDB(t)
	id := insertTask(t, db, 0, `{"taskName":"before","type":"mysql","status":"Stopped"}`)

	stored, err := UpdateTask(itoa(id), domain.Request{
		TaskName: "after", SourceType: "mongodb", Status: "Running",
	})
	if err != nil {
		t.Fatalf("UpdateTask: %v", err)
	}
	if stored.TaskName != "after" {
		t.Errorf("stored name = %q", stored.TaskName)
	}

	cfg := readConfig(t, db, id)
	if !strings.Contains(cfg, `"taskName":"after"`) || strings.Contains(cfg, `"type":"mysql"`) {
		t.Errorf("stored document = %s", cfg)
	}
}

// TestAnUpdateWipesWhateverTheRequestOmits records that unlike the backup
// update, this one carries nothing over from the stored document: a request that
// omits the connections stores nulls for them, and the running syncer keeps
// using the values it read at process start.
func TestAnUpdateWipesWhateverTheRequestOmits(t *testing.T) {
	db := useTempTaskDB(t)
	id := insertTask(t, db, 1,
		`{"taskName":"orders","type":"mongodb","sourceConn":{"host":"src"},"mappings":[{"a":1}]}`)

	if _, err := UpdateTask(itoa(id), domain.Request{TaskName: "orders", SourceType: "mongodb"}); err != nil {
		t.Fatalf("UpdateTask: %v", err)
	}

	cfg := readConfig(t, db, id)
	if !strings.Contains(cfg, `"sourceConn":null`) || !strings.Contains(cfg, `"mappings":null`) {
		t.Fatalf("the omitted fields survived: %s — a merge appears to have been "+
			"added, so assert that instead", cfg)
	}
}

// TestAnUpdateAlsoDefaultsTheNameAndStatus records that the update applies the
// same defaults as the create, so a request that omits the name renames the
// task to "Sync Task" rather than keeping what was stored. The backup update
// carries the stored name over; this one does not.
func TestAnUpdateAlsoDefaultsTheNameAndStatus(t *testing.T) {
	db := useTempTaskDB(t)
	id := insertTask(t, db, 1, `{"taskName":"orders","type":"mongodb","status":"Running"}`)

	stored, err := UpdateTask(itoa(id), domain.Request{SourceType: "mongodb"})
	if err != nil {
		t.Fatalf("UpdateTask: %v", err)
	}
	if stored.TaskName != "Sync Task" {
		t.Fatalf("TaskName = %q; the stored name appears to be carried over now, "+
			"so assert that instead", stored.TaskName)
	}
	if stored.Status != domain.StatusStopped {
		t.Errorf("Status = %q, want %q — an update with no status stops the task",
			stored.Status, domain.StatusStopped)
	}
	if !strings.Contains(readConfig(t, db, id), `"taskName":"Sync Task"`) {
		t.Error("the renamed task was not stored")
	}
}

func TestUpdateTaskOnAnUnknownID(t *testing.T) {
	useTempTaskDB(t)

	if _, err := UpdateTask("999", domain.Request{}); err != infra.ErrNoSuchTask {
		t.Errorf("UpdateTask on an unknown id = %v, want ErrNoSuchTask", err)
	}
}

func TestDeleteTask(t *testing.T) {
	db := useTempTaskDB(t)
	id := insertTask(t, db, 1, `{}`)

	if err := DeleteTask(itoa(id)); err != nil {
		t.Fatalf("DeleteTask: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sync_tasks`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Errorf("%d rows survived", count)
	}
}

func TestDeleteTaskOnAnUnknownID(t *testing.T) {
	useTempTaskDB(t)

	if err := DeleteTask("999"); err != infra.ErrNoSuchTask {
		t.Errorf("DeleteTask on an unknown id = %v, want ErrNoSuchTask", err)
	}
}

func TestStartAndStopFlipTheStoredStatus(t *testing.T) {
	db := useTempTaskDB(t)
	id := insertTask(t, db, 0, `{"taskName":"orders","status":"Stopped"}`)

	if err := StartTask(itoa(id)); err != nil {
		t.Fatalf("StartTask: %v", err)
	}
	var enable int
	if err := db.QueryRow(`SELECT enable FROM sync_tasks WHERE id=?`, id).Scan(&enable); err != nil {
		t.Fatalf("read enable: %v", err)
	}
	if enable != 1 {
		t.Errorf("enable = %d after StartTask, want 1", enable)
	}
	if !strings.Contains(readConfig(t, db, id), `"status":"Running"`) {
		t.Error("StartTask did not update the document")
	}

	if err := StopTask(itoa(id)); err != nil {
		t.Fatalf("StopTask: %v", err)
	}
	if err := db.QueryRow(`SELECT enable FROM sync_tasks WHERE id=?`, id).Scan(&enable); err != nil {
		t.Fatalf("read enable: %v", err)
	}
	if enable != 0 {
		t.Errorf("enable = %d after StopTask, want 0", enable)
	}
	if !strings.Contains(readConfig(t, db, id), `"status":"Stopped"`) {
		t.Error("StopTask did not update the document")
	}
}

// TestStartingATaskDoesNotStartASyncer records T-009 and T-010: the endpoints
// write the status and nothing else. The syncers are constructed once, at
// process start, from the configuration as it was then, so a task started here
// does not begin replicating and a task stopped here keeps going until the
// process restarts.
func TestStartingATaskDoesNotStartASyncer(t *testing.T) {
	db := useTempTaskDB(t)
	id := insertTask(t, db, 0, `{"taskName":"orders","type":"mongodb","status":"Stopped"}`)

	before := readConfig(t, db, id)
	if err := StartTask(itoa(id)); err != nil {
		t.Fatalf("StartTask: %v", err)
	}
	after := readConfig(t, db, id)

	// The only difference is the status. Nothing about the running process is
	// recorded, because nothing about it changed.
	if before == after {
		t.Fatal("StartTask changed nothing at all")
	}
	if !strings.Contains(after, `"type":"mongodb"`) {
		t.Error("the engine was lost")
	}
}

func TestStartTaskOnAnUnknownID(t *testing.T) {
	useTempTaskDB(t)

	if err := StartTask("999"); err == nil {
		t.Error("StartTask on an unknown id returned no error")
	}
}

func TestStartTaskReportsAnUnopenableDatabase(t *testing.T) {
	unopenableDB(t)

	if err := StartTask("1"); err == nil {
		t.Error("StartTask returned no error for an unopenable database")
	}
}
