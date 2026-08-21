package app

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

func TestListTasksResolvesEachConfiguration(t *testing.T) {
	db := useTempTaskDB(t)
	insertTask(t, db, 1, `{"taskName":"orders","type":"mongodb","status":"Running"}`)
	insertTask(t, db, 0, `{"type":"mysql"}`)

	views, err := ListTasks()
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(views) != 2 {
		t.Fatalf("ListTasks returned %d views, want 2", len(views))
	}

	if views[0].Name != "orders" || views[0].Status != "Running" {
		t.Errorf("first view = %q/%q", views[0].Name, views[0].Status)
	}
	if views[0].Config.Type != "mongodb" {
		t.Errorf("first config type = %q", views[0].Config.Type)
	}
	// The second task has no name, so one is generated from its id.
	if views[1].Name != "Sync Task "+itoa(int64(views[1].Task.ID())) {
		t.Errorf("second view name = %q", views[1].Name)
	}
	if views[1].Status != domain.StatusStopped {
		t.Errorf("second view status = %q, want %q", views[1].Status, domain.StatusStopped)
	}
}

// TestACorruptConfigurationIsListedWithEmptyFields records that a document that
// will not parse is logged and carried through as the zero value, so the task
// appears in the list with no engine, no connections and no mappings rather
// than failing the request. An operator sees a task that looks unconfigured.
func TestACorruptConfigurationIsListedWithEmptyFields(t *testing.T) {
	db := useTempTaskDB(t)
	insertTask(t, db, 1, `{"taskName":`)

	views, err := ListTasks()
	if err != nil {
		t.Fatalf("ListTasks returned an error for a corrupt document: %v — the "+
			"failure appears to be reported now, so assert that instead", err)
	}
	if len(views) != 1 {
		t.Fatalf("ListTasks returned %d views, want 1", len(views))
	}
	if views[0].Config.Type != "" || views[0].Config.SourceConn != nil {
		t.Errorf("the corrupt document produced %+v, want the zero value", views[0].Config)
	}
	if !strings.HasPrefix(views[0].Name, "Sync Task ") {
		t.Errorf("Name = %q, want a generated one", views[0].Name)
	}
	if views[0].Status != domain.StatusRunning {
		t.Errorf("Status = %q; the enable column is the only thing left to read",
			views[0].Status)
	}
}

func TestListTasksOnAnEmptyTable(t *testing.T) {
	useTempTaskDB(t)

	views, err := ListTasks()
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(views) != 0 {
		t.Errorf("ListTasks returned %d views", len(views))
	}
}

func TestListTasksPropagatesAStoreFailure(t *testing.T) {
	emptyTaskDB(t)

	if _, err := ListTasks(); err == nil {
		t.Error("ListTasks on a database with no tables returned no error")
	}
}

func TestTableProgressSummarisesTheDay(t *testing.T) {
	db := useTempTaskDB(t)
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	day := now.Format("2006-01-02")
	id := insertTask(t, db, 1, `{"type":"mysql"}`)

	insertMonitoringRow(t, db, int(id), day+" 01:00:00", "orders", 100, 100)
	insertMonitoringRow(t, db, int(id), day+" 02:00:00", "orders", 180, 175)

	stats, err := TableProgress(context.Background(), itoa(id), now)
	if err != nil {
		t.Fatalf("TableProgress: %v", err)
	}
	if len(stats) != 1 {
		t.Fatalf("TableProgress returned %d rows, want 1", len(stats))
	}
	if stats[0].SyncedToday != 75 || stats[0].TotalRows != 175 {
		t.Errorf("stats = %+v, want SyncedToday 75 and TotalRows 175", stats[0])
	}
}

func TestTableProgressOnAnEmptyLog(t *testing.T) {
	db := useTempTaskDB(t)
	id := insertTask(t, db, 1, `{"type":"mysql"}`)

	stats, err := TableProgress(context.Background(), itoa(id), time.Now().UTC())
	if err != nil {
		t.Fatalf("TableProgress: %v", err)
	}
	if len(stats) != 0 {
		t.Errorf("TableProgress returned %d rows, want 0", len(stats))
	}
}

// TestALiveCountIsOnlyAttemptedForMongoDB records that the live collection count
// is reserved for MongoDB tasks: a MySQL task's figures come from the monitoring
// log alone, however stale they are.
func TestALiveCountIsOnlyAttemptedForMongoDB(t *testing.T) {
	db := useTempTaskDB(t)
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	day := now.Format("2006-01-02")
	id := insertTask(t, db, 1, `{"type":"mysql"}`)
	insertMonitoringRow(t, db, int(id), day+" 01:00:00", "orders", 10, 10)

	stats, err := TableProgress(context.Background(), itoa(id), now)
	if err != nil {
		t.Fatalf("TableProgress: %v", err)
	}
	if stats[0].TotalRows != 10 {
		t.Errorf("TotalRows = %d, want the logged 10", stats[0].TotalRows)
	}
}

// TestAMongoDBTaskFallsBackToTheLogWhenTheConnectionFails records that a
// MongoDB task whose source is unreachable is reported with the logged figures
// and no error. The response cannot be told apart from one where the live count
// agreed with the log.
func TestAMongoDBTaskFallsBackToTheLogWhenTheConnectionFails(t *testing.T) {
	db := useTempTaskDB(t)
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	day := now.Format("2006-01-02")
	// Port 1 refuses connections, so the Mongo dial cannot succeed.
	id := insertTask(t, db, 1,
		`{"type":"mongodb","sourceConn":{"host":"127.0.0.1","port":"1","database":"src"}}`)
	insertMonitoringRow(t, db, int(id), day+" 01:00:00", "orders", 10, 10)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stats, err := TableProgress(ctx, itoa(id), now)
	if err != nil {
		t.Fatalf("TableProgress returned an error for an unreachable source: %v — "+
			"the failure appears to be reported now, so assert that instead", err)
	}
	if len(stats) != 1 || stats[0].TotalRows != 10 {
		t.Errorf("stats = %+v, want the logged figures", stats)
	}
}

// TestNoLiveCountIsAttemptedForAnEmptyLog records the short circuit: with no
// rows to annotate the MongoDB connection is never opened, so an unreachable
// source costs nothing.
func TestNoLiveCountIsAttemptedForAnEmptyLog(t *testing.T) {
	db := useTempTaskDB(t)
	id := insertTask(t, db, 1,
		`{"type":"mongodb","sourceConn":{"host":"127.0.0.1","port":"1","database":"src"}}`)

	start := time.Now()
	stats, err := TableProgress(context.Background(), itoa(id), time.Now().UTC())
	if err != nil {
		t.Fatalf("TableProgress: %v", err)
	}
	if len(stats) != 0 {
		t.Errorf("TableProgress returned %d rows", len(stats))
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("TableProgress took %v with nothing to annotate; the connection "+
			"appears to be opened unconditionally now", elapsed)
	}
}

func TestTableProgressPropagatesAStoreFailure(t *testing.T) {
	emptyTaskDB(t)

	if _, err := TableProgress(context.Background(), "1", time.Now().UTC()); err == nil {
		t.Error("TableProgress on a database with no tables returned no error")
	}
}
