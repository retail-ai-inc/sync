package app

import (
	"strings"
	"testing"

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
