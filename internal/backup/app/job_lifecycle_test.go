package app

import (
	"context"
	"testing"

	"github.com/retail-ai-inc/sync/internal/backup/domain"
	"github.com/retail-ai-inc/sync/internal/backup/infra"
)

func TestListJobsResolvesEachConfiguration(t *testing.T) {
	db := useTempJobDB(t)
	insertJob(t, db, 1, `{"name":"nightly","sourceType":"mongodb","status":"enabled"}`)
	insertJob(t, db, 0, `{"sourceType":"mysql"}`)

	views, err := ListJobs()
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(views) != 2 {
		t.Fatalf("ListJobs returned %d views, want 2", len(views))
	}
	if views[0].Name != "nightly" || views[0].Status != domain.StatusEnabled {
		t.Errorf("first view = %q/%q", views[0].Name, views[0].Status)
	}
	if views[1].Name != "Backup Task "+itoa(int64(views[1].Job.ID())) {
		t.Errorf("second view name = %q, want a generated one", views[1].Name)
	}
	if views[1].Status != domain.StatusDisabled {
		t.Errorf("second view status = %q", views[1].Status)
	}
}

// TestACorruptConfigurationIsListedWithEmptyFields records that a document that
// will not parse is logged and carried through as the zero value, so the job is
// served with no schedule and no destination rather than failing the request.
func TestACorruptConfigurationIsListedWithEmptyFields(t *testing.T) {
	db := useTempJobDB(t)
	insertJob(t, db, 1, `{"name":`)

	views, err := ListJobs()
	if err != nil {
		t.Fatalf("ListJobs returned an error for a corrupt document: %v — the failure "+
			"appears to be reported now, so assert that instead", err)
	}
	if len(views) != 1 {
		t.Fatalf("ListJobs returned %d views", len(views))
	}
	if views[0].Config.Schedule != "" || views[0].Config.Destination != nil {
		t.Errorf("the corrupt document produced %+v", views[0].Config)
	}
}

func TestListJobsOnAnEmptyTable(t *testing.T) {
	useTempJobDB(t)

	views, err := ListJobs()
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(views) != 0 {
		t.Errorf("ListJobs returned %d views", len(views))
	}
}

func TestListJobsPropagatesAStoreFailure(t *testing.T) {
	emptyJobDB(t)

	if _, err := ListJobs(); err == nil {
		t.Error("ListJobs on a database with no tables returned no error")
	}
}

func TestCreateJobStoresAnEnabledJob(t *testing.T) {
	db := useTempJobDB(t)

	id, name, status, err := CreateJob(domain.Request{Name: "nightly", Schedule: "0 3 * * *"})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if id == 0 {
		t.Fatal("CreateJob returned id 0")
	}
	if name != "nightly" {
		t.Errorf("name = %q", name)
	}
	if status != domain.StatusEnabled {
		t.Errorf("status = %q, want %q", status, domain.StatusEnabled)
	}

	var enable int
	if err := db.QueryRow(`SELECT enable FROM backup_tasks WHERE id=?`, id).Scan(&enable); err != nil {
		t.Fatalf("read enable: %v", err)
	}
	if enable != 1 {
		t.Errorf("enable = %d, want 1", enable)
	}
	if !contains(readConfig(t, db, id), `"schedule":"0 3 * * *"`) {
		t.Error("the schedule was not stored")
	}
}

// TestANewJobIsAlwaysEnabled records that the create endpoint has no way to add
// a paused job: the status and the enable column are hardcoded. A caller that
// wants one has to create it and then pause it, which is two writes and two
// crontab rewrites.
func TestANewJobIsAlwaysEnabled(t *testing.T) {
	useTempJobDB(t)

	_, _, status, err := CreateJob(domain.Request{Name: "n"})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if status != domain.StatusEnabled {
		t.Fatalf("status = %q; the create path appears to accept a status now, so "+
			"assert that instead", status)
	}
}

func TestCreateJobNamesAnUnnamedJob(t *testing.T) {
	useTempJobDB(t)

	_, name, _, err := CreateJob(domain.Request{})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if name != "Backup Task" {
		t.Errorf("name = %q, want the default", name)
	}
}

func TestCreateJobPropagatesAStoreFailure(t *testing.T) {
	emptyJobDB(t)

	if _, _, _, err := CreateJob(domain.Request{}); err == nil {
		t.Error("CreateJob on a database with no tables returned no error")
	}
}

func TestUpdateJobCarriesTheStoredStatusOver(t *testing.T) {
	db := useTempJobDB(t)
	id := insertJob(t, db, 1, `{"name":"before","status":"paused"}`)

	if err := UpdateJob(itoa(id), domain.Request{Name: "after"}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}

	cfg := readConfig(t, db, id)
	if !contains(cfg, `"status":"paused"`) {
		t.Errorf("the stored status was not carried over: %s", cfg)
	}
	if !contains(cfg, `"name":"after"`) {
		t.Errorf("the name was not replaced: %s", cfg)
	}
}

func TestUpdateJobCarriesTheStoredNameOverWhenOmitted(t *testing.T) {
	db := useTempJobDB(t)
	id := insertJob(t, db, 1, `{"name":"nightly","status":"enabled"}`)

	if err := UpdateJob(itoa(id), domain.Request{Schedule: "0 4 * * *"}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}

	if !contains(readConfig(t, db, id), `"name":"nightly"`) {
		t.Errorf("the stored name was not carried over: %s", readConfig(t, db, id))
	}
}

// TestTheTwoUpdatePathsDisagreeAboutOmittedNames records that the backup update
// reads the stored name back while the replication update applies the create
// default instead. The same omission renames one kind of task and not the other.
func TestTheTwoUpdatePathsDisagreeAboutOmittedNames(t *testing.T) {
	db := useTempJobDB(t)
	id := insertJob(t, db, 1, `{"name":"nightly"}`)

	if err := UpdateJob(itoa(id), domain.Request{}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	if !contains(readConfig(t, db, id), `"name":"nightly"`) {
		t.Fatalf("the backup update no longer carries the stored name over; the two " +
			"paths appear to agree now, so assert the shared rule")
	}
}

func TestUpdateJobGeneratesANameWhenThereIsNone(t *testing.T) {
	db := useTempJobDB(t)
	id := insertJob(t, db, 1, `{"schedule":"0 3 * * *"}`)

	if err := UpdateJob(itoa(id), domain.Request{}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	if !contains(readConfig(t, db, id), `"name":"Backup Task `+itoa(id)+`"`) {
		t.Errorf("no name was generated: %s", readConfig(t, db, id))
	}
}

func TestUpdateJobToleratesACorruptStoredConfig(t *testing.T) {
	db := useTempJobDB(t)
	id := insertJob(t, db, 0, `{"name":`)

	if err := UpdateJob(itoa(id), domain.Request{Name: "after"}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	// With nothing readable in the old document the status falls back to the
	// enable column.
	if !contains(readConfig(t, db, id), `"status":"disabled"`) {
		t.Errorf("stored document = %s", readConfig(t, db, id))
	}
}

func TestUpdateJobOnAnUnknownID(t *testing.T) {
	useTempJobDB(t)

	if err := UpdateJob("999", domain.Request{}); err == nil {
		t.Error("UpdateJob on an unknown id returned no error")
	}
}

// TestANonStringStoredStatusPanicsTheUpdate records T-112 through the use case:
// the unchecked assertion in the status derivation kills the request.
func TestANonStringStoredStatusPanicsTheUpdate(t *testing.T) {
	db := useTempJobDB(t)
	id := insertJob(t, db, 1, `{"name":"n","status":1}`)

	defer func() {
		if recover() == nil {
			t.Fatal("UpdateJob survived a numeric status; the assertion appears to be " +
				"checked now, so assert the error instead")
		}
	}()
	_ = UpdateJob(itoa(id), domain.Request{Name: "after"})
}

func TestDeleteJob(t *testing.T) {
	db := useTempJobDB(t)
	id := insertJob(t, db, 1, `{}`)

	if err := DeleteJob(itoa(id)); err != nil {
		t.Fatalf("DeleteJob: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM backup_tasks`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Errorf("%d rows survived", count)
	}
}

func TestDeleteJobOnAnUnknownID(t *testing.T) {
	useTempJobDB(t)

	if err := DeleteJob("999"); err != infra.ErrNoSuchJob {
		t.Errorf("DeleteJob on an unknown id = %v, want ErrNoSuchJob", err)
	}
}

func TestPauseAndResume(t *testing.T) {
	db := useTempJobDB(t)
	id := insertJob(t, db, 1, `{"name":"n","status":"enabled"}`)

	if err := PauseJob(itoa(id)); err != nil {
		t.Fatalf("PauseJob: %v", err)
	}
	var enable int
	if err := db.QueryRow(`SELECT enable FROM backup_tasks WHERE id=?`, id).Scan(&enable); err != nil {
		t.Fatalf("read enable: %v", err)
	}
	if enable != 0 {
		t.Errorf("enable = %d after pausing", enable)
	}
	if !contains(readConfig(t, db, id), `"status":"disabled"`) {
		t.Error("pausing did not update the document")
	}

	if err := ResumeJob(itoa(id)); err != nil {
		t.Fatalf("ResumeJob: %v", err)
	}
	if err := db.QueryRow(`SELECT enable FROM backup_tasks WHERE id=?`, id).Scan(&enable); err != nil {
		t.Fatalf("read enable: %v", err)
	}
	if enable != 1 {
		t.Errorf("enable = %d after resuming", enable)
	}
}

func TestPauseJobOnAnUnknownID(t *testing.T) {
	useTempJobDB(t)

	if err := PauseJob("999"); err == nil {
		t.Error("PauseJob on an unknown id returned no error")
	}
}

// TestSyncCrontabSwallowsEveryFailure records that the crontab rewrite every
// write endpoint performs after answering cannot report anything: it logs and
// returns. A schedule change that never reached the crontab is indistinguishable
// from one that did.
func TestSyncCrontabSwallowsEveryFailure(t *testing.T) {
	t.Run("no database", func(t *testing.T) {
		unopenableDB(t)
		SyncCrontab(context.Background(), "test") // must not panic
	})
	t.Run("no tables", func(t *testing.T) {
		emptyJobDB(t)
		SyncCrontab(context.Background(), "test")
	})
	t.Run("no crontab binary", func(t *testing.T) {
		useTempJobDB(t) // isolateCrontab has emptied PATH
		SyncCrontab(context.Background(), "test")
	})
}
