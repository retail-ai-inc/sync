package infra

import (
	"testing"

	"github.com/retail-ai-inc/sync/internal/backup/domain"
)

func TestListJobsReturnsRowsOldestFirst(t *testing.T) {
	db := useTempJobDB(t)
	insertJob(t, db, 1, `{"name":"first"}`)
	insertJob(t, db, 0, `{"name":"second"}`)

	jobs, err := ListJobs()
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("ListJobs returned %d jobs, want 2", len(jobs))
	}
	if jobs[0].ID() >= jobs[1].ID() {
		t.Errorf("jobs are not ordered by id: %d then %d", jobs[0].ID(), jobs[1].ID())
	}

	first, err := jobs[0].Config()
	if err != nil {
		t.Fatalf("Config: %v", err)
	}
	if first.Name != "first" {
		t.Errorf("first job name = %q", first.Name)
	}
	if jobs[0].Enable() != 1 || jobs[1].Enable() != 0 {
		t.Errorf("enable columns = %d/%d, want 1/0", jobs[0].Enable(), jobs[1].Enable())
	}
}

func TestListJobsOnAnEmptyTable(t *testing.T) {
	useTempJobDB(t)

	jobs, err := ListJobs()
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(jobs) != 0 {
		t.Errorf("ListJobs returned %d jobs from an empty table", len(jobs))
	}
}

func TestListJobsReportsAMissingTable(t *testing.T) {
	emptyJobDB(t)

	_, err := ListJobs()
	if err == nil {
		t.Fatal("ListJobs on a database with no tables returned no error")
	}
	if got := stageOf(err); got != StageQuery {
		t.Errorf("stage = %q, want %q", got, StageQuery)
	}
}

// TestNullTimestampsBecomeEmptyStrings records that the three DATETIME columns
// are read through COALESCE, so a NULL arrives as "" rather than failing the
// scan. The endpoint then hands "" to the JST converter.
func TestNullTimestampsBecomeEmptyStrings(t *testing.T) {
	db := useTempJobDB(t)
	if _, err := db.Exec(
		`INSERT INTO backup_tasks (enable, last_update_time, last_backup_time, next_backup_time, config_json)
		 VALUES (1, NULL, NULL, NULL, '{}')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	jobs, err := ListJobs()
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("ListJobs returned %d jobs", len(jobs))
	}
	if jobs[0].LastUpdateTime() != "" || jobs[0].LastBackupTime() != "" ||
		jobs[0].NextBackupTimeRaw() != "" {
		t.Errorf("NULL timestamps read as %q/%q/%q",
			jobs[0].LastUpdateTime(), jobs[0].LastBackupTime(), jobs[0].NextBackupTimeRaw())
	}
}

func TestInsertJobStoresTheConfiguration(t *testing.T) {
	db := useTempJobDB(t)

	id, err := InsertJob(1, "2026-08-21 00:00:00", "2026-08-22 00:00:00",
		domain.ConfigFrom(domain.Request{Name: "nightly", Schedule: "0 3 * * *"}, domain.StatusEnabled))
	if err != nil {
		t.Fatalf("InsertJob: %v", err)
	}
	if id == 0 {
		t.Fatal("InsertJob returned id 0")
	}

	var enable int
	var lastBackup, nextBackup string
	if err := db.QueryRow(
		`SELECT enable, COALESCE(last_backup_time,''), COALESCE(next_backup_time,'') FROM backup_tasks WHERE id=?`,
		id).Scan(&enable, &lastBackup, &nextBackup); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if enable != 1 {
		t.Errorf("enable = %d, want 1", enable)
	}
	if lastBackup != "" {
		t.Errorf("last_backup_time = %q for a new job, want empty", lastBackup)
	}
	if nextBackup != "2026-08-22 00:00:00" {
		t.Errorf("next_backup_time = %q", nextBackup)
	}

	cfg := readConfig(t, db, id)
	if cfg != `{"name":"nightly","sourceType":"","database":null,"destination":null,`+
		`"schedule":"0 3 * * *","format":"","backupType":"","query":null,"status":"enabled",`+
		`"compressionType":"","tableSelectionMode":"","regexPattern":""}` {
		t.Errorf("stored document = %s", cfg)
	}
}

// TestTheStoredDocumentSpellsOutEveryZeroValue records that the configuration
// is marshalled without omitempty, so every field is written even when unset.
// A job created from a two-field request stores a twelve-field document with
// nulls and empty strings, which is what the UI reads back.
func TestTheStoredDocumentSpellsOutEveryZeroValue(t *testing.T) {
	db := useTempJobDB(t)

	id, err := InsertJob(1, "now", "next", domain.Config{Name: "n"})
	if err != nil {
		t.Fatalf("InsertJob: %v", err)
	}

	cfg := readConfig(t, db, id)
	for _, want := range []string{`"database":null`, `"query":null`, `"format":""`} {
		if !contains(cfg, want) {
			t.Errorf("the stored document dropped %s: %s", want, cfg)
		}
	}
}

func TestInsertJobReportsAMissingTable(t *testing.T) {
	emptyJobDB(t)

	_, err := InsertJob(1, "now", "next", domain.Config{})
	if err == nil {
		t.Fatal("InsertJob on a database with no tables returned no error")
	}
	if got := stageOf(err); got != StageInsert {
		t.Errorf("stage = %q, want %q", got, StageInsert)
	}
}

func TestReadJobRow(t *testing.T) {
	db := useTempJobDB(t)
	id := insertJob(t, db, 1, `{"name":"nightly","status":"enabled"}`)

	cfg, enable, err := ReadJobRow(itoa(id))
	if err != nil {
		t.Fatalf("ReadJobRow: %v", err)
	}
	if enable != 1 {
		t.Errorf("enable = %d, want 1", enable)
	}
	if cfg != `{"name":"nightly","status":"enabled"}` {
		t.Errorf("config_json = %q", cfg)
	}
}

func TestReadJobRowOnAnUnknownID(t *testing.T) {
	useTempJobDB(t)

	_, _, err := ReadJobRow("999")
	if err == nil {
		t.Fatal("ReadJobRow on an unknown id returned no error")
	}
	if got := stageOf(err); got != StageFetchConfig {
		t.Errorf("stage = %q, want %q", got, StageFetchConfig)
	}
}

func TestUpdateJobReplacesTheConfiguration(t *testing.T) {
	db := useTempJobDB(t)
	id := insertJob(t, db, 1, `{"name":"before","schedule":"0 1 * * *"}`)

	if err := UpdateJob(itoa(id), "2026-08-21 05:00:00", "2026-08-22 05:00:00",
		domain.Config{Name: "after"}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}

	cfg := readConfig(t, db, id)
	if !contains(cfg, `"name":"after"`) {
		t.Errorf("the name was not replaced: %s", cfg)
	}
	if contains(cfg, `0 1 * * *`) {
		t.Errorf("the old schedule survived the replacement: %s", cfg)
	}

	if got := readTimestamp(t, db, "last_update_time", id); got != "2026-08-21 05:00:00" {
		t.Errorf("last_update_time = %q", got)
	}
}

// TestUpdateJobLeavesTheEnableColumnAlone records that an update rewrites the
// configuration but never the enable column, so a job disabled through the
// pause endpoint stays disabled while its document says whatever the update
// carried over.
func TestUpdateJobLeavesTheEnableColumnAlone(t *testing.T) {
	db := useTempJobDB(t)
	id := insertJob(t, db, 0, `{"name":"n","status":"disabled"}`)

	if err := UpdateJob(itoa(id), "now", "next",
		domain.Config{Name: "n", Status: domain.StatusEnabled}); err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}

	var enable int
	if err := db.QueryRow(`SELECT enable FROM backup_tasks WHERE id=?`, id).Scan(&enable); err != nil {
		t.Fatalf("read enable: %v", err)
	}
	if enable != 0 {
		t.Fatalf("enable = %d after an update to status enabled; the update appears to "+
			"write the column now, so assert that instead", enable)
	}
	if !contains(readConfig(t, db, id), `"status":"enabled"`) {
		t.Error("the document does not say enabled")
	}
}

func TestUpdateJobOnAnUnknownID(t *testing.T) {
	useTempJobDB(t)

	err := UpdateJob("999", "now", "next", domain.Config{})
	if err != ErrNoSuchJob {
		t.Errorf("UpdateJob on an unknown id = %v, want ErrNoSuchJob", err)
	}
}

func TestUpdateJobReportsAMissingTable(t *testing.T) {
	emptyJobDB(t)

	err := UpdateJob("1", "now", "next", domain.Config{})
	if got := stageOf(err); got != StageUpdate {
		t.Errorf("stage = %q, want %q (err = %v)", got, StageUpdate, err)
	}
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
		t.Errorf("%d rows survived the delete", count)
	}
}

func TestDeleteJobOnAnUnknownID(t *testing.T) {
	useTempJobDB(t)

	if err := DeleteJob("999"); err != ErrNoSuchJob {
		t.Errorf("DeleteJob on an unknown id = %v, want ErrNoSuchJob", err)
	}
}

func TestDeleteJobReportsAMissingTable(t *testing.T) {
	emptyJobDB(t)

	if got := stageOf(DeleteJob("1")); got != StageDelete {
		t.Errorf("stage = %q, want %q", got, StageDelete)
	}
}

func TestSetEnableFlipsBothTheColumnAndTheDocument(t *testing.T) {
	db := useTempJobDB(t)
	id := insertJob(t, db, 1, `{"name":"n","status":"enabled"}`)

	if err := SetEnable(itoa(id), false, "2026-08-21 06:00:00"); err != nil {
		t.Fatalf("SetEnable: %v", err)
	}

	var enable int
	if err := db.QueryRow(`SELECT enable FROM backup_tasks WHERE id=?`, id).Scan(&enable); err != nil {
		t.Fatalf("read enable: %v", err)
	}
	if enable != 0 {
		t.Errorf("enable = %d after pausing, want 0", enable)
	}
	if got := readTimestamp(t, db, "last_update_time", id); got != "2026-08-21 06:00:00" {
		t.Errorf("last_update_time = %q", got)
	}
	if !contains(readConfig(t, db, id), `"status":"disabled"`) {
		t.Errorf("the document was not updated: %s", readConfig(t, db, id))
	}

	if err := SetEnable(itoa(id), true, "2026-08-21 07:00:00"); err != nil {
		t.Fatalf("SetEnable(true): %v", err)
	}
	if !contains(readConfig(t, db, id), `"status":"enabled"`) {
		t.Errorf("resuming did not update the document: %s", readConfig(t, db, id))
	}
}

// TestSetEnableKeepsTheRestOfTheDocument records that flipping the status is the
// one merging write in this store: it decodes the document into a map, sets one
// key, and writes it back, so the other fields survive.
func TestSetEnableKeepsTheRestOfTheDocument(t *testing.T) {
	db := useTempJobDB(t)
	id := insertJob(t, db, 1, `{"name":"nightly","schedule":"0 3 * * *","status":"enabled"}`)

	if err := SetEnable(itoa(id), false, "now"); err != nil {
		t.Fatalf("SetEnable: %v", err)
	}

	cfg := readConfig(t, db, id)
	if !contains(cfg, `"name":"nightly"`) || !contains(cfg, `"schedule":"0 3 * * *"`) {
		t.Errorf("the rest of the document was lost: %s", cfg)
	}
}

// TestSetEnableReplacesACorruptDocument records that a configuration that will
// not parse is discarded and replaced with a document holding only the status.
// Pausing a job with a corrupt configuration therefore destroys whatever was
// left of it, silently.
func TestSetEnableReplacesACorruptDocument(t *testing.T) {
	db := useTempJobDB(t)
	id := insertJob(t, db, 1, `{"name":`)

	if err := SetEnable(itoa(id), false, "now"); err != nil {
		t.Fatalf("SetEnable: %v", err)
	}

	if got := readConfig(t, db, id); got != `{"status":"disabled"}` {
		t.Fatalf("the corrupt document became %s; it appears to be preserved or "+
			"reported now, so assert that instead", got)
	}
}

// TestSetEnablePanicsOnANullDocument records the defect T-124.
//
// `null` is valid JSON, so the unmarshal succeeds and leaves the map nil. The
// guard only tests the error, so the assignment that follows panics with
// "assignment to entry in nil map" and the request dies with a 500 and no body.
func TestSetEnablePanicsOnANullDocument(t *testing.T) {
	db := useTempJobDB(t)
	id := insertJob(t, db, 1, `null`)

	defer func() {
		if recover() == nil {
			t.Fatal("SetEnable survived a null document; the guard appears to check " +
				"the map as well as the error, so assert the new behaviour instead")
		}
	}()
	_ = SetEnable(itoa(id), false, "now")
}

func TestSetEnableOnAnUnknownID(t *testing.T) {
	useTempJobDB(t)

	if err := SetEnable("999", true, "now"); err == nil {
		t.Error("SetEnable on an unknown id returned no error")
	}
}

// TestSetEnableReportsAPlainErrorNotAFault records that this one call does not
// tag its failures with a stage, unlike every other write in the store. The
// endpoint therefore answers "pause fail" for a missing table, a missing row and
// a locked database alike.
func TestSetEnableReportsAPlainErrorNotAFault(t *testing.T) {
	emptyJobDB(t)

	err := SetEnable("1", true, "now")
	if err == nil {
		t.Fatal("SetEnable on a database with no tables returned no error")
	}
	if got := stageOf(err); got != "" {
		t.Fatalf("stage = %q; the call tags its failures now, so assert the stage", got)
	}
}

func TestJobExists(t *testing.T) {
	db := useTempJobDB(t)
	id := insertJob(t, db, 1, `{}`)

	exists, err := JobExists(itoa(id))
	if err != nil {
		t.Fatalf("JobExists: %v", err)
	}
	if !exists {
		t.Error("JobExists = false for a row that is there")
	}

	exists, err = JobExists("999")
	if err != nil {
		t.Fatalf("JobExists: %v", err)
	}
	if exists {
		t.Error("JobExists = true for an id that is not there")
	}
}

// TestJobExistsAcceptsANonNumericID records that the id reaches SQLite as a
// bound parameter, so a value that is not a number is compared rather than
// rejected: the row simply does not match.
func TestJobExistsAcceptsANonNumericID(t *testing.T) {
	useTempJobDB(t)

	exists, err := JobExists("'; DROP TABLE backup_tasks; --")
	if err != nil {
		t.Fatalf("JobExists: %v", err)
	}
	if exists {
		t.Error("JobExists = true for a non-numeric id")
	}
}

func TestJobExistsReportsAMissingTable(t *testing.T) {
	emptyJobDB(t)

	_, err := JobExists("1")
	if got := stageOf(err); got != StageLookup {
		t.Errorf("stage = %q, want %q (err = %v)", got, StageLookup, err)
	}
}

func TestStampLastBackup(t *testing.T) {
	db := useTempJobDB(t)
	id := insertJob(t, db, 1, `{}`)

	if err := StampLastBackup(itoa(id), "2026-08-21 09:00:00"); err != nil {
		t.Fatalf("StampLastBackup: %v", err)
	}

	if got := readTimestamp(t, db, "last_backup_time", id); got != "2026-08-21 09:00:00" {
		t.Errorf("last_backup_time = %q", got)
	}
}

// TestStampLastBackupOnAnUnknownIDSucceeds records that stamping a job that is
// not there is reported as success: the UPDATE matches no row and the rows
// affected count is never read. The run endpoint answers "started successfully"
// for a job it did not touch.
func TestStampLastBackupOnAnUnknownIDSucceeds(t *testing.T) {
	useTempJobDB(t)

	if err := StampLastBackup("999", "now"); err != nil {
		t.Fatalf("StampLastBackup on an unknown id = %v; the rows affected count "+
			"appears to be checked now, so assert the error instead", err)
	}
}

func TestStampLastBackupReportsAMissingTable(t *testing.T) {
	emptyJobDB(t)

	if got := stageOf(StampLastBackup("1", "now")); got != StageUpdate {
		t.Errorf("stage = %q, want %q", got, StageUpdate)
	}
}

func TestFaultCarriesItsStageAndCause(t *testing.T) {
	inner := errNoRows()
	f := faultAt(StageQuery, inner)

	if f.Stage != StageQuery {
		t.Errorf("Stage = %q", f.Stage)
	}
	if f.Unwrap() != inner {
		t.Error("Unwrap did not return the cause")
	}
	if got := f.Error(); got != StageQuery+": "+inner.Error() {
		t.Errorf("Error = %q", got)
	}
}
