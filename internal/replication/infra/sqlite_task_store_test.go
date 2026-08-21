package infra

import (
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

func TestListTasksReturnsRowsOldestFirst(t *testing.T) {
	db := useTempTaskDB(t)
	insertTask(t, db, 1, `{"taskName":"first","type":"mongodb"}`)
	insertTask(t, db, 0, `{"taskName":"second","type":"mysql"}`)

	tasks, err := ListTasks()
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(tasks) != 2 {
		t.Fatalf("ListTasks returned %d tasks, want 2", len(tasks))
	}
	if tasks[0].ID() >= tasks[1].ID() {
		t.Errorf("tasks are not ordered by id: %d then %d", tasks[0].ID(), tasks[1].ID())
	}
	if !tasks[0].IsEnabled() || tasks[1].IsEnabled() {
		t.Errorf("enable columns = %v/%v, want true/false", tasks[0].IsEnabled(), tasks[1].IsEnabled())
	}

	cfg, err := tasks[0].Config()
	if err != nil {
		t.Fatalf("Config: %v", err)
	}
	if cfg.TaskName != "first" || cfg.Type != "mongodb" {
		t.Errorf("first task = %q/%q", cfg.TaskName, cfg.Type)
	}
}

func TestListTasksOnAnEmptyTable(t *testing.T) {
	useTempTaskDB(t)

	tasks, err := ListTasks()
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(tasks) != 0 {
		t.Errorf("ListTasks returned %d tasks from an empty table", len(tasks))
	}
}

func TestListTasksReportsAMissingTable(t *testing.T) {
	emptyTaskDB(t)

	_, err := ListTasks()
	if err == nil {
		t.Fatal("ListTasks on a database with no tables returned no error")
	}
	if got := stageOf(err); got != StageQuery {
		t.Errorf("stage = %q, want %q", got, StageQuery)
	}
}

func TestListTasksReportsAScanFailure(t *testing.T) {
	db := useTempTaskDB(t)
	if _, err := db.Exec(
		`INSERT INTO sync_tasks (enable, last_update_time, last_run_time, config_json)
		 VALUES ('not a number', '', '', '{}')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	_, err := ListTasks()
	if err == nil {
		t.Fatal("ListTasks accepted a text enable column")
	}
	if got := stageOf(err); got != StageScan {
		t.Errorf("stage = %q, want %q (err = %v)", got, StageScan, err)
	}
}

// TestNullTimestampsBecomeEmptyStrings records that both DATETIME columns are
// read through COALESCE, so a NULL arrives as "" rather than failing the scan.
func TestNullTimestampsBecomeEmptyStrings(t *testing.T) {
	db := useTempTaskDB(t)
	if _, err := db.Exec(
		`INSERT INTO sync_tasks (enable, last_update_time, last_run_time, config_json)
		 VALUES (1, NULL, NULL, '{}')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	tasks, err := ListTasks()
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if tasks[0].LastUpdateTime() != "" || tasks[0].LastRunTime() != "" {
		t.Errorf("NULL timestamps read as %q/%q", tasks[0].LastUpdateTime(), tasks[0].LastRunTime())
	}
}

// TestACorruptConfigurationIsCarriedThroughNotRejected records that the store
// hands the document back verbatim and leaves the parsing to the caller, so a
// row whose config_json will not parse is listed rather than failing the query.
func TestACorruptConfigurationIsCarriedThroughNotRejected(t *testing.T) {
	db := useTempTaskDB(t)
	insertTask(t, db, 1, `{"taskName":`)

	tasks, err := ListTasks()
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("ListTasks returned %d tasks, want 1", len(tasks))
	}
	if _, err := tasks[0].Config(); err == nil {
		t.Fatal("the corrupt document parsed; the store appears to validate now")
	}
}

func TestInsertTaskStoresTheConfiguration(t *testing.T) {
	db := useTempTaskDB(t)

	id, err := InsertTask(1, "2026-08-21 00:00:00",
		domain.ConfigFrom(domain.Request{TaskName: "orders", SourceType: "mongodb", Status: "Running"}))
	if err != nil {
		t.Fatalf("InsertTask: %v", err)
	}
	if id == 0 {
		t.Fatal("InsertTask returned id 0")
	}

	var enable int
	var lastRun string
	if err := db.QueryRow(
		`SELECT enable, COALESCE(last_run_time,'') FROM sync_tasks WHERE id=?`, id).
		Scan(&enable, &lastRun); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if enable != 1 {
		t.Errorf("enable = %d, want 1", enable)
	}
	if lastRun != "" {
		t.Errorf("last_run_time = %q for a new task, want empty", lastRun)
	}

	cfg := readConfig(t, db, id)
	if !contains(cfg, `"type":"mongodb"`) || !contains(cfg, `"taskName":"orders"`) ||
		!contains(cfg, `"status":"Running"`) {
		t.Errorf("stored document = %s", cfg)
	}
}

// TestTheStoredDocumentIsWrittenInFull records that the configuration is
// marshalled without omitempty, so a two-field request still stores fourteen
// keys. The UI reads the nulls and empty strings back.
func TestTheStoredDocumentIsWrittenInFull(t *testing.T) {
	db := useTempTaskDB(t)

	id, err := InsertTask(0, "now", domain.Config{TaskName: "n"})
	if err != nil {
		t.Fatalf("InsertTask: %v", err)
	}

	cfg := readConfig(t, db, id)
	for _, want := range []string{`"sourceConn":null`, `"mappings":null`, `"securityEnabled":false`} {
		if !contains(cfg, want) {
			t.Errorf("the stored document dropped %s: %s", want, cfg)
		}
	}
}

func TestInsertTaskReportsAMissingTable(t *testing.T) {
	emptyTaskDB(t)

	_, err := InsertTask(1, "now", domain.Config{})
	if got := stageOf(err); got != StageInsert {
		t.Errorf("stage = %q, want %q (err = %v)", got, StageInsert, err)
	}
}

func TestUpdateTaskReplacesTheConfigurationAndTheEnableColumn(t *testing.T) {
	db := useTempTaskDB(t)
	id := insertTask(t, db, 0, `{"taskName":"before","type":"mysql"}`)

	if err := UpdateTask(itoa(id), 1, "2026-08-21 05:00:00",
		domain.Config{TaskName: "after", Type: "mongodb", Status: "Running"}); err != nil {
		t.Fatalf("UpdateTask: %v", err)
	}

	var enable int
	if err := db.QueryRow(`SELECT enable FROM sync_tasks WHERE id=?`, id).Scan(&enable); err != nil {
		t.Fatalf("read enable: %v", err)
	}
	if enable != 1 {
		t.Errorf("enable = %d, want 1 — unlike the backup store, this update does "+
			"write the column", enable)
	}
	cfg := readConfig(t, db, id)
	if !contains(cfg, `"taskName":"after"`) || contains(cfg, `"type":"mysql"`) {
		t.Errorf("the document was not replaced: %s", cfg)
	}
	if got := readTimestamp(t, db, "last_update_time", id); got != "2026-08-21 05:00:00" {
		t.Errorf("last_update_time = %q", got)
	}
}

// TestTheTwoStoresDisagreeAboutTheEnableColumn records that the replication
// update writes the enable column while the backup update does not. The same
// gesture in the UI therefore has different effects on the two kinds of task.
func TestTheTwoStoresDisagreeAboutTheEnableColumn(t *testing.T) {
	db := useTempTaskDB(t)
	id := insertTask(t, db, 0, `{"status":"Stopped"}`)

	if err := UpdateTask(itoa(id), 1, "now", domain.Config{Status: "Running"}); err != nil {
		t.Fatalf("UpdateTask: %v", err)
	}

	var enable int
	if err := db.QueryRow(`SELECT enable FROM sync_tasks WHERE id=?`, id).Scan(&enable); err != nil {
		t.Fatalf("read enable: %v", err)
	}
	if enable != 1 {
		t.Fatalf("enable = %d; the two stores agree now, so assert the shared rule", enable)
	}
}

func TestUpdateTaskOnAnUnknownID(t *testing.T) {
	useTempTaskDB(t)

	if err := UpdateTask("999", 1, "now", domain.Config{}); err != ErrNoSuchTask {
		t.Errorf("UpdateTask on an unknown id = %v, want ErrNoSuchTask", err)
	}
}

func TestUpdateTaskReportsAMissingTable(t *testing.T) {
	emptyTaskDB(t)

	if got := stageOf(UpdateTask("1", 1, "now", domain.Config{})); got != StageUpdate {
		t.Errorf("stage = %q, want %q", got, StageUpdate)
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
		t.Errorf("%d rows survived the delete", count)
	}
}

func TestDeleteTaskOnAnUnknownID(t *testing.T) {
	useTempTaskDB(t)

	if err := DeleteTask("999"); err != ErrNoSuchTask {
		t.Errorf("DeleteTask on an unknown id = %v, want ErrNoSuchTask", err)
	}
}

func TestDeleteTaskReportsAMissingTable(t *testing.T) {
	emptyTaskDB(t)

	if got := stageOf(DeleteTask("1")); got != StageDelete {
		t.Errorf("stage = %q, want %q", got, StageDelete)
	}
}

// TestDeletingATaskLeavesItsMonitoringLog records that removing a task does not
// remove the rows it produced. The monitoring log keeps growing with entries for
// task ids nothing can resolve.
func TestDeletingATaskLeavesItsMonitoringLog(t *testing.T) {
	db := useTempTaskDB(t)
	id := insertTask(t, db, 1, `{}`)
	insertMonitoringRow(t, db, int(id), "2026-08-21 01:00:00", "orders", 10, 10)

	if err := DeleteTask(itoa(id)); err != nil {
		t.Fatalf("DeleteTask: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM monitoring_log WHERE sync_task_id=?`, id).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count == 0 {
		t.Fatal("the monitoring rows were removed too; a cascade appears to have been " +
			"added, so assert that instead")
	}
}

func TestSetEnableFlipsBothTheColumnAndTheDocument(t *testing.T) {
	db := useTempTaskDB(t)
	id := insertTask(t, db, 0, `{"taskName":"orders","status":"Stopped"}`)

	if err := SetEnable(itoa(id), true); err != nil {
		t.Fatalf("SetEnable: %v", err)
	}

	var enable int
	if err := db.QueryRow(`SELECT enable FROM sync_tasks WHERE id=?`, id).Scan(&enable); err != nil {
		t.Fatalf("read enable: %v", err)
	}
	if enable != 1 {
		t.Errorf("enable = %d after starting, want 1", enable)
	}
	cfg := readConfig(t, db, id)
	if !contains(cfg, `"status":"Running"`) {
		t.Errorf("the document was not updated: %s", cfg)
	}
	if !contains(cfg, `"taskName":"orders"`) {
		t.Errorf("the rest of the document was lost: %s", cfg)
	}

	if err := SetEnable(itoa(id), false); err != nil {
		t.Fatalf("SetEnable(false): %v", err)
	}
	if !contains(readConfig(t, db, id), `"status":"Stopped"`) {
		t.Errorf("stopping did not update the document: %s", readConfig(t, db, id))
	}
}

// TestSetEnableReplacesACorruptDocument records that a configuration that will
// not parse is discarded and replaced with a document holding only the status.
// Starting a task whose configuration is corrupt therefore destroys its
// connections and mappings, silently — and the syncer that would have used them
// is not restarted either.
func TestSetEnableReplacesACorruptDocument(t *testing.T) {
	db := useTempTaskDB(t)
	id := insertTask(t, db, 0, `{"taskName":`)

	if err := SetEnable(itoa(id), true); err != nil {
		t.Fatalf("SetEnable: %v", err)
	}

	if got := readConfig(t, db, id); got != `{"status":"Running"}` {
		t.Fatalf("the corrupt document became %s; it appears to be preserved or "+
			"reported now, so assert that instead", got)
	}
}

// TestSetEnablePanicsOnANullDocument records the defect T-124 on the
// replication side: `null` is valid JSON, the unmarshal succeeds with a nil map,
// and the assignment that follows panics.
func TestSetEnablePanicsOnANullDocument(t *testing.T) {
	db := useTempTaskDB(t)
	id := insertTask(t, db, 0, `null`)

	defer func() {
		if recover() == nil {
			t.Fatal("SetEnable survived a null document; the guard appears to check " +
				"the map as well as the error, so assert the new behaviour instead")
		}
	}()
	_ = SetEnable(itoa(id), true)
}

func TestSetEnableOnAnUnknownID(t *testing.T) {
	useTempTaskDB(t)

	if err := SetEnable("999", true); err == nil {
		t.Error("SetEnable on an unknown id returned no error")
	}
}

// TestSetEnableReportsAPlainErrorNotAFault records that this call, like its
// backup counterpart, returns the driver's error untagged. The endpoint answers
// "start fail" for a missing table, a missing row and a locked database alike.
func TestSetEnableReportsAPlainErrorNotAFault(t *testing.T) {
	emptyTaskDB(t)

	err := SetEnable("1", true)
	if err == nil {
		t.Fatal("SetEnable on a database with no tables returned no error")
	}
	if got := stageOf(err); got != "" {
		t.Fatalf("stage = %q; the call tags its failures now, so assert the stage", got)
	}
}

func TestReadTaskEngine(t *testing.T) {
	db := useTempTaskDB(t)
	id := insertTask(t, db, 1, `{"type":"MongoDB"}`)

	if got := ReadTaskEngine(itoa(id)); got != "MongoDB" {
		t.Errorf("ReadTaskEngine = %q, want MongoDB — the value is returned as stored, "+
			"casing and all", got)
	}
}

// TestReadTaskEngineAnswersEmptyForEverySortOfFailure records that the four ways
// this can go wrong are indistinguishable: an unopenable database, a missing
// row, an empty document and a corrupt document all answer "". The caller
// treats every one of them as "not MongoDB" and skips the live row count.
func TestReadTaskEngineAnswersEmptyForEverySortOfFailure(t *testing.T) {
	t.Run("unknown id", func(t *testing.T) {
		useTempTaskDB(t)
		if got := ReadTaskEngine("999"); got != "" {
			t.Errorf("ReadTaskEngine = %q", got)
		}
	})
	t.Run("empty document", func(t *testing.T) {
		db := useTempTaskDB(t)
		id := insertTask(t, db, 1, ``)
		if got := ReadTaskEngine(itoa(id)); got != "" {
			t.Errorf("ReadTaskEngine = %q", got)
		}
	})
	t.Run("corrupt document", func(t *testing.T) {
		db := useTempTaskDB(t)
		id := insertTask(t, db, 1, `{"type":`)
		if got := ReadTaskEngine(itoa(id)); got != "" {
			t.Errorf("ReadTaskEngine = %q", got)
		}
	})
	t.Run("missing table", func(t *testing.T) {
		emptyTaskDB(t)
		if got := ReadTaskEngine("1"); got != "" {
			t.Errorf("ReadTaskEngine = %q", got)
		}
	})
	t.Run("unopenable database", func(t *testing.T) {
		unopenableDB(t)
		if got := ReadTaskEngine("1"); got != "" {
			t.Errorf("ReadTaskEngine = %q", got)
		}
	})
}

func TestTodayTableStatsSummarisesTheDay(t *testing.T) {
	db := useTempTaskDB(t)
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	day := now.Format("2006-01-02")

	insertMonitoringRow(t, db, 1, day+" 01:00:00", "orders", 100, 100)
	insertMonitoringRow(t, db, 1, day+" 02:00:00", "orders", 180, 175)

	stats, err := TodayTableStats("1", now)
	if err != nil {
		t.Fatalf("TodayTableStats: %v", err)
	}
	if len(stats) != 1 {
		t.Fatalf("TodayTableStats returned %d rows, want 1", len(stats))
	}
	if stats[0].TableName != "orders" {
		t.Errorf("TableName = %q", stats[0].TableName)
	}
	// MAX(tgt) - MIN(tgt) = 175 - 100
	if stats[0].SyncedToday != 75 {
		t.Errorf("SyncedToday = %d, want 75", stats[0].SyncedToday)
	}
	if stats[0].TotalRows != 175 {
		t.Errorf("TotalRows = %d, want 175", stats[0].TotalRows)
	}
}

func TestTodayTableStatsIgnoresOtherDaysAndOtherTasks(t *testing.T) {
	db := useTempTaskDB(t)
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)

	insertMonitoringRow(t, db, 1, "2026-08-20 01:00:00", "yesterday", 1, 1)
	insertMonitoringRow(t, db, 2, "2026-08-21 01:00:00", "other-task", 1, 1)

	stats, err := TodayTableStats("1", now)
	if err != nil {
		t.Fatalf("TodayTableStats: %v", err)
	}
	if len(stats) != 0 {
		t.Errorf("TodayTableStats returned %d rows, want 0: %+v", len(stats), stats)
	}
}

// TestTheWindowIsAUTCCalendarDay records T-055: the window runs from 00:00:00 to
// 23:59:59 in UTC while the endpoint labels the answer with a JST date. Between
// 00:00 and 09:00 JST the label names a day the figures do not cover.
func TestTheWindowIsAUTCCalendarDay(t *testing.T) {
	db := useTempTaskDB(t)
	// 2026-08-21 23:30 UTC is already 2026-08-22 in JST.
	now := time.Date(2026, 8, 21, 23, 30, 0, 0, time.UTC)

	insertMonitoringRow(t, db, 1, "2026-08-21 23:00:00", "orders", 10, 10)
	insertMonitoringRow(t, db, 1, "2026-08-22 00:30:00", "orders", 20, 20)

	stats, err := TodayTableStats("1", now)
	if err != nil {
		t.Fatalf("TodayTableStats: %v", err)
	}
	if len(stats) != 1 {
		t.Fatalf("TodayTableStats returned %d rows", len(stats))
	}
	// Only the 23:00 UTC row is inside the window, so the delta is zero even
	// though two measurements were taken in the same JST day.
	if stats[0].TotalRows != 10 {
		t.Fatalf("TotalRows = %d, want 10 — the window appears to follow JST now, "+
			"so assert that instead", stats[0].TotalRows)
	}
}

// TestANegativeDeltaIsClampedToZero records that a table which lost rows reports
// nothing synced rather than a negative figure, so the deletion leaves no trace.
func TestANegativeDeltaIsClampedToZero(t *testing.T) {
	db := useTempTaskDB(t)
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	day := now.Format("2006-01-02")

	// A later measurement with fewer rows cannot happen: the delta is
	// MAX - MIN, so it is never negative from this query. Feed the clamp
	// directly to record what it does with one.
	insertMonitoringRow(t, db, 1, day+" 01:00:00", "orders", 100, 100)

	stats, err := TodayTableStats("1", now)
	if err != nil {
		t.Fatalf("TodayTableStats: %v", err)
	}
	if stats[0].SyncedToday != 0 {
		t.Errorf("SyncedToday = %d for a single measurement, want 0", stats[0].SyncedToday)
	}
	if got := domain.ClampSyncedToday(-5); got != 0 {
		t.Errorf("ClampSyncedToday(-5) = %d, want 0", got)
	}
}

func TestTodayTableStatsOnAnEmptyLog(t *testing.T) {
	useTempTaskDB(t)

	stats, err := TodayTableStats("1", time.Now().UTC())
	if err != nil {
		t.Fatalf("TodayTableStats: %v", err)
	}
	if stats == nil {
		t.Fatal("TodayTableStats returned nil; the endpoint marshals it as an array")
	}
	if len(stats) != 0 {
		t.Errorf("TodayTableStats returned %d rows", len(stats))
	}
}

func TestTodayTableStatsReportsAMissingTable(t *testing.T) {
	emptyTaskDB(t)

	_, err := TodayTableStats("1", time.Now().UTC())
	if got := stageOf(err); got != StageMonitor {
		t.Errorf("stage = %q, want %q (err = %v)", got, StageMonitor, err)
	}
}

// TestARowThatWillNotScanIsSkippedNotReported records that a monitoring row
// whose counts are text is logged and dropped, so the table simply does not
// appear in the answer. A caller cannot tell a table with no traffic from a
// table whose rows are unreadable.
func TestARowThatWillNotScanIsSkippedNotReported(t *testing.T) {
	db := useTempTaskDB(t)
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	day := now.Format("2006-01-02")

	if _, err := db.Exec(
		`INSERT INTO monitoring_log (sync_task_id, logged_at, src_table, tgt_table, src_row_count, tgt_row_count)
		 VALUES (1, ?, 'orders', 'orders', 'x', 'y')`, day+" 01:00:00"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	stats, err := TodayTableStats("1", now)
	if err != nil {
		t.Fatalf("TodayTableStats returned an error for an unscannable row: %v — "+
			"the failure appears to be reported now, so assert that", err)
	}
	if len(stats) != 0 {
		t.Errorf("TodayTableStats returned %d rows, want 0: %+v", len(stats), stats)
	}
}

func TestEveryStoreCallReportsAnUnopenableDatabase(t *testing.T) {
	for _, tt := range []struct {
		name  string
		call  func() error
		stage string
	}{
		{"ListTasks", func() error { _, err := ListTasks(); return err }, StageOpen},
		{"InsertTask", func() error { _, err := InsertTask(1, "now", domain.Config{}); return err }, StageOpen},
		{"UpdateTask", func() error { return UpdateTask("1", 1, "now", domain.Config{}) }, StageDBFail},
		{"DeleteTask", func() error { return DeleteTask("1") }, StageOpen},
		{"TodayTableStats", func() error { _, err := TodayTableStats("1", time.Now()); return err }, StageOpen},
	} {
		t.Run(tt.name, func(t *testing.T) {
			unopenableDB(t)

			err := tt.call()
			if err == nil {
				t.Fatalf("%s returned no error for an unopenable database", tt.name)
			}
			if got := stageOf(err); got != tt.stage {
				t.Errorf("stage = %q, want %q (err = %v)", got, tt.stage, err)
			}
		})
	}
}

// TestUpdateTaskTagsAnUnopenableDatabaseDifferently records that the update path
// answers "db fail" where every other call answers "open db fail", for the same
// failure. The two messages describe one condition.
func TestUpdateTaskTagsAnUnopenableDatabaseDifferently(t *testing.T) {
	unopenableDB(t)

	if got := stageOf(UpdateTask("1", 1, "now", domain.Config{})); got != StageDBFail {
		t.Fatalf("stage = %q; the messages appear to agree now, so assert the shared one", got)
	}
	if StageDBFail == StageOpen {
		t.Error("the two stages are the same string now")
	}
}

func TestSetEnableDoesNotTagAnUnopenableDatabase(t *testing.T) {
	unopenableDB(t)

	err := SetEnable("1", true)
	if err == nil {
		t.Fatal("SetEnable returned no error for an unopenable database")
	}
	if got := stageOf(err); got != "" {
		t.Errorf("stage = %q, want no tag", got)
	}
}

func TestFaultCarriesItsStageAndCause(t *testing.T) {
	f := faultAt(StageQuery, ErrNoSuchTask)

	if f.Stage != StageQuery {
		t.Errorf("Stage = %q", f.Stage)
	}
	if f.Unwrap() != ErrNoSuchTask {
		t.Error("Unwrap did not return the cause")
	}
	if got := f.Error(); got != StageQuery+": "+ErrNoSuchTask.Error() {
		t.Errorf("Error = %q", got)
	}
}
