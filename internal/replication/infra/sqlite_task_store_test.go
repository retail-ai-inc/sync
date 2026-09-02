package infra

import (
	"encoding/base64"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/secret"
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
// update writes the enable column while the backup update does not.
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

// There is no foreign key and no cascade, so the monitoring log kept entries
// for task ids nothing could resolve — in the table that grows without bound.
func TestDeletingATaskRemovesItsMonitoringLog(t *testing.T) {
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
	if count != 0 {
		t.Errorf("%d monitoring rows are left for a task that no longer exists", count)
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

// A stored config_json of literal "null" is valid JSON, so the unmarshal
// succeeded and left the map nil.
func TestANullDocumentDoesNotPanic(t *testing.T) {
	db := useTempTaskDB(t)
	id := insertTask(t, db, 0, `null`)

	if err := SetEnable(itoa(id), true); err != nil {
		t.Fatalf("SetEnable: %v", err)
	}
	if got := readConfig(t, db, id); got != `{"status":"Running"}` {
		t.Errorf("stored document = %s", got)
	}
}

func TestSetEnableOnAnUnknownID(t *testing.T) {
	useTempTaskDB(t)

	if err := SetEnable("999", true); err == nil {
		t.Error("SetEnable on an unknown id returned no error")
	}
}

// TestSetEnableTagsWhatWentWrong covers the one message an operator gets back.
func TestSetEnableTagsWhatWentWrong(t *testing.T) {
	emptyTaskDB(t)

	err := SetEnable("1", true)
	if err == nil {
		t.Fatal("SetEnable on a database with no tables returned no error")
	}
	if got := stageOf(err); got != StageLookup {
		t.Errorf("stage = %q, want %q (err = %v)", got, StageLookup, err)
	}
}

// TestSetEnableOnAMissingRowSaysSo is the other half: a row that is not there is
// not the same failure as a table that is not there.
func TestSetEnableOnAMissingRowSaysSo(t *testing.T) {
	useTempTaskDB(t)

	if err := SetEnable("404", true); !errors.Is(err, ErrNoSuchTask) {
		t.Errorf("SetEnable on an unknown id = %v, want ErrNoSuchTask", err)
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

// TestReadTaskEngineAnswersEmptyForEverySortOfFailure records that the four
// ways this can go wrong are indistinguishable: an unopenable database, a
// missing row, an empty document and a corrupt document all answer "".
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

	// A later measurement with fewer rows cannot happen: the delta is MAX - MIN,
	// so it is never negative from this query.
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

// TestARowThatWillNotScanIsReported covers a monitoring row whose counts are
// text.
func TestARowThatWillNotScanIsReported(t *testing.T) {
	db := useTempTaskDB(t)
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	day := now.Format("2006-01-02")

	if _, err := db.Exec(
		`INSERT INTO monitoring_log
		   (sync_task_id, logged_at, db_type, src_table, tgt_table, src_row_count, tgt_row_count)
		 VALUES (1, ?, 'MONGODB', 'orders', 'orders', 'x', 'y')`, day+" 01:00:00"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	stats, err := TodayTableStats("1", now)
	if err == nil {
		t.Fatalf("TodayTableStats returned %d rows and no error for an unreadable row", len(stats))
	}
	if got := stageOf(err); got != StageScan {
		t.Errorf("stage = %q, want %q", got, StageScan)
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
		{"UpdateTask", func() error { return UpdateTask("1", 1, "now", domain.Config{}) }, StageOpen},
		{"SetEnable", func() error { return SetEnable("1", true) }, StageOpen},
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

// TestEveryCallNamesTheSameFailureTheSameWay covers two messages for one
// condition.
func TestEveryCallNamesTheSameFailureTheSameWay(t *testing.T) {
	unopenableDB(t)

	for name, call := range map[string]func() error{
		"UpdateTask": func() error { return UpdateTask("1", 1, "now", domain.Config{}) },
		"DeleteTask": func() error { return DeleteTask("1") },
		"SetEnable":  func() error { return SetEnable("1", true) },
	} {
		t.Run(name, func(t *testing.T) {
			if got := stageOf(call()); got != StageOpen {
				t.Errorf("stage = %q, want %q", got, StageOpen)
			}
		})
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

// withSealedCredentials configures a key for the duration of one test, so the
// store encrypts what it writes.
func withSealedCredentials(t *testing.T) {
	t.Helper()

	// 32 bytes as hex: "0123456789abcdef0123456789abcdef".
	t.Setenv("SYNC_CONFIG_KEY",
		"30313233343536373839616263646566"+"30313233343536373839616263646566")
	keeper, err := secret.KeeperFromEnv()
	if err != nil {
		t.Fatalf("KeeperFromEnv: %v", err)
	}
	previous := secret.Default
	secret.Default = keeper
	t.Cleanup(func() { secret.Default = previous })
}

func taskWithCredentials() domain.Config {
	return domain.ConfigFrom(domain.Request{
		TaskName:   "tokyo-to-osaka",
		SourceType: "mysql",
		SourceConn: map[string]string{"host": "tokyo", "user": "repl", "password": "tokyo-secret"},
		TargetConn: map[string]string{"host": "osaka", "user": "repl", "password": "osaka-secret"},
	})
}

// Masking them on the way out of the API does nothing about the file.
func TestTheStoredPasswordsAreNotReadable(t *testing.T) {
	db := useTempTaskDB(t)
	withSealedCredentials(t)

	id, err := InsertTask(1, "now", taskWithCredentials())
	if err != nil {
		t.Fatalf("InsertTask: %v", err)
	}

	stored := readConfig(t, db, id)
	for _, plaintext := range []string{"tokyo-secret", "osaka-secret"} {
		if contains(stored, plaintext) {
			t.Errorf("the stored document carries %q in the clear: %s", plaintext, stored)
		}
	}
	// The rest stays readable, so an operator can tell which task is which.
	for _, kept := range []string{"tokyo", "osaka", "repl", "tokyo-to-osaka"} {
		if !contains(stored, kept) {
			t.Errorf("the stored document lost %q: %s", kept, stored)
		}
	}
}

// TestTheCredentialsComeBackOnTheWayOut closes the loop.
func TestTheCredentialsComeBackOnTheWayOut(t *testing.T) {
	useTempTaskDB(t)
	withSealedCredentials(t)

	if _, err := InsertTask(1, "now", taskWithCredentials()); err != nil {
		t.Fatalf("InsertTask: %v", err)
	}

	tasks, err := ListTasks()
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("%d tasks, want one", len(tasks))
	}

	document := tasks[0].ConfigJSON()
	for _, want := range []string{"tokyo-secret", "osaka-secret"} {
		if !contains(document, want) {
			t.Errorf("the read document does not carry %q: %s", want, document)
		}
	}
}

// TestAnUpdateDoesNotSealTwice covers a configuration rewritten by a path that
// meant to change something else.
func TestAnUpdateDoesNotSealTwice(t *testing.T) {
	useTempTaskDB(t)
	withSealedCredentials(t)

	id, err := InsertTask(1, "now", taskWithCredentials())
	if err != nil {
		t.Fatalf("InsertTask: %v", err)
	}

	updated := taskWithCredentials()
	updated.TaskName = "renamed"
	if err := UpdateTask(strconv.FormatInt(id, 10), 1, "later", updated); err != nil {
		t.Fatalf("UpdateTask: %v", err)
	}

	tasks, err := ListTasks()
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	document := tasks[0].ConfigJSON()
	if !contains(document, "tokyo-secret") {
		t.Errorf("the password did not survive the update: %s", document)
	}
	if !contains(document, "renamed") {
		t.Errorf("the change that was made did not land: %s", document)
	}
}

// TestWithNoKeyTheStoreBehavesAsItAlwaysDid is the state of a deployment that
// has not configured one.
func TestWithNoKeyTheStoreBehavesAsItAlwaysDid(t *testing.T) {
	db := useTempTaskDB(t)
	previous := secret.Default
	secret.Default = nil
	t.Cleanup(func() { secret.Default = previous })

	id, err := InsertTask(1, "now", taskWithCredentials())
	if err != nil {
		t.Fatalf("InsertTask: %v", err)
	}

	if stored := readConfig(t, db, id); !contains(stored, "tokyo-secret") {
		t.Errorf("the stored document = %s", stored)
	}
}

// withKey points the credential keeper at a fixed test key for one test.
func withKey(t *testing.T) {
	t.Helper()

	t.Setenv("SYNC_CONFIG_KEY", base64.StdEncoding.EncodeToString(
		[]byte("0123456789abcdef0123456789abcdef")))
	k, err := secret.KeeperFromEnv()
	if err != nil {
		t.Fatalf("KeeperFromEnv: %v", err)
	}
	previous := secret.Default
	secret.Default = k
	t.Cleanup(func() { secret.Default = previous })
}

// withoutKey removes the keeper for one test, which is the state of every
// deployment that has not set the variable.
func withoutKey(t *testing.T) {
	t.Helper()

	previous := secret.Default
	secret.Default = nil
	t.Cleanup(func() { secret.Default = previous })
}

// TestAStoredPasswordIsNotReadableInTheFile is the point of the whole exercise:
// anybody holding the SQLite file — a backup, a volume snapshot, one cat inside
// the pod — must not thereby hold the credentials for both regions.
func TestAStoredPasswordIsNotReadableInTheFile(t *testing.T) {
	db := useTempTaskDB(t)
	withKey(t)

	id, err := InsertTask(1, "now", domain.ConfigFrom(domain.Request{
		TaskName:   "orders",
		SourceConn: map[string]string{"host": "tokyo", "password": "s3cret"},
		TargetConn: map[string]string{"host": "osaka", "password": "s3cret"},
	}))
	if err != nil {
		t.Fatalf("InsertTask: %v", err)
	}

	stored := readConfig(t, db, id)
	if contains(stored, "s3cret") {
		t.Errorf("the password is in the file in the clear: %s", stored)
	}
	// The host is deliberately left readable: it is not a secret.
	if !contains(stored, "tokyo") {
		t.Errorf("the stored document lost the host: %s", stored)
	}
}

// TestAStoredPasswordIsOpenedOnTheWayBack keeps the encryption invisible to the
// rest of the read path, which otherwise would connect with a ciphertext.
func TestAStoredPasswordIsOpenedOnTheWayBack(t *testing.T) {
	useTempTaskDB(t)
	withKey(t)

	if _, err := InsertTask(1, "now", domain.ConfigFrom(domain.Request{
		TaskName:   "orders",
		SourceConn: map[string]string{"password": "s3cret"},
	})); err != nil {
		t.Fatalf("InsertTask: %v", err)
	}

	tasks, err := ListTasks()
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("ListTasks returned %d tasks", len(tasks))
	}
	cfg, err := tasks[0].Config()
	if err != nil {
		t.Fatalf("Config: %v", err)
	}
	if got := cfg.SourceConn["password"]; got != "s3cret" {
		t.Errorf("password = %q, want the plaintext", got)
	}
}

// TestAnUpdateReplacesTheSealedPassword covers the write path the UI uses most,
// where a task edited once must not end up storing the ciphertext of its own
// ciphertext or a password nobody can open.
func TestAnUpdateReplacesTheSealedPassword(t *testing.T) {
	db := useTempTaskDB(t)
	withKey(t)
	id := insertTask(t, db, 1, `{"taskName":"orders"}`)

	if err := UpdateTask(itoa(id), 1, "now", domain.ConfigFrom(domain.Request{
		TaskName:   "orders",
		SourceConn: map[string]string{"password": "rotated"},
	})); err != nil {
		t.Fatalf("UpdateTask: %v", err)
	}

	if stored := readConfig(t, db, id); contains(stored, "rotated") {
		t.Errorf("the rotated password is in the file in the clear: %s", stored)
	}

	tasks, err := ListTasks()
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	cfg, err := tasks[0].Config()
	if err != nil {
		t.Fatalf("Config: %v", err)
	}
	if got := cfg.SourceConn["password"]; got != "rotated" {
		t.Errorf("password = %q after the update", got)
	}
}

// TestAPasswordNobodyCanOpenStillListsItsTask matters because the alternative is
// worse: one row sealed under a key that has since been lost would otherwise
// fail the whole listing, and with it every task the UI can see.
func TestAPasswordNobodyCanOpenStillListsItsTask(t *testing.T) {
	db := useTempTaskDB(t)
	withoutKey(t)
	insertTask(t, db, 1, `{"taskName":"orders","sourceConn":{"password":"enc:v1:AAAA"}}`)

	tasks, err := ListTasks()
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("ListTasks returned %d tasks, want the task listed anyway", len(tasks))
	}
	cfg, err := tasks[0].Config()
	if err != nil {
		t.Fatalf("Config: %v", err)
	}
	if got := cfg.SourceConn["password"]; got != "enc:v1:AAAA" {
		t.Errorf("password = %q, want the sealed form carried through", got)
	}
}

// TestWithoutAKeyThePasswordIsStoredAsItAlwaysWas pins the upgrade path: a
// deployment that has not set a key keeps working exactly as before rather than
// refusing to write.
func TestWithoutAKeyThePasswordIsStoredAsItAlwaysWas(t *testing.T) {
	db := useTempTaskDB(t)
	withoutKey(t)

	id, err := InsertTask(1, "now", domain.ConfigFrom(domain.Request{
		SourceConn: map[string]string{"password": "plain"},
	}))
	if err != nil {
		t.Fatalf("InsertTask: %v", err)
	}

	if stored := readConfig(t, db, id); !contains(stored, `"password":"plain"`) {
		t.Errorf("stored document = %s", stored)
	}
}
