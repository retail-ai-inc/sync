package config

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// newConfigDB creates a throwaway SQLite database carrying the two tables the
// loaders read, so they can be exercised without touching the sync.db tracked
// in this repository.
func newConfigDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "config.db"))
	if err != nil {
		t.Fatalf("open temp sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const schema = `
CREATE TABLE config_global (
    id                                INTEGER PRIMARY KEY,
    enable_table_row_count_monitoring INTEGER NOT NULL DEFAULT 0,
    log_level                         TEXT    NOT NULL DEFAULT 'info',
    monitor_interval                  INTEGER DEFAULT 60,
    slackWebhookURL                   TEXT,
    slackChannel                      TEXT
);
CREATE TABLE sync_tasks (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    enable           INTEGER NOT NULL DEFAULT 1,
    last_update_time DATETIME,
    last_run_time    DATETIME,
    config_json      TEXT NOT NULL
);`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	return db
}

func TestLoadGlobalConfig(t *testing.T) {
	db := newConfigDB(t)
	if _, err := db.Exec(`
INSERT INTO config_global (id, enable_table_row_count_monitoring, log_level, monitor_interval, slackWebhookURL, slackChannel)
VALUES (1, 1, 'debug', 300, 'https://hooks.example.com/x', '#alerts')`); err != nil {
		t.Fatalf("seed config_global: %v", err)
	}

	got := loadGlobalConfig(db)

	if !got.EnableTableRowCountMonitoring {
		t.Error("EnableTableRowCountMonitoring = false, want true")
	}
	if got.LogLevel != "debug" {
		t.Errorf("LogLevel = %q, want %q", got.LogLevel, "debug")
	}
	// The column stores seconds; the field is a Duration.
	if want := 300 * time.Second; got.MonitorInterval != want {
		t.Errorf("MonitorInterval = %v, want %v", got.MonitorInterval, want)
	}
	if got.SlackWebhookURL != "https://hooks.example.com/x" {
		t.Errorf("SlackWebhookURL = %q", got.SlackWebhookURL)
	}
	if got.SlackChannel != "#alerts" {
		t.Errorf("SlackChannel = %q", got.SlackChannel)
	}
}

func TestLoadGlobalConfigNullSlackColumns(t *testing.T) {
	db := newConfigDB(t)
	if _, err := db.Exec(`
INSERT INTO config_global (id, enable_table_row_count_monitoring, log_level, monitor_interval)
VALUES (1, 0, 'info', 60)`); err != nil {
		t.Fatalf("seed config_global: %v", err)
	}

	got := loadGlobalConfig(db)

	// The query wraps both Slack columns in COALESCE, so NULL must not fail the scan.
	if got.SlackWebhookURL != "" || got.SlackChannel != "" {
		t.Errorf("Slack fields = %q / %q, want empty", got.SlackWebhookURL, got.SlackChannel)
	}
	if got.EnableTableRowCountMonitoring {
		t.Error("EnableTableRowCountMonitoring = true, want false")
	}
}

const fullTaskJSON = `{
  "type": "mongodb",
  "taskName": "tokyo-to-osaka",
  "status": "Running",
  "securityEnabled": true,
  "sourceConn": {"user":"root","password":"root","host":"tokyo","port":"27017","database":"source_db"},
  "targetConn": {"user":"root","password":"root","host":"osaka","port":"27017","database":"target_db"},
  "mongodb_resume_token_path": "/var/lib/sync/tokens",
  "mappings": [
    {
      "sourceDatabase": "source_db",
      "targetDatabase": "target_db",
      "tables": [
        {
          "sourceTable": "users",
          "targetTable": "users",
          "countQuery": {"field": "created_at", "range": "yesterday"},
          "fieldSecurity": [
            {"field": "email", "securityType": "masked"},
            {"field": "profile.phone", "securityType": "encrypted"}
          ],
          "advancedSettings": {
            "syncIndexes": true,
            "ignoreDeleteOps": true,
            "uploadToGcs": true,
            "gcsAddress": "gs://bucket/prefix",
            "maxRetries": 7
          }
        }
      ]
    }
  ]
}`

func TestLoadSyncTasks(t *testing.T) {
	db := newConfigDB(t)
	if _, err := db.Exec(
		`INSERT INTO sync_tasks (id, enable, last_update_time, last_run_time, config_json) VALUES (1, 1, '2026-08-21 10:00:00', '2026-08-21 10:05:00', ?)`,
		fullTaskJSON); err != nil {
		t.Fatalf("seed sync_tasks: %v", err)
	}

	got := loadSyncTasks(db)
	if len(got) != 1 {
		t.Fatalf("loadSyncTasks returned %d tasks, want 1", len(got))
	}
	sc := got[0]

	if sc.ID != 1 || !sc.Enable {
		t.Errorf("ID/Enable = %d/%v, want 1/true", sc.ID, sc.Enable)
	}
	if sc.Type != "mongodb" || sc.TaskName != "tokyo-to-osaka" || sc.Status != "Running" {
		t.Errorf("Type/TaskName/Status = %q/%q/%q", sc.Type, sc.TaskName, sc.Status)
	}
	if sc.LastUpdateTime != "2026-08-21 10:00:00" || sc.LastRunTime != "2026-08-21 10:05:00" {
		t.Errorf("timestamps = %q / %q", sc.LastUpdateTime, sc.LastRunTime)
	}
	if sc.MongoDBResumeTokenPath != "/var/lib/sync/tokens" {
		t.Errorf("MongoDBResumeTokenPath = %q", sc.MongoDBResumeTokenPath)
	}

	// Connection maps are turned into DSNs during load, not at use time.
	if want := "mongodb://root:root@tokyo:27017/source_db?directConnection=true&authSource=admin"; sc.SourceConnection != want {
		t.Errorf("SourceConnection =\n  %q\nwant\n  %q", sc.SourceConnection, want)
	}
	if !strings.Contains(sc.TargetConnection, "osaka:27017/target_db") {
		t.Errorf("TargetConnection = %q", sc.TargetConnection)
	}

	if len(sc.Mappings) != 1 || len(sc.Mappings[0].Tables) != 1 {
		t.Fatalf("mappings shape = %d mappings", len(sc.Mappings))
	}
	m := sc.Mappings[0]
	if m.SourceDatabase != "source_db" || m.TargetDatabase != "target_db" {
		t.Errorf("mapping databases = %q -> %q", m.SourceDatabase, m.TargetDatabase)
	}

	tbl := m.Tables[0]
	if tbl.SourceTable != "users" || tbl.TargetTable != "users" {
		t.Errorf("table mapping = %q -> %q", tbl.SourceTable, tbl.TargetTable)
	}
	// securityEnabled lives at the root of config_json and is copied onto every table.
	if !tbl.SecurityEnabled {
		t.Error("SecurityEnabled = false, want true")
	}
	if len(tbl.FieldSecurity) != 2 {
		t.Errorf("FieldSecurity has %d entries, want 2", len(tbl.FieldSecurity))
	}
	if tbl.CountQuery["field"] != "created_at" {
		t.Errorf("CountQuery = %v", tbl.CountQuery)
	}

	as := tbl.AdvancedSettings
	if !as.SyncIndexes || !as.IgnoreDeleteOps || !as.UploadToGcs {
		t.Errorf("advanced booleans = %v/%v/%v", as.SyncIndexes, as.IgnoreDeleteOps, as.UploadToGcs)
	}
	if as.GcsAddress != "gs://bucket/prefix" {
		t.Errorf("GcsAddress = %q", as.GcsAddress)
	}
	if as.MaxRetries != 7 {
		t.Errorf("MaxRetries = %d, want 7", as.MaxRetries)
	}
	// Durations are absent here on purpose: supplying them as strings breaks
	// the whole task, see TestLoadSyncTasksStringDurationVoidsWholeTask.
	if as.BaseRetryDelay != 0 || as.MaxRetryDelay != 0 {
		t.Errorf("retry delays = %v / %v, want zero", as.BaseRetryDelay, as.MaxRetryDelay)
	}
}

func TestLoadSyncTasksOrdersByID(t *testing.T) {
	db := newConfigDB(t)
	for _, id := range []int{3, 1, 2} {
		if _, err := db.Exec(
			`INSERT INTO sync_tasks (id, enable, config_json) VALUES (?, 0, '{"type":"mysql"}')`, id); err != nil {
			t.Fatalf("seed sync_tasks: %v", err)
		}
	}

	got := loadSyncTasks(db)

	if len(got) != 3 {
		t.Fatalf("got %d tasks, want 3", len(got))
	}
	for i, want := range []int{1, 2, 3} {
		if got[i].ID != want {
			t.Errorf("task %d has ID %d, want %d", i, got[i].ID, want)
		}
	}
}

func TestLoadSyncTasksSynthesisesEmptyMapping(t *testing.T) {
	db := newConfigDB(t)
	if _, err := db.Exec(
		`INSERT INTO sync_tasks (id, enable, config_json) VALUES (1, 1, '{"type":"mysql","sourceConn":{"host":"h","port":"3306","database":"d"}}')`); err != nil {
		t.Fatalf("seed sync_tasks: %v", err)
	}

	got := loadSyncTasks(db)

	// A task with no mappings gets one empty DatabaseMapping so downstream
	// range loops do not have to special-case nil.
	if len(got) != 1 || len(got[0].Mappings) != 1 {
		t.Fatalf("mappings = %d, want 1 synthesised entry", len(got[0].Mappings))
	}
	if m := got[0].Mappings[0]; m.SourceDatabase != "" || len(m.Tables) != 0 {
		t.Errorf("synthesised mapping is not empty: %+v", m)
	}
}

// TestLoadSyncTasksSwallowsMalformedJSON records F-273. A config_json that
// does not parse produces a task with every field left at its zero value: no
// type, no connection strings, no mappings. Nothing propagates the failure, so
// the task is silently skipped by the dispatcher in cmd/sync as an unknown
// type. Validation belongs in the aggregate that owns these invariants.
func TestLoadSyncTasksSwallowsMalformedJSON(t *testing.T) {
	db := newConfigDB(t)
	if _, err := db.Exec(
		`INSERT INTO sync_tasks (id, enable, config_json) VALUES (1, 1, '{"type": not-json}')`); err != nil {
		t.Fatalf("seed sync_tasks: %v", err)
	}

	got := loadSyncTasks(db)

	if len(got) != 1 {
		t.Fatalf("got %d tasks, want 1", len(got))
	}
	sc := got[0]
	if sc.Type != "" || sc.SourceConnection != "" || sc.TargetConnection != "" || sc.Mappings != nil {
		t.Errorf("malformed JSON no longer yields a zero-valued task (%+v); "+
			"validation may have landed, so assert the new behaviour instead", sc)
	}
	// The row itself still loads, which is why the failure is invisible.
	if sc.ID != 1 || !sc.Enable {
		t.Errorf("ID/Enable = %d/%v, want 1/true", sc.ID, sc.Enable)
	}
}

func TestSyncConfigAccessors(t *testing.T) {
	sc := SyncConfig{PGReplicationSlotName: "sync_slot", PGPluginName: "pgoutput"}
	if got := sc.PGReplicationSlot(); got != "sync_slot" {
		t.Errorf("PGReplicationSlot() = %q", got)
	}
	if got := sc.PGPlugin(); got != "pgoutput" {
		t.Errorf("PGPlugin() = %q", got)
	}

	cfg := Config{SlackWebhookURL: "https://hooks.example.com/y", SlackChannel: "#ops"}
	if got := cfg.GetSlackWebhookURL(); got != "https://hooks.example.com/y" {
		t.Errorf("GetSlackWebhookURL() = %q", got)
	}
	if got := cfg.GetSlackChannel(); got != "#ops" {
		t.Errorf("GetSlackChannel() = %q", got)
	}
}

// TestLoadSyncTasksStringDurationVoidsWholeTask records a defect with a wide
// blast radius. AdvancedSettings declares BaseRetryDelay and MaxRetryDelay as
// time.Duration, an int64, while the field comments and the manual parsing
// further down this file both treat them as strings such as "5s". Supplying a
// string therefore fails the very first json.Unmarshal, and because that error
// is only logged (F-273) the task loses everything: type, connection strings,
// and mappings. cmd/sync then skips it as an unknown type and the task simply
// never syncs, with one warning line as the only trace.
//
// Two consequences worth spelling out:
//   - the format documented in the struct comments is unusable; only raw
//     nanosecond integers survive the unmarshal
//   - the time.ParseDuration branch in loadSyncTasks is unreachable, since it
//     only runs when the enclosing unmarshal already succeeded
func TestLoadSyncTasksStringDurationVoidsWholeTask(t *testing.T) {
	db := newConfigDB(t)
	const taskJSON = `{
	  "type": "mongodb",
	  "taskName": "retry-settings",
	  "sourceConn": {"host":"tokyo","port":"27017","database":"source_db"},
	  "mappings": [{"tables": [{"sourceTable":"users","targetTable":"users",
	    "advancedSettings": {"baseRetryDelay": "3s"}}]}]
	}`
	if _, err := db.Exec(`INSERT INTO sync_tasks (id, enable, config_json) VALUES (1, 1, ?)`, taskJSON); err != nil {
		t.Fatalf("seed sync_tasks: %v", err)
	}

	got := loadSyncTasks(db)
	if len(got) != 1 {
		t.Fatalf("got %d tasks, want 1", len(got))
	}
	sc := got[0]

	if sc.Type != "" || sc.TaskName != "" || sc.SourceConnection != "" || sc.Mappings != nil {
		t.Errorf("a string duration no longer voids the task (%+v); the field type "+
			"or the parsing may have been fixed, so assert the new behaviour instead", sc)
	}
}

// TestLoadSyncTasksNumericDurationSurvives shows the only shape that works: a
// raw nanosecond count, which is what the struct type actually accepts.
func TestLoadSyncTasksNumericDurationSurvives(t *testing.T) {
	db := newConfigDB(t)
	const taskJSON = `{
	  "type": "mongodb",
	  "sourceConn": {"host":"tokyo","port":"27017","database":"source_db"},
	  "mappings": [{"tables": [{"sourceTable":"users","targetTable":"users",
	    "advancedSettings": {"baseRetryDelay": 3000000000}}]}]
	}`
	if _, err := db.Exec(`INSERT INTO sync_tasks (id, enable, config_json) VALUES (1, 1, ?)`, taskJSON); err != nil {
		t.Fatalf("seed sync_tasks: %v", err)
	}

	got := loadSyncTasks(db)
	if len(got) != 1 || len(got[0].Mappings) != 1 || len(got[0].Mappings[0].Tables) != 1 {
		t.Fatalf("task did not load: %+v", got)
	}
	if want := 3 * time.Second; got[0].Mappings[0].Tables[0].AdvancedSettings.BaseRetryDelay != want {
		t.Errorf("BaseRetryDelay = %v, want %v",
			got[0].Mappings[0].Tables[0].AdvancedSettings.BaseRetryDelay, want)
	}
}
