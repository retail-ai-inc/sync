package domain

import (
	"encoding/json"
	"testing"
)

func TestRequestNormaliseFillsInDefaults(t *testing.T) {
	tests := []struct {
		name       string
		in         Request
		wantName   string
		wantStatus string
		wantEnable int
	}{
		{"empty request", Request{}, "Sync Task", StatusStopped, 0},
		{"name only", Request{TaskName: "orders"}, "orders", StatusStopped, 0},
		{"running", Request{TaskName: "orders", Status: "Running"}, "orders", "Running", 1},
		{"stopped", Request{TaskName: "orders", Status: "Stopped"}, "orders", "Stopped", 0},
		{"lowercase running", Request{Status: "running"}, "Sync Task", "running", 1},
		{"mixed case running", Request{Status: "RuNnInG"}, "Sync Task", "RuNnInG", 1},
		{"unknown status", Request{Status: "Paused"}, "Sync Task", "Paused", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := tt.in
			enable := req.Normalise()

			if req.TaskName != tt.wantName {
				t.Errorf("TaskName = %q, want %q", req.TaskName, tt.wantName)
			}
			if req.Status != tt.wantStatus {
				t.Errorf("Status = %q, want %q", req.Status, tt.wantStatus)
			}
			if enable != tt.wantEnable {
				t.Errorf("enable = %d, want %d", enable, tt.wantEnable)
			}
		})
	}
}

// TestNormaliseKeepsAnUnrecognisedStatusButTreatsItAsStopped records that the
// status is a free-text field: only "running" in any casing enables the task,
// and every other string is stored verbatim while the task stays disabled. A
// caller that sends "Started" gets a task that reports "Started" and never
// runs.
func TestNormaliseKeepsAnUnrecognisedStatusButTreatsItAsStopped(t *testing.T) {
	req := Request{Status: "Started"}

	if enable := req.Normalise(); enable != 0 {
		t.Errorf("enable = %d for status %q, want 0", enable, req.Status)
	}
	if req.Status != "Started" {
		t.Errorf("Status = %q; an unrecognised status is no longer stored verbatim, "+
			"so validation appears to have been added — assert the rejection instead", req.Status)
	}
}

func TestConfigFromCarriesEveryField(t *testing.T) {
	req := Request{
		TaskName:               "orders",
		SourceType:             "mongodb",
		Status:                 "Running",
		SourceConn:             map[string]string{"host": "src"},
		TargetConn:             map[string]string{"host": "tgt"},
		Mappings:               []map[string]interface{}{{"source_table": "a"}},
		PgReplicationSlot:      "slot",
		PgPlugin:               "pgoutput",
		PgPositionPath:         "/pg",
		PgPublicationNames:     "pub",
		MysqlPositionPath:      "/mysql",
		MongodbResumeTokenPath: "/mongo",
		RedisPositionPath:      "/redis",
		SecurityEnabled:        true,
	}

	got := ConfigFrom(req)

	// The engine is the one field whose name differs: the request calls it
	// sourceType, the stored document calls it type.
	if got.Type != req.SourceType {
		t.Errorf("Type = %q, want %q", got.Type, req.SourceType)
	}
	if got.TaskName != "orders" || got.Status != "Running" {
		t.Errorf("TaskName/Status = %q/%q", got.TaskName, got.Status)
	}
	if got.SourceConn["host"] != "src" || got.TargetConn["host"] != "tgt" {
		t.Errorf("connections = %v / %v", got.SourceConn, got.TargetConn)
	}
	if len(got.Mappings) != 1 {
		t.Errorf("Mappings = %v, want one entry", got.Mappings)
	}
	if got.PgReplicationSlot != "slot" || got.PgPlugin != "pgoutput" ||
		got.PgPositionPath != "/pg" || got.PgPublicationNames != "pub" ||
		got.MysqlPositionPath != "/mysql" || got.MongodbResumeTokenPath != "/mongo" ||
		got.RedisPositionPath != "/redis" || !got.SecurityEnabled {
		t.Errorf("engine-specific fields did not survive: %+v", got)
	}
}

// TestConfigFromIsAReplacementNotAMerge records that building a configuration
// from a request discards anything the request omits. The update endpoint has
// no way to change one field: a PUT carrying only a schedule wipes the
// connections, the mappings and the position paths.
func TestConfigFromIsAReplacementNotAMerge(t *testing.T) {
	partial := Request{TaskName: "orders", SourceType: "mongodb"}

	got := ConfigFrom(partial)

	if got.SourceConn != nil || got.TargetConn != nil || got.Mappings != nil {
		t.Fatalf("omitted fields survived: %+v — a merge appears to have been added, "+
			"so assert the merge instead", got)
	}
	if got.MongodbResumeTokenPath != "" {
		t.Errorf("MongodbResumeTokenPath = %q, want empty", got.MongodbResumeTokenPath)
	}
}

// TestTheStoredDocumentKeepsItsFieldNames pins the JSON the column holds. The
// front end reads these names, so a rename here is a breaking change that no
// compiler catches.
func TestTheStoredDocumentKeepsItsFieldNames(t *testing.T) {
	b, err := json.Marshal(ConfigFrom(Request{SourceType: "mongodb", TaskName: "n", Status: "Stopped"}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var raw map[string]interface{}
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	for _, key := range []string{
		"type", "taskName", "status", "sourceConn", "targetConn", "mappings",
		"pg_replication_slot", "pg_plugin", "pg_position_path", "pg_publication_names",
		"mysql_position_path", "mongodb_resume_token_path", "redis_position_path",
		"securityEnabled",
	} {
		if _, ok := raw[key]; !ok {
			t.Errorf("the stored document lost the %q key", key)
		}
	}
	if len(raw) != 14 {
		t.Errorf("the stored document has %d keys, want 14: %v", len(raw), raw)
	}
}

func TestSyncTaskAccessors(t *testing.T) {
	task := NewSyncTask(7, 1, "2026-08-21 01:00:00", "2026-08-21 02:00:00", `{"taskName":"orders"}`)

	if task.ID() != 7 {
		t.Errorf("ID = %d, want 7", task.ID())
	}
	if !task.IsEnabled() {
		t.Error("IsEnabled = false for enable=1")
	}
	if task.LastUpdateTime() != "2026-08-21 01:00:00" {
		t.Errorf("LastUpdateTime = %q", task.LastUpdateTime())
	}
	if task.LastRunTime() != "2026-08-21 02:00:00" {
		t.Errorf("LastRunTime = %q", task.LastRunTime())
	}
	if task.ConfigJSON() != `{"taskName":"orders"}` {
		t.Errorf("ConfigJSON = %q", task.ConfigJSON())
	}
}

// TestIsEnabledTreatsAnyNonZeroAsEnabled records that the enable column is read
// as a boolean rather than as the 0/1 the writers use, so a row holding 2 is
// enabled while the status derivation below calls it stopped.
func TestIsEnabledTreatsAnyNonZeroAsEnabled(t *testing.T) {
	task := NewSyncTask(1, 2, "", "", "")

	if !task.IsEnabled() {
		t.Error("IsEnabled = false for enable=2")
	}
	if got := task.Status(Config{}); got != StatusStopped {
		t.Errorf("Status = %q for enable=2, want %q — the two readings of the enable "+
			"column now agree, so assert the new behaviour", got, StatusStopped)
	}
}

func TestSyncTaskConfigParsing(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		wantType string
		wantErr  bool
	}{
		{"empty document", "", "", false},
		{"well formed", `{"type":"mongodb"}`, "mongodb", false},
		{"unknown fields ignored", `{"type":"mysql","nope":1}`, "mysql", false},
		{"malformed", `{"type":`, "", true},
		{"wrong shape", `[1,2,3]`, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := NewSyncTask(1, 0, "", "", tt.json).Config()

			if (err != nil) != tt.wantErr {
				t.Fatalf("Config() error = %v, wantErr %v", err, tt.wantErr)
			}
			if cfg.Type != tt.wantType {
				t.Errorf("Type = %q, want %q", cfg.Type, tt.wantType)
			}
		})
	}
}

func TestSyncTaskStatus(t *testing.T) {
	tests := []struct {
		name   string
		enable int
		config Config
		want   string
	}{
		{"enabled", 1, Config{}, StatusRunning},
		{"disabled", 0, Config{}, StatusStopped},
		{"config overrides enabled", 1, Config{Status: "Paused"}, "Paused"},
		{"config overrides disabled", 0, Config{Status: "Running"}, "Running"},
		{"empty config status does not override", 1, Config{Status: ""}, StatusRunning},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewSyncTask(1, tt.enable, "", "", "").Status(tt.config); got != tt.want {
				t.Errorf("Status = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestTheConfiguredStatusCanContradictTheEnableColumn records that the two
// places a task's state lives are never reconciled. A row with enable=0 whose
// document says Running is reported as Running, and nothing is replicating.
func TestTheConfiguredStatusCanContradictTheEnableColumn(t *testing.T) {
	task := NewSyncTask(1, 0, "", "", "")

	if got := task.Status(Config{Status: StatusRunning}); got != StatusRunning {
		t.Fatalf("Status = %q for enable=0 with a Running document, want %q — "+
			"the contradiction appears to be resolved now", got, StatusRunning)
	}
	if task.IsEnabled() {
		t.Error("IsEnabled = true for enable=0")
	}
}

func TestSyncTaskDisplayName(t *testing.T) {
	tests := []struct {
		name   string
		id     int
		config Config
		want   string
	}{
		{"named", 1, Config{TaskName: "orders"}, "orders"},
		{"unnamed", 42, Config{}, "Sync Task 42"},
		{"whitespace is a name", 1, Config{TaskName: " "}, " "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewSyncTask(tt.id, 0, "", "", "").DisplayName(tt.config); got != tt.want {
				t.Errorf("DisplayName = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIsMongoDB(t *testing.T) {
	for _, tt := range []struct {
		in   string
		want bool
	}{
		{"mongodb", true},
		{"MongoDB", true},
		{"MONGODB", true},
		{"mysql", false},
		{"", false},
		{"mongo", false},
	} {
		t.Run(tt.in, func(t *testing.T) {
			if got := IsMongoDB(Config{Type: tt.in}); got != tt.want {
				t.Errorf("IsMongoDB(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}
