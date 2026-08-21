package domain

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestConfigFromCarriesEveryField(t *testing.T) {
	req := Request{
		Name:               "nightly",
		SourceType:         "mongodb",
		Database:           map[string]interface{}{"name": "src"},
		Destination:        map[string]interface{}{"bucket": "gs://b"},
		Schedule:           "0 3 * * *",
		Format:             "json",
		BackupType:         "full",
		Query:              map[string]interface{}{"created_at": "yesterday"},
		CompressionType:    "zip",
		TableSelectionMode: "regex",
		RegexPattern:       "orders_.*",
	}

	got := ConfigFrom(req, StatusEnabled)

	if got.Name != "nightly" || got.SourceType != "mongodb" || got.Schedule != "0 3 * * *" ||
		got.Format != "json" || got.BackupType != "full" || got.CompressionType != "zip" ||
		got.TableSelectionMode != "regex" || got.RegexPattern != "orders_.*" {
		t.Errorf("scalar fields did not survive: %+v", got)
	}
	if got.Database["name"] != "src" || got.Destination["bucket"] != "gs://b" ||
		got.Query["created_at"] != "yesterday" {
		t.Errorf("map fields did not survive: %+v", got)
	}
	if got.Status != StatusEnabled {
		t.Errorf("Status = %q, want %q", got.Status, StatusEnabled)
	}
}

// TestTheStatusComesFromTheServerNotTheRequest records that Request has no
// status field at all: a caller cannot set one, and whatever the server passes
// wins. That is why an update has to read the stored status back before writing.
func TestTheStatusComesFromTheServerNotTheRequest(t *testing.T) {
	b, err := json.Marshal(Request{Name: "n"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var raw map[string]interface{}
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := raw["status"]; ok {
		t.Fatal("Request now carries a status; the server no longer decides it, " +
			"so assert the new rule instead")
	}
	if len(raw) != 11 {
		t.Errorf("Request has %d fields, want 11: %v", len(raw), raw)
	}
}

// TestConfigFromIsAReplacementNotAMerge records T-111: every field of the
// stored document comes from the request, so a PUT carrying only a schedule
// wipes the connections, the destination, the format and the compression.
func TestConfigFromIsAReplacementNotAMerge(t *testing.T) {
	got := ConfigFrom(Request{Schedule: "0 4 * * *"}, StatusEnabled)

	if got.Database != nil || got.Destination != nil || got.Query != nil {
		t.Fatalf("omitted maps survived: %+v — a merge appears to have been added", got)
	}
	if got.Format != "" || got.CompressionType != "" || got.SourceType != "" {
		t.Errorf("omitted scalars survived: %+v", got)
	}
}

// TestTheStoredDocumentKeepsItsFieldNames pins the JSON the config_json column
// holds. The UI reads these names.
func TestTheStoredDocumentKeepsItsFieldNames(t *testing.T) {
	b, err := json.Marshal(ConfigFrom(Request{}, StatusDisabled))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var raw map[string]interface{}
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	for _, key := range []string{
		"name", "sourceType", "database", "destination", "schedule", "format",
		"backupType", "query", "status", "compressionType", "tableSelectionMode",
		"regexPattern",
	} {
		if _, ok := raw[key]; !ok {
			t.Errorf("the stored document lost the %q key", key)
		}
	}
	if len(raw) != 12 {
		t.Errorf("the stored document has %d keys, want 12: %v", len(raw), raw)
	}
}

func TestBackupJobAccessors(t *testing.T) {
	job := NewBackupJob(7, 1, "2026-08-21 01:00:00", "2026-08-20 18:00:00",
		"2026-08-22 18:00:00", `{"name":"nightly"}`)

	if job.ID() != 7 {
		t.Errorf("ID = %d, want 7", job.ID())
	}
	if job.Enable() != 1 {
		t.Errorf("Enable = %d, want 1", job.Enable())
	}
	if job.LastUpdateTime() != "2026-08-21 01:00:00" {
		t.Errorf("LastUpdateTime = %q", job.LastUpdateTime())
	}
	if job.LastBackupTime() != "2026-08-20 18:00:00" {
		t.Errorf("LastBackupTime = %q", job.LastBackupTime())
	}
	if job.NextBackupTimeRaw() != "2026-08-22 18:00:00" {
		t.Errorf("NextBackupTimeRaw = %q", job.NextBackupTimeRaw())
	}
	if job.ConfigJSON() != `{"name":"nightly"}` {
		t.Errorf("ConfigJSON = %q", job.ConfigJSON())
	}
}

func TestBackupJobConfigParsing(t *testing.T) {
	for _, tt := range []struct {
		name     string
		json     string
		wantName string
		wantErr  bool
	}{
		{"empty document", "", "", false},
		{"well formed", `{"name":"nightly"}`, "nightly", false},
		{"unknown fields ignored", `{"name":"n","nope":1}`, "n", false},
		{"malformed", `{"name":`, "", true},
		{"wrong shape", `"a string"`, "", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := NewBackupJob(1, 0, "", "", "", tt.json).Config()

			if (err != nil) != tt.wantErr {
				t.Fatalf("Config() error = %v, wantErr %v", err, tt.wantErr)
			}
			if cfg.Name != tt.wantName {
				t.Errorf("Name = %q, want %q", cfg.Name, tt.wantName)
			}
		})
	}
}

func TestBackupJobStatus(t *testing.T) {
	for _, tt := range []struct {
		name   string
		enable int
		config Config
		want   string
	}{
		{"enabled", 1, Config{}, StatusEnabled},
		{"disabled", 0, Config{}, StatusDisabled},
		{"config overrides enabled", 1, Config{Status: "paused"}, "paused"},
		{"config overrides disabled", 0, Config{Status: StatusEnabled}, StatusEnabled},
		{"empty config status does not override", 1, Config{Status: ""}, StatusEnabled},
		{"enable 2 is not enabled", 2, Config{}, StatusDisabled},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewBackupJob(1, tt.enable, "", "", "", "").Status(tt.config); got != tt.want {
				t.Errorf("Status = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestTheEnableColumnOnlyRecognisesOne records that the status derivation tests
// enable == 1 rather than enable != 0, so a row holding any other non-zero
// value reports disabled while the row is, by every other reading, on.
func TestTheEnableColumnOnlyRecognisesOne(t *testing.T) {
	if got := NewBackupJob(1, 2, "", "", "", "").Status(Config{}); got != StatusDisabled {
		t.Fatalf("Status = %q for enable=2, want %q — the reading appears to have "+
			"changed, so assert the new one", got, StatusDisabled)
	}
}

func TestBackupJobDisplayName(t *testing.T) {
	for _, tt := range []struct {
		name   string
		id     int
		config Config
		want   string
	}{
		{"named", 1, Config{Name: "nightly"}, "nightly"},
		{"unnamed", 42, Config{}, "Backup Task 42"},
		{"whitespace is a name", 1, Config{Name: " "}, " "},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewBackupJob(tt.id, 0, "", "", "", "").DisplayName(tt.config); got != tt.want {
				t.Errorf("DisplayName = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIsTerminal(t *testing.T) {
	for status, want := range map[string]bool{
		RunCompleted: true,
		RunFailed:    true,
		RunRunning:   false,
		RunPending:   false,
		"":           false,
		"Completed":  false,
	} {
		if got := IsTerminal(status); got != want {
			t.Errorf("IsTerminal(%q) = %v, want %v", status, got, want)
		}
	}
}

func TestRunAdvanceStampsTerminalStates(t *testing.T) {
	for _, status := range []string{RunCompleted, RunFailed} {
		t.Run(status, func(t *testing.T) {
			run := &Run{TaskID: "t", Status: RunRunning}
			run.Advance(status, "done", nil)

			if run.Status != status || run.Message != "done" {
				t.Errorf("Status/Message = %q/%q", run.Status, run.Message)
			}
			if run.CompletedAt == nil {
				t.Fatal("CompletedAt was not stamped for a terminal status")
			}
			if time.Since(*run.CompletedAt) > time.Minute {
				t.Errorf("CompletedAt = %v, want roughly now", run.CompletedAt)
			}
		})
	}
}

func TestRunAdvanceLeavesNonTerminalStatesUnstamped(t *testing.T) {
	run := &Run{TaskID: "t", Status: RunPending}
	run.Advance(RunRunning, "started", nil)

	if run.CompletedAt != nil {
		t.Errorf("CompletedAt = %v for a running run, want nil", run.CompletedAt)
	}
}

func TestRunAdvanceRecordsTheError(t *testing.T) {
	run := &Run{TaskID: "t"}
	run.Advance(RunFailed, "boom", errors.New("disk full"))

	if run.Error != "disk full" {
		t.Errorf("Error = %q, want %q", run.Error, "disk full")
	}
}

// TestARecoveredRunKeepsItsStaleError records T-113: Advance only writes the
// error field when it is handed one, so a run that fails and is then advanced to
// completed still carries the failure text. A caller polling the run sees
// status "completed" next to an error message.
func TestARecoveredRunKeepsItsStaleError(t *testing.T) {
	run := &Run{TaskID: "t"}
	run.Advance(RunFailed, "boom", errors.New("disk full"))
	run.Advance(RunCompleted, "recovered", nil)

	if run.Status != RunCompleted {
		t.Fatalf("Status = %q, want %q", run.Status, RunCompleted)
	}
	if run.Error != "disk full" {
		t.Fatalf("Error = %q; the stale error is cleared now, so assert that instead", run.Error)
	}
}

func TestDeriveUpdateStatus(t *testing.T) {
	for _, tt := range []struct {
		name   string
		stored map[string]interface{}
		enable int
		want   string
	}{
		{"stored status wins", map[string]interface{}{"status": "paused"}, 1, "paused"},
		{"no stored status, enabled", map[string]interface{}{}, 1, StatusEnabled},
		{"no stored status, disabled", map[string]interface{}{}, 0, StatusDisabled},
		{"nil map, enabled", nil, 1, StatusEnabled},
		{"empty stored status still wins", map[string]interface{}{"status": ""}, 1, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := DeriveUpdateStatus(tt.stored, tt.enable); got != tt.want {
				t.Errorf("DeriveUpdateStatus = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestAnEmptyStoredStatusOverridesTheEnableColumn records a sharper edge than
// the list path has: DeriveUpdateStatus checks whether the key is present, not
// whether its value is usable, so a document holding "status": "" makes an
// update store an empty status. The list endpoint then falls back to the enable
// column and the two disagree.
func TestAnEmptyStoredStatusOverridesTheEnableColumn(t *testing.T) {
	got := DeriveUpdateStatus(map[string]interface{}{"status": ""}, 1)

	if got != "" {
		t.Fatalf("DeriveUpdateStatus = %q for a present-but-empty status, want %q — "+
			"presence and usability are distinguished now, so assert that", got, "")
	}
	if listView := NewBackupJob(1, 1, "", "", "", "").Status(Config{Status: got}); listView != StatusEnabled {
		t.Errorf("the list endpoint reads %q where the update stored %q", listView, got)
	}
}

// TestANonStringStoredStatusPanics records T-112: the assertion on the stored
// value is unchecked, so a document whose status is a number or a boolean kills
// the request with a runtime panic rather than an error.
func TestANonStringStoredStatusPanics(t *testing.T) {
	for _, value := range []interface{}{1, true, nil, []interface{}{}} {
		t.Run("", func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("DeriveUpdateStatus survived a %T status; the assertion "+
						"appears to be checked now, so assert the error instead", value)
				}
			}()
			DeriveUpdateStatus(map[string]interface{}{"status": value}, 1)
		})
	}
}

func TestDeriveUpdateName(t *testing.T) {
	if got := DeriveUpdateName(map[string]interface{}{"name": "nightly"}, "7"); got != "nightly" {
		t.Errorf("DeriveUpdateName = %q, want %q", got, "nightly")
	}
	if got := DeriveUpdateName(map[string]interface{}{}, "7"); got != "Backup Task 7" {
		t.Errorf("DeriveUpdateName = %q, want %q", got, "Backup Task 7")
	}
	if got := DeriveUpdateName(nil, "42"); got != "Backup Task 42" {
		t.Errorf("DeriveUpdateName = %q, want %q", got, "Backup Task 42")
	}
}

// TestTheGeneratedNamesDisagreeBetweenCreateAndUpdate records that the two
// paths format the same fallback differently: the list endpoint interpolates
// the numeric id, the update endpoint interpolates the id as it arrived in the
// URL. They agree today only because both render "7"; a non-numeric id makes
// them diverge, and the update path accepts one.
func TestTheGeneratedNamesDisagreeBetweenCreateAndUpdate(t *testing.T) {
	fromUpdate := DeriveUpdateName(nil, "not-a-number")
	fromList := NewBackupJob(0, 0, "", "", "", "").DisplayName(Config{})

	if fromUpdate != "Backup Task not-a-number" {
		t.Fatalf("DeriveUpdateName = %q; the id is validated now, so assert that", fromUpdate)
	}
	if fromList != "Backup Task 0" {
		t.Fatalf("DisplayName = %q", fromList)
	}
}

// TestANonStringStoredNamePanics records the same unchecked assertion on the
// name.
func TestANonStringStoredNamePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("DeriveUpdateName survived a numeric name; the assertion appears " +
				"to be checked now, so assert the error instead")
		}
	}()
	DeriveUpdateName(map[string]interface{}{"name": 42}, "7")
}

func TestParseStoredConfig(t *testing.T) {
	for _, tt := range []struct {
		name     string
		in       string
		wantKeys int
	}{
		{"well formed", `{"name":"n","status":"enabled"}`, 2},
		{"empty object", `{}`, 0},
		{"malformed", `{"name":`, 0},
		{"empty string", ``, 0},
		{"wrong shape", `[1,2]`, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseStoredConfig(tt.in)
			if got == nil {
				t.Fatal("ParseStoredConfig returned nil; callers index it without checking")
			}
			if len(got) != tt.wantKeys {
				t.Errorf("ParseStoredConfig(%q) has %d keys, want %d", tt.in, len(got), tt.wantKeys)
			}
		})
	}
}

// TestAJSONNullDocumentYieldsANilMap records a defect this test suite found.
//
// ParseStoredConfig guards on the unmarshal error, but the four bytes `null`
// are valid JSON: Unmarshal succeeds and leaves the map nil. Reading a nil map
// is harmless, so this function's own callers survive — but the same guard is
// written out again in both SetEnable implementations, which then assign to the
// map and panic with "assignment to entry in nil map".
//
// A config_json column holding `null` therefore makes pause, resume, start and
// stop crash the request. Nothing writes that value today; a hand-edited row or
// a failed migration would.
func TestAJSONNullDocumentYieldsANilMap(t *testing.T) {
	got := ParseStoredConfig("null")

	if got != nil {
		t.Fatal("ParseStoredConfig(\"null\") no longer returns nil; the guard appears " +
			"to check the map as well as the error, so assert the empty map instead")
	}

	defer func() {
		if recover() == nil {
			t.Error("assigning to the returned map no longer panics")
		}
	}()
	got["status"] = "enabled"
}

// TestACorruptStoredConfigIsIndistinguishableFromAnEmptyOne records that
// ParseStoredConfig answers with an empty map both for a document that will not
// parse and for one that is genuinely empty. An update therefore silently
// discards a corrupt configuration and writes a fresh one, with no trace that
// anything was lost.
func TestACorruptStoredConfigIsIndistinguishableFromAnEmptyOne(t *testing.T) {
	corrupt := ParseStoredConfig(`{"name":`)
	empty := ParseStoredConfig(`{}`)

	if len(corrupt) != len(empty) {
		t.Fatal("a corrupt document is now distinguishable from an empty one; " +
			"assert how the difference is reported")
	}
	if got := DeriveUpdateStatus(corrupt, 0); got != StatusDisabled {
		t.Errorf("a corrupt document derives status %q, want %q", got, StatusDisabled)
	}
}
