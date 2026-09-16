package main

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/config"
)

func cfgWith(tasks ...config.SyncConfig) *config.Config {
	return &config.Config{SyncConfigs: tasks}
}

func baseTask() config.SyncConfig {
	return config.SyncConfig{
		ID:               1,
		Enable:           true,
		Type:             "mongodb",
		TaskName:         "tokyo-to-osaka",
		SourceConnection: "mongodb://tokyo:27017/source_db",
		TargetConnection: "mongodb://osaka:27017/target_db",
		Mappings: []config.DatabaseMapping{{
			Tables: []config.TableMapping{{SourceTable: "users", TargetTable: "users"}},
		}},
	}
}

// TestOneTasksFingerprintIsItsOwn is what makes a restart surgical.
func TestOneTasksFingerprintIsItsOwn(t *testing.T) {
	first, second := baseTask(), baseTask()
	second.ID = 2
	second.TaskName = "untouched"

	edited := second
	edited.Enable = false

	if fingerprint(second) == fingerprint(edited) {
		t.Fatal("the edit was not detected at all")
	}
	if fingerprint(first) != fingerprint(baseTask()) {
		t.Error("an untouched task's fingerprint changed because another was edited")
	}
}

func TestTheFingerprintNoticesAChange(t *testing.T) {
	tests := map[string]func(*config.SyncConfig){
		"enable flag":       func(sc *config.SyncConfig) { sc.Enable = false },
		"mapping":           func(sc *config.SyncConfig) { sc.Mappings[0].Tables[0].TargetTable = "users_copy" },
		"advanced settings": func(sc *config.SyncConfig) { sc.Mappings[0].Tables[0].AdvancedSettings.MaxRetries = 3 },
		"connection":        func(sc *config.SyncConfig) { sc.TargetConnection = "mongodb://kobe:27017/target_db" },
		"engine":            func(sc *config.SyncConfig) { sc.Type = "mysql" },
	}

	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			changed := baseTask()
			changed.Mappings = []config.DatabaseMapping{{
				Tables: []config.TableMapping{{SourceTable: "users", TargetTable: "users"}},
			}}
			change(&changed)

			if fingerprint(baseTask()) == fingerprint(changed) {
				t.Errorf("a changed %s was not detected", name)
			}
		})
	}
}

// TestTheFingerprintIsSensitiveToTimestamps records that the bookkeeping
// columns are part of it, so a write that only touches last_run_time restarts
// that task.
func TestTheFingerprintIsSensitiveToTimestamps(t *testing.T) {
	touched := baseTask()
	touched.LastRunTime = "2026-08-21 10:00:00"

	if fingerprint(baseTask()) == fingerprint(touched) {
		t.Error("timestamps are no longer part of the fingerprint, which would " +
			"remove a class of spurious restarts")
	}
}

// TestTheGlobalFingerprintNoticesTheSettingsThatUsedToBeIgnored is the fix for
// a reload that compared only the task list: changing the monitor interval, the
// Slack webhook or the log level took effect on the next process restart and
// not before.
func TestTheGlobalFingerprintNoticesTheSettingsThatUsedToBeIgnored(t *testing.T) {
	base := cfgWith(baseTask())

	tests := map[string]func(*config.Config){
		"monitoring switch": func(c *config.Config) { c.EnableTableRowCountMonitoring = true },
		"monitor interval":  func(c *config.Config) { c.MonitorInterval = time.Hour },
		"slack webhook":     func(c *config.Config) { c.SlackWebhookURL = "https://hooks.example.com/x" },
		"slack channel":     func(c *config.Config) { c.SlackChannel = "#alerts" },
		"log level":         func(c *config.Config) { c.LogLevel = "debug" },
	}

	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			changed := cfgWith(baseTask())
			change(changed)

			if globalFingerprint(base) == globalFingerprint(changed) {
				t.Errorf("a changed %s was not detected", name)
			}
		})
	}
}

// TestTheGlobalFingerprintIgnoresTheTasks is the other half of the split: a
// task edit must not restart the monitor.
func TestTheGlobalFingerprintIgnoresTheTasks(t *testing.T) {
	other := baseTask()
	other.ID = 9

	if globalFingerprint(cfgWith(baseTask())) != globalFingerprint(cfgWith(other)) {
		t.Error("a task edit changed the global fingerprint")
	}
}

// TestAControlDatabaseThatHadToBeCreatedIsNotReady covers the failure that
// looks exactly like a healthy first run.
//
// A volume that does not mount leaves SYNC_DB_PATH pointing at nothing. The
// file is created, the schema applied, every query answered -- and the process
// comes up with no tasks, no users, a /readyz of 200 and not one metric to say
// that Tokyo is no longer being replicated anywhere.
func TestAControlDatabaseThatHadToBeCreatedIsNotReady(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mounted", "sync.db")
	t.Setenv("SYNC_DB_PATH", path)
	t.Setenv("SYNC_DB_ALLOW_CREATE", "")

	err := controlPlaneReady()
	if err == nil {
		t.Fatal("a control database that had to be created reported ready")
	}
	for _, want := range []string{"did not mount", "SYNC_DB_ALLOW_CREATE"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %v, want it to carry %q", err, want)
		}
	}

	// And it stays unready: the process came up on an empty database, and
	// having since written to it does not make that right.
	if err := controlPlaneReady(); err == nil {
		t.Error("the second look reported ready, so a restart is not needed to clear it")
	}
}

// And a genuine first run says so deliberately.
func TestAFirstRunCanBeDeclared(t *testing.T) {
	t.Setenv("SYNC_DB_PATH", filepath.Join(t.TempDir(), "sync.db"))
	t.Setenv("SYNC_DB_ALLOW_CREATE", "1")

	if err := controlPlaneReady(); err != nil {
		t.Errorf("a declared first run reported not ready: %v", err)
	}
}
