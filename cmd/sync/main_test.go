package main

import (
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
