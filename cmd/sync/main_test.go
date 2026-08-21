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

func TestConfigsEqual(t *testing.T) {
	t.Run("identical configurations", func(t *testing.T) {
		if !configsEqual(cfgWith(baseTask()), cfgWith(baseTask())) {
			t.Error("two identical configurations compared unequal")
		}
	})

	t.Run("both empty", func(t *testing.T) {
		if !configsEqual(cfgWith(), cfgWith()) {
			t.Error("two empty configurations compared unequal")
		}
	})

	t.Run("different task count", func(t *testing.T) {
		if configsEqual(cfgWith(baseTask()), cfgWith(baseTask(), baseTask())) {
			t.Error("configurations with different task counts compared equal")
		}
	})

	t.Run("changed enable flag", func(t *testing.T) {
		disabled := baseTask()
		disabled.Enable = false
		if configsEqual(cfgWith(baseTask()), cfgWith(disabled)) {
			t.Error("a flipped enable flag was not detected")
		}
	})

	t.Run("changed mapping", func(t *testing.T) {
		remapped := baseTask()
		remapped.Mappings[0].Tables[0].TargetTable = "users_copy"
		if configsEqual(cfgWith(baseTask()), cfgWith(remapped)) {
			t.Error("a changed table mapping was not detected")
		}
	})

	t.Run("changed advanced settings", func(t *testing.T) {
		tuned := baseTask()
		tuned.Mappings[0].Tables[0].AdvancedSettings.MaxRetries = 3
		if configsEqual(cfgWith(baseTask()), cfgWith(tuned)) {
			t.Error("a changed advanced setting was not detected")
		}
	})

	t.Run("global fields outside SyncConfigs are ignored", func(t *testing.T) {
		// Only SyncConfigs is compared, so a changed log level or monitor
		// interval does not restart the sync tasks.
		a := cfgWith(baseTask())
		b := cfgWith(baseTask())
		a.LogLevel, b.LogLevel = "debug", "error"
		a.MonitorInterval, b.MonitorInterval = time.Minute, time.Hour
		if !configsEqual(a, b) {
			t.Error("a changed global field triggered a restart")
		}
	})
}

// TestConfigsEqualIsSensitiveToTimestamps records that the comparison includes
// LastUpdateTime and LastRunTime, which are bookkeeping columns rather than
// configuration. Any write that touches them makes the ten-second reload loop
// consider the configuration changed and tear down every running sync task,
// not just the one that was edited.
func TestConfigsEqualIsSensitiveToTimestamps(t *testing.T) {
	touched := baseTask()
	touched.LastRunTime = "2026-08-21 10:00:00"

	if configsEqual(cfgWith(baseTask()), cfgWith(touched)) {
		t.Error("timestamps are no longer part of the comparison, which would " +
			"remove a class of spurious restarts")
	}
}

// TestConfigsEqualRestartsEveryTaskOnOneEdit records the blast radius of the
// reconciliation loop. The comparison is over the whole slice, so editing one
// task returns false and cmd/sync cancels the context shared by all of them.
func TestConfigsEqualRestartsEveryTaskOnOneEdit(t *testing.T) {
	first, second := baseTask(), baseTask()
	second.ID = 2
	second.TaskName = "untouched"

	edited := second
	before := cfgWith(first, second)
	after := cfgWith(first, func() config.SyncConfig { e := edited; e.Enable = false; return e }())

	if configsEqual(before, after) {
		t.Fatal("the edit was not detected at all")
	}
	// There is no per-task comparison to fall back on: the caller only learns
	// that something, somewhere, differs.
	if configsEqual(cfgWith(first), cfgWith(first)) != true {
		t.Error("the untouched task does not compare equal on its own")
	}
}

func TestConfigsEqualHandlesNilSlices(t *testing.T) {
	// A nil slice and an empty one both marshal to "null" and "[]" respectively,
	// so they are not interchangeable.
	nilCfg := &config.Config{SyncConfigs: nil}
	emptyCfg := &config.Config{SyncConfigs: []config.SyncConfig{}}

	if configsEqual(nilCfg, emptyCfg) {
		t.Error("a nil slice and an empty slice compared equal; if that was " +
			"intended, the comparison would need to normalise them")
	}
	if !configsEqual(nilCfg, nilCfg) {
		t.Error("a nil configuration is not equal to itself")
	}
}
