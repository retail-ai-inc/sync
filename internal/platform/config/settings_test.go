package config

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

func useSettingsDB(t *testing.T) {
	t.Helper()
	t.Setenv("SYNC_DB_PATH", filepath.Join(t.TempDir(), "sync.db"))
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_ = db.Close()
}

func TestSettingsRoundTripThroughTheControlDatabase(t *testing.T) {
	useSettingsDB(t)

	want := Settings{
		VerifyInterval:          90 * time.Minute,
		VerifyRepair:            true,
		LagAlertSeconds:         45,
		MonitoringRetentionDays: 14,
		BatchMaxEvents:          321,
		BatchMaxBytes:           4 << 20,
		MongoNoTransaction:      true,
	}
	if err := SaveSettings(want); err != nil {
		t.Fatalf("SaveSettings: %v", err)
	}
	got, err := LoadSettings()
	if err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}
	if got != want {
		t.Errorf("read back %+v, want %+v", got, want)
	}
}

// A database that could not be read is reported, not defaulted. Off because
// nobody turned it on and off because the settings could not be read are
// different things, and only one of them is a decision.
func TestSettingsThatCannotBeReadAreReported(t *testing.T) {
	t.Setenv("SYNC_DB_PATH", filepath.Join(t.TempDir(), "missing", "sync.db"))
	// A directory where the file should be: openable by name, not by SQLite.
	t.Setenv("SYNC_DB_PATH", t.TempDir())

	if _, err := LoadSettings(); err == nil {
		t.Error("a control database that cannot be opened reported settings")
	}
}

// A variable that is set takes precedence, because that is how this is
// deployed today and an upgrade must not change behaviour without being asked.
func TestAVariableOverridesTheStoredSetting(t *testing.T) {
	t.Setenv("SYNC_VERIFY_INTERVAL", "300")
	if got := DurationSetting("SYNC_VERIFY_INTERVAL", time.Hour, nil); got != 5*time.Minute {
		t.Errorf("DurationSetting = %v, want the variable's 5m", got)
	}
}

func TestTheStoredSettingIsUsedWhenNoVariableIsSet(t *testing.T) {
	t.Setenv("SYNC_VERIFY_INTERVAL", "")
	if got := DurationSetting("SYNC_VERIFY_INTERVAL", time.Hour, nil); got != time.Hour {
		t.Errorf("DurationSetting = %v, want the stored hour", got)
	}
}

// A variable that is not a number is not a reason to change anything.
func TestAnUnreadableVariableFallsBackToTheSetting(t *testing.T) {
	t.Setenv("SYNC_VERIFY_INTERVAL", "later")
	if got := DurationSetting("SYNC_VERIFY_INTERVAL", time.Hour, nil); got != time.Hour {
		t.Errorf("DurationSetting = %v, want the stored hour", got)
	}
}

func TestOverriddenNamesTheVariableAndItsValue(t *testing.T) {
	t.Setenv("SYNC_VERIFY_REPAIR", "true")
	over := Overridden()
	if over["verifyRepair"] != "SYNC_VERIFY_REPAIR=true" {
		t.Errorf("Overridden reports %q", over["verifyRepair"])
	}
	if _, named := over["batchMaxEvents"]; named {
		t.Error("a setting with no variable was reported as overridden")
	}
}
