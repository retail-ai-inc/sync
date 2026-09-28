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
		VerifyInterval:         90 * time.Minute,
		VerifyRepair:           true,
		LagAlertSeconds:        45,
		BatchMaxEvents:         321,
		BatchMaxBytes:          4 << 20,
		MongoNoTransaction:     true,
		QueueMaxEvents:         2048,
		QueueMaxBytes:          64 << 20,
		SnapshotQueueMaxEvents: 512,
		FlushInterval:          250 * time.Millisecond,
		CopyBatchRows:          750,
		MongoStreamAwait:       150 * time.Millisecond,
		MongoWholeDocuments:    true,
		// The one that is on unless it is turned off, written here as off so
		// that the round trip is carrying a value and not a default.
		RecopyOnUnusablePosition: false,
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
