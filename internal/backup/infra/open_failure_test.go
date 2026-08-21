package infra

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/retail-ai-inc/sync/internal/backup/domain"
)

// unopenableDB points SYNC_DB_PATH at a path whose parent is a regular file, so
// the directory creation inside OpenSQLiteDB fails immediately with ENOTDIR.
// Every store call has to answer for that, and each tags it with the same stage.
func unopenableDB(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	blocker := filepath.Join(dir, "not-a-directory")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatalf("write blocker: %v", err)
	}
	t.Setenv("SYNC_DB_PATH", filepath.Join(blocker, "sub", "sync.db"))
}

func TestEveryStoreCallReportsAnUnopenableDatabase(t *testing.T) {
	for _, tt := range []struct {
		name string
		call func() error
	}{
		{"ListJobs", func() error { _, err := ListJobs(); return err }},
		{"InsertJob", func() error { _, err := InsertJob(1, "now", "next", domain.Config{}); return err }},
		{"ReadJobRow", func() error { _, _, err := ReadJobRow("1"); return err }},
		{"UpdateJob", func() error { return UpdateJob("1", "now", "next", domain.Config{}) }},
		{"DeleteJob", func() error { return DeleteJob("1") }},
		{"JobExists", func() error { _, err := JobExists("1"); return err }},
		{"StampLastBackup", func() error { return StampLastBackup("1", "now") }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			unopenableDB(t)

			err := tt.call()
			if err == nil {
				t.Fatalf("%s returned no error for an unopenable database", tt.name)
			}
			if got := stageOf(err); got != StageOpen {
				t.Errorf("stage = %q, want %q (err = %v)", got, StageOpen, err)
			}
		})
	}
}

// TestSetEnableDoesNotTagAnUnopenableDatabase records the odd one out: SetEnable
// returns the driver's error untouched, so the endpoint cannot tell a database
// it could not open from a row it could not find.
func TestSetEnableDoesNotTagAnUnopenableDatabase(t *testing.T) {
	unopenableDB(t)

	err := SetEnable("1", true, "now")
	if err == nil {
		t.Fatal("SetEnable returned no error for an unopenable database")
	}
	if got := stageOf(err); got != "" {
		t.Fatalf("stage = %q; the call tags its failures now, so assert the stage", got)
	}
}

// TestListJobsReportsAScanFailure covers the branch a badly typed column takes.
// The enable column is declared INTEGER, but SQLite stores whatever it is given,
// so a row holding text fails the scan rather than the query.
func TestListJobsReportsAScanFailure(t *testing.T) {
	db := useTempJobDB(t)
	if _, err := db.Exec(
		`INSERT INTO backup_tasks (enable, last_update_time, last_backup_time, next_backup_time, config_json)
		 VALUES ('not a number', '', '', '', '{}')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	_, err := ListJobs()
	if err == nil {
		t.Fatal("ListJobs accepted a text enable column; SQLite appears to enforce " +
			"the declared type now, so assert the rejection at insert time instead")
	}
	if got := stageOf(err); got != StageScan {
		t.Errorf("stage = %q, want %q (err = %v)", got, StageScan, err)
	}
}
