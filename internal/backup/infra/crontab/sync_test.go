package crontab

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// stubCrontab puts a fake crontab command on PATH that records its invocations
// and, for `crontab -l`, prints whatever existing lines the test supplied. The
// real crontab of whoever runs the suite is never touched.
func stubCrontab(t *testing.T, existing string, exitCode int) (binDir, logPath string) {
	t.Helper()

	binDir = t.TempDir()
	logPath = filepath.Join(binDir, "invocations")
	existingPath := filepath.Join(binDir, "existing")
	if err := os.WriteFile(existingPath, []byte(existing), 0o600); err != nil {
		t.Fatalf("write existing crontab: %v", err)
	}

	script := "#!/bin/sh\n" +
		"PATH=/usr/bin:/bin\n" +
		"echo \"$@\" >> " + logPath + "\n" +
		"if [ \"$1\" = \"-l\" ]; then cat " + existingPath + "; exit " + itoa(exitCode) + "; fi\n" +
		"cp \"$1\" " + filepath.Join(binDir, "installed") + "\n" +
		"exit 0\n"
	if err := os.WriteFile(filepath.Join(binDir, "crontab"), []byte(script), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}

	t.Setenv("PATH", binDir)
	return binDir, logPath
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	return string(rune('0' + n))
}

// useTempJobDB returns a handle on a throwaway SQLite file carrying the
// backup_tasks table. The CronManager is handed the handle directly, so
// SYNC_DB_PATH is irrelevant here.
func useTempJobDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "sync.db"))
	if err != nil {
		t.Fatalf("open temp sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec(`
CREATE TABLE backup_tasks (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    enable           INTEGER NOT NULL DEFAULT 1,
    last_update_time DATETIME,
    last_backup_time DATETIME,
    next_backup_time DATETIME,
    config_json      TEXT NOT NULL
);`); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	return db
}

func insertJob(t *testing.T, db *sql.DB, enable int, cfg string) {
	t.Helper()

	if _, err := db.Exec(
		`INSERT INTO backup_tasks (enable, last_update_time, last_backup_time, next_backup_time, config_json)
		 VALUES (?, '2026-08-21 00:00:00', '2026-08-20 18:00:00', '2026-08-22 18:00:00', ?)`,
		enable, cfg); err != nil {
		t.Fatalf("insert backup task: %v", err)
	}
}

// installed returns what the stub was asked to install.
func installed(t *testing.T, binDir string) string {
	t.Helper()

	b, err := os.ReadFile(filepath.Join(binDir, "installed"))
	if err != nil {
		t.Fatalf("the stub was never asked to install a crontab: %v", err)
	}
	return string(b)
}

func TestSyncCrontabWritesTheEnabledJobs(t *testing.T) {
	db := useTempJobDB(t)
	insertJob(t, db, 1, `{"name":"nightly","schedule":"0 3 * * *"}`)
	insertJob(t, db, 0, `{"name":"paused","schedule":"0 4 * * *"}`)
	binDir, _ := stubCrontab(t, "", 0)

	if err := NewCronManager(db, "http://127.0.0.1:8080/api").SyncCrontab(context.Background()); err != nil {
		t.Fatalf("SyncCrontab: %v", err)
	}

	got := installed(t, binDir)
	if !strings.Contains(got, "BEGIN SYNC BACKUP TASKS") || !strings.Contains(got, "END SYNC BACKUP TASKS") {
		t.Errorf("the markers are missing: %s", got)
	}
	if !strings.Contains(got, "0 3 * * * /usr/bin/curl") {
		t.Errorf("the enabled job is missing: %s", got)
	}
	if strings.Contains(got, "paused") {
		t.Errorf("a disabled job was written: %s", got)
	}
	if !strings.Contains(got, "http://127.0.0.1:8080/api/backup/execute/1") {
		t.Errorf("the execute URL is wrong: %s", got)
	}
}

// TestTheOldSectionIsReplacedNotAppended records that a previous run's block is
// stripped before the new one is written, so repeated syncs do not accumulate.
func TestTheOldSectionIsReplacedNotAppended(t *testing.T) {
	db := useTempJobDB(t)
	insertJob(t, db, 1, `{"name":"nightly","schedule":"0 3 * * *"}`)
	existing := "# BEGIN SYNC BACKUP TASKS - DO NOT EDIT THIS SECTION\n" +
		"0 9 * * * /usr/bin/curl -s -X POST http://old/backup/execute/99\n" +
		"# END SYNC BACKUP TASKS\n"
	binDir, _ := stubCrontab(t, existing, 0)

	if err := NewCronManager(db, "http://new/api").SyncCrontab(context.Background()); err != nil {
		t.Fatalf("SyncCrontab: %v", err)
	}

	got := installed(t, binDir)
	if strings.Contains(got, "execute/99") {
		t.Errorf("the old section survived: %s", got)
	}
	if strings.Count(got, "BEGIN SYNC BACKUP TASKS") != 1 {
		t.Errorf("%d begin markers, want 1: %s", strings.Count(got, "BEGIN SYNC BACKUP TASKS"), got)
	}
}

// TestUnrelatedCrontabLinesAreKept records that entries outside the managed
// section survive, which is what makes it safe to share the crontab.
func TestUnrelatedCrontabLinesAreKept(t *testing.T) {
	db := useTempJobDB(t)
	insertJob(t, db, 1, `{"name":"nightly","schedule":"0 3 * * *"}`)
	binDir, _ := stubCrontab(t, "0 1 * * * /usr/local/bin/somebody-elses-job\n", 0)

	if err := NewCronManager(db, "http://api").SyncCrontab(context.Background()); err != nil {
		t.Fatalf("SyncCrontab: %v", err)
	}

	if got := installed(t, binDir); !strings.Contains(got, "somebody-elses-job") {
		t.Errorf("an unrelated entry was dropped: %s", got)
	}
}

// TestBlankLinesAreDroppedFromTheExistingCrontab records that the rewrite is not
// faithful: every empty line outside the managed section is removed, so a
// crontab with grouping blank lines comes back reflowed.
func TestBlankLinesAreDroppedFromTheExistingCrontab(t *testing.T) {
	db := useTempJobDB(t)
	binDir, _ := stubCrontab(t, "0 1 * * * job-a\n\n\n0 2 * * * job-b\n", 0)

	if err := NewCronManager(db, "http://api").SyncCrontab(context.Background()); err != nil {
		t.Fatalf("SyncCrontab: %v", err)
	}

	got := installed(t, binDir)
	if strings.Contains(got, "job-a\n\n\n0 2") {
		t.Fatal("the blank lines survived; the rewrite appears to be faithful now")
	}
	if !strings.Contains(got, "job-a") || !strings.Contains(got, "job-b") {
		t.Errorf("an entry was lost: %s", got)
	}
}

// TestAMissingCrontabIsTreatedAsEmpty records that a failing `crontab -l` — which
// is what a user with no crontab gets — is answered with a warning and an empty
// starting point rather than an error. The managed section is installed anyway.
func TestAMissingCrontabIsTreatedAsEmpty(t *testing.T) {
	db := useTempJobDB(t)
	insertJob(t, db, 1, `{"name":"nightly","schedule":"0 3 * * *"}`)
	binDir, _ := stubCrontab(t, "", 1) // crontab -l exits non-zero

	if err := NewCronManager(db, "http://api").SyncCrontab(context.Background()); err != nil {
		t.Fatalf("SyncCrontab: %v", err)
	}

	if got := installed(t, binDir); !strings.Contains(got, "BEGIN SYNC BACKUP TASKS") {
		t.Errorf("nothing was installed: %s", got)
	}
}

func TestSyncCrontabOnNoEnabledJobsStillWritesTheMarkers(t *testing.T) {
	db := useTempJobDB(t)
	insertJob(t, db, 0, `{"name":"paused","schedule":"0 4 * * *"}`)
	binDir, _ := stubCrontab(t, "", 0)

	if err := NewCronManager(db, "http://api").SyncCrontab(context.Background()); err != nil {
		t.Fatalf("SyncCrontab: %v", err)
	}

	got := installed(t, binDir)
	if !strings.Contains(got, "BEGIN SYNC BACKUP TASKS") {
		t.Errorf("the markers are missing: %s", got)
	}
	if strings.Contains(got, "curl") {
		t.Errorf("an entry was written for no enabled jobs: %s", got)
	}
}

func TestSyncCrontabReportsAMissingTable(t *testing.T) {
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "empty.db"))
	if err != nil {
		t.Fatalf("open temp sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	stubCrontab(t, "", 0)

	if err := NewCronManager(db, "http://api").SyncCrontab(context.Background()); err == nil {
		t.Error("SyncCrontab on a database with no tables returned no error")
	}
}

// TestSyncCrontabReportsAFailingInstall records that a crontab command that
// exits non-zero is reported, unlike the read which is tolerated.
func TestSyncCrontabReportsAFailingInstall(t *testing.T) {
	db := useTempJobDB(t)
	insertJob(t, db, 1, `{"name":"nightly","schedule":"0 3 * * *"}`)

	binDir := t.TempDir()
	script := "#!/bin/sh\nif [ \"$1\" = \"-l\" ]; then exit 0; fi\necho 'crontab: bad file' >&2\nexit 1\n"
	if err := os.WriteFile(filepath.Join(binDir, "crontab"), []byte(script), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}
	t.Setenv("PATH", binDir)

	err := NewCronManager(db, "http://api").SyncCrontab(context.Background())
	if err == nil {
		t.Fatal("SyncCrontab returned no error for a failing install")
	}
	if !strings.Contains(err.Error(), "failed to update system crontab") {
		t.Errorf("error = %q", err)
	}
}

// TestNoCrontabBinaryIsReported records what happens on a machine with no
// crontab at all: the read is tolerated, the install fails, and the error names
// the install.
func TestNoCrontabBinaryIsReported(t *testing.T) {
	db := useTempJobDB(t)
	t.Setenv("PATH", t.TempDir())

	err := NewCronManager(db, "http://api").SyncCrontab(context.Background())
	if err == nil {
		t.Fatal("SyncCrontab returned no error with no crontab binary")
	}
	if !strings.Contains(err.Error(), "failed to update system crontab") {
		t.Errorf("error = %q", err)
	}
}

// TestACorruptTaskIsSkippedNotReported records that a backup task whose
// configuration will not parse is left out of the crontab with one error line.
// The job silently stops running, and the sync that dropped it reports success.
func TestACorruptTaskIsSkippedNotReported(t *testing.T) {
	db := useTempJobDB(t)
	insertJob(t, db, 1, `{"name":`)
	insertJob(t, db, 1, `{"name":"good","schedule":"0 3 * * *"}`)
	binDir, _ := stubCrontab(t, "", 0)

	if err := NewCronManager(db, "http://api").SyncCrontab(context.Background()); err != nil {
		t.Fatalf("SyncCrontab = %v; a corrupt task appears to be reported now, so "+
			"assert that instead", err)
	}

	got := installed(t, binDir)
	if strings.Count(got, "curl") != 1 {
		t.Errorf("%d entries were written, want the one good job: %s",
			strings.Count(got, "curl"), got)
	}
}

// TestAnEmptyScheduleProducesAMalformedLine records that no validation stands
// between a job with no schedule and the crontab: the entry is written with the
// schedule field empty, which makes the whole crontab file invalid and can stop
// every other entry in it from running.
func TestAnEmptyScheduleProducesAMalformedLine(t *testing.T) {
	db := useTempJobDB(t)
	insertJob(t, db, 1, `{"name":"noschedule"}`)
	binDir, _ := stubCrontab(t, "", 0)

	if err := NewCronManager(db, "http://api").SyncCrontab(context.Background()); err != nil {
		t.Fatalf("SyncCrontab: %v", err)
	}

	got := installed(t, binDir)
	if !strings.Contains(got, " /usr/bin/curl") {
		t.Fatalf("the malformed entry is gone; validation appears to have been added, "+
			"so assert the rejection instead: %s", got)
	}
}
