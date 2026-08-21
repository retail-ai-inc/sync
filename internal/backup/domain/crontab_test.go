package domain

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestGenerateCrontabEntriesWrapsTasksInMarkers(t *testing.T) {
	tasks := []BackupTask{
		{ID: 1, ConfigJSON: `{"schedule":"0 3 * * *","name":"nightly orders"}`},
		{ID: 2, ConfigJSON: `{"schedule":"*/15 * * * *","name":"frequent"}`},
	}

	got := GenerateCrontabEntries(tasks, "http://127.0.0.1:8080/api")

	if got[0] != "# BEGIN SYNC BACKUP TASKS - DO NOT EDIT THIS SECTION" {
		t.Errorf("first line = %q", got[0])
	}
	if got[len(got)-1] != "# END SYNC BACKUP TASKS" {
		t.Errorf("last line = %q", got[len(got)-1])
	}

	joined := strings.Join(got, "\n")
	for _, want := range []string{
		"# Backup task: nightly orders (ID: 1)",
		"0 3 * * * /usr/bin/curl -s -X POST http://127.0.0.1:8080/api/backup/execute/1 > /dev/null 2>&1",
		"# Backup task: frequent (ID: 2)",
		"*/15 * * * * /usr/bin/curl -s -X POST http://127.0.0.1:8080/api/backup/execute/2 > /dev/null 2>&1",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("the output does not contain %q\ngot:\n%s", want, joined)
		}
	}
}

func TestGenerateCrontabEntriesOnNoTasks(t *testing.T) {
	got := GenerateCrontabEntries(nil, "http://127.0.0.1:8080/api")

	if len(got) != 2 {
		t.Errorf("got %d lines, want just the two markers: %#v", len(got), got)
	}
}

// A task whose config_json does not parse is logged and skipped, so it simply
// never appears in the crontab. The backup stops running with no entry, no
// alert, and nothing in the API to indicate the schedule was dropped.
func TestAnUnparseableTaskIsSilentlyLeftOutOfTheCrontab(t *testing.T) {
	tasks := []BackupTask{
		{ID: 1, ConfigJSON: `{not json`},
		{ID: 2, ConfigJSON: `{"schedule":"0 3 * * *","name":"kept"}`},
	}

	got := GenerateCrontabEntries(tasks, "http://api")
	joined := strings.Join(got, "\n")

	if strings.Contains(joined, "execute/1") {
		t.Fatalf("task 1 is now scheduled — the corrupt config appears to be handled; assert the new behaviour instead\n%s", joined)
	}
	if !strings.Contains(joined, "execute/2") {
		t.Errorf("task 2 was dropped along with the corrupt one:\n%s", joined)
	}
}

// A task with an empty schedule still produces a crontab line, which begins
// with the curl command instead of five time fields. crontab rejects the whole
// file when it hits that line, so one misconfigured task drops every backup
// schedule on the host.
func TestAnEmptyScheduleProducesAMalformedCrontabLine(t *testing.T) {
	cfg, err := json.Marshal(BackupConfig{Name: "no schedule"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	got := GenerateCrontabEntries([]BackupTask{{ID: 9, ConfigJSON: string(cfg)}}, "http://api")

	var entry string
	for _, line := range got {
		if strings.Contains(line, "execute/9") {
			entry = line
		}
	}
	if entry == "" {
		t.Fatalf("no entry was generated — an empty schedule appears to be rejected now:\n%s", strings.Join(got, "\n"))
	}
	if !strings.HasPrefix(entry, " /usr/bin/curl") {
		t.Fatalf("entry = %q — the empty schedule appears to be handled now", entry)
	}
}
