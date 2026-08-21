package backup

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestNewBackupExecutorKeepsItsHandle(t *testing.T) {
	if e := NewBackupExecutor(nil); e == nil {
		t.Fatal("NewBackupExecutor returned nil")
	} else if e.db != nil {
		t.Errorf("db = %v, want nil", e.db)
	}
}

func TestCopyFile(t *testing.T) {
	e := newExecutor()
	dir := t.TempDir()

	src := filepath.Join(dir, "src.json")
	want := bytes.Repeat([]byte("payload\n"), 20000) // larger than the 64KB buffer
	if err := os.WriteFile(src, want, 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}

	dst := filepath.Join(dir, "nested", "deeper", "dst.json")
	if err := e.copyFile(src, dst); err != nil {
		t.Fatalf("copyFile: %v", err)
	}

	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read destination: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("the copy is %d bytes, want %d", len(got), len(want))
	}
}

func TestCopyFileCreatesTheDestinationDirectory(t *testing.T) {
	e := newExecutor()
	dir := t.TempDir()

	src := filepath.Join(dir, "src")
	if err := os.WriteFile(src, []byte("x"), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}

	dst := filepath.Join(dir, "a", "b", "c", "dst")
	if err := e.copyFile(src, dst); err != nil {
		t.Fatalf("copyFile: %v", err)
	}
	if _, err := os.Stat(filepath.Dir(dst)); err != nil {
		t.Errorf("the destination directory was not created: %v", err)
	}
}

func TestCopyFileReportsAMissingSource(t *testing.T) {
	e := newExecutor()
	dir := t.TempDir()

	if err := e.copyFile(filepath.Join(dir, "absent"), filepath.Join(dir, "dst")); err == nil {
		t.Error("copyFile() = nil, want an error for a missing source")
	}
}

func TestCopyFileOverwritesTheDestination(t *testing.T) {
	e := newExecutor()
	dir := t.TempDir()

	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")
	if err := os.WriteFile(src, []byte("new"), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	if err := os.WriteFile(dst, []byte("this is the old and longer content"), 0o600); err != nil {
		t.Fatalf("write destination: %v", err)
	}

	if err := e.copyFile(src, dst); err != nil {
		t.Fatalf("copyFile: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read destination: %v", err)
	}
	if string(got) != "new" {
		t.Errorf("destination = %q, want %q — the old content was not truncated", got, "new")
	}
}

func TestWriteAndReadJSONFileRoundTrip(t *testing.T) {
	e := newExecutor()
	dir := t.TempDir()

	docs := []interface{}{
		map[string]interface{}{"_id": "1", "name": "alice"},
		map[string]interface{}{"_id": "2", "name": "bob"},
	}

	path := filepath.Join(dir, "orders_2026-08-20.json")
	if err := e.writeJSONFile(path, docs); err != nil {
		t.Fatalf("writeJSONFile: %v", err)
	}

	// One JSON object per line, JSONL.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if n := len(strings.Split(strings.TrimSpace(string(raw)), "\n")); n != 2 {
		t.Errorf("the file has %d lines, want 2 (content: %q)", n, raw)
	}

	got, err := e.readJSONFile(dir, "orders", "2026-08-20")
	if err != nil {
		t.Fatalf("readJSONFile: %v", err)
	}
	if !reflect.DeepEqual(got, docs) {
		t.Errorf("round trip = %#v, want %#v", got, docs)
	}
}

func TestWriteJSONFileOnAnEmptySlice(t *testing.T) {
	e := newExecutor()
	path := filepath.Join(t.TempDir(), "empty.json")

	if err := e.writeJSONFile(path, nil); err != nil {
		t.Fatalf("writeJSONFile: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() != 0 {
		t.Errorf("the file is %d bytes, want 0", info.Size())
	}
}

func TestWriteJSONFileRejectsAnUnmarshalableDocument(t *testing.T) {
	e := newExecutor()
	path := filepath.Join(t.TempDir(), "bad.json")

	err := e.writeJSONFile(path, []interface{}{make(chan int)})
	if err == nil {
		t.Fatal("writeJSONFile() = nil, want a marshal error")
	}
	if !strings.Contains(err.Error(), "marshal") {
		t.Errorf("err = %v, want it to mention marshalling", err)
	}
}

func TestWriteJSONFileReportsAnUncreatableTarget(t *testing.T) {
	e := newExecutor()

	err := e.writeJSONFile(filepath.Join(t.TempDir(), "absent-dir", "x.json"), nil)
	if err == nil {
		t.Fatal("writeJSONFile() = nil, want an error — the parent directory does not exist")
	}
}

func TestReadJSONFileReportsAMissingFile(t *testing.T) {
	e := newExecutor()

	_, err := e.readJSONFile(t.TempDir(), "orders", "2026-08-20")
	if err == nil {
		t.Fatal("readJSONFile() = nil, want an error for a missing file")
	}
	if !strings.Contains(err.Error(), "failed to read file") {
		t.Errorf("err = %v", err)
	}
}

func TestReadJSONFileSkipsBlankLines(t *testing.T) {
	e := newExecutor()
	dir := t.TempDir()

	body := "{\"a\":1}\n\n   \n{\"a\":2}\n"
	if err := os.WriteFile(filepath.Join(dir, "t_2026-08-20.json"), []byte(body), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	got, err := e.readJSONFile(dir, "t", "2026-08-20")
	if err != nil {
		t.Fatalf("readJSONFile: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("read %d documents, want 2", len(got))
	}
}

// A line that does not parse is logged at warn level and dropped, and the read
// still returns success. A truncated or partially corrupt mongoexport is
// therefore restored with documents silently missing — the caller has no way
// to tell how many were lost.
func TestReadJSONFileSilentlyDropsCorruptLines(t *testing.T) {
	e := newExecutor()
	dir := t.TempDir()

	body := "{\"id\":1}\n{\"id\":2,\n{\"id\":3}\nnot json at all\n{\"id\":5}\n"
	if err := os.WriteFile(filepath.Join(dir, "t_2026-08-20.json"), []byte(body), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	got, err := e.readJSONFile(dir, "t", "2026-08-20")
	if err != nil {
		t.Fatalf("readJSONFile() = %v — corrupt lines appear to be reported now; assert the error instead", err)
	}
	if len(got) != 3 {
		t.Fatalf("read %d documents from 5 lines, want 3 — the drop behaviour appears to have changed", len(got))
	}
}

func TestCountRecordsInFile(t *testing.T) {
	e := newExecutor()
	dir := t.TempDir()

	path := filepath.Join(dir, "t.json")
	body := "{\"id\":1}\n{\"id\":2}\n\n{\"id\":3}\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	count, sizeMB, err := e.countRecordsInFile(path)
	if err != nil {
		t.Fatalf("countRecordsInFile: %v", err)
	}
	if count != 3 {
		t.Errorf("count = %d, want 3", count)
	}
	if want := float64(len(body)) / 1024 / 1024; sizeMB != want {
		t.Errorf("sizeMB = %v, want %v", sizeMB, want)
	}
}

func TestCountRecordsInFileReportsAMissingFile(t *testing.T) {
	e := newExecutor()

	if _, _, err := e.countRecordsInFile(filepath.Join(t.TempDir(), "absent")); err == nil {
		t.Error("countRecordsInFile() = nil, want an error")
	}
}

// The count is "lines beginning with {", so anything else in the export is
// invisible: a pretty-printed document counts once for its opening brace and
// zero for its remaining lines, and a JSON array wrapper counts as zero
// records. The number is only correct for the JSONL that mongoexport emits by
// default.
func TestCountRecordsMiscountsNonJSONL(t *testing.T) {
	e := newExecutor()
	dir := t.TempDir()

	pretty := filepath.Join(dir, "pretty.json")
	if err := os.WriteFile(pretty, []byte("{\n  \"id\": 1\n}\n{\n  \"id\": 2\n}\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	count, _, err := e.countRecordsInFile(pretty)
	if err != nil {
		t.Fatalf("countRecordsInFile: %v", err)
	}
	if count != 2 {
		t.Fatalf("pretty-printed: count = %d — the counter appears to parse JSON now", count)
	}

	array := filepath.Join(dir, "array.json")
	if err := os.WriteFile(array, []byte("[\n{\"id\":1},\n{\"id\":2}\n]\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	count, _, err = e.countRecordsInFile(array)
	if err != nil {
		t.Fatalf("countRecordsInFile: %v", err)
	}
	if count != 2 {
		t.Fatalf("array-wrapped: count = %d, want 2 (the brace lines) — the counter appears to parse JSON now", count)
	}
}

// A JSONL line longer than the 1MB scanner buffer aborts the count with an
// error rather than being counted. MongoDB documents may be up to 16MB, so a
// single large document makes the record count for the whole file unavailable.
func TestCountRecordsFailsOnADocumentLargerThanOneMegabyte(t *testing.T) {
	e := newExecutor()
	path := filepath.Join(t.TempDir(), "big.json")

	big := fmt.Sprintf(`{"blob":"%s"}`, strings.Repeat("x", 2*1024*1024))
	if err := os.WriteFile(path, []byte(big+"\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	_, _, err := e.countRecordsInFile(path)
	if err == nil {
		t.Fatalf("countRecordsInFile() = nil — the buffer appears to have been raised; assert the count instead")
	}
	if !strings.Contains(err.Error(), "too long") {
		t.Errorf("err = %v, want a token-too-long error", err)
	}
}

func TestUseExternalCommandsHonoursTheEnvironmentVariable(t *testing.T) {
	e := newExecutor()

	t.Setenv("USE_EXTERNAL_BACKUP", "true")
	if !e.UseExternalCommands() {
		t.Error("UseExternalCommands() = false with USE_EXTERNAL_BACKUP=true")
	}

	t.Setenv("USE_EXTERNAL_BACKUP", "")
	if e.UseExternalCommands() {
		t.Error("UseExternalCommands() = true with the variable unset and low memory use")
	}
}

// The switch is an exact string comparison against "true", so the spellings
// operators normally use are silently ignored and the memory-hungry in-process
// path is taken instead.
func TestUseExternalCommandsRejectsOtherTruthySpellings(t *testing.T) {
	e := newExecutor()

	for _, value := range []string{"1", "TRUE", "True", "yes", "on", " true"} {
		t.Setenv("USE_EXTERNAL_BACKUP", value)
		if e.UseExternalCommands() {
			t.Fatalf("USE_EXTERNAL_BACKUP=%q is now accepted — the parsing appears to have been widened", value)
		}
	}
}
func TestGenerateCrontabEntriesWrapsTasksInMarkers(t *testing.T) {
	tasks := []BackupTask{
		{ID: 1, ConfigJSON: `{"schedule":"0 3 * * *","name":"nightly orders"}`},
		{ID: 2, ConfigJSON: `{"schedule":"*/15 * * * *","name":"frequent"}`},
	}

	got := generateCrontabEntries(tasks, "http://127.0.0.1:8080/api")

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
	got := generateCrontabEntries(nil, "http://127.0.0.1:8080/api")

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

	got := generateCrontabEntries(tasks, "http://api")
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

	got := generateCrontabEntries([]BackupTask{{ID: 9, ConfigJSON: string(cfg)}}, "http://api")

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

func TestNewCronManagerKeepsItsArguments(t *testing.T) {
	cm := NewCronManager(nil, "http://127.0.0.1:8080/api")

	if cm == nil {
		t.Fatal("NewCronManager returned nil")
	}
	if cm.apiServer != "http://127.0.0.1:8080/api" {
		t.Errorf("apiServer = %q", cm.apiServer)
	}
}

// executeCommand and executeCommandStreaming are deliberately untested: they
// have no callers anywhere in the repository (executeCommand only calls
// executeCommandStreaming, and nothing calls executeCommand), and the comment
// on executeCommand marks it deprecated. Exercising them is worse than
// pointless — their drain goroutines are never joined while cmd.Wait() runs,
// which os/exec documents as incorrect, so any test that reads the collected
// output is an unsynchronised read and fails the package under -race. See
// T-090 in docs/TEST_FINDINGS.md.
