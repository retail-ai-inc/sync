package backup

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// stubBin installs an executable stub on PATH under the given name. The stub
// appends its arguments to <dir>/<name>.args, runs the supplied shell body, and
// exits with the given status. Returns the directory so callers can read the
// recorded arguments.
func stubBin(t *testing.T, dir, name, body string, exitCode int) {
	t.Helper()

	// The outer PATH is restricted to the stub directory so exec.CommandContext
	// cannot reach a real mysqldump, zip or gsutil. The stub therefore needs its
	// own PATH to find coreutils.
	script := fmt.Sprintf(`#!/bin/sh
PATH=/usr/bin:/bin:/usr/local/bin
printf '%%s\n' "$@" >> %q
%s
exit %d
`, filepath.Join(dir, name+".args"), body, exitCode)

	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write stub %s: %v", name, err)
	}
}

// stubPATH points PATH at a fresh directory holding only the stubs a test
// installs, so nothing can reach a real mysqldump, zip or gsutil.
func stubPATH(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	t.Setenv("PATH", dir)
	return dir
}

// stubArgs returns the arguments a stub recorded, one per line.
func stubArgs(t *testing.T, dir, name string) []string {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(dir, name+".args"))
	if err != nil {
		t.Fatalf("stub %s was never invoked: %v", name, err)
	}
	return strings.Split(strings.TrimSpace(string(data)), "\n")
}

func stubWasNotInvoked(t *testing.T, dir, name string) {
	t.Helper()

	if _, err := os.Stat(filepath.Join(dir, name+".args")); err == nil {
		t.Errorf("stub %s was invoked but should not have been", name)
	}
}

// mysqlBackupConfig builds a config for the external MySQL path.
func mysqlBackupConfig(format, compression, gcsPath string) ExecutorBackupConfig {
	var cfg ExecutorBackupConfig
	cfg.SourceType = "mysql"
	cfg.Format = format
	cfg.CompressionType = compression
	cfg.Destination.GCSPath = gcsPath
	cfg.Database.Username = "svc"
	cfg.Database.Password = "hunter2"
	return cfg
}

// ------------------------------------------------------- mysqldump wiring

func TestExecuteExternalMySQLDumpBuildsItsArguments(t *testing.T) {
	binDir := stubPATH(t)
	stubBin(t, binDir, "mysqldump", "echo '-- dump'", 0)

	e := newExecutor()
	tempDir := t.TempDir()
	outputPath := filepath.Join(tempDir, "orders.sql")

	cfg := mysqlBackupConfig("sql", "", "gs://bucket/path")
	if err := e.executeExternalMySQLDump(context.Background(),
		"tokyo", "3306", "svc", "hunter2", "app", "orders", outputPath, cfg); err != nil {
		t.Fatalf("executeExternalMySQLDump: %v", err)
	}

	args := stubArgs(t, binDir, "mysqldump")
	joined := strings.Join(args, " ")
	for _, want := range []string{
		"--default-character-set=utf8mb4",
		"-h", "tokyo",
		"-P", "3306",
		"-u", "svc",
		"-phunter2",
		"--single-transaction",
		"--skip-lock-tables",
		"--no-tablespaces",
		"app", "orders",
	} {
		if !containsArg(args, want) {
			t.Errorf("argument %q was not passed (got: %s)", want, joined)
		}
	}

	// The dump is written to the output file, not swallowed.
	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if !strings.Contains(string(data), "-- dump") {
		t.Errorf("output = %q, want the stub's stdout", data)
	}
}

// The password is passed as -p<value> on the command line, so it is visible in
// the process list to every user on the host for the duration of the dump.
// maskMySQLPassword only masks the log line, not the argv.
func TestTheMySQLPasswordIsPassedOnTheCommandLine(t *testing.T) {
	binDir := stubPATH(t)
	stubBin(t, binDir, "mysqldump", "", 0)

	e := newExecutor()
	outputPath := filepath.Join(t.TempDir(), "orders.sql")

	if err := e.executeExternalMySQLDump(context.Background(),
		"h", "3306", "svc", "hunter2", "app", "orders", outputPath,
		mysqlBackupConfig("sql", "", "gs://b")); err != nil {
		t.Fatalf("executeExternalMySQLDump: %v", err)
	}

	if !containsArg(stubArgs(t, binDir, "mysqldump"), "-phunter2") {
		t.Fatalf("the password is no longer passed in argv — it appears to have moved to a file or the environment; assert the new mechanism instead")
	}
}

func TestExecuteExternalMySQLDumpOmitsAnEmptyPassword(t *testing.T) {
	binDir := stubPATH(t)
	stubBin(t, binDir, "mysqldump", "", 0)

	e := newExecutor()
	outputPath := filepath.Join(t.TempDir(), "orders.sql")

	if err := e.executeExternalMySQLDump(context.Background(),
		"h", "3306", "svc", "", "app", "orders", outputPath,
		mysqlBackupConfig("sql", "", "gs://b")); err != nil {
		t.Fatalf("executeExternalMySQLDump: %v", err)
	}

	for _, a := range stubArgs(t, binDir, "mysqldump") {
		if strings.HasPrefix(a, "-p") {
			t.Errorf("an empty password produced %q", a)
		}
	}
}

func TestExecuteExternalMySQLDumpAppliesAWhereClause(t *testing.T) {
	binDir := stubPATH(t)
	stubBin(t, binDir, "mysqldump", "", 0)

	e := newExecutor()
	outputPath := filepath.Join(t.TempDir(), "orders.sql")

	cfg := mysqlBackupConfig("sql", "", "gs://b")
	cfg.Query = map[string]map[string]interface{}{
		"orders": {"created_at": map[string]interface{}{
			// The converter only understands this shape; the offsets must be
			// float64, as they are after a JSON round trip.
			"type": "daily", "startOffset": float64(-1), "endOffset": float64(0),
		}},
	}

	if err := e.executeExternalMySQLDump(context.Background(),
		"h", "3306", "svc", "pw", "app", "orders", outputPath, cfg); err != nil {
		t.Fatalf("executeExternalMySQLDump: %v", err)
	}

	args := stubArgs(t, binDir, "mysqldump")
	idx := indexOfArg(args, "--where")
	if idx < 0 {
		t.Fatalf("--where was not passed: %v", args)
	}
	if idx+1 >= len(args) {
		t.Fatal("--where has no value")
	}
	where := args[idx+1]
	if !strings.Contains(where, "created_at") {
		t.Errorf("--where = %q, want it to mention created_at", where)
	}
}

func TestExecuteExternalMySQLDumpReportsAFailingCommand(t *testing.T) {
	binDir := stubPATH(t)
	stubBin(t, binDir, "mysqldump", "echo 'access denied' >&2", 1)

	e := newExecutor()
	outputPath := filepath.Join(t.TempDir(), "orders.sql")

	err := e.executeExternalMySQLDump(context.Background(),
		"h", "3306", "svc", "pw", "app", "orders", outputPath,
		mysqlBackupConfig("sql", "", "gs://b"))

	if err == nil {
		t.Fatal("executeExternalMySQLDump() = nil, want the non-zero exit")
	}
	if !strings.Contains(err.Error(), "mysqldump failed") {
		t.Errorf("err = %v", err)
	}
}

func TestExecuteExternalMySQLDumpReportsAnUncreatableOutput(t *testing.T) {
	binDir := stubPATH(t)
	stubBin(t, binDir, "mysqldump", "", 0)

	e := newExecutor()
	err := e.executeExternalMySQLDump(context.Background(),
		"h", "3306", "svc", "pw", "app", "orders",
		filepath.Join(t.TempDir(), "absent-dir", "orders.sql"),
		mysqlBackupConfig("sql", "", "gs://b"))

	if err == nil {
		t.Fatal("executeExternalMySQLDump() = nil, want a file-creation error")
	}
	if !strings.Contains(err.Error(), "failed to create output file") {
		t.Errorf("err = %v", err)
	}
	stubWasNotInvoked(t, binDir, "mysqldump")
}

// ------------------------------------------------------------- zip wiring

func TestExecuteExternalZipBuildsItsArguments(t *testing.T) {
	binDir := stubPATH(t)
	workDir := t.TempDir()
	input := filepath.Join(workDir, "orders.sql")
	output := filepath.Join(workDir, "orders.zip")

	if err := os.WriteFile(input, []byte("payload"), 0o600); err != nil {
		t.Fatalf("write input: %v", err)
	}
	// The stub must produce the output file, which the caller then stats.
	stubBin(t, binDir, "zip", "touch "+output, 0)

	e := newExecutor()
	if err := e.executeExternalZip(context.Background(), workDir, input, output); err != nil {
		t.Fatalf("executeExternalZip: %v", err)
	}

	args := stubArgs(t, binDir, "zip")
	if !containsArg(args, "-j") || !containsArg(args, output) || !containsArg(args, input) {
		t.Errorf("args = %v, want -j plus both paths", args)
	}
}

func TestExecuteExternalZipReportsAFailingCommand(t *testing.T) {
	binDir := stubPATH(t)
	stubBin(t, binDir, "zip", "echo 'disk full' >&2", 1)

	e := newExecutor()
	workDir := t.TempDir()
	err := e.executeExternalZip(context.Background(), workDir,
		filepath.Join(workDir, "in.sql"), filepath.Join(workDir, "out.zip"))

	if err == nil {
		t.Fatal("executeExternalZip() = nil, want the non-zero exit")
	}
	if !strings.Contains(err.Error(), "zip failed") || !strings.Contains(err.Error(), "disk full") {
		t.Errorf("err = %v, want it to carry the command output", err)
	}
}

// A zip command that exits 0 without producing the archive is caught by the
// stat that follows, so a silently broken compression step does not pass as
// success.
func TestExecuteExternalZipRejectsAMissingArchive(t *testing.T) {
	binDir := stubPATH(t)
	stubBin(t, binDir, "zip", "", 0) // exits 0, writes nothing

	e := newExecutor()
	workDir := t.TempDir()
	err := e.executeExternalZip(context.Background(), workDir,
		filepath.Join(workDir, "in.sql"), filepath.Join(workDir, "out.zip"))

	if err == nil {
		t.Fatal("executeExternalZip() = nil despite no archive being produced")
	}
	if !strings.Contains(err.Error(), "zip output file not created") {
		t.Errorf("err = %v", err)
	}
}

// ------------------------------------------------------------ gsutil wiring

func TestExecuteExternalGCSUploadBuildsItsArguments(t *testing.T) {
	binDir := stubPATH(t)
	stubBin(t, binDir, "gsutil", "", 0)

	e := newExecutor()
	if err := e.executeExternalGCSUpload(context.Background(),
		"/tmp/orders.zip", "gs://bucket/path/orders.zip"); err != nil {
		t.Fatalf("executeExternalGCSUpload: %v", err)
	}

	args := stubArgs(t, binDir, "gsutil")
	if len(args) != 3 || args[0] != "cp" || args[1] != "/tmp/orders.zip" || args[2] != "gs://bucket/path/orders.zip" {
		t.Errorf("args = %v, want [cp <local> <remote>]", args)
	}
}

func TestExecuteExternalGCSUploadReportsAFailingCommand(t *testing.T) {
	binDir := stubPATH(t)
	stubBin(t, binDir, "gsutil", "echo 'AccessDeniedException: 403' >&2", 1)

	e := newExecutor()
	err := e.executeExternalGCSUpload(context.Background(), "/tmp/x.zip", "gs://b/x.zip")

	if err == nil {
		t.Fatal("executeExternalGCSUpload() = nil, want the non-zero exit")
	}
	if !strings.Contains(err.Error(), "gsutil upload failed") || !strings.Contains(err.Error(), "403") {
		t.Errorf("err = %v, want it to carry the command output", err)
	}
}

// The upload does not verify that the object landed: gsutil's exit status is
// the only signal, and nothing reads back the object's size or checksum. A
// truncated or empty archive uploads as a success.
func TestTheUploadIsNotVerified(t *testing.T) {
	binDir := stubPATH(t)
	// Exits 0 without doing anything at all.
	stubBin(t, binDir, "gsutil", "", 0)

	e := newExecutor()
	if err := e.executeExternalGCSUpload(context.Background(),
		"/nonexistent/path/orders.zip", "gs://bucket/orders.zip"); err != nil {
		t.Fatalf("executeExternalGCSUpload() = %v — the upload appears to be verified now; assert the verification instead", err)
	}
}

// ------------------------------------------------- the full MySQL workflow

func TestTheMySQLBackupWorkflowRunsAllThreeSteps(t *testing.T) {
	binDir := stubPATH(t)
	tempDir := t.TempDir()

	stubBin(t, binDir, "mysqldump", "echo '-- dump'", 0)
	// zip must produce its output for the stat check to pass.
	stubBin(t, binDir, "zip", `for a in "$@"; do case "$a" in *.zip) touch "$a";; esac; done`, 0)
	stubBin(t, binDir, "gsutil", "", 0)

	e := newExecutor()
	cfg := mysqlBackupConfig("sql", "", "gs://bucket/backups")

	if err := e.executeExternalMySQLBackupSimple(context.Background(),
		"mysql://svc:hunter2@tokyo:3306/app", "app", "orders", tempDir, cfg); err != nil {
		t.Fatalf("executeExternalMySQLBackupSimple: %v", err)
	}

	stubArgs(t, binDir, "mysqldump")
	stubArgs(t, binDir, "zip")
	gsutil := stubArgs(t, binDir, "gsutil")
	if !strings.HasPrefix(gsutil[2], "gs://bucket/backups/") {
		t.Errorf("upload target = %q, want it under the configured GCS path", gsutil[2])
	}
	if !strings.HasSuffix(gsutil[2], ".zip") {
		t.Errorf("upload target = %q, want a .zip", gsutil[2])
	}

	// Both intermediate files are removed afterwards.
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("read temp dir: %v", err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".sql") || strings.HasSuffix(entry.Name(), ".zip") {
			t.Errorf("temporary file %q was left behind", entry.Name())
		}
	}
}

func TestTheMySQLBackupWorkflowSkipsCompressionWhenDisabled(t *testing.T) {
	binDir := stubPATH(t)
	tempDir := t.TempDir()

	stubBin(t, binDir, "mysqldump", "echo '-- dump'", 0)
	stubBin(t, binDir, "zip", "", 0)
	stubBin(t, binDir, "gsutil", "", 0)

	e := newExecutor()
	cfg := mysqlBackupConfig("sql", "none", "gs://bucket/backups")

	if err := e.executeExternalMySQLBackupSimple(context.Background(),
		"mysql://svc:pw@tokyo:3306/app", "app", "orders", tempDir, cfg); err != nil {
		t.Fatalf("executeExternalMySQLBackupSimple: %v", err)
	}

	stubWasNotInvoked(t, binDir, "zip")

	gsutil := stubArgs(t, binDir, "gsutil")
	if !strings.HasSuffix(gsutil[2], ".sql") {
		t.Errorf("upload target = %q, want the uncompressed .sql", gsutil[2])
	}
}

func TestTheMySQLBackupWorkflowRejectsAnUnsupportedFormat(t *testing.T) {
	stubPATH(t)

	e := newExecutor()
	err := e.executeExternalMySQLBackupSimple(context.Background(),
		"mysql://svc:pw@h:3306/app", "app", "orders", t.TempDir(),
		mysqlBackupConfig("parquet", "", "gs://b"))

	if err == nil {
		t.Fatal("an unsupported format was accepted")
	}
	if !strings.Contains(err.Error(), "unsupported format") {
		t.Errorf("err = %v", err)
	}
}

func TestTheMySQLBackupWorkflowStopsWhenTheDumpFails(t *testing.T) {
	binDir := stubPATH(t)
	stubBin(t, binDir, "mysqldump", "echo 'denied' >&2", 1)
	stubBin(t, binDir, "zip", "", 0)
	stubBin(t, binDir, "gsutil", "", 0)

	e := newExecutor()
	err := e.executeExternalMySQLBackupSimple(context.Background(),
		"mysql://svc:pw@h:3306/app", "app", "orders", t.TempDir(),
		mysqlBackupConfig("sql", "", "gs://b"))

	if err == nil {
		t.Fatal("the workflow reported success despite a failing dump")
	}
	if !strings.Contains(err.Error(), "external MySQL export failed") {
		t.Errorf("err = %v", err)
	}
	stubWasNotInvoked(t, binDir, "zip")
	stubWasNotInvoked(t, binDir, "gsutil")
}

func TestTheMySQLBackupWorkflowStopsWhenTheUploadFails(t *testing.T) {
	binDir := stubPATH(t)
	tempDir := t.TempDir()

	stubBin(t, binDir, "mysqldump", "echo '-- dump'", 0)
	stubBin(t, binDir, "zip", `for a in "$@"; do case "$a" in *.zip) touch "$a";; esac; done`, 0)
	stubBin(t, binDir, "gsutil", "echo '403' >&2", 1)

	e := newExecutor()
	err := e.executeExternalMySQLBackupSimple(context.Background(),
		"mysql://svc:pw@h:3306/app", "app", "orders", tempDir,
		mysqlBackupConfig("sql", "", "gs://b"))

	if err == nil {
		t.Fatal("the workflow reported success despite a failing upload")
	}
	if !strings.Contains(err.Error(), "external GCS upload failed") {
		t.Errorf("err = %v", err)
	}
}

// When the upload fails the intermediate .sql and .zip are left in the
// temporary directory: the cleanup only runs on the success path. A backup
// target that is unreachable for a while therefore fills the disk one dump at
// a time.
func TestAFailedUploadLeavesTheIntermediateFilesBehind(t *testing.T) {
	binDir := stubPATH(t)
	tempDir := t.TempDir()

	stubBin(t, binDir, "mysqldump", "echo '-- dump'", 0)
	stubBin(t, binDir, "zip", `for a in "$@"; do case "$a" in *.zip) touch "$a";; esac; done`, 0)
	stubBin(t, binDir, "gsutil", "echo '403' >&2", 1)

	e := newExecutor()
	_ = e.executeExternalMySQLBackupSimple(context.Background(),
		"mysql://svc:pw@h:3306/app", "app", "orders", tempDir,
		mysqlBackupConfig("sql", "", "gs://b"))

	entries, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("read temp dir: %v", err)
	}
	var left []string
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".sql") || strings.HasSuffix(entry.Name(), ".zip") {
			left = append(left, entry.Name())
		}
	}
	if len(left) == 0 {
		t.Fatalf("nothing was left behind — the cleanup appears to run on the failure path now; assert the cleanup instead")
	}
	t.Logf("left behind after a failed upload: %v", left)
}

func containsArg(args []string, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}

func indexOfArg(args []string, want string) int {
	for i, a := range args {
		if a == want {
			return i
		}
	}
	return -1
}

// ------------------------------------------------ mongoexport workflow

func mongoBackupConfig(gcsPath string) ExecutorBackupConfig {
	var cfg ExecutorBackupConfig
	cfg.SourceType = "mongodb"
	cfg.Destination.GCSPath = gcsPath
	return cfg
}

// zipStubBody produces the .zip argument it is handed, which the caller stats.
const zipStubBody = `for a in "$@"; do case "$a" in *.zip) touch "$a";; esac; done`

// mongoexportStubBody produces the --out file, which mongoexport writes itself
// (unlike mysqldump, whose stdout the caller redirects).
const mongoexportStubBody = `for a in "$@"; do case "$a" in *.json) touch "$a";; esac; done`

func TestTheMongoBackupWorkflowRunsAllThreeSteps(t *testing.T) {
	binDir := stubPATH(t)
	tempDir := t.TempDir()

	stubBin(t, binDir, "mongoexport", mongoexportStubBody, 0)
	stubBin(t, binDir, "zip", zipStubBody, 0)
	stubBin(t, binDir, "gsutil", "", 0)

	e := newExecutor()
	if err := e.executeExternalMongoExportSimple(context.Background(),
		"mongodb://tokyo:27017/app", "app", "orders", tempDir,
		mongoBackupConfig("gs://bucket/backups")); err != nil {
		t.Fatalf("executeExternalMongoExportSimple: %v", err)
	}

	stubArgs(t, binDir, "mongoexport")
	stubArgs(t, binDir, "zip")

	gsutil := stubArgs(t, binDir, "gsutil")
	if !strings.HasPrefix(gsutil[2], "gs://bucket/backups/") || !strings.HasSuffix(gsutil[2], ".zip") {
		t.Errorf("upload target = %q, want a .zip under the configured GCS path", gsutil[2])
	}

	// Both intermediates are removed on the success path.
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("read temp dir: %v", err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".json") || strings.HasSuffix(entry.Name(), ".zip") {
			t.Errorf("temporary file %q was left behind", entry.Name())
		}
	}
}

func TestTheMongoBackupWorkflowStopsWhenTheExportFails(t *testing.T) {
	binDir := stubPATH(t)
	stubBin(t, binDir, "mongoexport", "echo 'auth failed' >&2", 1)
	stubBin(t, binDir, "zip", zipStubBody, 0)
	stubBin(t, binDir, "gsutil", "", 0)

	e := newExecutor()
	err := e.executeExternalMongoExportSimple(context.Background(),
		"mongodb://h:27017/app", "app", "orders", t.TempDir(), mongoBackupConfig("gs://b"))

	if err == nil {
		t.Fatal("the workflow reported success despite a failing export")
	}
	if !strings.Contains(err.Error(), "external mongoexport failed") {
		t.Errorf("err = %v", err)
	}
	stubWasNotInvoked(t, binDir, "zip")
	stubWasNotInvoked(t, binDir, "gsutil")
}

func TestTheMongoBackupWorkflowStopsWhenTheZipFails(t *testing.T) {
	binDir := stubPATH(t)
	stubBin(t, binDir, "mongoexport", mongoexportStubBody, 0)
	stubBin(t, binDir, "zip", "echo 'disk full' >&2", 1)
	stubBin(t, binDir, "gsutil", "", 0)

	e := newExecutor()
	err := e.executeExternalMongoExportSimple(context.Background(),
		"mongodb://h:27017/app", "app", "orders", t.TempDir(), mongoBackupConfig("gs://b"))

	if err == nil {
		t.Fatal("the workflow reported success despite a failing zip")
	}
	if !strings.Contains(err.Error(), "external zip failed") {
		t.Errorf("err = %v", err)
	}
	stubWasNotInvoked(t, binDir, "gsutil")
}

func TestExecuteExternalMongoExportWithOptionsPassesTheConnection(t *testing.T) {
	binDir := stubPATH(t)
	stubBin(t, binDir, "mongoexport", mongoexportStubBody, 0)

	e := newExecutor()
	outputPath := filepath.Join(t.TempDir(), "orders.json")

	if err := e.executeExternalMongoExportWithOptions(context.Background(),
		"mongodb://tokyo:27017/app", "app", "orders", outputPath,
		mongoBackupConfig("gs://b")); err != nil {
		t.Fatalf("executeExternalMongoExportWithOptions: %v", err)
	}

	args := stubArgs(t, binDir, "mongoexport")
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "mongodb://tokyo:27017/app") {
		t.Errorf("the connection string was not passed: %s", joined)
	}
	if !strings.Contains(joined, "orders") {
		t.Errorf("the collection was not passed: %s", joined)
	}
	if !strings.Contains(joined, outputPath) {
		t.Errorf("the output path was not passed: %s", joined)
	}
}

func TestExecuteExternalMongoExportWithOptionsAppliesAQuery(t *testing.T) {
	binDir := stubPATH(t)
	stubBin(t, binDir, "mongoexport", mongoexportStubBody, 0)

	e := newExecutor()
	cfg := mongoBackupConfig("gs://b")
	cfg.Query = map[string]map[string]interface{}{
		"orders": {"created_at": map[string]interface{}{
			"type": "daily", "startOffset": float64(-1), "endOffset": float64(0),
		}},
	}

	if err := e.executeExternalMongoExportWithOptions(context.Background(),
		"mongodb://h:27017/app", "app", "orders",
		filepath.Join(t.TempDir(), "orders.json"), cfg); err != nil {
		t.Fatalf("executeExternalMongoExportWithOptions: %v", err)
	}

	joined := strings.Join(stubArgs(t, binDir, "mongoexport"), " ")
	if !strings.Contains(joined, "created_at") {
		t.Errorf("the query was not passed: %s", joined)
	}
}

// -------------------------------------------------------- CSV export path

func TestExecuteExternalMySQLCSVPipesThroughPython(t *testing.T) {
	binDir := stubPATH(t)
	// mysql emits two TSV rows; the real python3 converts them to CSV.
	stubBin(t, binDir, "mysql", `printf 'id\tname\n1\talice\n'`, 0)
	linkRealBinary(t, binDir, "python3")

	e := newExecutor()
	outputPath := filepath.Join(t.TempDir(), "orders.csv")

	if err := e.executeExternalMySQLCSV(context.Background(),
		"h", "3306", "svc", "hunter2", "app", "orders", outputPath,
		mysqlBackupConfig("csv", "", "gs://b")); err != nil {
		t.Fatalf("executeExternalMySQLCSV: %v", err)
	}

	args := stubArgs(t, binDir, "mysql")
	for _, want := range []string{"--default-character-set=utf8mb4", "-h", "h", "-P", "3306", "-u", "svc", "-phunter2", "app", "-e", "--batch"} {
		if !containsArg(args, want) {
			t.Errorf("argument %q was not passed (got: %v)", want, args)
		}
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	// QUOTE_ALL means every field is quoted.
	if !strings.Contains(string(data), `"id","name"`) || !strings.Contains(string(data), `"1","alice"`) {
		t.Errorf("output = %q, want quoted CSV rows", data)
	}
}

// MySQL's batch mode marks NULL as \N; the python converter turns it into an
// empty field, which is indistinguishable from an empty string in the CSV.
func TestCSVExportCannotDistinguishNullFromEmpty(t *testing.T) {
	binDir := stubPATH(t)
	stubBin(t, binDir, "mysql", `printf 'a\tb\n\\N\t\n'`, 0)
	linkRealBinary(t, binDir, "python3")

	e := newExecutor()
	outputPath := filepath.Join(t.TempDir(), "orders.csv")

	if err := e.executeExternalMySQLCSV(context.Background(),
		"h", "3306", "svc", "pw", "app", "orders", outputPath,
		mysqlBackupConfig("csv", "", "gs://b")); err != nil {
		t.Fatalf("executeExternalMySQLCSV: %v", err)
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if !strings.Contains(string(data), `"",""`) {
		t.Fatalf("output = %q — NULL appears to be distinguishable now; assert the new encoding instead", data)
	}
}

func TestExecuteExternalMySQLCSVReportsAFailingQuery(t *testing.T) {
	binDir := stubPATH(t)
	stubBin(t, binDir, "mysql", "echo 'ERROR 1146: no such table' >&2", 1)
	linkRealBinary(t, binDir, "python3")

	e := newExecutor()
	err := e.executeExternalMySQLCSV(context.Background(),
		"h", "3306", "svc", "pw", "app", "orders",
		filepath.Join(t.TempDir(), "orders.csv"),
		mysqlBackupConfig("csv", "", "gs://b"))

	if err == nil {
		t.Fatal("executeExternalMySQLCSV() = nil, want the non-zero exit")
	}
}

func TestTheMySQLBackupWorkflowUsesTheCSVPath(t *testing.T) {
	binDir := stubPATH(t)
	tempDir := t.TempDir()

	stubBin(t, binDir, "mysql", `printf 'id\n1\n'`, 0)
	linkRealBinary(t, binDir, "python3")
	stubBin(t, binDir, "zip", zipStubBody, 0)
	stubBin(t, binDir, "gsutil", "", 0)

	e := newExecutor()
	if err := e.executeExternalMySQLBackupSimple(context.Background(),
		"mysql://svc:pw@h:3306/app", "app", "orders", tempDir,
		mysqlBackupConfig("csv", "", "gs://bucket/backups")); err != nil {
		t.Fatalf("executeExternalMySQLBackupSimple: %v", err)
	}

	stubWasNotInvoked(t, binDir, "mysqldump")
	stubArgs(t, binDir, "mysql")

	gsutil := stubArgs(t, binDir, "gsutil")
	if !strings.HasSuffix(gsutil[2], ".zip") {
		t.Errorf("upload target = %q, want a .zip", gsutil[2])
	}
}

// linkRealBinary symlinks a real system binary into the stub directory, for
// commands a test needs to actually run rather than stub.
func linkRealBinary(t *testing.T, dir, name string) {
	t.Helper()

	for _, candidate := range []string{"/usr/bin/" + name, "/bin/" + name, "/usr/local/bin/" + name} {
		if _, err := os.Stat(candidate); err == nil {
			if err := os.Symlink(candidate, filepath.Join(dir, name)); err != nil {
				t.Fatalf("symlink %s: %v", name, err)
			}
			return
		}
	}
	t.Skipf("%s is not installed", name)
}
