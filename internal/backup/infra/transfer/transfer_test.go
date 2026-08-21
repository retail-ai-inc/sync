package transfer

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

func containsArg(args []string, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}

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

	if err := Zip(context.Background(), workDir, input, output); err != nil {
		t.Fatalf("transfer.Zip: %v", err)
	}

	args := stubArgs(t, binDir, "zip")
	if !containsArg(args, "-j") || !containsArg(args, output) || !containsArg(args, input) {
		t.Errorf("args = %v, want -j plus both paths", args)
	}
}

func TestExecuteExternalZipReportsAFailingCommand(t *testing.T) {
	binDir := stubPATH(t)
	stubBin(t, binDir, "zip", "echo 'disk full' >&2", 1)

	workDir := t.TempDir()
	err := Zip(context.Background(), workDir,
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

	workDir := t.TempDir()
	err := Zip(context.Background(), workDir,
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

	if err := UploadGCS(context.Background(),
		"/tmp/orders.zip", "gs://bucket/path/orders.zip"); err != nil {
		t.Fatalf("transfer.UploadGCS: %v", err)
	}

	args := stubArgs(t, binDir, "gsutil")
	if len(args) != 3 || args[0] != "cp" || args[1] != "/tmp/orders.zip" || args[2] != "gs://bucket/path/orders.zip" {
		t.Errorf("args = %v, want [cp <local> <remote>]", args)
	}
}

func TestExecuteExternalGCSUploadReportsAFailingCommand(t *testing.T) {
	binDir := stubPATH(t)
	stubBin(t, binDir, "gsutil", "echo 'AccessDeniedException: 403' >&2", 1)

	err := UploadGCS(context.Background(), "/tmp/x.zip", "gs://b/x.zip")

	if err == nil {
		t.Fatal("executeExternalGCSUpload() = nil, want the non-zero exit")
	}
	if !strings.Contains(err.Error(), "gsutil upload failed") || !strings.Contains(err.Error(), "403") {
		t.Errorf("err = %v, want it to carry the command output", err)
	}
}
