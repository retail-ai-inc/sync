package webui

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestUnzipDistFile(t *testing.T) {
	if _, err := exec.LookPath("zip"); err != nil {
		t.Skip("the zip command is unavailable")
	}

	work := t.TempDir()
	payload := filepath.Join(work, "hello.txt")
	if err := os.WriteFile(payload, []byte("contents"), 0o644); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	archive := filepath.Join(work, "dist.zip")
	cmd := exec.Command("zip", "-j", archive, payload)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("could not build the archive: %v (%s)", err, out)
	}

	dest := filepath.Join(work, "out")
	if err := UnzipDistFile(archive, dest); err != nil {
		t.Fatalf("UnzipDistFile: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(dest, "hello.txt"))
	if err != nil {
		t.Fatalf("read extracted file: %v", err)
	}
	if string(got) != "contents" {
		t.Errorf("extracted %q, want %q", got, "contents")
	}
}

func TestUnzipDistFileMissingArchive(t *testing.T) {
	err := UnzipDistFile(filepath.Join(t.TempDir(), "absent.zip"), filepath.Join(t.TempDir(), "out"))

	if err == nil {
		t.Error("UnzipDistFile succeeded for a missing archive, want an error")
	}
}
