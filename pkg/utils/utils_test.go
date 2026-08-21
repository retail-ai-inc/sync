package utils

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestParseInt(t *testing.T) {
	tests := []struct {
		in      string
		want    int
		wantErr bool
	}{
		{"42", 42, false},
		{"-7", -7, false},
		{"0", 0, false},
		{"", 0, true},
		{"1.5", 0, true},
		{"abc", 0, true},
		{" 42", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := ParseInt(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseInt(%q) succeeded with %d, want an error", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseInt(%q) returned %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("ParseInt(%q) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

func TestGetCurrentTime(t *testing.T) {
	before := time.Now()
	got := GetCurrentTime()
	after := time.Now()

	if got.Before(before) || got.After(after) {
		t.Errorf("GetCurrentTime returned %v, outside [%v, %v]", got, before, after)
	}
}

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
