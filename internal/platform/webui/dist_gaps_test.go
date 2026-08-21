package webui

import (
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestUnzipDistFileDependsOnAnExternalBinary(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PATH", dir) // an empty PATH: no unzip anywhere

	err := UnzipDistFile(filepath.Join(dir, "x.zip"), filepath.Join(dir, "out"))
	if err == nil {
		t.Fatal("UnzipDistFile() = nil without unzip on PATH — it appears to use archive/zip now")
	}
	if !strings.Contains(err.Error(), "system unzip command") {
		t.Fatalf("err = %v — the external dependency appears to be gone", err)
	}
}
