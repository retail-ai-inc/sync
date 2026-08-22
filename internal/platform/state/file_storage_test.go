package state

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFileStateStoreSaveAndLoad(t *testing.T) {
	store := NewFileStateStore(t.TempDir())

	if err := store.Save("resume.json", []byte("token-1")); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := store.Load("resume.json")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if string(got) != "token-1" {
		t.Errorf("Load returned %q, want %q", got, "token-1")
	}
}

func TestFileStateStoreOverwrites(t *testing.T) {
	store := NewFileStateStore(t.TempDir())

	if err := store.Save("k", []byte("first-value-is-longer")); err != nil {
		t.Fatalf("first Save: %v", err)
	}
	if err := store.Save("k", []byte("short")); err != nil {
		t.Fatalf("second Save: %v", err)
	}

	got, err := store.Load("k")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// A shorter value must fully replace the longer one, not leave a tail behind.
	if string(got) != "short" {
		t.Errorf("Load returned %q, want %q", got, "short")
	}
}

func TestFileStateStoreLoadMissingKey(t *testing.T) {
	store := NewFileStateStore(t.TempDir())

	if _, err := store.Load("absent"); !os.IsNotExist(err) {
		t.Errorf("Load of a missing key returned %v, want a not-exist error", err)
	}
}

// TestSaveCreatesItsDirectory covers a store pointed at a path that does not
// exist yet. Save used to fail every write, and because the syncers log and
// carry on the symptom appeared much later, as a checkpoint that never advanced.
func TestSaveCreatesItsDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "not-created-yet")
	store := NewFileStateStore(dir)

	if err := store.Save("k", []byte("v")); err != nil {
		t.Fatalf("Save into a missing directory: %v", err)
	}

	got, err := store.Load("k")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if string(got) != "v" {
		t.Errorf("Load returned %q, want %q", got, "v")
	}
}

// TestAKeyCannotNameAFileOutsideTheStore covers keys derived from database and
// collection names, which an operator sets through the task configuration. The
// key used to be joined onto the directory unchecked, so one containing a path
// separator wrote wherever it liked.
func TestAKeyCannotNameAFileOutsideTheStore(t *testing.T) {
	root := t.TempDir()
	inner := filepath.Join(root, "state")
	store := NewFileStateStore(inner)

	for _, key := range []string{
		filepath.Join("..", "escaped"),
		"sub/dir",
		"..",
		"",
	} {
		if err := store.Save(key, []byte("v")); err == nil {
			t.Errorf("Save(%q) succeeded, want a refusal", key)
		}
	}

	if _, err := os.Stat(filepath.Join(root, "escaped")); err == nil {
		t.Error("a file landed outside the store")
	}
}

// TestSaveIsAtomic covers a crash during the write. Save used to write the
// destination in place, so an interrupted write left a truncated checkpoint that
// on restart either would not parse or parsed as an earlier position — which
// replays events already applied. The write now lands on a temporary file and is
// renamed over the destination.
func TestSaveIsAtomic(t *testing.T) {
	dir := t.TempDir()
	store := NewFileStateStore(dir)

	if err := store.Save("k", []byte("first")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := store.Save("k", []byte("second")); err != nil {
		t.Fatalf("Save: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	// The temporary file is renamed, not left behind.
	if len(entries) != 1 || entries[0].Name() != "k" {
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		t.Errorf("directory holds %v, want just the destination", names)
	}

	got, err := store.Load("k")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if string(got) != "second" {
		t.Errorf("Load returned %q, want %q", got, "second")
	}
}
