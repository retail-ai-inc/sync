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

// TestFileStateStoreSaveRequiresExistingDirectory records that Save does not
// create its directory. A checkpoint store pointed at a path that does not yet
// exist fails every write, and since the syncers log and continue, the failure
// shows up later as a checkpoint that never advances.
func TestFileStateStoreSaveRequiresExistingDirectory(t *testing.T) {
	store := NewFileStateStore(filepath.Join(t.TempDir(), "not-created-yet"))

	if err := store.Save("k", []byte("v")); err == nil {
		t.Error("Save into a missing directory succeeded; it may now create the " +
			"directory, so assert that instead")
	}
}

// TestFileStateStoreKeysAreNotSanitised records that the key is joined onto the
// directory without validation, so a key containing path separators escapes the
// store. Keys are derived from database and collection names, which an operator
// controls through the task configuration.
func TestFileStateStoreKeysAreNotSanitised(t *testing.T) {
	root := t.TempDir()
	inner := filepath.Join(root, "state")
	if err := os.MkdirAll(inner, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	store := NewFileStateStore(inner)

	if err := store.Save(filepath.Join("..", "escaped"), []byte("v")); err != nil {
		t.Fatalf("Save with a traversing key: %v", err)
	}

	if _, err := os.Stat(filepath.Join(root, "escaped")); err != nil {
		t.Errorf("the file did not land outside the store (%v); keys may now be "+
			"sanitised, which would be an improvement", err)
	}
}

// TestFileStateStoreSaveIsNotAtomic records that Save writes in place with
// os.WriteFile rather than writing a temporary file and renaming it. A crash
// during the write leaves a truncated checkpoint, which on restart is either
// unparseable or, worse, parses as an earlier position.
func TestFileStateStoreSaveIsNotAtomic(t *testing.T) {
	dir := t.TempDir()
	store := NewFileStateStore(dir)

	if err := store.Save("k", []byte("v")); err != nil {
		t.Fatalf("Save: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	// An atomic implementation would leave no temporary file behind, but it
	// would also not write the destination directly; the marker here is that
	// exactly one file exists and it is the destination itself.
	if len(entries) != 1 || entries[0].Name() != "k" {
		t.Errorf("directory holds %d entries (%v); the write path may have changed",
			len(entries), entries)
	}
}
