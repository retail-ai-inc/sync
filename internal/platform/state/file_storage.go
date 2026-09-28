package state

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// FileStateStore keeps one value per key as one file under a directory.
// Nothing in the tree uses it today — the replication checkpoints live in
// SQLite — but what it holds is a resume position, so the three things a
// position store has to get right are worth getting right here rather than
// leaving as a trap: the directory has to exist, the key must not be able to
// name a file outside the store, and a crash mid-write must not leave a half-
// written position behind.
type FileStateStore struct {
	dir string
}

func NewFileStateStore(dir string) *FileStateStore {
	return &FileStateStore{dir: dir}
}

// Save writes value under key, replacing whatever was there. The write goes to
// a temporary file in the same directory and is then renamed over the
// destination, which on a POSIX filesystem is atomic: a reader sees either the
// old position or the new one, never a truncated one.
func (f *FileStateStore) Save(key string, value []byte) error {
	path, err := f.path(key)
	if err != nil {
		return err
	}

	// A store pointed at a directory that does not exist yet used to fail every
	// write, and the syncers log and carry on, so it showed up much later as a
	// checkpoint that never advanced.
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create the state directory: %w", err)
	}

	temporary, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return fmt.Errorf("create a temporary file for %s: %w", key, err)
	}
	name := temporary.Name()
	defer func() { _ = os.Remove(name) }() // no-op once the rename has happened

	if _, err := temporary.Write(value); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write %s: %w", key, err)
	}
	// The rename is only atomic with respect to what has reached the disk, so
	// the contents are flushed before it.
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("flush %s: %w", key, err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close %s: %w", key, err)
	}
	if err := os.Chmod(name, 0o644); err != nil {
		return fmt.Errorf("set the mode of %s: %w", key, err)
	}
	if err := os.Rename(name, path); err != nil {
		return fmt.Errorf("replace %s: %w", key, err)
	}
	return nil
}

func (f *FileStateStore) Load(key string) ([]byte, error) {
	path, err := f.path(key)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(path)
}

// path resolves a key to a file inside the store, refusing one that would name
// a file outside it. Keys come from database and collection names, which an
// operator sets through the task configuration.
func (f *FileStateStore) path(key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("state: the key is empty")
	}
	if strings.ContainsRune(key, os.PathSeparator) || strings.Contains(key, "/") {
		return "", fmt.Errorf("state: %q names a path, not a key", key)
	}
	if key == "." || key == ".." {
		return "", fmt.Errorf("state: %q is not a key", key)
	}
	return filepath.Join(f.dir, key), nil
}
