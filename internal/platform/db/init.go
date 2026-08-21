package db

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

var (
	// Ensure initialization only happens once
	dbPathOnce sync.Once
)

func init() {
	dbPathOnce.Do(initDBPath)
}

// initDBPath initializes the database path environment variable
func initDBPath() {
	// Check if environment variable is already set
	if os.Getenv("SYNC_DB_PATH") != "" {
		return
	}

	// Get the directory of the current execution file
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return
	}

	// Build the path to the project root directory (three levels up from
	// internal/platform/db). Keep this in step with the file's location: the
	// fallback is a build-time source path, so moving this file moves the path.
	rootDir := filepath.Join(filepath.Dir(filename), "..", "..", "..")
	absDBPath := filepath.Join(rootDir, "sync.db")

	// Set the absolute path as an environment variable
	os.Setenv("SYNC_DB_PATH", absDBPath)
}
