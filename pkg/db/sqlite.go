package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func OpenSQLiteDB() (*sql.DB, error) {
	dbPath := os.Getenv("SYNC_DB_PATH")
	if dbPath == "" {
		dbPath = "sync.db"
	}

	if !filepath.IsAbs(dbPath) {
		absPath, err := filepath.Abs(dbPath)
		if err == nil {
			dbPath = absPath
		}
	}

	// Ensure parent directory exists
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create database directory: %v", err)
	}

	// Use advanced connection string with important parameters to handle locking issues
	// _journal=WAL: Use WAL mode to reduce locking
	// _busy_timeout: Set longer busy timeout
	// _mutex=no: Reduce mutex contention
	dsn := fmt.Sprintf("%s?_journal=WAL&_busy_timeout=5000&_mutex=no", dbPath)

	// Try to open database connection up to 5 times
	var db *sql.DB
	var err error
	maxRetries := 5

	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			time.Sleep(time.Second) // Wait 1 second before retrying
		}

		db, err = sql.Open("sqlite3", dsn)
		if err != nil {
			continue
		}

		// Set connection parameters
		db.SetMaxOpenConns(1)                   // SQLite works best with a single connection
		db.SetConnMaxLifetime(time.Second * 30) // Longer connection lifetime
		db.SetMaxIdleConns(1)                   // Keep one idle connection

		// Verify connection is usable
		if err = db.Ping(); err == nil {
			break // Successfully connected
		}

		db.Close() // Close failed connection
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to database (tried %d times): %v", maxRetries, err)
	}

	return db, nil
}
