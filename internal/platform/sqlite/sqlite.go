package sqlite

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sirupsen/logrus"
)

// openRetryPause is the wait between attempts to open the control database.
//
// Short on purpose. The DSN above already carries _busy_timeout=5000, so lock
// contention is waited out inside the driver; what reaches the loop is a
// failure the busy timeout could not fix, and a second between attempts only
// made the caller wait four of them to be told so. The readiness probe opens
// this database, and a probe that took four seconds to answer would be timed
// out by the thing asking.
const openRetryPause = 50 * time.Millisecond

// DefaultPath is where the control database lives when SYNC_DB_PATH says
// nothing: alongside the working directory, not a path baked in at build time.
const DefaultPath = "sync.db"

// createdFresh records that a control database had to be created because the
// file was not there.
//
// A lost volume and a first run look identical from inside: both give an empty
// database, an empty task list, a /readyz that answers 200 and not one metric
// to say anything is wrong -- the process runs, replicating nothing. Where the
// path was named deliberately, the two are not the same thing at all, and the
// caller is given the means to tell.
var createdFresh sync.Map // absolute path -> struct{}

// CreatedFresh reports whether the database at this path had to be created by
// this process. A caller that named the path itself should treat that as a
// misconfiguration rather than a first run.
//
// By path rather than a single flag: the answer must not depend on what some
// other database in the same process did, and it must stay true once it is --
// a process that came up on an empty control database does not become correct
// because it has since written to it.
func CreatedFresh(path string) bool {
	if path == "" {
		return false
	}
	if absolute, err := filepath.Abs(path); err == nil {
		path = absolute
	}
	_, found := createdFresh.Load(path)
	return found
}

func OpenSQLiteDB() (*sql.DB, error) {
	dbPath := os.Getenv("SYNC_DB_PATH")
	if dbPath == "" {
		dbPath = DefaultPath
	}

	if !filepath.IsAbs(dbPath) {
		absPath, err := filepath.Abs(dbPath)
		if err == nil {
			dbPath = absPath
		}
	}

	// Whether the file was already there decides whether this is a first run or
	// a misconfiguration, and the two are otherwise indistinguishable: both
	// start the process with an empty task list.
	_, statErr := os.Stat(dbPath)
	fresh := os.IsNotExist(statErr)

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
			time.Sleep(openRetryPause)
		}

		db, err = sql.Open("sqlite3", dsn)
		if err != nil {
			continue
		}

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

	if err := applySchema(db, dbPath); err != nil {
		_ = db.Close()
		return nil, err
	}

	if fresh {
		createdFresh.Store(dbPath, struct{}{})
		logrus.Warnf("[SQLite] Created a new control database at %s. If this is not "+
			"a first run, SYNC_DB_PATH is pointing somewhere unintended and this "+
			"process has started with no sync tasks and no users.", dbPath)
	}

	return db, nil
}
