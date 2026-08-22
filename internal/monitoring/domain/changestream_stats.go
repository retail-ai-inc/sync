package domain

import "time"

// ChangeStreamInfo describes what one collection's change stream has done.
//
// It used to come with a package-level registry — RegisterChangeStream and six
// functions that updated it — and nothing in the tree ever called any of them.
// The map was permanently empty, so the collector asked a task for its streams,
// got nothing, wrote nothing, and logged that the statistics had been stored:
// the table it fills has held nothing but zeroes since it was created, which a
// copy of the production database confirms.
//
// The registry is gone. These values are built from the counters the
// replication side actually maintains; see monitoring/infra.
//
// Two of its functions also disagreed with each other about what they meant —
// one replaced the received and executed counts while incrementing the event
// count, another added to all three — so a stream that had both applied to it
// produced numbers that could not be read either way. There is one producer now.
type ChangeStreamInfo struct {
	SyncTaskID     int       // Sync task ID this ChangeStream belongs to
	Database       string    // Database name
	Collection     string    // Collection name
	Created        time.Time // Creation time
	LastActivity   time.Time // Last activity time
	EventCount     int64     // Number of processed events
	ErrorCount     int       // Error count
	Active         bool      // Whether it's active
	LastErrorMsg   string    // Last error message
	LastErrorTime  time.Time // Last error time
	ReceivedEvents int       // Number of received events
	ExecutedEvents int       // Number of executed events
	// Detailed operation counts
	InsertedCount int // Number of insert operations
	UpdatedCount  int // Number of update/replace operations
	DeletedCount  int // Number of delete operations
}
