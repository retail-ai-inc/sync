package domain

import "time"

// ChangeStreamInfo describes what one collection's change stream has done. It
// used to come with a package-level registry — RegisterChangeStream and six
// functions that updated it — and nothing in the tree ever called any of them.
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
