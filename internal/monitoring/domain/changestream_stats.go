package domain

import (
	"fmt"
	"time"

	// "github.com/sirupsen/logrus"

	"sync"

	"github.com/sirupsen/logrus"
)

// ChangeStreamInfo tracks info about a single ChangeStream
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

var (
	// changeStreamTracker stores information about all active ChangeStreams
	changeStreamTracker = make(map[string]*ChangeStreamInfo)
	csTrackerMutex      = &sync.RWMutex{}
)

// RegisterChangeStream registers a new ChangeStream
func RegisterChangeStream(syncTaskID int, database, collection string) {
	key := fmt.Sprintf("%s.%s", database, collection)
	csTrackerMutex.Lock()
	defer csTrackerMutex.Unlock()

	changeStreamTracker[key] = &ChangeStreamInfo{
		SyncTaskID:   syncTaskID,
		Database:     database,
		Collection:   collection,
		Created:      time.Now(),
		LastActivity: time.Now(),
		Active:       true,
	}
}

// UpdateChangeStreamActivity updates ChangeStream activity information
func UpdateChangeStreamActivity(database, collection string, eventCount int, receivedEvents, executedEvents int) {
	csTrackerMutex.Lock()
	defer csTrackerMutex.Unlock()

	key := fmt.Sprintf("%s.%s", database, collection)
	if stream, exists := changeStreamTracker[key]; exists {
		stream.LastActivity = time.Now()
		stream.EventCount += int64(eventCount)
		stream.ReceivedEvents = receivedEvents
		stream.ExecutedEvents = executedEvents
	}
}

// UpdateChangeStreamDetailedActivity updates ChangeStream activity with detailed operation counts
func UpdateChangeStreamDetailedActivity(database, collection string, eventCount int, receivedEvents, executedEvents, insertedCount, updatedCount, deletedCount int) {
	csTrackerMutex.Lock()
	defer csTrackerMutex.Unlock()

	key := fmt.Sprintf("%s.%s", database, collection)
	if stream, exists := changeStreamTracker[key]; exists {
		stream.LastActivity = time.Now()
		stream.EventCount += int64(eventCount)
		stream.ReceivedEvents = receivedEvents
		stream.ExecutedEvents = executedEvents
		stream.InsertedCount = insertedCount
		stream.UpdatedCount = updatedCount
		stream.DeletedCount = deletedCount
	}
}

// AccumulateChangeStreamActivity accumulates ChangeStream statistics instead of replacing them
func AccumulateChangeStreamActivity(database, collection string, eventCount, receivedEvents, executedEvents, insertedCount, updatedCount, deletedCount int) {
	csTrackerMutex.Lock()
	defer csTrackerMutex.Unlock()

	key := fmt.Sprintf("%s.%s", database, collection)
	if stream, exists := changeStreamTracker[key]; exists {
		stream.LastActivity = time.Now()
		stream.EventCount += int64(eventCount)
		stream.ReceivedEvents += receivedEvents
		stream.ExecutedEvents += executedEvents
		stream.InsertedCount += insertedCount
		stream.UpdatedCount += updatedCount
		stream.DeletedCount += deletedCount
	}
}

// RecordChangeStreamError records ChangeStream errors
func RecordChangeStreamError(database, collection, errorMsg string) {
	key := fmt.Sprintf("%s.%s", database, collection)
	csTrackerMutex.Lock()
	defer csTrackerMutex.Unlock()

	if cs, exists := changeStreamTracker[key]; exists {
		cs.ErrorCount++
		cs.LastErrorMsg = errorMsg
		cs.LastErrorTime = time.Now()
	}
}

// DeactivateChangeStream marks a ChangeStream as inactive
func DeactivateChangeStream(database, collection string) {
	key := fmt.Sprintf("%s.%s", database, collection)
	csTrackerMutex.Lock()
	defer csTrackerMutex.Unlock()

	if cs, exists := changeStreamTracker[key]; exists {
		cs.Active = false
	}
}

// GetActiveChangeStreams gets information about all active ChangeStreams
func GetActiveChangeStreams() map[string]*ChangeStreamInfo {
	csTrackerMutex.RLock()
	defer csTrackerMutex.RUnlock()

	// Create a copy to avoid concurrency issues
	result := make(map[string]*ChangeStreamInfo, len(changeStreamTracker))
	for k, v := range changeStreamTracker {
		result[k] = v
	}
	return result
}

// GetActiveChangeStreamsByTaskID gets information about active ChangeStreams for a specific sync task
func GetActiveChangeStreamsByTaskID(syncTaskID int) map[string]*ChangeStreamInfo {
	csTrackerMutex.RLock()
	defer csTrackerMutex.RUnlock()

	// Create a copy filtering by sync task ID
	result := make(map[string]*ChangeStreamInfo)
	for k, v := range changeStreamTracker {
		if v.SyncTaskID == syncTaskID && v.Active {
			result[k] = v
		}
	}
	return result
}

// ResetInMemoryStatistics resets ChangeStreamInfo statistics in memory for a specific sync task
func ResetInMemoryStatistics(syncTaskID int) {
	csTrackerMutex.Lock()
	defer csTrackerMutex.Unlock()

	resetCount := 0
	for key, cs := range changeStreamTracker {
		if cs.SyncTaskID == syncTaskID {
			// Reset all accumulated statistics to 0
			cs.ReceivedEvents = 0
			cs.ExecutedEvents = 0
			cs.InsertedCount = 0
			cs.UpdatedCount = 0
			cs.DeletedCount = 0
			cs.ErrorCount = 0
			cs.EventCount = 0
			// Keep other fields like Created, LastActivity, Active unchanged
			resetCount++
			logrus.Debugf("[MongoDB] Reset in-memory statistics for ChangeStream: %s", key)
		}
	}

	logrus.Infof("[MongoDB] Reset in-memory statistics for %d ChangeStreams of task_id=%d",
		resetCount, syncTaskID)
}
