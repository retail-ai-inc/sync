package domain

import (
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
)

// resetTracker clears the package-level registry so each test starts clean.
// The registry is global state, so these tests must not run in parallel.
func resetTracker(t *testing.T) {
	t.Helper()

	csTrackerMutex.Lock()
	changeStreamTracker = make(map[string]*ChangeStreamInfo)
	csTrackerMutex.Unlock()

	t.Cleanup(func() {
		csTrackerMutex.Lock()
		changeStreamTracker = make(map[string]*ChangeStreamInfo)
		csTrackerMutex.Unlock()
	})
}

func TestRegisterChangeStream(t *testing.T) {
	resetTracker(t)

	RegisterChangeStream(7, "source_db", "users")

	streams := GetActiveChangeStreams()
	if len(streams) != 1 {
		t.Fatalf("registry holds %d streams, want 1", len(streams))
	}
	cs, ok := streams["source_db.users"]
	if !ok {
		t.Fatalf("registry keys are %v, want source_db.users", streams)
	}
	if cs.SyncTaskID != 7 || cs.Database != "source_db" || cs.Collection != "users" {
		t.Errorf("entry = %+v", cs)
	}
	if !cs.Active {
		t.Error("a freshly registered stream is not active")
	}
	if cs.EventCount != 0 || cs.ErrorCount != 0 {
		t.Errorf("counters start at %d/%d, want zero", cs.EventCount, cs.ErrorCount)
	}
	if cs.Created.IsZero() || cs.LastActivity.IsZero() {
		t.Error("timestamps were not set")
	}
}

// TestRegisterChangeStreamResetsCounters records that re-registering a stream
// discards its statistics. The guardian loop reopens a change stream on every
// recoverable error, so any reconnect would zero the counters that the
// monitoring UI reports — cumulative totals cannot survive a network blip.
func TestRegisterChangeStreamResetsCounters(t *testing.T) {
	resetTracker(t)

	RegisterChangeStream(1, "db", "coll")
	AccumulateChangeStreamActivity("db", "coll", 10, 10, 10, 5, 3, 2)

	before := GetActiveChangeStreams()["db.coll"]
	if before.EventCount != 10 {
		t.Fatalf("setup failed: EventCount = %d", before.EventCount)
	}

	RegisterChangeStream(1, "db", "coll")

	after := GetActiveChangeStreams()["db.coll"]
	if after.EventCount != 0 || after.ReceivedEvents != 0 || after.InsertedCount != 0 {
		t.Errorf("counters survived re-registration (%d/%d/%d); if that is now "+
			"intended, assert the preserved values instead",
			after.EventCount, after.ReceivedEvents, after.InsertedCount)
	}
}

// TestChangeStreamKeyIgnoresTaskID records that entries are keyed on
// database.collection with no task identifier, so two tasks replicating the
// same source collection to different targets overwrite one another. The second
// registration wins, and GetActiveChangeStreamsByTaskID then reports nothing for
// the first task.
func TestChangeStreamKeyIgnoresTaskID(t *testing.T) {
	resetTracker(t)

	RegisterChangeStream(1, "db", "coll")
	RegisterChangeStream(2, "db", "coll")

	if n := len(GetActiveChangeStreams()); n != 1 {
		t.Fatalf("registry holds %d entries; the key may now include the task id, "+
			"so assert the separate entries instead", n)
	}
	if got := GetActiveChangeStreams()["db.coll"].SyncTaskID; got != 2 {
		t.Errorf("the surviving entry belongs to task %d, want 2", got)
	}
	if n := len(GetActiveChangeStreamsByTaskID(1)); n != 0 {
		t.Errorf("task 1 still reports %d streams after task 2 overwrote the entry", n)
	}
}

// TestUpdateVersusAccumulateSemantics pins a trap in the API: the two updater
// functions treat the same fields differently. UpdateChangeStreamActivity
// *replaces* ReceivedEvents and ExecutedEvents while *adding* to EventCount,
// whereas AccumulateChangeStreamActivity adds to all of them. Mixing the two
// against one stream produces totals that mean neither thing.
func TestUpdateVersusAccumulateSemantics(t *testing.T) {
	t.Run("update replaces received and executed", func(t *testing.T) {
		resetTracker(t)
		RegisterChangeStream(1, "db", "coll")

		UpdateChangeStreamActivity("db", "coll", 5, 100, 90)
		UpdateChangeStreamActivity("db", "coll", 5, 200, 180)

		cs := GetActiveChangeStreams()["db.coll"]
		if cs.ReceivedEvents != 200 || cs.ExecutedEvents != 180 {
			t.Errorf("received/executed = %d/%d, want 200/180 (replaced)",
				cs.ReceivedEvents, cs.ExecutedEvents)
		}
		// EventCount is the exception: it accumulates.
		if cs.EventCount != 10 {
			t.Errorf("EventCount = %d, want 10 (accumulated)", cs.EventCount)
		}
	})

	t.Run("accumulate adds to everything", func(t *testing.T) {
		resetTracker(t)
		RegisterChangeStream(1, "db", "coll")

		AccumulateChangeStreamActivity("db", "coll", 5, 100, 90, 60, 20, 10)
		AccumulateChangeStreamActivity("db", "coll", 5, 100, 90, 60, 20, 10)

		cs := GetActiveChangeStreams()["db.coll"]
		if cs.ReceivedEvents != 200 || cs.ExecutedEvents != 180 {
			t.Errorf("received/executed = %d/%d, want 200/180 (accumulated)",
				cs.ReceivedEvents, cs.ExecutedEvents)
		}
		if cs.InsertedCount != 120 || cs.UpdatedCount != 40 || cs.DeletedCount != 20 {
			t.Errorf("operation counts = %d/%d/%d, want 120/40/20",
				cs.InsertedCount, cs.UpdatedCount, cs.DeletedCount)
		}
	})
}

func TestUpdateChangeStreamDetailedActivityReplaces(t *testing.T) {
	resetTracker(t)
	RegisterChangeStream(1, "db", "coll")

	UpdateChangeStreamDetailedActivity("db", "coll", 1, 10, 9, 5, 3, 1)
	UpdateChangeStreamDetailedActivity("db", "coll", 1, 20, 18, 10, 6, 2)

	cs := GetActiveChangeStreams()["db.coll"]
	if cs.InsertedCount != 10 || cs.UpdatedCount != 6 || cs.DeletedCount != 2 {
		t.Errorf("operation counts = %d/%d/%d, want 10/6/2 (replaced)",
			cs.InsertedCount, cs.UpdatedCount, cs.DeletedCount)
	}
}

func TestUpdatesToUnknownStreamAreDropped(t *testing.T) {
	resetTracker(t)

	// Every updater silently returns when the key is absent, so activity for a
	// stream that was never registered is lost without a trace.
	UpdateChangeStreamActivity("db", "coll", 5, 10, 10)
	AccumulateChangeStreamActivity("db", "coll", 5, 10, 10, 1, 1, 1)
	RecordChangeStreamError("db", "coll", "boom")
	DeactivateChangeStream("db", "coll")

	if n := len(GetActiveChangeStreams()); n != 0 {
		t.Errorf("registry gained %d entries from updates alone", n)
	}
}

func TestRecordChangeStreamError(t *testing.T) {
	resetTracker(t)
	RegisterChangeStream(1, "db", "coll")

	RecordChangeStreamError("db", "coll", "connection refused")
	RecordChangeStreamError("db", "coll", "cursor not found")

	cs := GetActiveChangeStreams()["db.coll"]
	if cs.ErrorCount != 2 {
		t.Errorf("ErrorCount = %d, want 2", cs.ErrorCount)
	}
	// Only the most recent message is kept.
	if cs.LastErrorMsg != "cursor not found" {
		t.Errorf("LastErrorMsg = %q, want the most recent", cs.LastErrorMsg)
	}
	if cs.LastErrorTime.IsZero() {
		t.Error("LastErrorTime was not set")
	}
	// An error does not deactivate the stream.
	if !cs.Active {
		t.Error("recording an error deactivated the stream")
	}
}

func TestDeactivateChangeStream(t *testing.T) {
	resetTracker(t)
	RegisterChangeStream(1, "db", "coll")

	DeactivateChangeStream("db", "coll")

	// The entry stays in the registry; only the flag changes.
	if n := len(GetActiveChangeStreams()); n != 1 {
		t.Errorf("GetActiveChangeStreams returns %d entries; despite the name it "+
			"returns every entry, active or not", n)
	}
	if GetActiveChangeStreams()["db.coll"].Active {
		t.Error("the stream is still marked active")
	}
	// The by-task view does filter on the flag.
	if n := len(GetActiveChangeStreamsByTaskID(1)); n != 0 {
		t.Errorf("GetActiveChangeStreamsByTaskID returns %d entries, want 0", n)
	}
}

func TestGetActiveChangeStreamsByTaskID(t *testing.T) {
	resetTracker(t)
	RegisterChangeStream(1, "db", "a")
	RegisterChangeStream(1, "db", "b")
	RegisterChangeStream(2, "db", "c")

	if n := len(GetActiveChangeStreamsByTaskID(1)); n != 2 {
		t.Errorf("task 1 has %d streams, want 2", n)
	}
	if n := len(GetActiveChangeStreamsByTaskID(2)); n != 1 {
		t.Errorf("task 2 has %d streams, want 1", n)
	}
	if n := len(GetActiveChangeStreamsByTaskID(99)); n != 0 {
		t.Errorf("an unknown task has %d streams, want 0", n)
	}
}

// TestGetActiveChangeStreamsSharesPointers records that the defensive copy is
// shallow. The comment in the implementation says the copy avoids concurrency
// issues, but only the map is copied — the values are the same pointers, so a
// caller reading a returned entry races with the writers that hold the mutex.
func TestGetActiveChangeStreamsSharesPointers(t *testing.T) {
	resetTracker(t)
	RegisterChangeStream(1, "db", "coll")

	snapshot := GetActiveChangeStreams()["db.coll"]
	AccumulateChangeStreamActivity("db", "coll", 1, 1, 1, 1, 0, 0)

	if snapshot.EventCount == 0 {
		t.Skip("the snapshot is now a deep copy, which removes the race")
	}
	t.Logf("the returned entry changed under the caller (EventCount=%d): the copy "+
		"is shallow and the values are shared", snapshot.EventCount)
}

func TestChangeStreamActivityUpdatesTimestamp(t *testing.T) {
	resetTracker(t)
	RegisterChangeStream(1, "db", "coll")

	before := GetActiveChangeStreams()["db.coll"].LastActivity
	time.Sleep(2 * time.Millisecond)
	AccumulateChangeStreamActivity("db", "coll", 1, 1, 1, 1, 0, 0)
	after := GetActiveChangeStreams()["db.coll"].LastActivity

	if !after.After(before) {
		t.Errorf("LastActivity did not advance: %v -> %v", before, after)
	}
}
