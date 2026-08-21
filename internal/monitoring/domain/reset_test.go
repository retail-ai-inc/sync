package domain

import (
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
)

func TestResetInMemoryStatistics(t *testing.T) {
	resetTracker(t)

	RegisterChangeStream(1, "db", "a")
	RegisterChangeStream(1, "db", "b")
	RegisterChangeStream(2, "db", "c")
	AccumulateChangeStreamActivity("db", "a", 10, 10, 8, 5, 3, 2)
	AccumulateChangeStreamActivity("db", "c", 20, 20, 20, 10, 5, 5)
	RecordChangeStreamError("db", "a", "boom")

	before := GetActiveChangeStreamsByTaskID(1)["db.a"]
	created, lastActivity, active := before.Created, before.LastActivity, before.Active

	ResetInMemoryStatistics(1)

	a := GetActiveChangeStreamsByTaskID(1)["db.a"]
	if a.ReceivedEvents != 0 || a.ExecutedEvents != 0 || a.EventCount != 0 ||
		a.InsertedCount != 0 || a.UpdatedCount != 0 || a.DeletedCount != 0 || a.ErrorCount != 0 {
		t.Errorf("task 1 counters were not cleared: %+v", a)
	}
	if !a.Created.Equal(created) || !a.LastActivity.Equal(lastActivity) || a.Active != active {
		t.Error("ResetInMemoryStatistics changed a field other than the counters")
	}

	c := GetActiveChangeStreamsByTaskID(2)["db.c"]
	if c.ReceivedEvents != 20 {
		t.Errorf("task 2 received = %d, want it left alone", c.ReceivedEvents)
	}
}

// The error message is kept while the count that justified it is cleared, so
// after a daily reset a stream reports zero errors alongside a stale
// LastErrorMsg from a previous day.
func TestResetInMemoryStatisticsKeepsTheStaleErrorMessage(t *testing.T) {
	resetTracker(t)

	RegisterChangeStream(1, "db", "a")
	RecordChangeStreamError("db", "a", "yesterday's failure")

	ResetInMemoryStatistics(1)

	a := GetActiveChangeStreamsByTaskID(1)["db.a"]
	if a.ErrorCount != 0 {
		t.Fatalf("ErrorCount = %d, want 0", a.ErrorCount)
	}
	if a.LastErrorMsg != "yesterday's failure" {
		t.Fatalf("LastErrorMsg = %q — it appears to be cleared now; assert the empty value instead", a.LastErrorMsg)
	}
}

func TestResetInMemoryStatisticsOnAnUnknownTask(t *testing.T) {
	resetTracker(t)
	RegisterChangeStream(1, "db", "a")

	ResetInMemoryStatistics(99) // must not panic or touch task 1

	if n := len(GetActiveChangeStreamsByTaskID(1)); n != 1 {
		t.Errorf("task 1 has %d streams, want 1", n)
	}
}
