package domain

import "testing"

func TestDifferenceIsAbsolute(t *testing.T) {
	for _, c := range []struct {
		name           string
		source, target int64
		want           int64
	}{
		{"target behind", 100, 60, 40},
		{"target ahead", 60, 100, 40},
		{"equal", 100, 100, 0},
		{"both empty", 0, 0, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := RowCountSample{Source: c.source, Target: c.target}
			if got := s.Difference(); got != c.want {
				t.Errorf("Difference(src=%d, tgt=%d) = %d, want %d",
					c.source, c.target, got, c.want)
			}
		})
	}
}

func TestQualifiedTableNamesTheTask(t *testing.T) {
	s := RowCountSample{Table: "orders", TaskID: "7"}
	if got, want := s.QualifiedTable(), "taskID:7_orders"; got != want {
		t.Errorf("QualifiedTable() = %q, want %q", got, want)
	}
}

func TestLogEntryMatchesIsCaseInsensitiveOnTheLevel(t *testing.T) {
	entry := LogEntry{Level: "ERROR", Message: "connection refused"}
	if !entry.Matches("error", "") {
		t.Error(`Matches("error") = false; the UI sends lower case and the column holds upper`)
	}
	if entry.Matches("warn", "") {
		t.Error(`Matches("warn") = true on an ERROR line`)
	}
}

func TestLogEntrySearchIsCaseInsensitiveSubstring(t *testing.T) {
	entry := LogEntry{Level: "INFO", Message: "Copied 17218 keys"}
	if !entry.Matches("", "COPIED") {
		t.Error(`Matches(search "COPIED") = false`)
	}
	if entry.Matches("", "flushed") {
		t.Error(`Matches(search "flushed") = true on a line without it`)
	}
}

func TestAnEmptyFilterMatchesEverything(t *testing.T) {
	entry := LogEntry{Level: "DEBUG", Message: "anything"}
	if !entry.Matches("", "") {
		t.Error("Matches with no filters = false")
	}
}

func TestMatchingLogsKeepsOrderAndNeverReturnsNil(t *testing.T) {
	kept := MatchingLogs([]LogEntry{
		{Level: "INFO", Message: "first"},
		{Level: "ERROR", Message: "second"},
		{Level: "ERROR", Message: "third"},
	}, "error", "")

	if len(kept) != 2 || kept[0].Message != "second" || kept[1].Message != "third" {
		t.Fatalf("MatchingLogs kept %v", kept)
	}
	if MatchingLogs(nil, "", "") == nil {
		t.Error("MatchingLogs(nil) = nil; the endpoint renders an empty list, not null")
	}
}

// TestSummariseLeavesOutRowsWithNoTask covers the rule that statistics are
// stored per task, so a zero there is a row written before the task was known.
// Counting it inflates every total on the status page.
func TestSummariseLeavesOutRowsWithNoTask(t *testing.T) {
	report := SummariseChangeStreams([]ChangeStreamStat{
		{TaskID: 0, Collection: "orphan", Received: 500, Executed: 500, Pending: 9, Errors: 4},
		{TaskID: 39, Collection: "orders", Received: 10, Executed: 8, Pending: 2, Errors: 1},
	})

	if report.ActiveStreams != 1 {
		t.Errorf("ActiveStreams = %d, want 1", report.ActiveStreams)
	}
	if report.TotalReceived != 10 || report.TotalExecuted != 8 ||
		report.TotalPending != 2 || report.TotalErrors != 1 {
		t.Errorf("a row with no task was counted: %+v", report)
	}
	if report.TasksCount != 1 {
		t.Errorf("TasksCount = %d, want 1", report.TasksCount)
	}
	if len(report.Streams) != 1 || report.Streams[0].Collection != "orders" {
		t.Errorf("Streams = %+v", report.Streams)
	}
}

func TestSummariseCountsEachTaskOnce(t *testing.T) {
	report := SummariseChangeStreams([]ChangeStreamStat{
		{TaskID: 39, Collection: "orders", Received: 1},
		{TaskID: 39, Collection: "payments", Received: 2},
		{TaskID: 41, Collection: "trials", Received: 4},
	})

	if report.TasksCount != 2 {
		t.Errorf("TasksCount = %d, want 2 for three streams across two tasks", report.TasksCount)
	}
	if report.ActiveStreams != 3 {
		t.Errorf("ActiveStreams = %d, want 3", report.ActiveStreams)
	}
	if report.TotalReceived != 7 {
		t.Errorf("TotalReceived = %d, want 7", report.TotalReceived)
	}
}

func TestSummariseReportsTheLatestUpdate(t *testing.T) {
	report := SummariseChangeStreams([]ChangeStreamStat{
		{TaskID: 39, LastUpdated: "2026-09-05 01:14:00"},
		{TaskID: 39, LastUpdated: "2026-09-05 03:02:11"},
		{TaskID: 41, LastUpdated: "2026-09-04 23:59:59"},
	})
	if want := "2026-09-05 03:02:11"; report.LastUpdated != want {
		t.Errorf("LastUpdated = %q, want %q", report.LastUpdated, want)
	}
}

func TestSummariseOfNothing(t *testing.T) {
	report := SummariseChangeStreams(nil)
	if report.Streams == nil {
		t.Error("Streams = nil; the endpoint renders an empty list, not null")
	}
	if report.ActiveStreams != 0 || report.TasksCount != 0 || report.LastUpdated != "" {
		t.Errorf("SummariseChangeStreams(nil) = %+v", report)
	}
}
