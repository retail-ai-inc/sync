package domain

import "strings"

// What the monitoring endpoints report, and the rules for turning stored rows
// into it.
//
// These used to be built inside the HTTP handlers, next to the SQL that read
// them: the level filter, the source/target/diff fan-out and the changestream
// totals were all expressed once, in a place that could only be exercised
// through a request. The rules are here and the reading is in infra, so what
// the numbers mean can be tested without either.

// Times are carried as stored rather than parsed. The stored format has no
// zone, and the endpoints hand it to a JST converter that expects exactly what
// the column holds; parsing here would assume a zone the column does not name.

// RowCountSample is one comparison of a table's source and target counts.
type RowCountSample struct {
	LoggedAt string
	Table    string
	Source   int64
	Target   int64
	TaskID   string
}

// Difference reports how far apart the two counts are, without a sign: which
// side is ahead is a separate question from how far apart they are, and the
// chart plots the distance.
func (s RowCountSample) Difference() int64 {
	if diff := s.Source - s.Target; diff >= 0 {
		return diff
	}
	return s.Target - s.Source
}

// QualifiedTable names the table for a chart covering every task, where a bare
// table name is ambiguous: two tasks replicating the same table would draw one
// series.
func (s RowCountSample) QualifiedTable() string {
	return "taskID:" + s.TaskID + "_" + s.Table
}

// LogEntry is one line of a task's log.
type LogEntry struct {
	LoggedAt string
	Level    string
	Message  string
}

// Matches reports whether an entry passes a level and a substring filter.
// Both are optional and an empty one matches everything. The level is compared
// case-insensitively because the UI sends "error" and the column holds "ERROR".
func (e LogEntry) Matches(level, search string) bool {
	if level != "" && !strings.EqualFold(e.Level, level) {
		return false
	}
	if search == "" {
		return true
	}
	return strings.Contains(strings.ToLower(e.Message), strings.ToLower(search))
}

// MatchingLogs keeps the entries that pass both filters, in order.
func MatchingLogs(entries []LogEntry, level, search string) []LogEntry {
	kept := make([]LogEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.Matches(level, search) {
			kept = append(kept, entry)
		}
	}
	return kept
}

// ChangeStreamStat is one collection's change stream counters.
type ChangeStreamStat struct {
	TaskID      int
	Collection  string
	Received    int
	Executed    int
	Pending     int
	Errors      int
	Inserted    int
	Updated     int
	Deleted     int
	LastUpdated string
}

// ChangeStreamReport is the whole picture the status endpoint answers with.
type ChangeStreamReport struct {
	Streams       []ChangeStreamStat
	TotalReceived int
	TotalExecuted int
	TotalPending  int
	TotalErrors   int
	ActiveStreams int
	TasksCount    int
	LastUpdated   string
}

// SummariseChangeStreams totals the per-collection counters.
//
// A row with no task is left out. Statistics are stored per task, so a zero
// there is a row written before the task was known rather than a stream
// belonging to task zero, and counting it would inflate every total.
func SummariseChangeStreams(stats []ChangeStreamStat) ChangeStreamReport {
	report := ChangeStreamReport{Streams: make([]ChangeStreamStat, 0, len(stats))}
	tasks := make(map[int]bool, len(stats))

	for _, stat := range stats {
		if stat.TaskID == 0 {
			continue
		}
		tasks[stat.TaskID] = true
		report.TotalReceived += stat.Received
		report.TotalExecuted += stat.Executed
		report.TotalPending += stat.Pending
		report.TotalErrors += stat.Errors
		report.ActiveStreams++
		report.Streams = append(report.Streams, stat)

		// Lexicographic order is chronological order for the stored format,
		// which is zero-padded and most-significant-first.
		if stat.LastUpdated > report.LastUpdated {
			report.LastUpdated = stat.LastUpdated
		}
	}

	report.TasksCount = len(tasks)
	return report
}
