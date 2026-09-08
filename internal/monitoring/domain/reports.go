package domain

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
