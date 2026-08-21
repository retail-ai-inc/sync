package domain

import "strings"

// lower is strings.ToLower, named so the engine comparisons read the same way
// wherever they appear.
func lower(s string) string { return strings.ToLower(s) }

// TableStat is one table's replication progress for a day, as the tables
// endpoint reports it.
type TableStat struct {
	TableName    string
	SyncedToday  int64
	TotalRows    int64
	LastSyncTime string
}

// ClampSyncedToday corrects a negative daily delta to zero.
//
// The delta is MAX(tgt_row_count) - MIN(tgt_row_count) over the day, so it goes
// negative whenever rows were deleted, and the correction hides that rather
// than reporting it.
func ClampSyncedToday(n int64) int64 {
	if n < 0 {
		return 0
	}
	return n
}
