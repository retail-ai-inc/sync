package infra

import (
	"database/sql"
	"errors"
	"time"

	"github.com/retail-ai-inc/sync/internal/monitoring/domain"
	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
)

// Reads behind the monitoring endpoints.
//
// The statements used to be written inside the HTTP handlers, which opened the
// control database themselves. Reading is this layer's job, and having it here
// is what lets the endpoints be about the request and the shape of the answer.

// The stored time format, which has no zone.
const storedTimeFormat = "2006-01-02 15:04:05"

// ErrNoTask reports that a task id names no row, which the monitor endpoint
// answers differently from a failure to read.
var ErrNoTask = errors.New("no such task")

// TaskEnabled reports whether a task is switched on.
func TaskEnabled(taskID string) (bool, error) {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return false, err
	}
	defer db.Close()

	var enable sql.NullInt32
	switch err := db.QueryRow(`SELECT enable FROM sync_tasks WHERE id=?`, taskID).
		Scan(&enable); {
	case errors.Is(err, sql.ErrNoRows):
		return false, ErrNoTask
	case err != nil:
		return false, err
	}
	return enable.Int32 == 1, nil
}

// RowCountHistory reads the stored source/target comparisons for one task, or
// for every task when taskID is "0".
//
// A zero since means no window. The limit is the one the chart draws and is
// applied in the database rather than after: a task logged every minute for a
// month is forty thousand rows.
func RowCountHistory(taskID string, since time.Time) ([]domain.RowCountSample, error) {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	query := `
SELECT logged_at, tgt_table, src_row_count, tgt_row_count, sync_task_id
FROM monitoring_log
`
	var params []interface{}
	var clauses []string
	if taskID != "0" {
		clauses = append(clauses, "sync_task_id=?")
		params = append(params, taskID)
	}
	if !since.IsZero() {
		clauses = append(clauses, "logged_at >= ?")
		params = append(params, since.UTC().Format(storedTimeFormat))
	}
	for i, clause := range clauses {
		if i == 0 {
			query += "WHERE " + clause + "\n"
			continue
		}
		query += "  AND " + clause + "\n"
	}
	query += "ORDER BY logged_at ASC\nLIMIT 1000\n"

	rows, err := db.Query(query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var samples []domain.RowCountSample
	for rows.Next() {
		var sample domain.RowCountSample
		if err := rows.Scan(&sample.LoggedAt, &sample.Table,
			&sample.Source, &sample.Target, &sample.TaskID); err != nil {
			return nil, err
		}
		samples = append(samples, sample)
	}
	return samples, rows.Err()
}

// TaskLogs reads a task's most recent log lines, newest first.
//
// The level and search filters are not applied here. They are applied to what
// this returns, which is the behaviour the endpoint has always had: the limit
// counts stored lines, so filtering narrows the window rather than reaching
// further back for more matches.
func TaskLogs(taskID string, since time.Time) ([]domain.LogEntry, error) {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	query := `
SELECT log_time, level, message
FROM sync_log
WHERE sync_task_id=?
`
	params := []interface{}{taskID}
	if !since.IsZero() {
		query += "  AND log_time >= ?\n"
		params = append(params, since.UTC().Format(storedTimeFormat))
	}
	query += "ORDER BY log_time DESC\nLIMIT 500\n"

	rows, err := db.Query(query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []domain.LogEntry
	for rows.Next() {
		var entry domain.LogEntry
		if err := rows.Scan(&entry.LoggedAt, &entry.Level, &entry.Message); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

// ChangeStreamStatistics reads every stored change stream counter.
//
// A row that cannot be scanned is skipped rather than failing the read: the
// endpoint is a status page, and one malformed row should not blank the rest.
func ChangeStreamStatistics() ([]domain.ChangeStreamStat, error) {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query(`
SELECT task_id, collection_name, received, executed, pending, errors,
       inserted, updated, deleted, last_updated
FROM changestream_statistics
ORDER BY task_id, collection_name
`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []domain.ChangeStreamStat
	for rows.Next() {
		var stat domain.ChangeStreamStat
		if err := rows.Scan(&stat.TaskID, &stat.Collection, &stat.Received,
			&stat.Executed, &stat.Pending, &stat.Errors, &stat.Inserted,
			&stat.Updated, &stat.Deleted, &stat.LastUpdated); err != nil {
			continue
		}
		stats = append(stats, stat)
	}
	return stats, rows.Err()
}
