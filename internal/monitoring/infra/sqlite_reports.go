package infra

import (
	"database/sql"
	"errors"

	"github.com/retail-ai-inc/sync/internal/monitoring/domain"
	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
)

// Reads behind the monitoring endpoints.
//
// The statements used to be written inside the HTTP handlers, which opened the
// control database themselves. Reading is this layer's job, and having it here
// is what lets the endpoints be about the request and the shape of the answer.

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
