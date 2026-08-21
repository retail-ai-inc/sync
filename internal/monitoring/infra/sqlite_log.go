package infra

import (
	"database/sql"
	"fmt"

	"github.com/retail-ai-inc/sync/internal/monitoring/domain"

	"github.com/retail-ai-inc/sync/internal/platform/sqlite"

	// "github.com/sirupsen/logrus"
	"context"

	"github.com/sirupsen/logrus"
)

// getRowCountWithContext is used by MySQL / MariaDB / PostgreSQL
func getRowCountWithContext(ctx context.Context, db *sql.DB, table string) int64 {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	var cnt int64
	if err := db.QueryRowContext(ctx, query).Scan(&cnt); err != nil {
		return -1
	}
	return cnt
}

// ------------------------------------------------------------------
// Added function: Insert monitoring results into the monitoring_log table
func storeMonitoringLog(syncTaskID int, dbType, srcDB, srcTable string, srcCount int64,
	tgtDB, tgtTable string, tgtCount int64, action string) {

	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		// If failed, just log the error
		logrus.Errorf("Failed to open local DB for monitoring_log: %v", err)
		return
	}
	defer db.Close()

	const insSQL = `
INSERT INTO monitoring_log (
	sync_task_id,
	db_type,
	src_db,
	src_table,
	src_row_count,
	tgt_db,
	tgt_table,
	tgt_row_count,
	monitor_action
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
`
	_, err = db.Exec(insSQL,
		syncTaskID,
		dbType,
		srcDB,
		srcTable,
		srcCount,
		tgtDB,
		tgtTable,
		tgtCount,
		action,
	)
	if err != nil {
		logrus.Errorf("Failed to insert into monitoring_log: %v", err)
	}
}

// StoreChangeStreamStatistics stores ChangeStream statistics to changestream_statistics table
func StoreChangeStreamStatistics(syncTaskID int, activeStreams map[string]*domain.ChangeStreamInfo) error {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return fmt.Errorf("failed to open local DB for changestream_statistics: %w", err)
	}
	defer db.Close()

	// Begin transaction for better performance
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Check if we need to reset daily statistics (at midnight)
	if err := resetDailyStatisticsIfNeeded(tx, syncTaskID); err != nil {
		return fmt.Errorf("failed to reset daily statistics: %w", err)
	}

	const upsertSQL = `
INSERT INTO changestream_statistics (
	task_id,
	collection_name,
	received,
	executed,
	pending,
	errors,
	inserted,
	updated,
	deleted,
	last_updated
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
ON CONFLICT(task_id, collection_name) DO UPDATE SET
	received = excluded.received,
	executed = excluded.executed,
	pending = excluded.pending,
	errors = excluded.errors,
	inserted = excluded.inserted,
	updated = excluded.updated,
	deleted = excluded.deleted,
	last_updated = CURRENT_TIMESTAMP;
`

	stmt, err := tx.Prepare(upsertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare upsert statement: %w", err)
	}
	defer stmt.Close()

	// Insert/update statistics for each active ChangeStream
	for collectionKey, csInfo := range activeStreams {
		if !csInfo.Active {
			continue // Skip inactive streams
		}

		pending := csInfo.ReceivedEvents - csInfo.ExecutedEvents
		if pending < 0 {
			pending = 0 // Ensure pending is not negative
		}

		_, err = stmt.Exec(
			syncTaskID,
			collectionKey,
			csInfo.ReceivedEvents,
			csInfo.ExecutedEvents,
			pending,
			csInfo.ErrorCount,
			csInfo.InsertedCount,
			csInfo.UpdatedCount,
			csInfo.DeletedCount,
		)
		if err != nil {
			logrus.Errorf("Failed to upsert changestream_statistics for %s: %v", collectionKey, err)
			continue
		}

		logrus.Debugf("[MongoDB] Updated changestream_statistics: task_id=%d, collection=%s, received=%d, executed=%d, pending=%d",
			syncTaskID, collectionKey, csInfo.ReceivedEvents, csInfo.ExecutedEvents, pending)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit changestream_statistics transaction: %w", err)
	}

	logrus.Debugf("[MongoDB] Successfully stored ChangeStream statistics for task_id=%d (%d active streams)",
		syncTaskID, len(activeStreams))
	return nil
}
