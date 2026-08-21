package infra

import (
	"database/sql"
	"fmt"
	"time"

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

// resetDailyStatisticsIfNeeded checks if it's a new day and resets statistics if needed
func resetDailyStatisticsIfNeeded(tx *sql.Tx, syncTaskID int) error {
	// Use Japan timezone (JST) for daily reset logic
	jst, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		logrus.Warnf("[MongoDB] Failed to load JST timezone: %v, falling back to local time", err)
		jst = time.Local
	}

	now := time.Now().In(jst)
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, jst)

	// Check if any records exist for this sync task and if we already reset today
	checkSQL := `
		SELECT COUNT(*), 
		       MAX(last_updated) as last_updated_time,
		       COALESCE(MAX(CASE WHEN DATE(last_updated, 'localtime') = ? THEN 1 ELSE 0 END), 0) as reset_today
		FROM changestream_statistics 
		WHERE task_id = ?
	`

	todayJSTStr := today.Format("2006-01-02")
	var recordCount int
	var lastUpdatedTime sql.NullString
	var resetToday int

	err = tx.QueryRow(checkSQL, todayJSTStr, syncTaskID).Scan(&recordCount, &lastUpdatedTime, &resetToday)
	if err != nil {
		return fmt.Errorf("failed to check existing records: %w", err)
	}

	// If no records exist, no need to reset
	if recordCount == 0 {
		logrus.Debugf("[MongoDB] No existing records for task_id=%d, skipping daily reset check", syncTaskID)
		return nil
	}

	// If we already reset today, skip
	if resetToday > 0 {
		logrus.Debugf("[MongoDB] Already reset today for task_id=%d, skipping daily reset", syncTaskID)
		return nil
	}

	// Parse the last update time to determine if we need to reset
	if !lastUpdatedTime.Valid {
		logrus.Debugf("[MongoDB] No valid last_updated time found for task_id=%d", syncTaskID)
		return nil
	}

	lastUpdate, err := time.Parse("2006-01-02 15:04:05", lastUpdatedTime.String)
	if err != nil {
		return fmt.Errorf("failed to parse last update time: %w", err)
	}

	// Convert to JST for comparison
	lastUpdateJST := lastUpdate.UTC().In(jst)
	lastUpdateDateJST := time.Date(lastUpdateJST.Year(), lastUpdateJST.Month(), lastUpdateJST.Day(), 0, 0, 0, 0, jst)

	// Check if we need to reset (if last update was before today in JST)
	if lastUpdateDateJST.Before(today) {
		logrus.Infof("[MongoDB] Daily reset triggered for task_id=%d: last_date=%s (JST), today=%s (JST)",
			syncTaskID, lastUpdateDateJST.Format("2006-01-02"), today.Format("2006-01-02"))

		// Reset all statistics to 0 for this sync task
		resetSQL := `
			UPDATE changestream_statistics 
			SET received = 0,
				executed = 0,
				pending = 0,
				errors = 0,
				inserted = 0,
				updated = 0,
				deleted = 0,
				last_updated = CURRENT_TIMESTAMP
			WHERE task_id = ?
		`

		result, err := tx.Exec(resetSQL, syncTaskID)
		if err != nil {
			return fmt.Errorf("failed to reset daily statistics: %w", err)
		}

		rowsAffected, _ := result.RowsAffected()

		// CRITICAL FIX: Also reset in-memory domain.ChangeStreamInfo statistics for this sync task
		domain.ResetInMemoryStatistics(syncTaskID)

		logrus.Infof("[MongoDB] Daily statistics reset completed for task_id=%d: %d records reset (database + memory)",
			syncTaskID, rowsAffected)
	} else {
		logrus.Debugf("[MongoDB] No daily reset needed for task_id=%d: last_date=%s is today in JST",
			syncTaskID, lastUpdateDateJST.Format("2006-01-02"))
	}

	return nil
}
