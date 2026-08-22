package infra

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/retail-ai-inc/sync/internal/monitoring/domain"

	// "github.com/sirupsen/logrus"

	"github.com/sirupsen/logrus"
)

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
	// The guard compares against JST, which is what the reset itself is aligned
	// to. It used to say 'localtime' — the machine's zone — so on a UTC host,
	// which is every container here, the two disagreed for nine hours a day and
	// the guard did nothing during that window.
	checkSQL := `
		SELECT COUNT(*), 
		       MAX(last_updated) as last_updated_time,
		       COALESCE(MAX(CASE WHEN DATE(last_updated, '+9 hours') = ? THEN 1 ELSE 0 END), 0) as reset_today
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

	lastUpdate, err := parseStoredTime(lastUpdatedTime.String)
	if err != nil {
		// A timestamp that will not parse used to abort the whole call, and
		// StoreChangeStreamStatistics returns before writing anything — so one
		// row with an unexpected format, from a hand edit or an older schema,
		// stopped every statistic for that task for good. Reset instead: the
		// counters are daily, so starting them again is the safe answer.
		logrus.Warnf("[MongoDB] task_id=%d has last_updated=%q, which is not a "+
			"timestamp this understands; the daily counters are being started "+
			"again: %v", syncTaskID, lastUpdatedTime.String, err)
		return resetInMemoryAndStored(tx, syncTaskID)
	}

	// Convert to JST for comparison
	lastUpdateJST := lastUpdate.UTC().In(jst)
	lastUpdateDateJST := time.Date(lastUpdateJST.Year(), lastUpdateJST.Month(), lastUpdateJST.Day(), 0, 0, 0, 0, jst)

	// Check if we need to reset (if last update was before today in JST)
	if lastUpdateDateJST.Before(today) {
		logrus.Infof("[MongoDB] Daily reset triggered for task_id=%d: last_date=%s (JST), today=%s (JST)",
			syncTaskID, lastUpdateDateJST.Format("2006-01-02"), today.Format("2006-01-02"))

		if err := resetInMemoryAndStored(tx, syncTaskID); err != nil {
			return err
		}
	} else {
		logrus.Debugf("[MongoDB] No daily reset needed for task_id=%d: last_date=%s is today in JST",
			syncTaskID, lastUpdateDateJST.Format("2006-01-02"))
	}

	return nil
}

// parseStoredTime reads a stored last_updated in the layouts SQLite may have
// written it in. It used to accept exactly one, so a row written by a different
// build — or edited by hand — stopped that task's statistics permanently.
func parseStoredTime(value string) (time.Time, error) {
	layouts := []string{
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02 15:04:05-07:00",
		time.RFC3339Nano,
	}
	var err error
	for _, layout := range layouts {
		var parsed time.Time
		if parsed, err = time.Parse(layout, value); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, err
}

// resetInMemoryAndStored starts a task's daily counters again.
func resetInMemoryAndStored(tx *sql.Tx, syncTaskID int) error {
	const resetSQL = `
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

	// Also reset in-memory domain.ChangeStreamInfo statistics for this sync task
	domain.ResetInMemoryStatistics(syncTaskID)

	logrus.Infof("[MongoDB] Daily statistics reset completed for task_id=%d: %d records reset (database + memory)",
		syncTaskID, rowsAffected)
	return nil
}
