package infra

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/monitoring/domain"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
)

// qualifiedName matches a "database.table" a task may name. The two parts are
// interpolated into a COUNT statement, and they come from the task's
// configuration, so what they may contain is worth being explicit about: this
// used to be pasted in unquoted and unchecked, which makes every monitored table
// name a fragment of a query run against both databases.
var qualifiedName = regexp.MustCompile("^[A-Za-z_][A-Za-z0-9_$]*(\\.[A-Za-z_][A-Za-z0-9_$]*)?$")

// getRowCountWithContext is used by MySQL / MariaDB / PostgreSQL. It used to
// flatten every failure to -1 and hand that back as a count: a table that does
// not exist, a permission that was not granted, a connection that dropped and
// a cancelled context were indistinguishable from each other and, once stored,
// from a real measurement.
func getRowCountWithContext(ctx context.Context, db *sql.DB, table string) (int64, error) {
	if !qualifiedName.MatchString(table) {
		return 0, fmt.Errorf("%q is not a table name", table)
	}

	// The name goes into the statement unquoted: the four engines quote
	// identifiers differently — backticks for MySQL, double quotes for
	// PostgreSQL — and this function does not know which it is talking to. What
	// makes that safe is the check above, which admits nothing but an
	// identifier. The cost is that a table named after a reserved word cannot be
	// counted, which is a better trade than a name that can carry a clause.
	var cnt int64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&cnt); err != nil {
		return 0, err
	}
	return cnt, nil
}

// countOrMark reports a row count and the action to record it under, so that a
// measurement and a failure to measure are two different rows rather than one
// number nobody can interpret.
func countOrMark(ctx context.Context, db *sql.DB, table string, log *logrus.Logger) (int64, bool) {
	count, err := getRowCountWithContext(ctx, db, table)
	if err != nil {
		log.WithError(err).WithField("table", table).
			Error("[Monitor] Could not count the rows")
		return -1, false
	}
	return count, true
}

// actionFor names the kind of row being written.
const (
	actionRowCount = "row_count_minutely"
	// actionCountFailed marks a row whose counts could not be taken. Writing
	// nothing at all would leave the last good numbers on the dashboard looking
	// current; writing -1 under the ordinary action made a failure look like a
	// measurement.
	actionCountFailed = "row_count_failed"
)

func rowCountAction(srcOK, tgtOK bool) string {
	if srcOK && tgtOK {
		return actionRowCount
	}
	return actionCountFailed
}

// publishRowCounts reports a comparison to the scraper.
//
// Only the full comparison: the daily summary measures a window of one day,
// and publishing that under the same name would report a day's rows as the
// size of the object.
//
// The label is `object` rather than `table` because the four engines compare
// tables, collections and whole databases, and one name for the thing being
// compared is what lets one panel cover them all. A standalone Redis task
// compares a database and has no object name; it is published under the
// database's number.
func publishRowCounts(syncTaskID int, dbType, srcDB, object string,
	source, target int64, action string) {

	if action != actionRowCount && action != actionCountFailed {
		return
	}
	if object == "" {
		object = "db" + srcDB
	}
	metrics.SetRowCounts(metrics.Labels{
		"task":   strconv.Itoa(syncTaskID),
		"engine": strings.ToLower(dbType),
		"object": object,
	}, source, target, time.Now())
}

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

	// Insert/update statistics for each ChangeStream, active or not.
	//
	// An inactive stream used to be skipped, so its row kept whatever numbers it
	// had when it last reported — and a stream that had died and a stream that
	// simply had no events in the last minute looked identical. The row is
	// written either way; whether the stream is alive is a column.
	for collectionKey, csInfo := range activeStreams {
		pending := csInfo.ReceivedEvents - csInfo.ExecutedEvents
		if pending < 0 {
			pending = 0 // Ensure pending is not negative
		}

		if !csInfo.Active {
			logrus.Warnf("[MongoDB] The change stream for %s is not running; its "+
				"figures below are the last ones it reported", collectionKey)
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

// countBothEnds counts a pair's two sides at the same time.
//
// They are independent -- different servers, different pools -- and each is an
// exact count over a whole table, which is the expensive part of a pass. Asking
// them one after the other doubled how long a pass took for no reason.
func countBothEnds(ctx context.Context, source *sql.DB, sourceTable string,
	target *sql.DB, targetTable string, log *logrus.Logger) (
	sourceCount int64, sourceOK bool, targetCount int64, targetOK bool) {

	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		sourceCount, sourceOK = countOrMark(ctx, source, sourceTable, log)
	}()
	go func() {
		defer wait.Done()
		targetCount, targetOK = countOrMark(ctx, target, targetTable, log)
	}()
	wait.Wait()
	return sourceCount, sourceOK, targetCount, targetOK
}
