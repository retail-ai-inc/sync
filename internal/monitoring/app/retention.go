package app

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
)

// The monitoring log grows without bound: a row per table per monitoring
// interval, for as long as the syncer runs. On a staging deployment monitoring a
// few dozen tables every minute it had reached 565 MB, on the same volume the
// control-plane database and the change buffer live on — so what fills up is not
// a log directory somebody can truncate, it is the disk replication itself
// depends on.
//
// Old rows are also worth very little. The log answers "is the replica keeping
// up" and "what did it do last night"; nobody reads the row counts from four
// months ago, and the daily summaries are derived from them at the time.
const (
	defaultRetentionDays = 30
	// retentionSweepEvery is how often rows past that are removed. Daily,
	// because the point is to bound the size rather than to be prompt.
	retentionSweepEvery = 24 * time.Hour
	// retentionBatch caps one delete, so the sweep does not hold the single
	// SQLite writer for as long as it takes to remove a year of rows. The
	// syncer shares that writer with everything else it records.
	retentionBatch = 5000
)

// retentionDays reports how much monitoring history to keep, which
// SYNC_MONITORING_RETENTION_DAYS overrides. Zero or less turns the sweep off,
// which is a choice an operator can make and this one will not make for them.
func retentionDays() int {
	raw := os.Getenv("SYNC_MONITORING_RETENTION_DAYS")
	if raw == "" {
		return defaultRetentionDays
	}
	days, err := strconv.Atoi(raw)
	if err != nil {
		return defaultRetentionDays
	}
	return days
}

// StartMonitoringRetention removes monitoring rows older than the retention
// window, on a schedule, for as long as the context lives.
func StartMonitoringRetention(ctx context.Context, log *logrus.Logger) {
	days := retentionDays()
	if days <= 0 {
		log.Warn("[Monitor] Monitoring history is kept indefinitely; the log grows " +
			"by a row per table per interval and shares a volume with the " +
			"replication state. Set SYNC_MONITORING_RETENTION_DAYS to bound it.")
		return
	}

	log.Infof("[Monitor] Keeping %d days of monitoring history", days)
	watch(func() {
		// Once at startup: a deployment that has been running without this for
		// months should not wait a day for the first sweep.
		sweepMonitoringLog(ctx, log, days)

		ticker := time.NewTicker(retentionSweepEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sweepMonitoringLog(ctx, log, days)
			}
		}
	})
}

func sweepMonitoringLog(ctx context.Context, log *logrus.Logger, days int) {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		log.Errorf("[Monitor] Could not open the local database to trim the "+
			"monitoring log: %v", err)
		return
	}
	defer db.Close()

	cutoff := time.Now().UTC().AddDate(0, 0, -days).Format("2006-01-02 15:04:05")
	removed, err := deleteOlderThan(ctx, db, cutoff)
	if err != nil {
		log.Errorf("[Monitor] Could not trim the monitoring log: %v", err)
		return
	}
	if removed > 0 {
		log.Infof("[Monitor] Removed %d monitoring rows older than %s", removed, cutoff)
	}
}

func deleteOlderThan(ctx context.Context, db *sql.DB, cutoff string) (int64, error) {
	var total int64
	for {
		result, err := db.ExecContext(ctx,
			`DELETE FROM monitoring_log WHERE id IN (
			   SELECT id FROM monitoring_log WHERE logged_at < ? LIMIT ?)`,
			cutoff, retentionBatch)
		if err != nil {
			return total, fmt.Errorf("delete monitoring rows before %s: %w", cutoff, err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return total, err
		}
		total += affected
		if affected < retentionBatch {
			return total, nil
		}
		// Let the writer go for a moment: the syncer records through the same
		// single SQLite connection.
		select {
		case <-ctx.Done():
			return total, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
