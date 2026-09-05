package app

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/retail-ai-inc/sync/internal/monitoring/infra"

	"context"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/sirupsen/logrus"
)

// tasks reports the current task list rather than being read from cfg once.
// The row count sweep walks it on every tick, and a list captured at start-up went stale
// the moment a task was added, edited, disabled or deleted.
func StartRowCountMonitoring(ctx context.Context, cfg *config.Config, log *logrus.Logger,
	interval time.Duration, tasks func() []config.SyncConfig) {
	ticker := time.NewTicker(interval)

	watch(func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				jst, err := time.LoadLocation("Asia/Tokyo")
				if err != nil {
					log.Warnf("[Monitor] Failed to load JST timezone: %v, falling back to local time", err)
					jst = time.Local
				}

				now := time.Now().In(jst)
				nextRunTime := time.Date(now.Year(), now.Month(), now.Day(), 0, 5, 0, 0, jst)

				// If it's already past 00:05 today, schedule for tomorrow
				if now.After(nextRunTime) {
					nextRunTime = nextRunTime.AddDate(0, 0, 1)
				}

				durationUntilRun := nextRunTime.Sub(now)
				log.Infof("[Monitor] Daily summary scheduled to run at: %s (in %v)",
					nextRunTime.Format("2006-01-02 15:04:05 JST"), durationUntilRun)

				select {
				case <-ctx.Done():
					return
				case <-time.After(durationUntilRun):
					// Run daily summary for dateRange tables
					logYesterdayDataVolume(ctx, cfg, log)
				}
			}
		}
	})

	watch(func() {
		defer ticker.Stop()

		measure := func() {
			for _, sc := range tasks() {
				if !sc.Enable {
					continue
				}
				countAndLogTables(ctx, sc, log)
			}
		}

		// Once at startup, before waiting out the first interval. A process that
		// restarts more often than the interval used to produce no measurement
		// at all — and a restart is exactly when somebody wants to see one.
		measure()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				measure()
			}
		}
	})
}

func countAndLogTables(ctx context.Context, sc config.SyncConfig, log *logrus.Logger) {
	switch strings.ToLower(sc.Type) {
	case "mysql", "mariadb":
		infra.CountAndLogMySQLOrMariaDB(ctx, sc, log)
	case "postgresql":
		infra.CountAndLogPostgreSQL(ctx, sc, log)
	case "mongodb":
		infra.CountAndLogMongoDB(ctx, sc, log)
	case "redis":
		infra.CountAndLogRedis(ctx, sc, log)
	default:
		log.Debugf("Monitoring for type %s not implemented", sc.Type)
	}
}

func logYesterdayDataVolume(ctx context.Context, cfg *config.Config, log *logrus.Logger) {
	log.Infof("[Monitor] Starting daily summary for yesterday's data volume...")

	jst, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		log.Warnf("[Monitor] Failed to load JST timezone: %v, falling back to local time", err)
		jst = time.Local
	}

	now := time.Now().In(jst)
	yesterday := now.AddDate(0, 0, -1)
	yesterdayStart := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, jst)
	yesterdayEnd := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 23, 59, 59, 999999999, jst)

	log.Infof("[Monitor] Processing yesterday's data volume: %s to %s (JST)",
		yesterdayStart.Format("2006-01-02 15:04:05"), yesterdayEnd.Format("2006-01-02 15:04:05"))

	var covered, skipped []string
	for _, sc := range cfg.SyncConfigs {
		if !sc.Enable {
			continue
		}

		switch strings.ToLower(sc.Type) {
		case "mongodb":
			infra.LogYesterdayMongoDBVolume(ctx, sc, log, yesterdayStart, yesterdayEnd)
			covered = append(covered, strconv.Itoa(sc.ID))
		default:
			skipped = append(skipped, fmt.Sprintf("%d (%s)", sc.ID, sc.Type))
		}
	}

	// "Daily summary completed" used to be printed whatever had happened, and
	// only MongoDB is implemented — so a MySQL task with a dateRange condition
	// never appeared in a summary that said it was complete.
	if len(skipped) > 0 {
		log.Warnf("[Monitor] Daily summary covered %d task(s); it has no "+
			"implementation for %s, so those are missing from it",
			len(covered), strings.Join(skipped, ", "))
		return
	}
	log.Infof("[Monitor] Daily summary completed for %d task(s)", len(covered))
}
