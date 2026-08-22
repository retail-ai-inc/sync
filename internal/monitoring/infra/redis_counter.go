package infra

import (
	"strings"
	// "github.com/sirupsen/logrus"
	"context"

	goredis "github.com/redis/go-redis/v9"
	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/sirupsen/logrus"
)

func CountAndLogRedis(ctx context.Context, sc config.SyncConfig, log *logrus.Logger) {
	dbType := strings.ToUpper(sc.Type)

	srcOptions, err := goredis.ParseURL(sc.SourceConnection)
	if err != nil {
		log.WithError(err).WithField("db_type", dbType).
			Error("[Monitor] Fail to parse source Redis DSN")
		return
	}
	srcClient := goredis.NewClient(srcOptions)
	defer srcClient.Close()

	if err := srcClient.Ping(ctx).Err(); err != nil {
		log.WithError(err).WithField("db_type", dbType).
			Error("[Monitor] Fail to connect to source Redis")
		return
	}

	tgtOptions, err := goredis.ParseURL(sc.TargetConnection)
	if err != nil {
		log.WithError(err).WithField("db_type", dbType).
			Error("[Monitor] Fail to parse target Redis DSN")
		return
	}
	tgtClient := goredis.NewClient(tgtOptions)
	defer tgtClient.Close()

	if err := tgtClient.Ping(ctx).Err(); err != nil {
		log.WithError(err).WithField("db_type", dbType).
			Error("[Monitor] Fail to connect to target Redis")
		return
	}

	srcDBName := dsn.GetDatabaseName(sc.Type, sc.SourceConnection)
	tgtDBName := dsn.GetDatabaseName(sc.Type, sc.TargetConnection)

	srcCount, srcErr := srcClient.DBSize(ctx).Result()
	if srcErr != nil {
		log.WithError(srcErr).WithField("db_type", dbType).
			Error("Failed to get source DB size")
		srcCount = -1
	}

	tgtCount, tgtErr := tgtClient.DBSize(ctx).Result()
	if tgtErr != nil {
		log.WithError(tgtErr).WithField("db_type", dbType).
			Error("Failed to get target DB size")
		tgtCount = -1
	}

	// One row. What is being reported is the size of each database, which has
	// nothing to do with how many mappings the task lists — and the loop used to
	// be over the mappings, so three mappings wrote the same row three times and
	// a task with none wrote nothing at all while the sizes were measured and
	// thrown away.
	action := rowCountAction(srcErr == nil, tgtErr == nil)
	log.WithFields(logrus.Fields{
		"db_type":        dbType,
		"src_db":         srcDBName,
		"src_row_count":  srcCount,
		"tgt_db":         tgtDBName,
		"tgt_row_count":  tgtCount,
		"monitor_action": action,
	}).Info(action)

	// Insert into database monitoring_log with sync_task_id
	storeMonitoringLog(sc.ID, dbType, srcDBName, "", srcCount, tgtDBName, "", tgtCount, action)
}

// getRowCount is used by MySQL / MariaDB / PostgreSQL
// func getRowCount(db *sql.DB, table string) int64 {
// 	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
// 	var cnt int64
// 	if err := db.QueryRow(query).Scan(&cnt); err != nil {
// 		return -1
// 	}
// 	return cnt
// }
