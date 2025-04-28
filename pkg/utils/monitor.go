package utils

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/retail-ai-inc/sync/pkg/config"
	"github.com/retail-ai-inc/sync/pkg/db"
	"github.com/retail-ai-inc/sync/pkg/syncer/common"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// StartRowCountMonitoring periodically logs row counts to console + DB
func StartRowCountMonitoring(ctx context.Context, cfg *config.Config, log *logrus.Logger, interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for _, sc := range cfg.SyncConfigs {
					if !sc.Enable {
						continue
					}
					countAndLogTables(ctx, sc, log)
				}
			}
		}
	}()
}

func countAndLogTables(ctx context.Context, sc config.SyncConfig, log *logrus.Logger) {
	switch strings.ToLower(sc.Type) {
	case "mysql", "mariadb":
		countAndLogMySQLOrMariaDB(ctx, sc, log)
	case "postgresql":
		countAndLogPostgreSQL(ctx, sc, log)
	case "mongodb":
		countAndLogMongoDB(ctx, sc, log)
	case "redis":
		countAndLogRedis(ctx, sc, log)
	default:
		log.Debugf("Monitoring for type %s not implemented", sc.Type)
	}
}

// countAndLogMySQLOrMariaDB obtains row counts for MySQL / MariaDB tables
func countAndLogMySQLOrMariaDB(ctx context.Context, sc config.SyncConfig, log *logrus.Logger) {
	db, err := sql.Open("mysql", sc.SourceConnection)
	if err != nil {
		log.WithError(err).WithField("db_type", sc.Type).
			Error("[Monitor] Fail to connect to source")
		return
	}
	defer db.Close()

	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	if err := db.PingContext(ctx); err != nil {
		log.WithError(err).WithField("db_type", sc.Type).
			Error("[Monitor] Fail to ping source database")
		return
	}

	db2, err := sql.Open("mysql", sc.TargetConnection)
	if err != nil {
		log.WithError(err).WithField("db_type", sc.Type).
			Error("[Monitor] Fail to connect to target")
		return
	}
	defer db2.Close()

	db2.SetConnMaxLifetime(time.Minute * 3)
	db2.SetMaxOpenConns(10)
	db2.SetMaxIdleConns(10)

	if err := db2.PingContext(ctx); err != nil {
		log.WithError(err).WithField("db_type", sc.Type).
			Error("[Monitor] Fail to ping target database")
		return
	}

	dbType := strings.ToUpper(sc.Type)
	srcDBName := common.GetDatabaseName(sc.Type, sc.SourceConnection)
	tgtDBName := common.GetDatabaseName(sc.Type, sc.TargetConnection)

	for _, mapping := range sc.Mappings {
		for _, tblMap := range mapping.Tables {
			srcName := tblMap.SourceTable
			tgtName := tblMap.TargetTable

			srcCount := getRowCountWithContext(ctx, db, fmt.Sprintf("%s.%s", srcDBName, srcName))
			tgtCount := getRowCountWithContext(ctx, db2, fmt.Sprintf("%s.%s", tgtDBName, tgtName))

			// 1) Log output
			log.WithFields(logrus.Fields{
				"db_type":        dbType,
				"src_db":         srcDBName,
				"src_table":      srcName,
				"src_row_count":  srcCount,
				"tgt_db":         tgtDBName,
				"tgt_table":      tgtName,
				"tgt_row_count":  tgtCount,
				"monitor_action": "row_count_minutely",
			}).Info("row_count_minutely")

			// 2) Insert into database monitoring_log with sync_task_id
			storeMonitoringLog(sc.ID, dbType, srcDBName, srcName, srcCount, tgtDBName, tgtName, tgtCount, "row_count_minutely")
		}
	}
}

// countAndLogPostgreSQL obtains row counts for PostgreSQL tables
func countAndLogPostgreSQL(ctx context.Context, sc config.SyncConfig, log *logrus.Logger) {
	db, err := sql.Open("postgres", sc.SourceConnection)
	if err != nil {
		log.WithError(err).WithField("db_type", "POSTGRESQL").
			Error("[Monitor] Fail to connect to source")
		return
	}
	defer db.Close()

	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	if err := db.PingContext(ctx); err != nil {
		log.WithError(err).WithField("db_type", "POSTGRESQL").
			Error("[Monitor] Fail to ping source database")
		return
	}

	db2, err := sql.Open("postgres", sc.TargetConnection)
	if err != nil {
		log.WithError(err).WithField("db_type", "POSTGRESQL").
			Error("[Monitor] Fail to connect to target")
		return
	}
	defer db2.Close()

	db2.SetConnMaxLifetime(time.Minute * 3)
	db2.SetMaxOpenConns(10)
	db2.SetMaxIdleConns(10)

	if err := db2.PingContext(ctx); err != nil {
		log.WithError(err).WithField("db_type", "POSTGRESQL").
			Error("[Monitor] Fail to ping target database")
		return
	}

	dbType := strings.ToUpper(sc.Type)
	srcDBName := common.GetDatabaseName(sc.Type, sc.SourceConnection)
	tgtDBName := common.GetDatabaseName(sc.Type, sc.TargetConnection)

	for _, mapping := range sc.Mappings {
		srcSchema := mapping.SourceSchema
		if srcSchema == "" {
			srcSchema = "public"
		}
		tgtSchema := mapping.TargetSchema
		if tgtSchema == "" {
			tgtSchema = "public"
		}

		for _, tblMap := range mapping.Tables {
			srcName := tblMap.SourceTable
			tgtName := tblMap.TargetTable

			fullSrc := fmt.Sprintf("%s.%s", srcSchema, srcName)
			fullTgt := fmt.Sprintf("%s.%s", tgtSchema, tgtName)

			srcCount := getRowCountWithContext(ctx, db, fullSrc)
			tgtCount := getRowCountWithContext(ctx, db2, fullTgt)

			log.WithFields(logrus.Fields{
				"db_type":        dbType,
				"src_schema":     srcSchema,
				"src_table":      srcName,
				"src_db":         srcDBName,
				"src_row_count":  srcCount,
				"tgt_schema":     tgtSchema,
				"tgt_table":      tgtName,
				"tgt_db":         tgtDBName,
				"tgt_row_count":  tgtCount,
				"monitor_action": "row_count_minutely",
			}).Info("row_count_minutely")

			// Insert into database monitoring_log with sync_task_id
			storeMonitoringLog(sc.ID, dbType, srcDBName, srcName, srcCount, tgtDBName, tgtName, tgtCount, "row_count_minutely")
		}
	}
}

// countAndLogMongoDB obtains document counts for MongoDB collections
func countAndLogMongoDB(ctx context.Context, sc config.SyncConfig, log *logrus.Logger) {
	srcClient, err := mongo.Connect(ctx, options.Client().ApplyURI(sc.SourceConnection))
	if err != nil {
		log.WithError(err).WithField("db_type", "MONGODB").
			Error("[Monitor] Fail to connect to source")
		return
	}
	defer func() {
		_ = srcClient.Disconnect(ctx)
	}()

	tgtClient, err := mongo.Connect(ctx, options.Client().ApplyURI(sc.TargetConnection))
	if err != nil {
		log.WithError(err).WithField("db_type", "MONGODB").
			Error("[Monitor] Fail to connect to target")
		return
	}
	defer func() {
		_ = tgtClient.Disconnect(ctx)
	}()

	dbType := strings.ToUpper(sc.Type)
	srcDBName := common.GetDatabaseName(sc.Type, sc.SourceConnection)
	tgtDBName := common.GetDatabaseName(sc.Type, sc.TargetConnection)

	for _, mapping := range sc.Mappings {
		for _, tblMap := range mapping.Tables {
			srcColl := srcClient.Database(srcDBName).Collection(tblMap.SourceTable)
			tgtColl := tgtClient.Database(tgtDBName).Collection(tblMap.TargetTable)

			srcCount, err := srcColl.EstimatedDocumentCount(ctx)
			if err != nil {
				log.WithError(err).WithFields(logrus.Fields{
					"db_type":   dbType,
					"src_db":    srcDBName,
					"src_coll":  tblMap.SourceTable,
					"tgt_db":    tgtDBName,
					"tgt_coll":  tblMap.TargetTable,
					"operation": "source_count",
				}).Error("Failed to get source collection count")
				srcCount = -1
			}

			tgtCount, err := tgtColl.EstimatedDocumentCount(ctx)
			if err != nil {
				log.WithError(err).WithFields(logrus.Fields{
					"db_type":   dbType,
					"tgt_db":    tgtDBName,
					"tgt_coll":  tblMap.TargetTable,
					"operation": "target_count",
				}).Error("Failed to get target collection count")
				tgtCount = -1
			}

			log.WithFields(logrus.Fields{
				"db_type":        dbType,
				"src_db":         srcDBName,
				"src_coll":       tblMap.SourceTable,
				"src_row_count":  srcCount,
				"tgt_db":         tgtDBName,
				"tgt_coll":       tblMap.TargetTable,
				"tgt_row_count":  tgtCount,
				"monitor_action": "row_count_minutely",
			}).Info("row_count_minutely")

			// Insert into database monitoring_log with sync_task_id
			storeMonitoringLog(sc.ID, dbType, srcDBName, tblMap.SourceTable, srcCount, tgtDBName, tblMap.TargetTable, tgtCount, "row_count_minutely")
		}
	}
}

func countAndLogRedis(ctx context.Context, sc config.SyncConfig, log *logrus.Logger) {
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

	srcDBName := common.GetDatabaseName(sc.Type, sc.SourceConnection)
	tgtDBName := common.GetDatabaseName(sc.Type, sc.TargetConnection)

	srcCount, err := srcClient.DBSize(ctx).Result()
	if err != nil {
		log.WithError(err).WithField("db_type", dbType).
			Error("Failed to get source DB size")
		srcCount = -1
	}

	tgtCount, err := tgtClient.DBSize(ctx).Result()
	if err != nil {
		log.WithError(err).WithField("db_type", dbType).
			Error("Failed to get target DB size")
		tgtCount = -1
	}

	for range sc.Mappings {
		log.WithFields(logrus.Fields{
			"db_type":        dbType,
			"src_db":         srcDBName,
			"src_row_count":  srcCount,
			"tgt_db":         tgtDBName,
			"tgt_row_count":  tgtCount,
			"monitor_action": "row_count_minutely",
		}).Info("row_count_minutely")

		// Insert into database monitoring_log with sync_task_id
		storeMonitoringLog(sc.ID, dbType, srcDBName, "", srcCount, tgtDBName, "", tgtCount, "row_count_minutely")
	}
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

	db, err := db.OpenSQLiteDB()
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
