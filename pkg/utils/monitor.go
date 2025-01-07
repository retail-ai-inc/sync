package utils

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/retail-ai-inc/sync/pkg/config"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	goredis "github.com/redis/go-redis/v9"
)

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

	db2, err := sql.Open("mysql", sc.TargetConnection)
	if err != nil {
		log.WithError(err).WithField("db_type", sc.Type).
			Error("[Monitor] Fail to connect to target")
		return
	}
	defer db2.Close()

	dbType := strings.ToUpper(sc.Type) // "MYSQL" or "MARIADB"
	for _, mapping := range sc.Mappings {
		srcDBName := mapping.SourceDatabase
		tgtDBName := mapping.TargetDatabase
		for _, tblMap := range mapping.Tables {
			srcName := tblMap.SourceTable
			tgtName := tblMap.TargetTable

			// Source table
			srcCount := getRowCount(db, fmt.Sprintf("%s.%s", srcDBName, srcName))
			// Target table
			tgtCount := getRowCount(db2, fmt.Sprintf("%s.%s", tgtDBName, tgtName))

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

	db2, err := sql.Open("postgres", sc.TargetConnection)
	if err != nil {
		log.WithError(err).WithField("db_type", "POSTGRESQL").
			Error("[Monitor] Fail to connect to target")
		return
	}
	defer db2.Close()

	for _, mapping := range sc.Mappings {
		srcDBName := mapping.SourceDatabase
		tgtDBName := mapping.TargetDatabase
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

			srcCount := getRowCount(db, fullSrc)
			tgtCount := getRowCount(db2, fullTgt)

			log.WithFields(logrus.Fields{
				"db_type":        "POSTGRESQL",
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

	for _, mapping := range sc.Mappings {
		srcDBName := mapping.SourceDatabase
		tgtDBName := mapping.TargetDatabase
		for _, tblMap := range mapping.Tables {
			srcName := tblMap.SourceTable
			tgtName := tblMap.TargetTable

			srcColl := srcClient.Database(srcDBName).Collection(srcName)
			tgtColl := tgtClient.Database(tgtDBName).Collection(tgtName)

			srcCount, err := srcColl.EstimatedDocumentCount(ctx)
			if err != nil {
				log.WithError(err).WithFields(logrus.Fields{
					"db_type":   "MONGODB",
					"src_db":    srcDBName,
					"src_coll":  srcName,
					"tgt_db":    tgtDBName,
					"tgt_coll":  tgtName,
					"operation": "source_count",
				}).Error("Failed to get source collection count")
				srcCount = -1
			}

			tgtCount, err := tgtColl.EstimatedDocumentCount(ctx)
			if err != nil {
				log.WithError(err).WithFields(logrus.Fields{
					"db_type":   "MONGODB",
					"tgt_db":    tgtDBName,
					"tgt_coll":  tgtName,
					"operation": "target_count",
				}).Error("Failed to get target collection count")
				tgtCount = -1
			}

			log.WithFields(logrus.Fields{
				"db_type":        "MONGODB",
				"src_db":         srcDBName,
				"src_coll":       srcName,
				"src_row_count":  srcCount,
				"tgt_db":         tgtDBName,
				"tgt_coll":       tgtName,
				"tgt_row_count":  tgtCount,
				"monitor_action": "row_count_minutely",
			}).Info("row_count_minutely")
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

	for _, mapping := range sc.Mappings {
		srcDBName := mapping.SourceDatabase
		tgtDBName := mapping.TargetDatabase

		log.WithFields(logrus.Fields{
			"db_type":        dbType,
			"src_db":         srcDBName,
			"src_row_count":  srcCount,
			"tgt_db":         tgtDBName,
			"tgt_row_count":  tgtCount,
			"monitor_action": "row_count_minutely",
		}).Info("row_count_minutely")
	}
}

func getRowCount(db *sql.DB, table string) int64 {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	var cnt int64
	if err := db.QueryRow(query).Scan(&cnt); err != nil {
		return -1
	}
	return cnt
}