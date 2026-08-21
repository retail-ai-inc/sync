package infra

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	// "github.com/sirupsen/logrus"
	"context"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/sirupsen/logrus"
)

// CountAndLogMySQLOrMariaDB obtains row counts for MySQL / MariaDB tables
func CountAndLogMySQLOrMariaDB(ctx context.Context, sc config.SyncConfig, log *logrus.Logger) {
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
	srcDBName := dsn.GetDatabaseName(sc.Type, sc.SourceConnection)
	tgtDBName := dsn.GetDatabaseName(sc.Type, sc.TargetConnection)

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
