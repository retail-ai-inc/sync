package infra

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"context"

	_ "github.com/go-sql-driver/mysql" // this package opens MySQL itself
	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/sirupsen/logrus"
)

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

	pairs, err := sqlPairs(ctx, sc, db, srcDBName)
	if err != nil {
		log.WithError(err).WithField("db_type", dbType).
			Error("[Monitor] Could not read the source's tables")
		return
	}

	for _, pair := range pairs {
		srcName, tgtName := pair.Source, pair.Target

		srcCount, srcOK := countOrMark(ctx, db, fmt.Sprintf("%s.%s", srcDBName, srcName), log)
		tgtCount, tgtOK := countOrMark(ctx, db2, fmt.Sprintf("%s.%s", tgtDBName, tgtName), log)
		action := rowCountAction(srcOK, tgtOK)

		log.WithFields(logrus.Fields{
			"db_type":        dbType,
			"src_db":         srcDBName,
			"src_table":      srcName,
			"src_row_count":  srcCount,
			"tgt_db":         tgtDBName,
			"tgt_table":      tgtName,
			"tgt_row_count":  tgtCount,
			"monitor_action": action,
		}).Info(action)

		storeMonitoringLog(sc.ID, dbType, srcDBName, srcName, srcCount, tgtDBName, tgtName, tgtCount, action)
	}
}
