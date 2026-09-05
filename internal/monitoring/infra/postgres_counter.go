package infra

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"context"

	_ "github.com/lib/pq" // this package opens PostgreSQL itself
	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/sirupsen/logrus"
)

func CountAndLogPostgreSQL(ctx context.Context, sc config.SyncConfig, log *logrus.Logger) {
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
	srcDBName := dsn.GetDatabaseName(sc.Type, sc.SourceConnection)
	tgtDBName := dsn.GetDatabaseName(sc.Type, sc.TargetConnection)

	// One open for the pass, not one per table.
	record := openMonitoringLog()
	defer record.close()

	// One pass per schema the task names, and the default schema when it names
	// none -- a task that lists no tables replicates the whole of it.
	schemas := sc.Mappings
	if len(schemas) == 0 {
		schemas = []config.DatabaseMapping{{}}
	}

	for _, mapping := range schemas {
		srcSchema := mapping.SourceSchema
		if srcSchema == "" {
			srcSchema = "public"
		}
		tgtSchema := mapping.TargetSchema
		if tgtSchema == "" {
			tgtSchema = "public"
		}

		pairs, err := postgresPairs(ctx, mapping, db, srcSchema)
		if err != nil {
			log.WithError(err).WithField("db_type", dbType).
				Error("[Monitor] Could not read the source's tables")
			return
		}

		for _, pair := range pairs {
			srcName, tgtName := pair.Source, pair.Target

			fullSrc := fmt.Sprintf("%s.%s", srcSchema, srcName)
			fullTgt := fmt.Sprintf("%s.%s", tgtSchema, tgtName)

			srcCount, srcOK, tgtCount, tgtOK := countBothEnds(ctx, db, fullSrc, db2, fullTgt, log)
			action := rowCountAction(srcOK, tgtOK)

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
				"monitor_action": action,
			}).Info(action)

			// Insert into database monitoring_log with sync_task_id
			record.write(sc.ID, dbType, srcDBName, srcName, srcCount, tgtDBName, tgtName, tgtCount, action)
		}
	}
}
