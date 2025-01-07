package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql" // MySQL / MariaDB driver
	_ "github.com/lib/pq"              // PostgreSQL driver
	"github.com/retail-ai-inc/sync/pkg/config"
	"github.com/retail-ai-inc/sync/pkg/logger"
	"github.com/retail-ai-inc/sync/pkg/syncer"
	"github.com/sirupsen/logrus"
	"github.com/retail-ai-inc/sync/pkg/utils"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Interval for row count monitoring every minute
const monitorInterval = time.Second * 60

func main() {
	// Initialize context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Capture system interrupt signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		logger.Log.Info("Received interrupt signal, exiting...")
		cancel()
	}()

	// Load configuration
	cfg := config.NewConfig()
	log := logger.InitLogger(cfg.LogLevel)

	// Start backend synchronization
	var wg sync.WaitGroup
	for _, syncCfg := range cfg.SyncConfigs {
		if !syncCfg.Enable {
			continue
		}
		wg.Add(1)
		switch syncCfg.Type {
		case "mongodb":
			go func(syncCfg config.SyncConfig) {
				defer wg.Done()
				syncer := syncer.NewMongoDBSyncer(syncCfg, log)
				syncer.Start(ctx)
			}(syncCfg)
		case "mysql":
			go func(syncCfg config.SyncConfig) {
				defer wg.Done()
				syncer := syncer.NewMySQLSyncer(syncCfg, log)
				syncer.Start(ctx)
			}(syncCfg)
		case "mariadb":
			go func(syncCfg config.SyncConfig) {
				defer wg.Done()
				syncer := syncer.NewMariaDBSyncer(syncCfg, log)
				syncer.Start(ctx)
			}(syncCfg)
		case "postgresql":
			go func(syncCfg config.SyncConfig) {
				defer wg.Done()
				syncer := syncer.NewPostgreSQLSyncer(syncCfg, log)
				syncer.Start(ctx)
			}(syncCfg)
		case "redis":
			go func(syncCfg config.SyncConfig) {
				defer wg.Done()
				syncer := syncer.NewRedisSyncer(syncCfg, log)
				syncer.Start(ctx)
			}(syncCfg)
		default:
			log.Errorf("Unknown sync type: %s", syncCfg.Type)
			wg.Done()
		}
	}

	// Start monitoring goroutine: output row counts every minute for each mapped table (source/target)
	if cfg.EnableTableRowCountMonitoring {
		utils.StartRowCountMonitoring(ctx, cfg, log, monitorInterval)
	}

	// Wait for all sync to complete
	wg.Wait()
	logger.Log.Info("All synchronization tasks have completed.")

	// Wait for program to end
	<-ctx.Done()
	logger.Log.Info("Program has exited")
}

// countAndLogTables logs row counts for a single database connection
func countAndLogTables(ctx context.Context, sc config.SyncConfig) {
	switch strings.ToLower(sc.Type) {
	case "mysql", "mariadb":
		countAndLogMySQLOrMariaDB(ctx, sc)
	case "postgresql":
		countAndLogPostgreSQL(ctx, sc)
	case "mongodb":
		countAndLogMongoDB(ctx, sc)
	default:
		// Unsupported or not needed
	}
}

// countAndLogMySQLOrMariaDB obtains row counts for MySQL / MariaDB tables
func countAndLogMySQLOrMariaDB(ctx context.Context, sc config.SyncConfig) {
	db, err := sql.Open("mysql", sc.SourceConnection)
	if err != nil {
		logger.Log.WithError(err).WithField("db_type", sc.Type).
			Error("[Monitor] Fail connect to source")
		return
	}
	defer db.Close()

	db2, err := sql.Open("mysql", sc.TargetConnection)
	if err != nil {
		logger.Log.WithError(err).WithField("db_type", sc.Type).
			Error("[Monitor] Fail connect to target")
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

			logger.Log.WithFields(logrus.Fields{
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
func countAndLogPostgreSQL(ctx context.Context, sc config.SyncConfig) {
	db, err := sql.Open("postgres", sc.SourceConnection)
	if err != nil {
		logger.Log.WithError(err).WithField("db_type", "POSTGRESQL").
			Error("[Monitor] Fail connect to source")
		return
	}
	defer db.Close()

	db2, err := sql.Open("postgres", sc.TargetConnection)
	if err != nil {
		logger.Log.WithError(err).WithField("db_type", "POSTGRESQL").
			Error("[Monitor] Fail connect to target")
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

			logger.Log.WithFields(logrus.Fields{
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
func countAndLogMongoDB(ctx context.Context, sc config.SyncConfig) {
	srcClient, err := mongo.Connect(ctx, options.Client().ApplyURI(sc.SourceConnection))
	if err != nil {
		logger.Log.WithError(err).WithField("db_type", "MONGODB").
			Error("[Monitor] Fail connect to source")
		return
	}
	defer func() {
		_ = srcClient.Disconnect(ctx)
	}()

	tgtClient, err := mongo.Connect(ctx, options.Client().ApplyURI(sc.TargetConnection))
	if err != nil {
		logger.Log.WithError(err).WithField("db_type", "MONGODB").
			Error("[Monitor] Fail connect to target")
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

			srcCount, _ := srcColl.EstimatedDocumentCount(ctx)
			tgtCount, _ := tgtColl.EstimatedDocumentCount(ctx)

			logger.Log.WithFields(logrus.Fields{
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

// getRowCount is a general SQL count function
func getRowCount(db *sql.DB, table string) int64 {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	var cnt int64
	if err := db.QueryRow(query).Scan(&cnt); err != nil {
		return -1
	}
	return cnt
}
