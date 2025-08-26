package utils

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/retail-ai-inc/sync/pkg/config"
	"github.com/retail-ai-inc/sync/pkg/db"
	"github.com/retail-ai-inc/sync/pkg/syncer/common"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ChangeStreamInfo tracks info about a single ChangeStream
type ChangeStreamInfo struct {
	SyncTaskID     int       // Sync task ID this ChangeStream belongs to
	Database       string    // Database name
	Collection     string    // Collection name
	Created        time.Time // Creation time
	LastActivity   time.Time // Last activity time
	EventCount     int64     // Number of processed events
	ErrorCount     int       // Error count
	Active         bool      // Whether it's active
	LastErrorMsg   string    // Last error message
	LastErrorTime  time.Time // Last error time
	ReceivedEvents int       // Number of received events
	ExecutedEvents int       // Number of executed events
	// Detailed operation counts
	InsertedCount int // Number of insert operations
	UpdatedCount  int // Number of update/replace operations
	DeletedCount  int // Number of delete operations
}

var (
	// changeStreamTracker stores information about all active ChangeStreams
	changeStreamTracker = make(map[string]*ChangeStreamInfo)
	csTrackerMutex      = &sync.RWMutex{}
)

// RegisterChangeStream registers a new ChangeStream
func RegisterChangeStream(syncTaskID int, database, collection string) {
	key := fmt.Sprintf("%s.%s", database, collection)
	csTrackerMutex.Lock()
	defer csTrackerMutex.Unlock()

	changeStreamTracker[key] = &ChangeStreamInfo{
		SyncTaskID:   syncTaskID,
		Database:     database,
		Collection:   collection,
		Created:      time.Now(),
		LastActivity: time.Now(),
		Active:       true,
	}
}

// UpdateChangeStreamActivity updates ChangeStream activity information
func UpdateChangeStreamActivity(database, collection string, eventCount int, receivedEvents, executedEvents int) {
	csTrackerMutex.Lock()
	defer csTrackerMutex.Unlock()

	key := fmt.Sprintf("%s.%s", database, collection)
	if stream, exists := changeStreamTracker[key]; exists {
		stream.LastActivity = time.Now()
		stream.EventCount += int64(eventCount)
		stream.ReceivedEvents = receivedEvents
		stream.ExecutedEvents = executedEvents
	}
}

// UpdateChangeStreamDetailedActivity updates ChangeStream activity with detailed operation counts
func UpdateChangeStreamDetailedActivity(database, collection string, eventCount int, receivedEvents, executedEvents, insertedCount, updatedCount, deletedCount int) {
	csTrackerMutex.Lock()
	defer csTrackerMutex.Unlock()

	key := fmt.Sprintf("%s.%s", database, collection)
	if stream, exists := changeStreamTracker[key]; exists {
		stream.LastActivity = time.Now()
		stream.EventCount += int64(eventCount)
		stream.ReceivedEvents = receivedEvents
		stream.ExecutedEvents = executedEvents
		stream.InsertedCount = insertedCount
		stream.UpdatedCount = updatedCount
		stream.DeletedCount = deletedCount
	}
}

// AccumulateChangeStreamActivity accumulates ChangeStream statistics instead of replacing them
func AccumulateChangeStreamActivity(database, collection string, eventCount, receivedEvents, executedEvents, insertedCount, updatedCount, deletedCount int) {
	csTrackerMutex.Lock()
	defer csTrackerMutex.Unlock()

	key := fmt.Sprintf("%s.%s", database, collection)
	if stream, exists := changeStreamTracker[key]; exists {
		stream.LastActivity = time.Now()
		stream.EventCount += int64(eventCount)
		stream.ReceivedEvents += receivedEvents
		stream.ExecutedEvents += executedEvents
		stream.InsertedCount += insertedCount
		stream.UpdatedCount += updatedCount
		stream.DeletedCount += deletedCount
	}
}

// RecordChangeStreamError records ChangeStream errors
func RecordChangeStreamError(database, collection, errorMsg string) {
	key := fmt.Sprintf("%s.%s", database, collection)
	csTrackerMutex.Lock()
	defer csTrackerMutex.Unlock()

	if cs, exists := changeStreamTracker[key]; exists {
		cs.ErrorCount++
		cs.LastErrorMsg = errorMsg
		cs.LastErrorTime = time.Now()
	}
}

// DeactivateChangeStream marks a ChangeStream as inactive
func DeactivateChangeStream(database, collection string) {
	key := fmt.Sprintf("%s.%s", database, collection)
	csTrackerMutex.Lock()
	defer csTrackerMutex.Unlock()

	if cs, exists := changeStreamTracker[key]; exists {
		cs.Active = false
	}
}

// GetActiveChangeStreams gets information about all active ChangeStreams
func GetActiveChangeStreams() map[string]*ChangeStreamInfo {
	csTrackerMutex.RLock()
	defer csTrackerMutex.RUnlock()

	// Create a copy to avoid concurrency issues
	result := make(map[string]*ChangeStreamInfo, len(changeStreamTracker))
	for k, v := range changeStreamTracker {
		result[k] = v
	}
	return result
}

// GetActiveChangeStreamsByTaskID gets information about active ChangeStreams for a specific sync task
func GetActiveChangeStreamsByTaskID(syncTaskID int) map[string]*ChangeStreamInfo {
	csTrackerMutex.RLock()
	defer csTrackerMutex.RUnlock()

	// Create a copy filtering by sync task ID
	result := make(map[string]*ChangeStreamInfo)
	for k, v := range changeStreamTracker {
		if v.SyncTaskID == syncTaskID && v.Active {
			result[k] = v
		}
	}
	return result
}

// StartRowCountMonitoring periodically logs row counts to console + DB
func StartRowCountMonitoring(ctx context.Context, cfg *config.Config, log *logrus.Logger, interval time.Duration) {
	ticker := time.NewTicker(interval)

	// Start daily summary at 00:05 JST
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				// Calculate time until next 00:05 JST
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

				// Wait until the scheduled time
				select {
				case <-ctx.Done():
					return
				case <-time.After(durationUntilRun):
					// Run daily summary for dateRange tables
					logYesterdayDataVolume(ctx, cfg, log)
				}
			}
		}
	}()

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
			var srcCount int64
			var tgtCount int64
			var err error

			// Parse count query if exists
			var countQuery *CountQuery
			if tblMap.CountQuery != nil && len(tblMap.CountQuery) > 0 {
				if conditions, ok := tblMap.CountQuery["conditions"]; ok {
					conditionBytes, err := json.Marshal(conditions)
					if err == nil {
						var conditionsList []CountCondition
						if err := json.Unmarshal(conditionBytes, &conditionsList); err == nil {
							countQuery = &CountQuery{
								Conditions: conditionsList,
							}
							log.Debugf("[Monitor] Using conditions for %s: %+v",
								tblMap.SourceTable, countQuery.Conditions)
						}
					}
				}
			}

			queryCounter := NewQueryCounter(log)

			// Count source collection
			srcCount, err = queryCounter.CountMongoDBDocuments(ctx, srcClient, srcDBName, tblMap.SourceTable, countQuery)
			if err != nil {
				log.WithError(err).WithFields(logrus.Fields{
					"db_type":   dbType,
					"src_db":    srcDBName,
					"src_coll":  tblMap.SourceTable,
					"operation": "source_count",
				}).Error("Failed to get source collection count")
				srcCount = -1
			}

			// Count target collection
			tgtCount, err = queryCounter.CountMongoDBDocuments(ctx, tgtClient, tgtDBName, tblMap.TargetTable, countQuery)
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

	// Log comprehensive ChangeStream status for each sync task
	activeStreams := GetActiveChangeStreamsByTaskID(sc.ID)

	// Always check ChangeStream statistics and daily reset
	csDetails := make([]string, 0, len(activeStreams))
	activeCount := 0
	receivedTotal := 0
	executedTotal := 0

	for key, cs := range activeStreams {
		activeCount++
		receivedTotal += cs.ReceivedEvents
		executedTotal += cs.ExecutedEvents
		details := fmt.Sprintf("%s[events:%d,received:%d,executed:%d,errors:%d]",
			key, cs.EventCount, cs.ReceivedEvents, cs.ExecutedEvents, cs.ErrorCount)
		csDetails = append(csDetails, details)
	}

	// Calculate pending total for logging
	pendingTotal := receivedTotal - executedTotal

	// Always store ChangeStream statistics to database (even if no active streams)
	// This ensures daily reset logic is always executed
	if err := StoreChangeStreamStatistics(sc.ID, activeStreams); err != nil {
		log.WithError(err).Error("[MongoDB] Failed to store ChangeStream statistics to database")
	} else {
		log.WithFields(logrus.Fields{
			"monitor_action": "changestream_comprehensive_status",
			"sync_task_id":   sc.ID,
			"active_count":   activeCount,
			"total_received": receivedTotal,
			"total_executed": executedTotal,
			"total_pending":  pendingTotal,
		}).Debugf("[MongoDB] ChangeStream statistics stored to database: %d active streams", activeCount)
	}

	// Only check server-side ChangeStreams if we have active streams
	if len(activeStreams) > 0 {
		serverActiveStreams, serverCount, err := getMongoDBActiveChangeStreams(ctx, srcClient)
		if err != nil {
			log.WithError(err).WithField("db_type", dbType).
				Debug("[Monitor] Failed to get server-side active ChangeStreams")
		} else if serverCount > 0 {
			log.WithFields(logrus.Fields{
				"db_type":              dbType,
				"server_changestreams": serverCount,
				"server_details":       serverActiveStreams,
				"monitor_action":       "changestream_server_status",
			}).Debug("MongoDB server-side ChangeStreams")
		}
	}
}

func getMongoDBActiveChangeStreams(ctx context.Context, client *mongo.Client) ([]string, int, error) {
	var fullResult bson.M
	err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "currentOp", Value: 1}, {Key: "active", Value: true}}).Decode(&fullResult)
	if err != nil {
		return nil, 0, fmt.Errorf("run full currentOp command failed: %w", err)
	}

	if data, err := json.Marshal(fullResult); err == nil {
		logrus.Debugf("[MongoDB Monitor] Full currentOp result: %s", string(data))

		if inprog, ok := fullResult["inprog"].(bson.A); ok && len(inprog) > 0 {
			if first, ok := inprog[0].(bson.M); ok {
				if firstData, err := json.Marshal(first); err == nil {
					logrus.Debugf("[MongoDB Monitor] Sample operation: %s", string(firstData))
				}

				keys := make([]string, 0)
				for k := range first {
					keys = append(keys, k)
				}
				logrus.Debugf("[MongoDB Monitor] Available fields: %v", keys)
			}
		}
	}

	cmd := bson.D{
		{Key: "currentOp", Value: 1},
		{Key: "active", Value: true},
		{Key: "$or", Value: bson.A{
			bson.M{"desc": bson.M{"$regex": ".*[cC]hange[sS]tream.*"}},
			bson.M{"command.pipeline": bson.M{"$exists": true}},
			bson.M{"command.aggregate": bson.M{"$exists": true}},
			bson.M{"op": "getmore"},
		}},
	}

	var result bson.M
	err = client.Database("admin").RunCommand(ctx, cmd).Decode(&result)
	if err != nil {
		return nil, 0, fmt.Errorf("run filtered currentOp command failed: %w", err)
	}

	activeStreams := []string{}
	csCount := 0

	if inprog, ok := result["inprog"].(bson.A); ok {
		for _, op := range inprog {
			if opDoc, ok := op.(bson.M); ok {
				isChangeStream := false
				changeStreamInfo := ""

				if descStr, hasDesc := opDoc["desc"].(string); hasDesc &&
					(strings.Contains(strings.ToLower(descStr), "changestream") ||
						strings.Contains(strings.ToLower(descStr), "change stream")) {
					isChangeStream = true
					changeStreamInfo = descStr
				} else if ns, hasNs := opDoc["ns"].(string); hasNs {
					changeStreamInfo = ns
					if command, hasCmd := opDoc["command"].(bson.M); hasCmd {
						if _, hasPipeline := command["pipeline"]; hasPipeline {
							isChangeStream = true
						} else if _, hasAggregate := command["aggregate"]; hasAggregate {
							isChangeStream = true
						}
					}
				}

				if isChangeStream {
					csCount++
					activeStreams = append(activeStreams, changeStreamInfo)

					if csData, err := json.Marshal(opDoc); err == nil {
						logrus.Debugf("[MongoDB Monitor] Found ChangeStream: %s", string(csData))
					}
				}
			}
		}
	}

	if csCount != len(activeStreams) {
		logrus.Warnf("[MongoDB Monitor] Inconsistent ChangeStream count: detected=%d, listed=%d",
			csCount, len(activeStreams))
	}

	return activeStreams, csCount, nil
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

// StoreChangeStreamStatistics stores ChangeStream statistics to changestream_statistics table
func StoreChangeStreamStatistics(syncTaskID int, activeStreams map[string]*ChangeStreamInfo) error {
	db, err := db.OpenSQLiteDB()
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

	// Insert/update statistics for each active ChangeStream
	for collectionKey, csInfo := range activeStreams {
		if !csInfo.Active {
			continue // Skip inactive streams
		}

		pending := csInfo.ReceivedEvents - csInfo.ExecutedEvents
		if pending < 0 {
			pending = 0 // Ensure pending is not negative
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

// resetDailyStatisticsIfNeeded checks if it's a new day and resets statistics if needed
func resetDailyStatisticsIfNeeded(tx *sql.Tx, syncTaskID int) error {
	// Use Japan timezone (JST) for daily reset logic
	jst, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		logrus.Warnf("[MongoDB] Failed to load JST timezone: %v, falling back to local time", err)
		jst = time.Local
	}

	now := time.Now().In(jst)
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, jst)

	// Check if any records exist for this sync task and if we already reset today
	checkSQL := `
		SELECT COUNT(*), 
		       MAX(last_updated) as last_updated_time,
		       COALESCE(MAX(CASE WHEN DATE(last_updated, 'localtime') = ? THEN 1 ELSE 0 END), 0) as reset_today
		FROM changestream_statistics 
		WHERE task_id = ?
	`

	todayJSTStr := today.Format("2006-01-02")
	var recordCount int
	var lastUpdatedTime sql.NullString
	var resetToday int

	err = tx.QueryRow(checkSQL, todayJSTStr, syncTaskID).Scan(&recordCount, &lastUpdatedTime, &resetToday)
	if err != nil {
		return fmt.Errorf("failed to check existing records: %w", err)
	}

	// If no records exist, no need to reset
	if recordCount == 0 {
		logrus.Debugf("[MongoDB] No existing records for task_id=%d, skipping daily reset check", syncTaskID)
		return nil
	}

	// If we already reset today, skip
	if resetToday > 0 {
		logrus.Debugf("[MongoDB] Already reset today for task_id=%d, skipping daily reset", syncTaskID)
		return nil
	}

	// Parse the last update time to determine if we need to reset
	if !lastUpdatedTime.Valid {
		logrus.Debugf("[MongoDB] No valid last_updated time found for task_id=%d", syncTaskID)
		return nil
	}

	lastUpdate, err := time.Parse("2006-01-02 15:04:05", lastUpdatedTime.String)
	if err != nil {
		return fmt.Errorf("failed to parse last update time: %w", err)
	}

	// Convert to JST for comparison
	lastUpdateJST := lastUpdate.UTC().In(jst)
	lastUpdateDateJST := time.Date(lastUpdateJST.Year(), lastUpdateJST.Month(), lastUpdateJST.Day(), 0, 0, 0, 0, jst)

	// Check if we need to reset (if last update was before today in JST)
	if lastUpdateDateJST.Before(today) {
		logrus.Infof("[MongoDB] Daily reset triggered for task_id=%d: last_date=%s (JST), today=%s (JST)",
			syncTaskID, lastUpdateDateJST.Format("2006-01-02"), today.Format("2006-01-02"))

		// Reset all statistics to 0 for this sync task
		resetSQL := `
			UPDATE changestream_statistics 
			SET received = 0,
				executed = 0,
				pending = 0,
				errors = 0,
				inserted = 0,
				updated = 0,
				deleted = 0,
				last_updated = CURRENT_TIMESTAMP
			WHERE task_id = ?
		`

		result, err := tx.Exec(resetSQL, syncTaskID)
		if err != nil {
			return fmt.Errorf("failed to reset daily statistics: %w", err)
		}

		rowsAffected, _ := result.RowsAffected()

		// CRITICAL FIX: Also reset in-memory ChangeStreamInfo statistics for this sync task
		resetInMemoryStatistics(syncTaskID)

		logrus.Infof("[MongoDB] Daily statistics reset completed for task_id=%d: %d records reset (database + memory)",
			syncTaskID, rowsAffected)
	} else {
		logrus.Debugf("[MongoDB] No daily reset needed for task_id=%d: last_date=%s is today in JST",
			syncTaskID, lastUpdateDateJST.Format("2006-01-02"))
	}

	return nil
}

// resetInMemoryStatistics resets ChangeStreamInfo statistics in memory for a specific sync task
func resetInMemoryStatistics(syncTaskID int) {
	csTrackerMutex.Lock()
	defer csTrackerMutex.Unlock()

	resetCount := 0
	for key, cs := range changeStreamTracker {
		if cs.SyncTaskID == syncTaskID {
			// Reset all accumulated statistics to 0
			cs.ReceivedEvents = 0
			cs.ExecutedEvents = 0
			cs.InsertedCount = 0
			cs.UpdatedCount = 0
			cs.DeletedCount = 0
			cs.ErrorCount = 0
			cs.EventCount = 0
			// Keep other fields like Created, LastActivity, Active unchanged
			resetCount++
			logrus.Debugf("[MongoDB] Reset in-memory statistics for ChangeStream: %s", key)
		}
	}

	logrus.Infof("[MongoDB] Reset in-memory statistics for %d ChangeStreams of task_id=%d",
		resetCount, syncTaskID)
}

// logYesterdayDataVolume logs yesterday's data volume for tables with dateRange conditions
func logYesterdayDataVolume(ctx context.Context, cfg *config.Config, log *logrus.Logger) {
	log.Infof("[Monitor] Starting daily summary for yesterday's data volume...")

	// Get Japan timezone
	jst, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		log.Warnf("[Monitor] Failed to load JST timezone: %v, falling back to local time", err)
		jst = time.Local
	}

	// Calculate yesterday's date range in JST
	now := time.Now().In(jst)
	yesterday := now.AddDate(0, 0, -1)
	yesterdayStart := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, jst)
	yesterdayEnd := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 23, 59, 59, 999999999, jst)

	log.Infof("[Monitor] Processing yesterday's data volume: %s to %s (JST)",
		yesterdayStart.Format("2006-01-02 15:04:05"), yesterdayEnd.Format("2006-01-02 15:04:05"))

	for _, sc := range cfg.SyncConfigs {
		if !sc.Enable {
			continue
		}

		switch strings.ToLower(sc.Type) {
		case "mongodb":
			logYesterdayMongoDBVolume(ctx, sc, log, yesterdayStart, yesterdayEnd)
		default:
			log.Debugf("[Monitor] Daily summary for type %s not implemented", sc.Type)
		}
	}

	log.Infof("[Monitor] Daily summary completed")
}

// logYesterdayMongoDBVolume logs yesterday's MongoDB data volume for dateRange tables
func logYesterdayMongoDBVolume(ctx context.Context, sc config.SyncConfig, log *logrus.Logger, yesterdayStart, yesterdayEnd time.Time) {
	srcClient, err := mongo.Connect(ctx, options.Client().ApplyURI(sc.SourceConnection))
	if err != nil {
		log.WithError(err).WithField("db_type", "MONGODB").
			Error("[Monitor] Failed to connect to source for daily summary")
		return
	}
	defer func() {
		_ = srcClient.Disconnect(ctx)
	}()

	tgtClient, err := mongo.Connect(ctx, options.Client().ApplyURI(sc.TargetConnection))
	if err != nil {
		log.WithError(err).WithField("db_type", "MONGODB").
			Error("[Monitor] Failed to connect to target for daily summary")
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
			// Parse original count query conditions
			var originalConditions []CountCondition
			var hasDateRangeCondition bool
			var dateRangeField string

			if tblMap.CountQuery != nil && len(tblMap.CountQuery) > 0 {
				if conditions, ok := tblMap.CountQuery["conditions"]; ok {
					conditionBytes, err := json.Marshal(conditions)
					if err == nil {
						if err := json.Unmarshal(conditionBytes, &originalConditions); err == nil {
							for _, condition := range originalConditions {
								if condition.Operator == "dateRange" && condition.Field != "" {
									hasDateRangeCondition = true
									dateRangeField = condition.Field
									break
								}
							}
						}
					}
				}
			}

			// Skip tables without dateRange conditions
			if !hasDateRangeCondition {
				continue
			}

			log.Infof("[Monitor] Processing daily summary for dateRange table: %s (field: %s, total conditions: %d)",
				tblMap.SourceTable, dateRangeField, len(originalConditions))

			// Create yesterday's query conditions based on original conditions
			// Replace dateRange value from "daily" to "yesterday", keep all other conditions
			var yesterdayConditions []CountCondition
			for _, condition := range originalConditions {
				if condition.Operator == "dateRange" && condition.Field == dateRangeField {
					// Replace dateRange value with "yesterday"
					yesterdayConditions = append(yesterdayConditions, CountCondition{
						Field:    condition.Field,
						Operator: condition.Operator,
						Table:    condition.Table,
						Value:    "yesterday", // Custom value for yesterday
					})
				} else {
					// Keep other conditions as is
					yesterdayConditions = append(yesterdayConditions, condition)
				}
			}

			yesterdayQuery := &CountQuery{
				Conditions: yesterdayConditions,
			}

			// Log the conditions being used for yesterday's query for debugging
			if len(yesterdayConditions) > 1 {
				conditionDetails := make([]string, len(yesterdayConditions))
				for i, cond := range yesterdayConditions {
					conditionDetails[i] = fmt.Sprintf("%s %s %v", cond.Field, cond.Operator, cond.Value)
				}
				log.Debugf("[Monitor] Yesterday query conditions for %s: [%s]",
					tblMap.SourceTable, strings.Join(conditionDetails, ", "))
			}

			queryCounter := NewQueryCounterWithYesterdaySupport(log, yesterdayStart, yesterdayEnd)

			// Count source collection for yesterday
			srcCount, err := queryCounter.CountMongoDBDocuments(ctx, srcClient, srcDBName, tblMap.SourceTable, yesterdayQuery)
			if err != nil {
				log.WithError(err).WithFields(logrus.Fields{
					"sync_task_id": sc.ID,
					"db_type":      dbType,
					"src_db":       srcDBName,
					"src_coll":     tblMap.SourceTable,
					"date_field":   dateRangeField,
					"operation":    "yesterday_source_count",
				}).Error("Failed to get yesterday's source collection count")
				srcCount = -1
			}

			// Count target collection for yesterday
			tgtCount, err := queryCounter.CountMongoDBDocuments(ctx, tgtClient, tgtDBName, tblMap.TargetTable, yesterdayQuery)
			if err != nil {
				log.WithError(err).WithFields(logrus.Fields{
					"sync_task_id": sc.ID,
					"db_type":      dbType,
					"tgt_db":       tgtDBName,
					"tgt_coll":     tblMap.TargetTable,
					"date_field":   dateRangeField,
					"operation":    "yesterday_target_count",
				}).Error("Failed to get yesterday's target collection count")
				tgtCount = -1
			}

			// Calculate synced count
			syncedCount := tgtCount
			if srcCount >= 0 && tgtCount >= 0 {
				// For daily sync, synced count is typically the target count
				// as we're measuring how many records were successfully synced
				syncedCount = tgtCount
			}

			// Log the daily summary with special format for GCP Logging alerts
			log.WithFields(logrus.Fields{
				"sync_task_id":        sc.ID,
				"db_type":             dbType,
				"src_db":              srcDBName,
				"src_coll":            tblMap.SourceTable,
				"src_yesterday_count": srcCount,
				"tgt_db":              tgtDBName,
				"tgt_coll":            tblMap.TargetTable,
				"tgt_yesterday_count": tgtCount,
				"synced_yesterday":    syncedCount,
				"date_field":          dateRangeField,
				"yesterday_date":      yesterdayStart.Format("2006-01-02"),
				"monitor_action":      "daily_sync_summary",
			}).Info("daily_sync_summary")

			// Store to database for historical tracking
			storeMonitoringLog(sc.ID, dbType, srcDBName, tblMap.SourceTable, srcCount,
				tgtDBName, tblMap.TargetTable, tgtCount, "daily_sync_summary")

			log.Infof("[Monitor] Daily summary: Task %d, Table %s.%s -> %s.%s, "+
				"Yesterday (%s): Source=%d, Target=%d, Synced=%d (field: %s)",
				sc.ID, srcDBName, tblMap.SourceTable, tgtDBName, tblMap.TargetTable,
				yesterdayStart.Format("2006-01-02"), srcCount, tgtCount, syncedCount, dateRangeField)

			// Send Slack notification for this table comparison
			SendTableComparisonSlackNotification(ctx, sc, srcDBName, tblMap.SourceTable, tgtDBName, tblMap.TargetTable,
				srcCount, tgtCount, yesterdayStart, log)
		}
	}
}

// SendTableComparisonSlackNotification sends Slack notification for a single table comparison
func SendTableComparisonSlackNotification(ctx context.Context, sc config.SyncConfig, srcDB, srcTable, tgtDB, tgtTable string,
	srcCount, tgtCount int64, yesterdayStart time.Time, log *logrus.Logger) {

	// Skip if counts are invalid (error occurred during counting)
	if srcCount < 0 || tgtCount < 0 {
		return
	}

	// Get global config to access Slack settings
	cfg := config.NewConfig()
	slackNotifier := NewSlackNotifierFromConfig(cfg, log)
	if !slackNotifier.IsConfigured() {
		return
	}

	// Calculate difference and determine status
	difference := srcCount - tgtCount
	status := "✅"
	alertType := SlackAlertGood

	if difference != 0 {
		status = "⚠️"
		alertType = SlackAlertWarning
	}

	// Format message
	yesterday := yesterdayStart.Format("2006-01-02")
	message := fmt.Sprintf("%s Daily Data Comparison\n\nTask ID: %d\nDate: %s (JST)\nType: MONGODB\n\n%s %s.%s → %s.%s\nSource: %d\nTarget: %d\nDifference: %d",
		status, sc.ID, yesterday, status, srcDB, srcTable, tgtDB, tgtTable, srcCount, tgtCount, difference)

	// Send notification asynchronously
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		err := slackNotifier.SendNotification(ctx, message, &SlackNotificationOptions{
			AlertType: alertType,
			Trigger:   "daily-data-comparison",
		})
		if err != nil {
			log.Warnf("[Monitor] Failed to send Slack notification for task %d, table %s.%s: %v", sc.ID, srcDB, srcTable, err)
		} else {
			log.Infof("[Monitor] Slack notification sent for task %d, table %s.%s (diff: %d)", sc.ID, srcDB, srcTable, difference)
		}
	}()
}
