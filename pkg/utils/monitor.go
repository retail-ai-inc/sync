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
}

var (
	// changeStreamTracker stores information about all active ChangeStreams
	changeStreamTracker = make(map[string]*ChangeStreamInfo)
	csTrackerMutex      = &sync.RWMutex{}
)

// RegisterChangeStream registers a new ChangeStream
func RegisterChangeStream(database, collection string) {
	key := fmt.Sprintf("%s.%s", database, collection)
	csTrackerMutex.Lock()
	defer csTrackerMutex.Unlock()

	changeStreamTracker[key] = &ChangeStreamInfo{
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

	activeStreams := GetActiveChangeStreams()
	csDetails := make([]string, 0, len(activeStreams))
	activeCount := 0

	for key, cs := range activeStreams {
		if cs.Active {
			activeCount++
			details := fmt.Sprintf("%s[events:%d,errors:%d]", key, cs.EventCount, cs.ErrorCount)
			csDetails = append(csDetails, details)
		}
	}

	log.WithFields(logrus.Fields{
		"db_type":              dbType,
		"active_changestreams": activeCount,
		"changestream_details": csDetails,
		"total_tracked":        len(activeStreams),
		"monitor_action":       "changestream_status",
	}).Info("MongoDB active ChangeStreams status")

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
