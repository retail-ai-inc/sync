package infra

import (
	"fmt"
	"strings"
	"time"

	"github.com/retail-ai-inc/sync/internal/monitoring/domain"

	// "github.com/sirupsen/logrus"
	"context"
	"encoding/json"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// CountAndLogMongoDB obtains document counts for MongoDB collections
func CountAndLogMongoDB(ctx context.Context, sc config.SyncConfig, log *logrus.Logger) {
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
	srcDBName := dsn.GetDatabaseName(sc.Type, sc.SourceConnection)
	tgtDBName := dsn.GetDatabaseName(sc.Type, sc.TargetConnection)

	for _, mapping := range sc.Mappings {
		for _, tblMap := range mapping.Tables {
			var srcCount int64
			var tgtCount int64
			var err error

			// Parse count query if exists
			var countQuery *domain.CountQuery
			if tblMap.CountQuery != nil && len(tblMap.CountQuery) > 0 {
				if conditions, ok := tblMap.CountQuery["conditions"]; ok {
					conditionBytes, err := json.Marshal(conditions)
					if err == nil {
						var conditionsList []domain.CountCondition
						if err := json.Unmarshal(conditionBytes, &conditionsList); err == nil {
							countQuery = &domain.CountQuery{
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
	activeStreams := domain.GetActiveChangeStreamsByTaskID(sc.ID)

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

// LogYesterdayMongoDBVolume logs yesterday's MongoDB data volume for dateRange tables
func LogYesterdayMongoDBVolume(ctx context.Context, sc config.SyncConfig, log *logrus.Logger, yesterdayStart, yesterdayEnd time.Time) {
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
	srcDBName := dsn.GetDatabaseName(sc.Type, sc.SourceConnection)
	tgtDBName := dsn.GetDatabaseName(sc.Type, sc.TargetConnection)

	for _, mapping := range sc.Mappings {
		for _, tblMap := range mapping.Tables {
			// Parse original count query conditions
			var originalConditions []domain.CountCondition
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
			var yesterdayConditions []domain.CountCondition
			for _, condition := range originalConditions {
				if condition.Operator == "dateRange" && condition.Field == dateRangeField {
					// Replace dateRange value with "yesterday"
					yesterdayConditions = append(yesterdayConditions, domain.CountCondition{
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

			yesterdayQuery := &domain.CountQuery{
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
