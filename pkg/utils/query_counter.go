package utils

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// CountQuery represents the query conditions to count documents
type CountQuery struct {
	Conditions []CountCondition `json:"conditions"`
}

// CountCondition represents a single condition for counting
type CountCondition struct {
	Field    string `json:"field"`
	Operator string `json:"operator"`
	Table    string `json:"table"`
	Value    string `json:"value"`
}

// QueryCounter handles executing count queries with specific conditions
type QueryCounter struct {
	logger *logrus.Logger
}

// NewQueryCounter creates a new QueryCounter instance
func NewQueryCounter(logger *logrus.Logger) *QueryCounter {
	if logger == nil {
		logger = logrus.New()
	}
	return &QueryCounter{
		logger: logger,
	}
}

// CountMongoDBDocuments counts documents in a MongoDB collection with specified conditions
func (qc *QueryCounter) CountMongoDBDocuments(ctx context.Context, client *mongo.Client, database, collection string, query *CountQuery) (int64, error) {
	qc.logger.Debugf("[QueryCounter] Executing count on %s.%s", database, collection)

	// If no query is provided, use EstimatedDocumentCount
	if query == nil || len(query.Conditions) == 0 {
		coll := client.Database(database).Collection(collection)
		count, err := coll.EstimatedDocumentCount(ctx)
		if err != nil {
			qc.logger.Errorf("[QueryCounter] EstimatedDocumentCount failed for %s.%s: %v", database, collection, err)
			return -1, fmt.Errorf("estimated document count failed: %w", err)
		}
		return count, nil
	}

	// Build query filter
	filter := bson.M{}
	relevantConditions := 0

	// Get Japan timezone
	jst, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		qc.logger.Warnf("[QueryCounter] Failed to load JST timezone: %v, falling back to local time", err)
		jst = time.Local
	}

	for _, condition := range query.Conditions {
		// Check if the condition is for this table
		if condition.Table != collection {
			continue
		}

		relevantConditions++

		// Handle dateRange operator specifically
		if condition.Operator == "dateRange" && condition.Field != "" {
			switch strings.ToLower(condition.Value) {
			case "daily", "today":
				// Create date range using JST timezone
				now := time.Now().In(jst)
				startOfDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, jst)
				endOfDay := time.Date(now.Year(), now.Month(), now.Day(), 23, 59, 59, 999999999, jst)

				// Convert to UTC for MongoDB query
				startOfDayUTC := startOfDay.UTC()
				endOfDayUTC := endOfDay.UTC()

				filter[condition.Field] = bson.M{
					"$gte": startOfDayUTC,
					"$lte": endOfDayUTC,
				}

				// Log equivalent MongoDB query for debugging
				qc.logger.Debugf("[QueryCounter] MongoDB query: db.%s.countDocuments({%s: {$gte: ISODate(\"%s\"), $lte: ISODate(\"%s\")}})",
					collection, condition.Field, startOfDayUTC.Format("2006-01-02T15:04:05Z"), endOfDayUTC.Format("2006-01-02T15:04:05Z"))

			case "weekly":
				now := time.Now().In(jst)
				// Get first day of week (Sunday as week start)
				startOfWeek := now.AddDate(0, 0, -int(now.Weekday()))
				startOfWeek = time.Date(startOfWeek.Year(), startOfWeek.Month(), startOfWeek.Day(), 0, 0, 0, 0, jst)
				endOfWeek := time.Date(now.Year(), now.Month(), now.Day(), 23, 59, 59, 999999999, jst)

				// Convert to UTC
				startOfWeekUTC := startOfWeek.UTC()
				endOfWeekUTC := endOfWeek.UTC()

				filter[condition.Field] = bson.M{
					"$gte": startOfWeekUTC,
					"$lte": endOfWeekUTC,
				}

				// Log equivalent MongoDB query for debugging
				qc.logger.Debugf("[QueryCounter] MongoDB query: db.%s.countDocuments({%s: {$gte: ISODate(\"%s\"), $lte: ISODate(\"%s\")}})",
					collection, condition.Field, startOfWeekUTC.Format("2006-01-02T15:04:05Z"), endOfWeekUTC.Format("2006-01-02T15:04:05Z"))

			case "monthly":
				now := time.Now().In(jst)
				startOfMonth := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, jst)
				endOfMonth := time.Date(now.Year(), now.Month(), now.Day(), 23, 59, 59, 999999999, jst)

				// Convert to UTC
				startOfMonthUTC := startOfMonth.UTC()
				endOfMonthUTC := endOfMonth.UTC()

				filter[condition.Field] = bson.M{
					"$gte": startOfMonthUTC,
					"$lte": endOfMonthUTC,
				}

				// Log equivalent MongoDB query for debugging
				qc.logger.Debugf("[QueryCounter] MongoDB query: db.%s.countDocuments({%s: {$gte: ISODate(\"%s\"), $lte: ISODate(\"%s\")}})",
					collection, condition.Field, startOfMonthUTC.Format("2006-01-02T15:04:05Z"), endOfMonthUTC.Format("2006-01-02T15:04:05Z"))

			default:
				qc.logger.Warnf("[QueryCounter] Unknown date range type: %s", condition.Value)
			}
		}
	}

	if relevantConditions == 0 {
		qc.logger.Warnf("[QueryCounter] No relevant conditions found for %s.%s in query",
			database, collection)
	}

	// Execute count with filter
	coll := client.Database(database).Collection(collection)
	count, err := coll.CountDocuments(ctx, filter)
	if err != nil {
		qc.logger.Errorf("[QueryCounter] CountDocuments failed for %s.%s: %v",
			database, collection, err)
		return -1, fmt.Errorf("count documents failed: %w", err)
	}

	qc.logger.Debugf("[QueryCounter] Counted %d documents in %s.%s", count, database, collection)
	return count, nil
}
