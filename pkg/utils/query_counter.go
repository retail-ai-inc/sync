package utils

import (
	"context"
	"fmt"
	"strconv"
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
	startTime := time.Now()

	// If no query is provided, use EstimatedDocumentCount
	if query == nil || len(query.Conditions) == 0 {
		qc.logger.Debugf("[MongoDB] SQL: db.%s.estimatedDocumentCount() | START: %s", collection, startTime.Format("15:04:05.000"))
		coll := client.Database(database).Collection(collection)
		count, err := coll.EstimatedDocumentCount(ctx)
		endTime := time.Now()
		qc.logger.Debugf("[MongoDB] RESULT: %d | END: %s | Duration: %dms", count, endTime.Format("15:04:05.000"), endTime.Sub(startTime).Milliseconds())
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

		// Handle different operators
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
		} else if condition.Operator == "=" && condition.Field != "" && condition.Value != "" {
			// Handle equality operator - try to convert to number first for id fields
			if intValue, err := strconv.ParseInt(condition.Value, 10, 64); err == nil {
				filter[condition.Field] = intValue
				qc.logger.Debugf("[QueryCounter] Converted equality value '%s' to int64: %d", condition.Value, intValue)
			} else if floatValue, err := strconv.ParseFloat(condition.Value, 64); err == nil {
				filter[condition.Field] = floatValue
				qc.logger.Debugf("[QueryCounter] Converted equality value '%s' to float64: %f", condition.Value, floatValue)
			} else {
				filter[condition.Field] = condition.Value
				qc.logger.Debugf("[QueryCounter] Using string value for equality: '%s'", condition.Value)
			}
		} else if condition.Field != "" && condition.Value != "" {
			// Handle comparison operators (>, <, >=, <=)
			switch condition.Operator {
			case ">":
				// Try to convert value to number for numeric comparison (int first, then float)
				if intValue, err := strconv.ParseInt(condition.Value, 10, 64); err == nil {
					filter[condition.Field] = bson.M{"$gt": intValue}
				} else if floatValue, err := strconv.ParseFloat(condition.Value, 64); err == nil {
					filter[condition.Field] = bson.M{"$gt": floatValue}
				} else {
					filter[condition.Field] = bson.M{"$gt": condition.Value}
				}
			case ">=":
				if intValue, err := strconv.ParseInt(condition.Value, 10, 64); err == nil {
					filter[condition.Field] = bson.M{"$gte": intValue}
				} else if floatValue, err := strconv.ParseFloat(condition.Value, 64); err == nil {
					filter[condition.Field] = bson.M{"$gte": floatValue}
				} else {
					filter[condition.Field] = bson.M{"$gte": condition.Value}
				}
			case "<":
				if intValue, err := strconv.ParseInt(condition.Value, 10, 64); err == nil {
					filter[condition.Field] = bson.M{"$lt": intValue}
				} else if floatValue, err := strconv.ParseFloat(condition.Value, 64); err == nil {
					filter[condition.Field] = bson.M{"$lt": floatValue}
				} else {
					filter[condition.Field] = bson.M{"$lt": condition.Value}
				}
			case "<=":
				if intValue, err := strconv.ParseInt(condition.Value, 10, 64); err == nil {
					filter[condition.Field] = bson.M{"$lte": intValue}
				} else if floatValue, err := strconv.ParseFloat(condition.Value, 64); err == nil {
					filter[condition.Field] = bson.M{"$lte": floatValue}
				} else {
					filter[condition.Field] = bson.M{"$lte": condition.Value}
				}
			case "!=", "<>":
				if intValue, err := strconv.ParseInt(condition.Value, 10, 64); err == nil {
					filter[condition.Field] = bson.M{"$ne": intValue}
				} else if floatValue, err := strconv.ParseFloat(condition.Value, 64); err == nil {
					filter[condition.Field] = bson.M{"$ne": floatValue}
				} else {
					filter[condition.Field] = bson.M{"$ne": condition.Value}
				}
			default:
				// Handle other operators (like id-based queries)
				qc.logger.Debugf("[QueryCounter] Unhandled operator: %s for field: %s", condition.Operator, condition.Field)
			}
		} else {
			// Handle other operators (like id-based queries)
			qc.logger.Debugf("[QueryCounter] Unhandled operator: %s for field: %s", condition.Operator, condition.Field)
		}

		// Log what was actually added to the filter for this condition
		if currentValue, exists := filter[condition.Field]; exists {
			qc.logger.Debugf("[QueryCounter] Added to filter: %s = %+v (type: %T)", condition.Field, currentValue, currentValue)
		}
	}

	if relevantConditions == 0 {
		qc.logger.Warnf("[QueryCounter] No relevant conditions found for %s.%s in query",
			database, collection)
	}

	// Log the complete query after all conditions are processed
	queryStr := qc.buildReadableQueryString(collection, filter)
	qc.logger.Debugf("[QueryCounter] MongoDB query: %s", queryStr)

	// Log the filter being used and execute count - use the same format for consistency
	qc.logger.Debugf("[MongoDB] SQL: %s | START: %s", queryStr, startTime.Format("15:04:05.000"))

	coll := client.Database(database).Collection(collection)
	count, err := coll.CountDocuments(ctx, filter)
	endTime := time.Now()
	qc.logger.Debugf("[MongoDB] RESULT: %d | END: %s | Duration: %dms", count, endTime.Format("15:04:05.000"), endTime.Sub(startTime).Milliseconds())

	if err != nil {
		qc.logger.Errorf("[QueryCounter] CountDocuments failed for %s.%s: %v",
			database, collection, err)
		return -1, fmt.Errorf("count documents failed: %w", err)
	}

	return count, nil
}

// buildReadableQueryString builds a human-readable MongoDB query string using ISODate format
func (qc *QueryCounter) buildReadableQueryString(collection string, filter bson.M) string {
	if len(filter) == 0 {
		return fmt.Sprintf("db.%s.countDocuments({})", collection)
	}

	var conditions []string

	for field, value := range filter {
		conditionStr := qc.formatFilterCondition(field, value)
		if conditionStr != "" {
			conditions = append(conditions, conditionStr)
		}
	}

	if len(conditions) == 0 {
		return fmt.Sprintf("db.%s.countDocuments({})", collection)
	}

	return fmt.Sprintf("db.%s.countDocuments({%s})", collection, strings.Join(conditions, ", "))
}

// formatFilterCondition formats a single filter condition to readable string
func (qc *QueryCounter) formatFilterCondition(field string, value interface{}) string {
	switch v := value.(type) {
	case bson.M:
		// Handle operators like $gt, $gte, $lt, $lte
		var parts []string
		for op, opValue := range v {
			switch op {
			case "$gt":
				parts = append(parts, fmt.Sprintf("$gt: %v", qc.formatValue(opValue)))
			case "$gte":
				if t, ok := opValue.(time.Time); ok {
					parts = append(parts, fmt.Sprintf("$gte: ISODate(\"%s\")", t.Format("2006-01-02T15:04:05.000Z")))
				} else {
					parts = append(parts, fmt.Sprintf("$gte: %v", qc.formatValue(opValue)))
				}
			case "$lt":
				parts = append(parts, fmt.Sprintf("$lt: %v", qc.formatValue(opValue)))
			case "$lte":
				if t, ok := opValue.(time.Time); ok {
					parts = append(parts, fmt.Sprintf("$lte: ISODate(\"%s\")", t.Format("2006-01-02T15:04:05.000Z")))
				} else {
					parts = append(parts, fmt.Sprintf("$lte: %v", qc.formatValue(opValue)))
				}
			case "$ne":
				parts = append(parts, fmt.Sprintf("$ne: %v", qc.formatValue(opValue)))
			default:
				parts = append(parts, fmt.Sprintf("%s: %v", op, qc.formatValue(opValue)))
			}
		}
		if len(parts) > 0 {
			return fmt.Sprintf("%s: {%s}", field, strings.Join(parts, ", "))
		}
	default:
		return fmt.Sprintf("%s: %v", field, qc.formatValue(value))
	}
	return ""
}

// formatValue formats a value for display
func (qc *QueryCounter) formatValue(value interface{}) string {
	switch v := value.(type) {
	case time.Time:
		return fmt.Sprintf("ISODate(\"%s\")", v.Format("2006-01-02T15:04:05.000Z"))
	case string:
		return fmt.Sprintf("\"%s\"", v)
	case int, int32, int64, float32, float64:
		return fmt.Sprintf("%v", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}
