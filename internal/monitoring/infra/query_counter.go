package infra

import (
	"fmt"
	"strings"
	"time"

	"github.com/retail-ai-inc/sync/internal/monitoring/domain"

	// "github.com/sirupsen/logrus"
	"context"
	"strconv"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// QueryCounter handles executing count queries with specific conditions
type QueryCounter struct {
	logger *logrus.Logger
	// Support for custom date ranges (for yesterday support)
	customYesterdayStart *time.Time
	customYesterdayEnd   *time.Time
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

// NewQueryCounterWithYesterdaySupport creates a QueryCounter with custom yesterday date range
func NewQueryCounterWithYesterdaySupport(logger *logrus.Logger, yesterdayStart, yesterdayEnd time.Time) *QueryCounter {
	if logger == nil {
		logger = logrus.New()
	}
	return &QueryCounter{
		logger:               logger,
		customYesterdayStart: &yesterdayStart,
		customYesterdayEnd:   &yesterdayEnd,
	}
}

// CountMongoDBDocuments counts documents in a MongoDB collection with specified conditions
func (qc *QueryCounter) CountMongoDBDocuments(ctx context.Context, client *mongo.Client, database, collection string, query *domain.CountQuery) (int64, error) {
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
	// dropped names the conditions this could not express. A count taken with
	// some of its conditions missing is a count of something else — usually the
	// whole collection — and it used to be reported as though it were the
	// number that was asked for.
	var dropped []string

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

		// The filter is a map keyed by field, so a second condition on the same
		// field replaces the first: "total > 10 AND total < 100" — a range, the
		// obvious thing to want — used to count with only the second half.
		if _, already := filter[condition.Field]; already && condition.Field != "" {
			dropped = append(dropped, fmt.Sprintf(
				"a second condition on %q, which this cannot combine with the first",
				condition.Field))
			continue
		}

		if condition.Operator == "dateRange" && condition.Field != "" {
			start, finish, known := qc.namedRange(condition.Value, jst)
			if !known {
				dropped = append(dropped, fmt.Sprintf("%s: unknown date range %q",
					condition.Field, condition.Value))
			} else {
				// MongoDB stores instants in UTC, so the window is converted
				// rather than compared in the local zone.
				filter[condition.Field] = bson.M{"$gte": start.UTC(), "$lte": finish.UTC()}
				qc.logger.Debugf(
					"[QueryCounter] MongoDB query: db.%s.countDocuments({%s: {$gte: ISODate(\"%s\"), $lte: ISODate(\"%s\")}})",
					collection, condition.Field,
					start.UTC().Format("2006-01-02T15:04:05Z"), finish.UTC().Format("2006-01-02T15:04:05Z"))
			}
		} else if condition.Operator == "=" && condition.Field != "" && condition.Value != "" {
			// A value that looks like a number is matched as a number *or* as the
			// string it was written as. It used to be converted and the string
			// dropped, so a field stored as text — an order id, a postcode —
			// matched nothing and the collection counted zero, which reads as a
			// replica that has lost everything.
			filter[condition.Field] = equalityFilter(condition.Value)
			qc.logger.Debugf("[QueryCounter] Equality on %s matches %v", condition.Field, filter[condition.Field])
		} else if condition.Field != "" && condition.Value != "" {
			if operator, known := mongoComparisons[condition.Operator]; known {
				filter[condition.Field] = bson.M{operator: comparableValue(condition.Value)}
			} else {
				dropped = append(dropped, fmt.Sprintf("%s: unknown operator %q",
					condition.Field, condition.Operator))
			}
		} else {
			// A dateRange with no field lands here, as does a condition with no
			// value: neither can be rendered, and neither used to be reported.
			dropped = append(dropped, fmt.Sprintf("%q on %q with the value %q",
				condition.Operator, condition.Field, condition.Value))
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

	if len(dropped) > 0 {
		return -1, fmt.Errorf("counting %s.%s: %d of its conditions cannot be "+
			"expressed (%s), and counting without them would answer a different "+
			"question", database, collection, len(dropped), strings.Join(dropped, "; "))
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

// equalityFilter matches a value however the collection stores it.
//
// Nothing here knows whether a given field holds 123 or "123", and guessing
// wrong makes the count zero. Matching both costs one more index probe and
// cannot be wrong.
func equalityFilter(value string) interface{} {
	if intValue, err := strconv.ParseInt(value, 10, 64); err == nil {
		return bson.M{"$in": bson.A{intValue, value}}
	}
	if floatValue, err := strconv.ParseFloat(value, 64); err == nil {
		return bson.M{"$in": bson.A{floatValue, value}}
	}
	return value
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

// mongoComparisons maps the operators a count query is written with onto the
// ones MongoDB understands.
//
// Each of these used to be its own case, and each case carried its own copy of
// the same three-step conversion below — five copies of a ladder that has to
// stay identical, because a condition that reads a value differently from its
// neighbour counts a different set of documents.
var mongoComparisons = map[string]string{
	">":  "$gt",
	">=": "$gte",
	"<":  "$lt",
	"<=": "$lte",
	"!=": "$ne",
	"<>": "$ne",
}

// comparableValue reads a condition's value as the narrowest type it fits: an
// integer, then a floating-point number, then the string as it was written.
//
// The order matters. MongoDB compares a number against a number and a string
// against a string, so a threshold written as "1000" against a numeric field has
// to be sent as a number or it matches nothing — and a value that is not a
// number at all has to be sent as it stands rather than dropped.
func comparableValue(value string) interface{} {
	if whole, err := strconv.ParseInt(value, 10, 64); err == nil {
		return whole
	}
	if fractional, err := strconv.ParseFloat(value, 64); err == nil {
		return fractional
	}
	return value
}

// namedRange reports the window a named date range covers, in the given zone.
//
// The four ranges each used to compute their own bounds, convert them, set the
// filter and log the query — four copies of the same twenty lines, which is how
// "weekly" and "monthly" came to end at the end of today rather than at the end
// of the period while "daily" ended at the end of its day. The bounds are here;
// what is done with them is in one place at the call site.
//
// A range is inclusive at both ends: the last nanosecond of the last day is part
// of it, because a document written at 23:59:59 belongs to that day.
func (qc *QueryCounter) namedRange(name string, zone *time.Location) (start, end time.Time, known bool) {
	now := time.Now().In(zone)
	dayStart := func(t time.Time) time.Time {
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, zone)
	}
	dayEnd := func(t time.Time) time.Time {
		return time.Date(t.Year(), t.Month(), t.Day(), 23, 59, 59, 999999999, zone)
	}

	switch strings.ToLower(name) {
	case "daily", "today":
		return dayStart(now), dayEnd(now), true

	case "yesterday":
		// The daily summary computes the window once, for every task, so that
		// two tasks compared in the same run cover the same day even if the run
		// crosses midnight.
		if qc.customYesterdayStart != nil && qc.customYesterdayEnd != nil {
			return *qc.customYesterdayStart, *qc.customYesterdayEnd, true
		}
		yesterday := now.AddDate(0, 0, -1)
		return dayStart(yesterday), dayEnd(yesterday), true

	case "weekly":
		// Week starting Sunday, ending today: this is "this week so far", not a
		// whole week. Recorded as-is; which one the dashboard wants is T-198.
		return dayStart(now.AddDate(0, 0, -int(now.Weekday()))), dayEnd(now), true

	case "monthly":
		// "This month so far", for the same reason.
		return time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, zone), dayEnd(now), true
	}
	return time.Time{}, time.Time{}, false
}
