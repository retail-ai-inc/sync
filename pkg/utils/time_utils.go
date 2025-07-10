package utils

import (
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

// TimeRangeQuery represents a time range query object
type TimeRangeQuery struct {
	StartOffset int    `json:"startOffset"`
	EndOffset   int    `json:"endOffset"`
	Type        string `json:"type"`
}

// MongoDBTimeRange represents a MongoDB time range query
type MongoDBTimeRange struct {
	Gte map[string]interface{} `json:"$gte"`
	Lt  map[string]interface{} `json:"$lt"`
}

// ReplaceDatePlaceholders replaces date placeholders in a pattern with actual date values
func ReplaceDatePlaceholders(pattern string) string {
	now := time.Now()
	result := pattern

	// Replace various date format placeholders
	result = strings.ReplaceAll(result, "YYYY", now.Format("2006"))
	result = strings.ReplaceAll(result, "MM", now.Format("01"))
	result = strings.ReplaceAll(result, "DD", now.Format("02"))
	result = strings.ReplaceAll(result, "yyyy", now.Format("2006"))
	result = strings.ReplaceAll(result, "mm", now.Format("01"))
	result = strings.ReplaceAll(result, "dd", now.Format("02"))

	return result
}

// GetTodayDateString returns today's date in YYYY-MM-DD format
func GetTodayDateString() string {
	return time.Now().Format("2006-01-02")
}

// ParseDatabaseTimestamp parses database timestamp string to time.Time
func ParseDatabaseTimestamp(timestamp string) (time.Time, error) {
	return time.Parse("2006-01-02 15:04:05", timestamp)
}

// ProcessTimeRangeQuery converts time range query object to actual MongoDB time query
func ProcessTimeRangeQuery(queryObj map[string]interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	// Process each field in the query object
	for key, value := range queryObj {
		if timeRangeObj, ok := value.(map[string]interface{}); ok {
			// Check if this is a time range query object
			if typeVal, hasType := timeRangeObj["type"]; hasType {
				if typeStr, ok := typeVal.(string); ok && typeStr == "daily" {
					// Convert to MongoDB time range
					mongoTimeRange, err := convertToMongoDBTimeRange(timeRangeObj)
					if err != nil {
						logrus.Warnf("[TimeUtils] Failed to convert time range for field %s: %v", key, err)
						result[key] = value
					} else {
						result[key] = mongoTimeRange
						logrus.Infof("[TimeUtils] Converted time range query for field: %s", key)
					}
				} else {
					// Not a daily time range query, keep as is
					result[key] = value
				}
			} else {
				// Not a time range query object, keep as is
				result[key] = value
			}
		} else {
			// Not a map, keep as is
			result[key] = value
		}
	}

	return result, nil
}

// convertToMongoDBTimeRange converts time range object to MongoDB time range query
func convertToMongoDBTimeRange(timeRangeObj map[string]interface{}) (map[string]interface{}, error) {
	// Get offset values
	startOffset, hasStartOffset := timeRangeObj["startOffset"]
	endOffset, hasEndOffset := timeRangeObj["endOffset"]

	if !hasStartOffset || !hasEndOffset {
		return nil, fmt.Errorf("missing offset values in time range query")
	}

	// Convert offsets to int
	startOffsetInt, startOk := startOffset.(float64)
	endOffsetInt, endOk := endOffset.(float64)

	if !startOk || !endOk {
		return nil, fmt.Errorf("invalid offset values in time range query")
	}

	// Calculate actual time range in JST
	jstLocation, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		return nil, fmt.Errorf("failed to load JST timezone: %w", err)
	}

	// Get current time in JST
	now := time.Now().In(jstLocation)

	// Calculate start and end dates based on offsets
	startDate := now.AddDate(0, 0, int(startOffsetInt))
	endDate := now.AddDate(0, 0, int(endOffsetInt))

	// Set time to start of day for start date
	startOfDay := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, jstLocation)
	// Set time to start of day for end date (exclusive) - endOffset already represents the boundary
	endOfDay := time.Date(endDate.Year(), endDate.Month(), endDate.Day(), 0, 0, 0, 0, jstLocation)

	// Convert to UTC for database query
	startUTC := startOfDay.UTC()
	endUTC := endOfDay.UTC()

	// Create MongoDB time range query
	result := map[string]interface{}{
		"$gte": map[string]interface{}{
			"$date": startUTC.Format("2006-01-02T15:04:05.000Z"),
		},
		"$lt": map[string]interface{}{
			"$date": endUTC.Format("2006-01-02T15:04:05.000Z"),
		},
	}

	logrus.Infof("[TimeUtils] Converted time range query: start=%s, end=%s (JST) -> start=%s, end=%s (UTC)",
		startOfDay.Format("2006-01-02T15:04:05 JST"),
		endOfDay.Format("2006-01-02T15:04:05 JST"),
		startUTC.Format("2006-01-02T15:04:05.000Z"),
		endUTC.Format("2006-01-02T15:04:05.000Z"))

	return result, nil
}

// GetJSTTimeRange returns JST time range for given offsets
func GetJSTTimeRange(startOffset, endOffset int) (time.Time, time.Time, error) {
	jstLocation, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("failed to load JST timezone: %w", err)
	}

	now := time.Now().In(jstLocation)

	startDate := now.AddDate(0, 0, startOffset)
	endDate := now.AddDate(0, 0, endOffset)

	startOfDay := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, jstLocation)
	endOfDay := time.Date(endDate.Year(), endDate.Month(), endDate.Day(), 0, 0, 0, 0, jstLocation)

	return startOfDay, endOfDay, nil
}

// GetUTCTimeRange returns UTC time range for given offsets
func GetUTCTimeRange(startOffset, endOffset int) (time.Time, time.Time, error) {
	startJST, endJST, err := GetJSTTimeRange(startOffset, endOffset)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	return startJST.UTC(), endJST.UTC(), nil
}
