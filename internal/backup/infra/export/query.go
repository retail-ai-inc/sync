package export

import (
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
)

// convertTimeRangeQuery Convert dynamic time range query to concrete MongoDB query
func (e *BackupExecutor) convertTimeRangeQuery(query map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	for key, value := range query {
		if timeQuery, ok := value.(map[string]interface{}); ok {
			if timeType, exists := timeQuery["type"]; exists && timeType == "daily" {
				// Parse offset values
				startOffset := -1
				endOffset := 0

				if so, ok := timeQuery["startOffset"]; ok {
					if offset, ok := so.(float64); ok {
						startOffset = int(offset)
					}
				}
				if eo, ok := timeQuery["endOffset"]; ok {
					if offset, ok := eo.(float64); ok {
						endOffset = int(offset)
					}
				}

				// Calculate JST time range and convert to UTC for database query
				now := time.Now()
				jst := time.FixedZone("JST", 9*3600)

				// Get current JST time and truncate to start of day
				nowJST := now.In(jst)

				// Calculate start and end days in JST
				startDayJST := time.Date(nowJST.Year(), nowJST.Month(), nowJST.Day()+startOffset, 0, 0, 0, 0, jst)
				endDayJST := time.Date(nowJST.Year(), nowJST.Month(), nowJST.Day()+endOffset, 0, 0, 0, 0, jst)

				// Convert JST times to UTC
				startUTC := startDayJST.UTC()
				endUTC := endDayJST.UTC()

				logrus.Infof("[BackupExecutor] Time calculation: now=%s, startOffset=%d, endOffset=%d",
					nowJST.Format("2006-01-02 15:04:05 JST"), startOffset, endOffset)
				logrus.Infof("[BackupExecutor] JST range: %s to %s",
					startDayJST.Format("2006-01-02 15:04:05 JST"), endDayJST.Format("2006-01-02 15:04:05 JST"))
				logrus.Infof("[BackupExecutor] UTC range: %s to %s",
					startUTC.Format("2006-01-02T15:04:05.000Z"), endUTC.Format("2006-01-02T15:04:05.000Z"))

				// Create MongoDB date range query
				mongoQuery := map[string]interface{}{
					"$gte": map[string]interface{}{
						"$date": startUTC.Format("2006-01-02T15:04:05.000Z"),
					},
					"$lt": map[string]interface{}{
						"$date": endUTC.Format("2006-01-02T15:04:05.000Z"),
					},
				}

				result[key] = mongoQuery
				logrus.Infof("[BackupExecutor] Converted time range query for field %s: %s to %s",
					key, startUTC.Format("2006-01-02T15:04:05.000Z"), endUTC.Format("2006-01-02T15:04:05.000Z"))
			} else {
				// Keep non-time queries as is
				result[key] = value
			}
		} else {
			// Keep non-object values as is
			result[key] = value
		}
	}

	return result
}

// cleanQueryStringValues Clean string values in query condition to remove extra escaping
func cleanQueryStringValues(queryObj map[string]interface{}) map[string]interface{} {
	cleaned := make(map[string]interface{})

	for key, value := range queryObj {
		switch v := value.(type) {
		case string:
			// Remove surrounding quotes if they exist (handle over-escaping)
			cleanValue := v
			// Remove extra double quotes from the beginning and end
			if strings.HasPrefix(cleanValue, `"`) && strings.HasSuffix(cleanValue, `"`) {
				cleanValue = strings.TrimPrefix(cleanValue, `"`)
				cleanValue = strings.TrimSuffix(cleanValue, `"`)
			}
			// Remove extra single quotes from the beginning and end
			if strings.HasPrefix(cleanValue, `'`) && strings.HasSuffix(cleanValue, `'`) {
				cleanValue = strings.TrimPrefix(cleanValue, `'`)
				cleanValue = strings.TrimSuffix(cleanValue, `'`)
			}
			cleaned[key] = cleanValue
		case map[string]interface{}:
			// Recursively clean nested objects
			cleaned[key] = cleanQueryStringValues(v)
		default:
			// Keep other types as is
			cleaned[key] = value
		}
	}

	return cleaned
}

// buildMySQLSelectQuery builds SELECT query with WHERE conditions and field selection
func (e *BackupExecutor) buildMySQLSelectQuery(table string, config ExecutorBackupConfig) string {
	// Build field list
	fields := "*"
	if fieldList, exists := config.Database.Fields[table]; exists && len(fieldList) > 0 && fieldList[0] != "all" {
		fields = strings.Join(fieldList, ", ")
	}

	// Build WHERE clause
	whereClause := ""
	if queryConditions, exists := config.Query[table]; exists && len(queryConditions) > 0 {
		whereClause = e.convertTimeRangeQueryForMySQL(queryConditions)
	}

	// Construct SELECT query
	query := fmt.Sprintf("SELECT %s FROM %s", fields, table)
	if whereClause != "" {
		query += " WHERE " + whereClause
	}

	logrus.Infof("[BackupExecutor] Built SELECT query: %s", query)
	return query
}

// convertTimeRangeQueryForMySQL converts dynamic time range query to MySQL WHERE clause
func (e *BackupExecutor) convertTimeRangeQueryForMySQL(query map[string]interface{}) string {
	var conditions []string

	for key, value := range query {
		if timeQuery, ok := value.(map[string]interface{}); ok {
			if timeType, exists := timeQuery["type"]; exists && timeType == "daily" {
				// Parse offset values
				startOffset := -1
				endOffset := 0

				if so, ok := timeQuery["startOffset"]; ok {
					if offset, ok := so.(float64); ok {
						startOffset = int(offset)
					}
				}
				if eo, ok := timeQuery["endOffset"]; ok {
					if offset, ok := eo.(float64); ok {
						endOffset = int(offset)
					}
				}

				// Calculate JST time range and convert to UTC for database query
				now := time.Now()
				jst := time.FixedZone("JST", 9*3600)

				// Get current JST time and truncate to start of day
				nowJST := now.In(jst)

				// Calculate start and end days in JST
				startDayJST := time.Date(nowJST.Year(), nowJST.Month(), nowJST.Day()+startOffset, 0, 0, 0, 0, jst)
				endDayJST := time.Date(nowJST.Year(), nowJST.Month(), nowJST.Day()+endOffset, 0, 0, 0, 0, jst)

				// Convert JST times to UTC
				startUTC := startDayJST.UTC()
				endUTC := endDayJST.UTC()

				logrus.Infof("[BackupExecutor] Time calculation: now=%s, startOffset=%d, endOffset=%d",
					nowJST.Format("2006-01-02 15:04:05 JST"), startOffset, endOffset)
				logrus.Infof("[BackupExecutor] JST range: %s to %s",
					startDayJST.Format("2006-01-02 15:04:05 JST"), endDayJST.Format("2006-01-02 15:04:05 JST"))
				logrus.Infof("[BackupExecutor] UTC range: %s to %s",
					startUTC.Format("2006-01-02 15:04:05"), endUTC.Format("2006-01-02 15:04:05"))

				// Create MySQL WHERE clause
				condition := fmt.Sprintf("%s >= '%s' AND %s < '%s'",
					key, startUTC.Format("2006-01-02 15:04:05"),
					key, endUTC.Format("2006-01-02 15:04:05"))

				conditions = append(conditions, condition)
				logrus.Infof("[BackupExecutor] Converted time range query for field %s: %s", key, condition)
			} else {
				// Handle other query types if needed
				logrus.Warnf("[BackupExecutor] Unsupported query type for field %s: %v", key, timeType)
			}
		} else {
			// Simple equality condition
			switch v := value.(type) {
			case string:
				conditions = append(conditions, fmt.Sprintf("%s = '%s'", key, strings.ReplaceAll(v, "'", "''")))
			case float64:
				conditions = append(conditions, fmt.Sprintf("%s = %v", key, v))
			case int:
				conditions = append(conditions, fmt.Sprintf("%s = %d", key, v))
			default:
				logrus.Warnf("[BackupExecutor] Unsupported value type for field %s: %T", key, value)
			}
		}
	}

	return strings.Join(conditions, " AND ")
}
