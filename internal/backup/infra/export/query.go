package export

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/retail-ai-inc/sync/internal/platform/timex"
	"github.com/sirupsen/logrus"
)

// convertTimeRangeQuery converts a dynamic time range condition into a
// concrete MongoDB query. The window arithmetic lives in timex and is shared
// with the MySQL WHERE clause and with table selection.
func (e *BackupExecutor) convertTimeRangeQuery(query map[string]interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	for key, value := range query {
		timeQuery, ok := value.(map[string]interface{})
		if !ok {
			// Keep non-object values as is
			result[key] = value
			continue
		}
		if timeType, exists := timeQuery["type"]; !exists || timeType != "daily" {
			// Keep non-time queries as is
			result[key] = value
			continue
		}

		startUTC, endUTC, err := dailyWindow(timeQuery)
		if err != nil {
			return nil, fmt.Errorf("the time range on %s: %w", key, err)
		}

		result[key] = map[string]interface{}{
			"$gte": map[string]interface{}{
				"$date": startUTC.Format("2006-01-02T15:04:05.000Z"),
			},
			"$lt": map[string]interface{}{
				"$date": endUTC.Format("2006-01-02T15:04:05.000Z"),
			},
		}
		logrus.Infof("[BackupExecutor] Time range on %s: %s to %s (UTC)",
			key, startUTC.Format("2006-01-02T15:04:05.000Z"), endUTC.Format("2006-01-02T15:04:05.000Z"))
	}

	return result, nil
}

// dailyWindow resolves one {"type":"daily"} condition to the half-open UTC
// interval it names.
func dailyWindow(spec map[string]interface{}) (time.Time, time.Time, error) {
	startOffset, endOffset, err := timex.DailyOffsets(spec)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	return timex.GetUTCTimeRange(startOffset, endOffset)
}

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

func (e *BackupExecutor) buildMySQLSelectQuery(table string, config ExecutorBackupConfig) (string, error) {
	// Build field list
	fields := "*"
	if fieldList, exists := config.Database.Fields[table]; exists && len(fieldList) > 0 && fieldList[0] != "all" {
		fields = strings.Join(fieldList, ", ")
	}

	// Build WHERE clause
	whereClause := ""
	if queryConditions, exists := config.Query[table]; exists && len(queryConditions) > 0 {
		var err error
		if whereClause, err = e.convertTimeRangeQueryForMySQL(queryConditions); err != nil {
			return "", fmt.Errorf("build the filter for %s: %w", table, err)
		}
	}

	// Construct SELECT query
	query := fmt.Sprintf("SELECT %s FROM %s", fields, table)
	if whereClause != "" {
		query += " WHERE " + whereClause
	}

	logrus.Infof("[BackupExecutor] Built SELECT query: %s", query)
	return query, nil
}

// convertTimeRangeQueryForMySQL converts a dynamic time range query into a
// MySQL WHERE clause.
//
// A condition it cannot render is now an error rather than a warning. Dropping
// it left the clause empty, which turns a filtered export into a full-table
// export — a much larger file, quietly, with the job still reporting success.
func (e *BackupExecutor) convertTimeRangeQueryForMySQL(query map[string]interface{}) (string, error) {
	var conditions []string

	for key, value := range query {
		column, err := quoteMySQLIdentifier(key)
		if err != nil {
			return "", err
		}

		if timeQuery, ok := value.(map[string]interface{}); ok {
			timeType, exists := timeQuery["type"]
			if !exists || timeType != "daily" {
				return "", fmt.Errorf("the condition on %s is a %v range, which this "+
					"does not know how to express", key, timeType)
			}

			startUTC, endUTC, err := dailyWindow(timeQuery)
			if err != nil {
				return "", fmt.Errorf("the time range on %s: %w", key, err)
			}

			conditions = append(conditions, fmt.Sprintf("%s >= '%s' AND %s < '%s'",
				column, startUTC.Format("2006-01-02 15:04:05"),
				column, endUTC.Format("2006-01-02 15:04:05")))
			continue
		}

		switch v := value.(type) {
		case string:
			conditions = append(conditions, fmt.Sprintf("%s = '%s'", column, escapeMySQLString(v)))
		case float64:
			conditions = append(conditions, fmt.Sprintf("%s = %v", column, v))
		case int:
			conditions = append(conditions, fmt.Sprintf("%s = %d", column, v))
		case bool:
			conditions = append(conditions, fmt.Sprintf("%s = %t", column, v))
		default:
			return "", fmt.Errorf("the condition on %s is a %T, which this does not "+
				"know how to express", key, value)
		}
	}

	// The order a map iterates in is not stable, and the clause ends up in a
	// command line and in the logs, so it is sorted to stay comparable.
	sort.Strings(conditions)
	return strings.Join(conditions, " AND "), nil
}

// identifier is what a column name is allowed to look like. The name comes from
// a backup job's configuration document and used to be interpolated into the
// SQL text with no escaping and no check at all, which made every WHERE clause
// an injection point into a statement the mysql client then executed.
var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]*$`)

func quoteMySQLIdentifier(name string) (string, error) {
	if !identifier.MatchString(name) {
		return "", fmt.Errorf("%q is not a column name", name)
	}
	return "`" + name + "`", nil
}

// escapeMySQLString escapes a value for a single-quoted literal.
//
// Doubling the quote was the whole of the old escaping, and MySQL treats a
// backslash as an escape character by default, so a value ending in one escaped
// the closing quote and let the rest of the value out into the statement.
func escapeMySQLString(value string) string {
	replacer := strings.NewReplacer(
		"\\", "\\\\",
		"'", "''",
		"\x00", "\\0",
		"\n", "\\n",
		"\r", "\\r",
		"\x1a", "\\Z",
	)
	return replacer.Replace(value)
}
