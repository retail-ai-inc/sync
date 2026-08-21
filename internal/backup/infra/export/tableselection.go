package export

import (
	"context"
	"fmt"
	"regexp"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// expandAndGroupTables Expand regex patterns and group tables for merging
func (e *BackupExecutor) ExpandAndGroupTables(ctx context.Context, config *ExecutorBackupConfig) (map[string][]string, error) {

	tableGroups := make(map[string][]string)

	// Check if regex mode is enabled
	if config.TableSelectionMode == "regex" && config.RegexPattern != "" {
		var actualTables []string
		var err error

		// Get actual tables based on database type
		switch config.SourceType {
		case "mongodb":
			actualTables, err = e.getMongoDBCollections(ctx, config, config.RegexPattern)
			if err != nil {
				return nil, fmt.Errorf("failed to get MongoDB collections: %w", err)
			}
		case "mysql":
			actualTables, err = e.getMySQLTables(ctx, config, config.RegexPattern)
			if err != nil {
				return nil, fmt.Errorf("failed to get MySQL tables: %w", err)
			}
		default:
			return nil, fmt.Errorf("regex mode not supported for database type: %s", config.SourceType)
		}

		// Group tables by common prefix
		tempGroups := e.groupTablesByPrefix(actualTables)

		// Apply time range filtering to each group
		for groupName, tables := range tempGroups {
			filteredTables := e.filterRelevantTables(tables, config.Query, groupName)
			if len(filteredTables) > 0 {
				tableGroups[groupName] = filteredTables
				logrus.Infof("[BackupExecutor] Regex mode: filtered %d/%d tables for group %s: %v",
					len(filteredTables), len(tables), groupName, filteredTables)
			}
		}
		logrus.Debugf("[BackupExecutor] Regex mode enabled, found %d table groups from pattern %s (after filtering)",
			len(tableGroups), config.RegexPattern)
	} else {
		// Manual mode: group tables by common prefix for merging
		tempGroups := e.groupTablesByPrefix(config.Database.Tables)

		// If only one group found with multiple tables, merge them
		if len(tempGroups) == 1 {
			for groupName, tables := range tempGroups {
				if len(tables) > 1 {
					// Apply time range filtering for merged tables
					filteredTables := e.filterRelevantTables(tables, config.Query, groupName)
					if len(filteredTables) > 0 {
						tableGroups[groupName] = filteredTables
						logrus.Infof("[BackupExecutor] Manual mode: found %d/%d related tables for merging: %v",
							len(filteredTables), len(tables), filteredTables)
					}
				} else {
					// Single table, treat as individual
					tableGroups[tables[0]] = []string{tables[0]}
				}
			}
		} else {
			// Multiple groups or no grouping possible, treat each table individually
			for _, table := range config.Database.Tables {
				// Apply time range filtering for individual tables
				filteredTables := e.filterRelevantTables([]string{table}, config.Query, table)
				if len(filteredTables) > 0 {
					tableGroups[table] = filteredTables
				}
			}
			logrus.Infof("[BackupExecutor] Manual mode: processing %d individual tables (after filtering)", len(tableGroups))
		}
	}

	return tableGroups, nil
}

// getMongoDBCollections Get collections from MongoDB that match the regex pattern
func (e *BackupExecutor) getMongoDBCollections(ctx context.Context, config *ExecutorBackupConfig, pattern string) ([]string, error) {

	// Build connection string
	connStr := buildMongoDBConnectionString(config.Database.URL, config.Database.Username, config.Database.Password)

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connStr))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}
	defer client.Disconnect(ctx)

	// Get database
	database := client.Database(config.Database.Database)

	// List collections matching the pattern
	filter := bson.M{"name": primitive.Regex{Pattern: pattern, Options: ""}}
	cursor, err := database.ListCollections(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to list collections: %w", err)
	}
	defer cursor.Close(ctx)

	var collections []string
	for cursor.Next(ctx) {
		var result bson.M
		if err := cursor.Decode(&result); err != nil {
			continue
		}
		if name, ok := result["name"].(string); ok {
			collections = append(collections, name)
		}
	}

	logrus.Infof("[BackupExecutor] Found %d collections matching pattern %s: %v",
		len(collections), pattern, collections)
	return collections, nil
}

// groupTablesByPrefix Group tables by common prefix for merging
func (e *BackupExecutor) groupTablesByPrefix(tables []string) map[string][]string {
	groups := make(map[string][]string)

	for _, table := range tables {
		// Extract prefix by removing date/month suffixes
		prefix := e.extractTablePrefix(table)
		groups[prefix] = append(groups[prefix], table)
	}

	return groups
}

// extractTablePrefix Extract common prefix from table name
func (e *BackupExecutor) extractTablePrefix(tableName string) string {
	// Common patterns for date-based table names
	patterns := []string{
		`_\d{6}$`, // _YYYYMM (monthly)
		`_\d{8}$`, // _YYYYMMDD (daily)
		`_\d{4}$`, // _YYYY (yearly)
		`\d{6}$`,  // YYYYMM (monthly without underscore)
		`\d{8}$`,  // YYYYMMDD (daily without underscore)
		`\d+$`,    // Simple number suffix (e.g., users1, users2)
	}

	for _, pattern := range patterns {
		re := regexp.MustCompile(pattern)
		if re.MatchString(tableName) {
			result := re.ReplaceAllString(tableName, "")
			// For simple number suffix, ensure we have a meaningful prefix
			if pattern == `\d+$` && len(result) > 0 {
				logrus.Debugf("[BackupExecutor] Extracted prefix '%s' from table '%s'", result, tableName)
				return result
			} else if pattern != `\d+$` {
				return result
			}
		}
	}

	// If no pattern matches, return the original table name
	return tableName
}

// filterRelevantTables Filter tables based on query time range to avoid unnecessary exports
func (e *BackupExecutor) filterRelevantTables(tables []string, queryConditions map[string]map[string]interface{}, groupName string) []string {
	// If no query conditions exist, return all tables
	if len(queryConditions) == 0 {
		return tables
	}

	// Look for time range query in any table's query conditions
	var timeRange *TimeRange
	for _, query := range queryConditions {
		if tr := e.extractTimeRange(query); tr != nil {
			timeRange = tr
			break
		}
	}

	// If no time range found, return all tables
	if timeRange == nil {
		return tables
	}

	var relevantTables []string
	for _, table := range tables {
		if e.isTableRelevantForTimeRange(table, timeRange) {
			relevantTables = append(relevantTables, table)
		} else {
			logrus.Debugf("[BackupExecutor] ⏭ Skipping table %s (outside time range)", table)
		}
	}

	// If no relevant tables found, return the first table as fallback
	if len(relevantTables) == 0 {
		logrus.Warnf("[BackupExecutor] No tables match time range, using first table as fallback: %s", tables[0])
		return []string{tables[0]}
	}

	return relevantTables
}

// TimeRange represents a time range for filtering
type TimeRange struct {
	Start time.Time
	End   time.Time
}

// extractTimeRange Extract time range from query conditions
func (e *BackupExecutor) extractTimeRange(query map[string]interface{}) *TimeRange {
	for _, value := range query {
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

				// Calculate JST time range
				now := time.Now()
				jst := time.FixedZone("JST", 9*3600)

				startJST := now.In(jst).AddDate(0, 0, startOffset).Truncate(24 * time.Hour)
				endJST := now.In(jst).AddDate(0, 0, endOffset+1).Truncate(24 * time.Hour)

				// Convert to UTC
				startUTC := startJST.Add(-9 * time.Hour)
				endUTC := endJST.Add(-9 * time.Hour)

				return &TimeRange{
					Start: startUTC,
					End:   endUTC,
				}
			}
		}
	}
	return nil
}

// isTableRelevantForTimeRange Check if a table is relevant for the given time range
func (e *BackupExecutor) isTableRelevantForTimeRange(tableName string, timeRange *TimeRange) bool {
	// Extract table time pattern
	tableTime := e.extractTableTimePattern(tableName)
	if tableTime == nil {
		// If we can't determine table time, include it to be safe
		return true
	}

	// Check if table time range overlaps with query time range
	return !(tableTime.End.Before(timeRange.Start) || tableTime.Start.After(timeRange.End))
}

// extractTableTimePattern Extract time pattern from table name and return its time range
func (e *BackupExecutor) extractTableTimePattern(tableName string) *TimeRange {
	// Pattern for YYYYMM (monthly tables)
	if re := regexp.MustCompile(`_(\d{6})$`); re.MatchString(tableName) {
		matches := re.FindStringSubmatch(tableName)
		if len(matches) >= 2 {
			if year, month, err := parseYearMonth(matches[1]); err == nil {
				start := time.Date(year, month, 1, 0, 0, 0, 0, time.UTC)
				end := start.AddDate(0, 1, 0)
				return &TimeRange{Start: start, End: end}
			}
		}
	}

	// Pattern for YYYYMMDD (daily tables)
	if re := regexp.MustCompile(`_(\d{8})$`); re.MatchString(tableName) {
		matches := re.FindStringSubmatch(tableName)
		if len(matches) >= 2 {
			if date, err := time.Parse("20060102", matches[1]); err == nil {
				start := date.UTC()
				end := start.AddDate(0, 0, 1)
				return &TimeRange{Start: start, End: end}
			}
		}
	}

	// Pattern for YYYY (yearly tables)
	if re := regexp.MustCompile(`_(\d{4})$`); re.MatchString(tableName) {
		matches := re.FindStringSubmatch(tableName)
		if len(matches) >= 2 {
			if year, err := parseYear(matches[1]); err == nil {
				start := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
				end := start.AddDate(1, 0, 0)
				return &TimeRange{Start: start, End: end}
			}
		}
	}

	return nil
}

// parseYearMonth Parse YYYYMM string to year and month
func parseYearMonth(yyyymm string) (int, time.Month, error) {
	if len(yyyymm) != 6 {
		return 0, 0, fmt.Errorf("invalid YYYYMM format: %s", yyyymm)
	}

	year := 0
	month := 0

	for _, r := range yyyymm[:4] {
		if r < '0' || r > '9' {
			return 0, 0, fmt.Errorf("invalid year: %s", yyyymm[:4])
		}
		year = year*10 + int(r-'0')
	}

	for _, r := range yyyymm[4:] {
		if r < '0' || r > '9' {
			return 0, 0, fmt.Errorf("invalid month: %s", yyyymm[4:])
		}
		month = month*10 + int(r-'0')
	}

	if month < 1 || month > 12 {
		return 0, 0, fmt.Errorf("invalid month: %d", month)
	}

	return year, time.Month(month), nil
}

// parseYear Parse YYYY string to year
func parseYear(yyyy string) (int, error) {
	if len(yyyy) != 4 {
		return 0, fmt.Errorf("invalid YYYY format: %s", yyyy)
	}

	year := 0
	for _, r := range yyyy {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("invalid year: %s", yyyy)
		}
		year = year*10 + int(r-'0')
	}

	return year, nil
}
