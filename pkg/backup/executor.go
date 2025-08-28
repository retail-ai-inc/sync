package backup

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/retail-ai-inc/sync/pkg/utils"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// BackupExecutor Backup executor
type BackupExecutor struct {
	db *sql.DB
}

// ExecutorBackupConfig Configuration structure for backup operations
type ExecutorBackupConfig struct {
	Name       string `json:"name"`
	SourceType string `json:"sourceType"`
	Database   struct {
		URL      string              `json:"url"`
		Username string              `json:"username"`
		Password string              `json:"password"`
		Database string              `json:"database"`
		Tables   []string            `json:"tables"`
		Fields   map[string][]string `json:"fields"`
	} `json:"database"`
	Destination struct {
		GCSPath         string `json:"gcsPath"`
		Retention       int    `json:"retention"`
		ServiceAccount  string `json:"serviceAccount"`
		FileNamePattern string `json:"fileNamePattern"`
	} `json:"destination"`
	Format             string                            `json:"format"`
	BackupType         string                            `json:"backupType"`
	Query              map[string]map[string]interface{} `json:"query"`
	CompressionType    string                            `json:"compressionType"`
	TableSelectionMode string                            `json:"tableSelectionMode"`
	RegexPattern       string                            `json:"regexPattern"`
}

// NewBackupExecutor Create a new backup executor
func NewBackupExecutor(db *sql.DB) *BackupExecutor {
	return &BackupExecutor{db: db}
}

// copyFile Copy file from source to destination
func (e *BackupExecutor) copyFile(src, dst string) error {
	// Log memory before file copy
	var memStatsBefore runtime.MemStats
	runtime.ReadMemStats(&memStatsBefore)
	logrus.Infof("[BackupExecutor] 📋 Memory BEFORE copyFile: Alloc=%.2fMB, Sys=%.2fMB",
		float64(memStatsBefore.Alloc)/1024/1024, float64(memStatsBefore.Sys)/1024/1024)

	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	// Create destination directory if it doesn't exist
	dstDir := filepath.Dir(dst)
	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return err
	}

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	// Use buffered copy with controlled buffer size to avoid memory spike
	bufSize := 64 * 1024 // 64KB buffer instead of default 32KB
	buf := make([]byte, bufSize)

	logrus.Infof("[BackupExecutor] 📋 Starting file copy with %dKB buffer...", bufSize/1024)

	for {
		n, readErr := sourceFile.Read(buf)
		if n > 0 {
			_, writeErr := destFile.Write(buf[:n])
			if writeErr != nil {
				return writeErr
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}

	// Log memory after file copy
	var memStatsAfter runtime.MemStats
	runtime.ReadMemStats(&memStatsAfter)
	logrus.Infof("[BackupExecutor] 📋 Memory AFTER copyFile: Alloc=%.2fMB, Sys=%.2fMB (Delta: +%.2fMB)",
		float64(memStatsAfter.Alloc)/1024/1024,
		float64(memStatsAfter.Sys)/1024/1024,
		float64(memStatsAfter.Alloc-memStatsBefore.Alloc)/1024/1024)

	return nil
}

// processFileNamePattern Process file name pattern and replace date placeholders
func processFileNamePattern(pattern, tableName string) string {
	if pattern == "" {
		// Fallback to default pattern if not specified, use yesterday's date
		dateStr := time.Now().AddDate(0, 0, -1).Format("2006-01-02")
		return fmt.Sprintf("%s_%s", tableName, dateStr)
	}

	// Remove regex anchors if present
	cleanPattern := strings.TrimPrefix(pattern, "^")
	cleanPattern = strings.TrimSuffix(cleanPattern, "$")

	// Replace date placeholders using utils function with yesterday's date
	yesterdayDate := time.Now().AddDate(0, 0, -1)
	result := utils.ReplaceDatePlaceholdersWithDate(cleanPattern, yesterdayDate)

	// If the pattern contains table placeholder, replace it
	if strings.Contains(result, "{table}") || strings.Contains(result, "{TABLE}") {
		result = strings.ReplaceAll(result, "{table}", tableName)
		result = strings.ReplaceAll(result, "{TABLE}", strings.ToUpper(tableName))
	} else {
		// If no table placeholder, prepend table name
		ext := filepath.Ext(result)
		if ext != "" {
			nameWithoutExt := strings.TrimSuffix(result, ext)
			result = fmt.Sprintf("%s_%s%s", tableName, nameWithoutExt, ext)
		} else {
			result = fmt.Sprintf("%s_%s", tableName, result)
		}
	}

	logrus.Debugf("[BackupExecutor] Processed file name pattern '%s' for table '%s' -> '%s'", pattern, tableName, result)
	return result
}

// Execute Execute backup task
func (e *BackupExecutor) Execute(ctx context.Context, taskID int) error {
	// Query backup task
	task, err := e.getBackupTask(ctx, taskID)
	if err != nil {
		return fmt.Errorf("failed to get backup task: %w", err)
	}

	// Parse configuration
	var config ExecutorBackupConfig

	if err := json.Unmarshal([]byte(task.ConfigJSON), &config); err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	logrus.Debugf("[BackupExecutor] Starting backup for task %d (%s, type: %s)",
		taskID, config.Name, config.SourceType)

	// Expand regex patterns and group tables for merging
	tableGroups, err := e.ExpandAndGroupTables(ctx, &config)
	if err != nil {
		return fmt.Errorf("failed to expand table patterns: %w", err)
	}

	// Process each table group separately
	for groupName, tables := range tableGroups {
		logrus.Debugf("[BackupExecutor] Processing table group: %s (%d tables)", groupName, len(tables))

		// Create temporary directory for this table group
		tempDir, err := os.MkdirTemp("", fmt.Sprintf("backup_%d_%s_", taskID, groupName))
		if err != nil {
			logrus.Errorf("[BackupExecutor] Failed to create temp directory for table group %s: %v", groupName, err)
			continue
		}

		// 🚀 Use external command mode directly for backup
		var exportErr error
		switch config.SourceType {
		case "mongodb":
			if len(tables) == 1 {
				// Single table export: use external command mode directly
				logrus.Infof("[BackupExecutor] 🚀 Starting external command backup for single table: %s", tables[0])
				connStr := buildMongoDBConnectionString(config.Database.URL, config.Database.Username, config.Database.Password)
				exportErr = e.executeExternalMongoExportSimple(ctx, connStr, config.Database.Database, tables[0], tempDir, config)
			} else {
				// Multi-table merged export: use external command mode
				logrus.Infof("[BackupExecutor] 🚀 Starting external command backup for %d merged tables: %v", len(tables), tables)
				connStr := buildMongoDBConnectionString(config.Database.URL, config.Database.Username, config.Database.Password)
				exportErr = e.exportMongoDBMergedTables(ctx, connStr, config.Database.Database, tables, tempDir, config)
			}
		default:
			exportErr = fmt.Errorf("unsupported database type: %s", config.SourceType)
		}

		if exportErr != nil {
			logrus.Errorf("[BackupExecutor] External command backup failed for table group %s: %v", groupName, exportErr)
			os.RemoveAll(tempDir) // Clean up failed export
			continue
		}

		// 🎉 External command mode has completed the full backup workflow (export + compression + upload)
		logrus.Infof("[BackupExecutor] ✅ External command backup completed successfully for table group: %s", groupName)

		// Clean up temporary directory
		if err := os.RemoveAll(tempDir); err != nil {
			logrus.Warnf("[BackupExecutor] Failed to remove temp directory %s: %v", tempDir, err)
		} else {
			logrus.Debugf("[BackupExecutor] 🗑️  Cleaned up temp directory: %s", tempDir)
		}
	}

	logrus.Debugf("[BackupExecutor] All table backups completed for task %d", taskID)
	return nil
}

// getBackupTask Get backup task information
func (e *BackupExecutor) getBackupTask(ctx context.Context, taskID int) (BackupTask, error) {
	var task BackupTask
	var lastUpdateTime, lastBackupTime, nextBackupTime sql.NullString

	query := `SELECT id, enable, last_update_time, last_backup_time, next_backup_time, config_json 
			 FROM backup_tasks WHERE id = ?`

	err := e.db.QueryRowContext(ctx, query, taskID).Scan(
		&task.ID, &task.Enable, &lastUpdateTime, &lastBackupTime, &nextBackupTime, &task.ConfigJSON)

	if err != nil {
		return task, fmt.Errorf("failed to query backup task: %w", err)
	}

	// Parse timestamps
	if lastUpdateTime.Valid {
		t, _ := utils.ParseDatabaseTimestamp(lastUpdateTime.String)
		task.LastUpdateTime = t
	}
	if lastBackupTime.Valid {
		t, _ := utils.ParseDatabaseTimestamp(lastBackupTime.String)
		task.LastBackupTime = t
	}
	if nextBackupTime.Valid {
		t, _ := utils.ParseDatabaseTimestamp(nextBackupTime.String)
		task.NextBackupTime = t
	}

	return task, nil
}

// executeCommand General function to execute commands (deprecated - use executeCommandStreaming)
func executeCommand(cmd *exec.Cmd, commandName string) error {
	return executeCommandStreaming(cmd, commandName)
}

// executeCommandStreaming Execute command with streaming output to reduce memory usage
func executeCommandStreaming(cmd *exec.Cmd, commandName string) error {
	logrus.Infof("[BackupExecutor] Executing command: %s %s", filepath.Base(cmd.Path), strings.Join(cmd.Args[1:], " "))

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start command: %w", err)
	}

	// Process stdout in streaming fashion, log detailed progress
	var exportedCount string
	var progressLines []string
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			// Log all important lines for debugging
			if strings.Contains(line, "exported") || strings.Contains(line, "documents") || strings.Contains(line, "records") || strings.Contains(line, "progress") {
				logrus.Infof("[BackupExecutor] %s output: %s", commandName, line)
				progressLines = append(progressLines, line)
				if strings.Contains(line, "exported") && strings.Contains(line, "records") {
					exportedCount = strings.TrimSpace(line)
				}
			} else {
				// Log other output periodically to avoid spam but maintain visibility
				if len(progressLines)%100 == 0 {
					logrus.Debugf("[BackupExecutor] %s: %s", commandName, line)
				}
			}
		}
	}()

	// Collect error output (errors need to be preserved)
	var errorLines []string
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			errorLines = append(errorLines, scanner.Text())
		}
	}()

	// Wait for command completion
	err = cmd.Wait()

	// Log only key information
	if exportedCount != "" {
		logrus.Infof("[BackupExecutor] %s: %s", commandName, exportedCount)
	}

	if err != nil {
		errorMsg := strings.Join(errorLines, "\n")
		return fmt.Errorf("%s command failed: %w, output: %s", commandName, err, errorMsg)
	}

	return nil
}

// expandAndGroupTables Expand regex patterns and group tables for merging
func (e *BackupExecutor) ExpandAndGroupTables(ctx context.Context, config *ExecutorBackupConfig) (map[string][]string, error) {

	tableGroups := make(map[string][]string)

	// Check if regex mode is enabled
	if config.TableSelectionMode == "regex" && config.RegexPattern != "" {
		// Get actual collections from MongoDB using regex pattern
		actualTables, err := e.getMongoDBCollections(ctx, config, config.RegexPattern)
		if err != nil {
			return nil, fmt.Errorf("failed to get MongoDB collections: %w", err)
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

// readJSONFile Read JSON documents from exported file
func (e *BackupExecutor) readJSONFile(tempDir, tableName, dateStr string) ([]interface{}, error) {
	fileName := fmt.Sprintf("%s_%s.json", tableName, dateStr)
	filePath := filepath.Join(tempDir, fileName)

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", filePath, err)
	}

	// MongoDB mongoexport outputs each document on a separate line (JSONL format)
	// We need to parse each line as a separate JSON object
	var documents []interface{}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")

	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue // Skip empty lines
		}

		var doc interface{}
		if err := json.Unmarshal([]byte(line), &doc); err != nil {
			logrus.Warnf("[BackupExecutor] Failed to parse JSON line %d in %s: %v", i+1, filePath, err)
			continue
		}
		documents = append(documents, doc)
	}

	return documents, nil
}

// writeJSONFile Write JSON documents to file in JSONL format (one JSON object per line)
func (e *BackupExecutor) writeJSONFile(filePath string, documents []interface{}) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filePath, err)
	}
	defer file.Close()

	// Write each document as a separate line (JSONL format)
	for _, doc := range documents {
		docBytes, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("failed to marshal JSON document: %w", err)
		}

		// Write document followed by newline
		if _, err := file.Write(docBytes); err != nil {
			return fmt.Errorf("failed to write document to file: %w", err)
		}
		if _, err := file.WriteString("\n"); err != nil {
			return fmt.Errorf("failed to write newline to file: %w", err)
		}
	}

	return nil
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

// buildMongoDBConnectionString Build MongoDB connection string with authentication
func buildMongoDBConnectionString(url, username, password string) string {
	// Ensure localhost is preserved and not replaced
	if strings.Contains(url, "localhost") {
		logrus.Infof("[BackupExecutor] Using localhost MongoDB connection: %s", url)
	}

	var connStr string
	if username != "" && password != "" {
		connStr = fmt.Sprintf("mongodb://%s:%s@%s/?authSource=admin&directConnection=true", username, password, url)
	} else {
		connStr = fmt.Sprintf("mongodb://%s/?directConnection=true", url)
	}

	return connStr
}
