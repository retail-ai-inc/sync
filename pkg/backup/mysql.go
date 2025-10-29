package backup

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
)

// executeExternalMySQLBackupSimple executes complete external command backup for MySQL single table
func (e *BackupExecutor) executeExternalMySQLBackupSimple(ctx context.Context, connectionURL, database, table, tempDir string, config ExecutorBackupConfig) error {
	logrus.Infof("[BackupExecutor] 🚀 Starting MySQL external command backup for table: %s (format: %s)", table, config.Format)

	e.logMemoryUsage("MYSQL_BACKUP_START")

	// Parse connection URL
	host, port, username, password := buildMySQLConnectionString(connectionURL, config.Database.Username, config.Database.Password)

	// Clean table name and generate file paths
	baseTableName := e.extractTablePrefix(table)
	logrus.Infof("[BackupExecutor] 🔍 Original table name: %s, extracted base name: %s", table, baseTableName)

	dateStr := time.Now().AddDate(0, 0, -1).Format("2006-01-02")

	var outputPath, zipPath string
	var exportErr error

	// Determine format: SQL or CSV
	format := config.Format
	if format == "" {
		format = "sql" // Default to SQL format
	}

	switch strings.ToLower(format) {
	case "sql":
		outputPath = fmt.Sprintf("%s/%s%s%s.sql", tempDir, baseTableName, JSONFilenameSeparator, dateStr)
		zipPath = fmt.Sprintf("%s/%s%s%s.zip", tempDir, baseTableName, ZIPFilenameSeparator, dateStr)

		// Step 1: Execute mysqldump
		logrus.Infof("[BackupExecutor] 📤 Step 1: External mysqldump (SQL format)")
		exportErr = e.executeExternalMySQLDump(ctx, host, port, username, password, database, table, outputPath, config)

	case "csv":
		outputPath = fmt.Sprintf("%s/%s%s%s.csv", tempDir, baseTableName, JSONFilenameSeparator, dateStr)
		zipPath = fmt.Sprintf("%s/%s%s%s.zip", tempDir, baseTableName, ZIPFilenameSeparator, dateStr)

		// Step 1: Execute mysql CSV export
		logrus.Infof("[BackupExecutor] 📤 Step 1: External mysql CSV export")
		exportErr = e.executeExternalMySQLCSV(ctx, host, port, username, password, database, table, outputPath, config)

	default:
		return fmt.Errorf("unsupported format: %s (supported: sql, csv)", format)
	}

	if exportErr != nil {
		return fmt.Errorf("external MySQL export failed: %w", exportErr)
	}

	e.logMemoryUsage("AFTER_MYSQL_EXPORT")

	// Step 2: External zip command (both formats use zip)
	logrus.Infof("[BackupExecutor] 🗜️ Step 2: External zip compression")
	if err := e.executeExternalZip(ctx, tempDir, outputPath, zipPath); err != nil {
		return fmt.Errorf("external zip failed: %w", err)
	}

	e.logMemoryUsage("AFTER_ZIP")

	// Step 3: External GCS upload
	zipFileName := fmt.Sprintf("%s%s%s.zip", baseTableName, ZIPFilenameSeparator, dateStr)
	gcsPath := fmt.Sprintf("%s/%s", config.Destination.GCSPath, zipFileName)
	logrus.Infof("[BackupExecutor] ☁️ Step 3: External GCS upload")
	if err := e.executeExternalGCSUpload(ctx, zipPath, gcsPath); err != nil {
		return fmt.Errorf("external GCS upload failed: %w", err)
	}

	e.logMemoryUsage("MYSQL_BACKUP_COMPLETE")

	// Clean up temporary files
	if err := os.Remove(outputPath); err != nil {
		logrus.Warnf("[BackupExecutor] Failed to remove output file %s: %v", outputPath, err)
	} else {
		logrus.Debugf("[BackupExecutor] 🗑️  Cleaned up output file: %s", outputPath)
	}

	if err := os.Remove(zipPath); err != nil {
		logrus.Warnf("[BackupExecutor] Failed to remove ZIP file %s: %v", zipPath, err)
	} else {
		logrus.Debugf("[BackupExecutor] 🗑️  Cleaned up ZIP file: %s", zipPath)
	}

	logrus.Infof("[BackupExecutor] ✅ MySQL backup workflow completed for table: %s", table)
	return nil
}

// executeExternalMySQLDump executes mysqldump command with options
func (e *BackupExecutor) executeExternalMySQLDump(ctx context.Context, host, port, username, password, database, table, outputPath string, config ExecutorBackupConfig) error {
	args := []string{
		"-h", host,
		"-P", port,
		"-u", username,
	}

	// Add password if provided
	if password != "" {
		args = append(args, "-p"+password)
	}

	// Add mysqldump options
	args = append(args,
		"--single-transaction",
		"--skip-lock-tables",
		"--no-tablespaces",
		database,
		table,
	)

	// Add WHERE clause if query conditions exist
	if queryConditions, exists := config.Query[table]; exists && len(queryConditions) > 0 {
		whereClause := e.convertTimeRangeQueryForMySQL(queryConditions)
		if whereClause != "" {
			args = append(args, "--where", whereClause)
			logrus.Infof("[BackupExecutor] Applied WHERE clause for table %s: %s", table, whereClause)
		}
	} else {
		logrus.Warnf("[BackupExecutor] ⚠️  No query conditions found for table %s, exporting all data", table)
	}

	cmd := exec.CommandContext(ctx, "mysqldump", args...)

	// Create output file
	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	cmd.Stdout = outFile
	cmd.Stderr = os.Stderr

	// Display command line arguments with password masked
	logrus.Infof("[BackupExecutor] Executing: %s", e.maskMySQLPassword(append([]string{"mysqldump"}, args...)))

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("mysqldump failed: %w", err)
	}

	// Check output file and log size
	if stat, err := os.Stat(outputPath); err == nil {
		logrus.Infof("[BackupExecutor] ✅ Mysqldump completed: %.2f MB", float64(stat.Size())/1024/1024)
	} else {
		return fmt.Errorf("mysqldump output file not created: %w", err)
	}

	return nil
}

// executeExternalMySQLCSV executes mysql command to export CSV format
// Converts TSV output to standard CSV with quoted fields
func (e *BackupExecutor) executeExternalMySQLCSV(ctx context.Context, host, port, username, password, database, table, outputPath string, config ExecutorBackupConfig) error {
	args := []string{
		"-h", host,
		"-P", port,
		"-u", username,
	}

	// Add password if provided
	if password != "" {
		args = append(args, "-p"+password)
	}

	// Add database
	args = append(args, database)

	// Build SELECT query
	selectQuery := e.buildMySQLSelectQuery(table, config)

	args = append(args,
		"-e", selectQuery,
		"--batch",
		"--raw",
	)

	// Create mysql command
	mysqlCmd := exec.CommandContext(ctx, "mysql", args...)

	// Create sed command to convert TSV to standard CSV
	// sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g'
	sedCmd := exec.CommandContext(ctx, "sed", `s/\t/","/g;s/^/"/;s/$/"/;s/\n//g`)

	// Create output file
	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	// Pipe mysql output to sed, then to output file
	sedCmd.Stdin, err = mysqlCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create pipe: %w", err)
	}

	sedCmd.Stdout = outFile
	mysqlCmd.Stderr = os.Stderr
	sedCmd.Stderr = os.Stderr

	// Display command line arguments with password masked
	logrus.Infof("[BackupExecutor] Executing: %s | sed 's/\\t/\",\"/g;s/^/\"/;s/$/\"/;s/\\n//g' > %s",
		e.maskMySQLPassword(append([]string{"mysql"}, args...)), outputPath)

	// Start sed command first
	if err := sedCmd.Start(); err != nil {
		return fmt.Errorf("failed to start sed command: %w", err)
	}

	// Start mysql command
	if err := mysqlCmd.Start(); err != nil {
		return fmt.Errorf("failed to start mysql command: %w", err)
	}

	// Wait for mysql command to finish
	if err := mysqlCmd.Wait(); err != nil {
		return fmt.Errorf("mysql command failed: %w", err)
	}

	// Wait for sed command to finish
	if err := sedCmd.Wait(); err != nil {
		return fmt.Errorf("sed command failed: %w", err)
	}

	// Check output file and log size
	if stat, err := os.Stat(outputPath); err == nil {
		logrus.Infof("[BackupExecutor] ✅ MySQL CSV export completed: %.2f MB", float64(stat.Size())/1024/1024)
	} else {
		return fmt.Errorf("mysql CSV output file not created: %w", err)
	}

	return nil
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

// exportMySQLMergedTables performs multi-table merged backup for MySQL
func (e *BackupExecutor) exportMySQLMergedTables(ctx context.Context, connectionURL, database string, tables []string, tempDir string, config ExecutorBackupConfig) error {
	logrus.Infof("[BackupExecutor] 🚀 Starting MySQL multi-table merge backup for %d tables: %v", len(tables), tables)

	e.logMemoryUsage("MYSQL_MERGED_START")

	// Parse connection URL
	host, port, username, password := buildMySQLConnectionString(connectionURL, config.Database.Username, config.Database.Password)

	// Extract base name (remove date suffix)
	baseTableName := e.extractTablePrefix(tables[0])
	logrus.Infof("[BackupExecutor] 🔍 Original table name: %s, extracted base name: %s", tables[0], baseTableName)

	// Determine format
	format := config.Format
	if format == "" {
		format = "sql" // Default to SQL format
	}

	// Generate file names
	dateStr := time.Now().AddDate(0, 0, -1).Format("2006-01-02")

	var mergedFilePath, zipPath string
	var fileExt string

	switch strings.ToLower(format) {
	case "sql":
		fileExt = ".sql"
	case "csv":
		fileExt = ".csv"
	default:
		return fmt.Errorf("unsupported format: %s", format)
	}

	fileName := fmt.Sprintf("%s%s%s%s", baseTableName, JSONFilenameSeparator, dateStr, fileExt)
	zipFileName := fmt.Sprintf("%s%s%s.zip", baseTableName, ZIPFilenameSeparator, dateStr)

	mergedFilePath = filepath.Join(tempDir, fileName)
	zipPath = filepath.Join(tempDir, zipFileName)

	// Step 1: Export each table and merge
	logrus.Infof("[BackupExecutor] 📤 Step 1: Exporting and merging %d tables", len(tables))

	// Create merged file
	mergedFile, err := os.Create(mergedFilePath)
	if err != nil {
		return fmt.Errorf("failed to create merged file: %w", err)
	}
	defer mergedFile.Close()

	for i, table := range tables {
		logrus.Infof("[BackupExecutor] 📄 Exporting table %d/%d: %s", i+1, len(tables), table)

		// Create temporary file for each table
		tempTablePath := fmt.Sprintf("%s/%s%s%s_temp%s", tempDir, table, JSONFilenameSeparator, dateStr, fileExt)

		var exportErr error
		switch strings.ToLower(format) {
		case "sql":
			exportErr = e.executeExternalMySQLDump(ctx, host, port, username, password, database, table, tempTablePath, config)
		case "csv":
			exportErr = e.executeExternalMySQLCSV(ctx, host, port, username, password, database, table, tempTablePath, config)
		}

		if exportErr != nil {
			// If skipped due to no query conditions, continue processing next table
			if strings.Contains(exportErr.Error(), "no query conditions") {
				logrus.Infof("[BackupExecutor] ⏭️  Skipping table %s (no query conditions)", table)
				continue
			}
			return fmt.Errorf("failed to export table %s: %w", table, exportErr)
		}

		// Read temporary file and append to merged file
		content, err := os.ReadFile(tempTablePath)
		if err != nil {
			return fmt.Errorf("failed to read temp file for table %s: %w", table, err)
		}

		if len(content) > 0 {
			if _, err := mergedFile.Write(content); err != nil {
				return fmt.Errorf("failed to write data for table %s: %w", table, err)
			}
		}

		// Clean up temporary file
		if err := os.Remove(tempTablePath); err != nil {
			logrus.Warnf("[BackupExecutor] Failed to remove temp file %s: %v", tempTablePath, err)
		} else {
			logrus.Debugf("[BackupExecutor] 🗑️  Cleaned up temp file: %s", tempTablePath)
		}

		logrus.Infof("[BackupExecutor] ✅ Table %s merged successfully", table)
	}

	mergedFile.Close()

	if stat, err := os.Stat(mergedFilePath); err == nil {
		logrus.Infof("[BackupExecutor] ✅ Merge completed: %.2f MB", float64(stat.Size())/1024/1024)
	} else {
		logrus.Errorf("[BackupExecutor] ❌ Failed to stat merged file: %v", err)
	}

	e.logMemoryUsage("AFTER_MYSQL_MERGE")

	// Step 2: External zip command
	logrus.Infof("[BackupExecutor] 🗜️ Step 2: External zip compression")
	if err := e.executeExternalZip(ctx, tempDir, mergedFilePath, zipPath); err != nil {
		return fmt.Errorf("external zip failed: %w", err)
	}

	e.logMemoryUsage("AFTER_ZIP")

	// Step 3: External GCS upload
	gcsPath := fmt.Sprintf("%s/%s", config.Destination.GCSPath, zipFileName)
	logrus.Infof("[BackupExecutor] ☁️ Step 3: External GCS upload")
	if err := e.executeExternalGCSUpload(ctx, zipPath, gcsPath); err != nil {
		return fmt.Errorf("external GCS upload failed: %w", err)
	}

	e.logMemoryUsage("MYSQL_MERGED_COMPLETE")

	// Clean up temporary files
	if err := os.Remove(mergedFilePath); err != nil {
		logrus.Warnf("[BackupExecutor] Failed to remove merged file %s: %v", mergedFilePath, err)
	} else {
		logrus.Debugf("[BackupExecutor] 🗑️  Cleaned up merged file: %s", mergedFilePath)
	}

	if err := os.Remove(zipPath); err != nil {
		logrus.Warnf("[BackupExecutor] Failed to remove ZIP file %s: %v", zipPath, err)
	} else {
		logrus.Debugf("[BackupExecutor] 🗑️  Cleaned up ZIP file: %s", zipPath)
	}

	logrus.Infof("[BackupExecutor] ✅ MySQL multi-table merge backup completed successfully for %d tables", len(tables))
	return nil
}

// getMySQLTables queries MySQL INFORMATION_SCHEMA to get tables matching regex pattern
func (e *BackupExecutor) getMySQLTables(ctx context.Context, config *ExecutorBackupConfig, pattern string) ([]string, error) {
	// Parse connection URL
	host, port, username, password := parseMySQLConnectionURL(config.Database.URL)

	// Build DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", username, password, host, port, config.Database.Database)

	// Connect to MySQL
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MySQL: %w", err)
	}
	defer db.Close()

	// Test connection
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping MySQL: %w", err)
	}

	// Query tables from INFORMATION_SCHEMA
	query := `
		SELECT TABLE_NAME 
		FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA = ? 
		AND TABLE_TYPE = 'BASE TABLE'
		ORDER BY TABLE_NAME
	`

	rows, err := db.QueryContext(ctx, query, config.Database.Database)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var allTables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			continue
		}
		allTables = append(allTables, tableName)
	}

	// Filter tables by regex pattern
	var matchedTables []string
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid regex pattern: %w", err)
	}

	for _, table := range allTables {
		if re.MatchString(table) {
			matchedTables = append(matchedTables, table)
		}
	}

	logrus.Infof("[BackupExecutor] Found %d tables matching pattern %s: %v",
		len(matchedTables), pattern, matchedTables)
	return matchedTables, nil
}

// parseMySQLConnectionURL parses MySQL connection URL into components
// Format: host:port or just host
func parseMySQLConnectionURL(url string) (host, port, username, password string) {
	parts := strings.Split(url, ":")
	host = "localhost"
	port = "3306"

	if len(parts) >= 1 && parts[0] != "" {
		host = parts[0]
	}
	if len(parts) >= 2 && parts[1] != "" {
		port = parts[1]
	}

	return host, port, "", ""
}

// buildMySQLConnectionString builds MySQL connection parameters
func buildMySQLConnectionString(url, username, password string) (host, port, user, pass string) {
	host, port, _, _ = parseMySQLConnectionURL(url)
	user = username
	pass = password
	return
}

// maskMySQLPassword masks MySQL password in command arguments
func (e *BackupExecutor) maskMySQLPassword(args []string) string {
	maskedArgs := make([]string, len(args))
	copy(maskedArgs, args)

	for i, arg := range maskedArgs {
		// Mask password argument (-pPASSWORD)
		if strings.HasPrefix(arg, "-p") && len(arg) > 2 {
			maskedArgs[i] = "-p***"
		}
	}

	return strings.Join(maskedArgs, " ")
}
