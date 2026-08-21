package export

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
	"github.com/retail-ai-inc/sync/internal/backup/infra/transfer"
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

	// Step 2: External zip command, unless compression is explicitly disabled
	uploadPath := zipPath
	uploadName := fmt.Sprintf("%s%s%s.zip", baseTableName, ZIPFilenameSeparator, dateStr)
	skipCompression := isCompressionDisabled(config.CompressionType)

	if skipCompression {
		uploadPath = outputPath
		uploadName = filepath.Base(outputPath)
		logrus.Infof("[BackupExecutor] ⏭️  Step 2: Compression disabled, uploading %s as-is", uploadName)
	} else {
		logrus.Infof("[BackupExecutor] 🗜️ Step 2: External zip compression")
		if err := transfer.Zip(ctx, tempDir, outputPath, zipPath); err != nil {
			return fmt.Errorf("external zip failed: %w", err)
		}
		e.logMemoryUsage("AFTER_ZIP")
	}

	// Step 3: External GCS upload
	gcsPath := fmt.Sprintf("%s/%s", config.Destination.GCSPath, uploadName)
	logrus.Infof("[BackupExecutor] ☁️ Step 3: External GCS upload")
	if err := transfer.UploadGCS(ctx, uploadPath, gcsPath); err != nil {
		return fmt.Errorf("external GCS upload failed: %w", err)
	}

	e.logMemoryUsage("MYSQL_BACKUP_COMPLETE")

	// Clean up temporary files
	if err := os.Remove(outputPath); err != nil {
		logrus.Warnf("[BackupExecutor] Failed to remove output file %s: %v", outputPath, err)
	} else {
		logrus.Debugf("[BackupExecutor] 🗑️  Cleaned up output file: %s", outputPath)
	}

	if !skipCompression {
		if err := os.Remove(zipPath); err != nil {
			logrus.Warnf("[BackupExecutor] Failed to remove ZIP file %s: %v", zipPath, err)
		} else {
			logrus.Debugf("[BackupExecutor] 🗑️  Cleaned up ZIP file: %s", zipPath)
		}
	}

	logrus.Infof("[BackupExecutor] ✅ MySQL backup workflow completed for table: %s", table)
	return nil
}

// executeExternalMySQLDump executes mysqldump command with options
func (e *BackupExecutor) executeExternalMySQLDump(ctx context.Context, host, port, username, password, database, table, outputPath string, config ExecutorBackupConfig) error {
	args := []string{
		// Pin the connection charset so 4-byte characters (emoji, rare CJK)
		// survive the dump instead of being replaced with '?'. Relying on the
		// client default is fragile: it varies by client build and utf8mb3 is
		// deprecated in both MySQL 8.0 and MariaDB.
		"--default-character-set=utf8mb4",
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

// executeExternalMySQLCSV executes mysql command to export CSV format using Python csv module
// This method properly handles all special characters (quotes, newlines, tabs, commas, etc.)
// Works with remote MySQL servers without requiring FILE privilege or secure_file_priv configuration
func (e *BackupExecutor) executeExternalMySQLCSV(ctx context.Context, host, port, username, password, database, table, outputPath string, config ExecutorBackupConfig) error {
	// Build SELECT query
	selectQuery := e.buildMySQLSelectQuery(table, config)

	// Build mysql command arguments
	mysqlArgs := []string{
		// See executeExternalMySQLDump: pin the charset rather than inheriting
		// the client default, so 4-byte characters are not lost as '?'.
		"--default-character-set=utf8mb4",
		"-h", host,
		"-P", port,
		"-u", username,
	}

	// Add password if provided
	if password != "" {
		mysqlArgs = append(mysqlArgs, "-p"+password)
	}

	// Add database and query
	// Note: Do NOT use --raw flag as it disables escaping which causes issues with special characters
	// Without --raw, MySQL will properly escape tabs (\t) and newlines (\n) in field values
	mysqlArgs = append(mysqlArgs,
		database,
		"-e", selectQuery,
		"--batch", // Output in batch mode (TSV format)
	)

	// Python script for TSV to CSV conversion with MySQL escape sequence handling
	// MySQL --batch mode uses its own escape sequences: \n \t \\ \N (NULL)
	pythonScript := `
import csv, sys

def unescape_mysql(value):
    """Unescape MySQL batch mode escape sequences"""
    if value == '\\N':  # MySQL NULL representation
        return ''
    # Replace MySQL escape sequences
    value = value.replace('\\\\', '\x00')  # Temporarily replace \\ with placeholder
    value = value.replace('\\n', '\n')     # \n -> newline
    value = value.replace('\\t', '\t')     # \t -> tab
    value = value.replace('\\r', '\r')     # \r -> carriage return
    value = value.replace('\\0', '\0')     # \0 -> null byte
    value = value.replace('\x00', '\\')    # Restore backslash
    return value

try:
    writer = csv.writer(sys.stdout, quoting=csv.QUOTE_ALL, lineterminator='\n')
    for line in sys.stdin:
        line = line.rstrip('\n\r')  # Remove line ending
        fields = line.split('\t')   # Split by tab
        fields = [unescape_mysql(f) for f in fields]  # Unescape each field
        writer.writerow(fields)
except Exception as e:
    sys.stderr.write(f'Python CSV conversion error: {e}\n')
    sys.exit(1)
`

	// Create mysql command
	mysqlCmd := exec.CommandContext(ctx, "mysql", mysqlArgs...)

	// Create python command
	pythonCmd := exec.CommandContext(ctx, "python3", "-c", pythonScript)

	// Create output file
	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	// Setup pipeline: mysql | python3 > output.csv
	pythonCmd.Stdin, err = mysqlCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create pipe: %w", err)
	}

	pythonCmd.Stdout = outFile
	mysqlCmd.Stderr = os.Stderr
	pythonCmd.Stderr = os.Stderr

	// Display command line arguments with password masked
	logrus.Infof("[BackupExecutor] Executing: %s | python3 -c '<csv conversion>' > %s",
		e.maskMySQLPassword(append([]string{"mysql"}, mysqlArgs...)), outputPath)
	logrus.Infof("[BackupExecutor] Using Python csv module for proper CSV formatting with special character handling")

	// Start python command first
	if err := pythonCmd.Start(); err != nil {
		return fmt.Errorf("failed to start python command: %w", err)
	}

	// Start mysql command
	if err := mysqlCmd.Start(); err != nil {
		return fmt.Errorf("failed to start mysql command: %w", err)
	}

	// Wait for mysql command to finish
	if err := mysqlCmd.Wait(); err != nil {
		return fmt.Errorf("mysql command failed: %w", err)
	}

	// Wait for python command to finish
	if err := pythonCmd.Wait(); err != nil {
		return fmt.Errorf("python csv conversion failed: %w", err)
	}

	// Check output file and log size
	if stat, err := os.Stat(outputPath); err == nil {
		logrus.Infof("[BackupExecutor] ✅ MySQL CSV export completed: %.2f MB", float64(stat.Size())/1024/1024)
	} else {
		return fmt.Errorf("mysql CSV output file not created: %w", err)
	}

	return nil
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

	// Step 2: External zip command, unless compression is explicitly disabled
	uploadPath := zipPath
	uploadName := zipFileName
	skipCompression := isCompressionDisabled(config.CompressionType)

	if skipCompression {
		uploadPath = mergedFilePath
		uploadName = fileName
		logrus.Infof("[BackupExecutor] ⏭️  Step 2: Compression disabled, uploading %s as-is", uploadName)
	} else {
		logrus.Infof("[BackupExecutor] 🗜️ Step 2: External zip compression")
		if err := transfer.Zip(ctx, tempDir, mergedFilePath, zipPath); err != nil {
			return fmt.Errorf("external zip failed: %w", err)
		}
		e.logMemoryUsage("AFTER_ZIP")
	}

	// Step 3: External GCS upload
	gcsPath := fmt.Sprintf("%s/%s", config.Destination.GCSPath, uploadName)
	logrus.Infof("[BackupExecutor] ☁️ Step 3: External GCS upload")
	if err := transfer.UploadGCS(ctx, uploadPath, gcsPath); err != nil {
		return fmt.Errorf("external GCS upload failed: %w", err)
	}

	e.logMemoryUsage("MYSQL_MERGED_COMPLETE")

	// Clean up temporary files
	if err := os.Remove(mergedFilePath); err != nil {
		logrus.Warnf("[BackupExecutor] Failed to remove merged file %s: %v", mergedFilePath, err)
	} else {
		logrus.Debugf("[BackupExecutor] 🗑️  Cleaned up merged file: %s", mergedFilePath)
	}

	if !skipCompression {
		if err := os.Remove(zipPath); err != nil {
			logrus.Warnf("[BackupExecutor] Failed to remove ZIP file %s: %v", zipPath, err)
		} else {
			logrus.Debugf("[BackupExecutor] 🗑️  Cleaned up ZIP file: %s", zipPath)
		}
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

// isCompressionDisabled reports whether the backup output should be uploaded
// uncompressed. Anything other than an explicit "none" keeps the historical
// behaviour of zipping, so existing tasks are unaffected.
func isCompressionDisabled(compressionType string) bool {
	return strings.EqualFold(strings.TrimSpace(compressionType), "none")
}
