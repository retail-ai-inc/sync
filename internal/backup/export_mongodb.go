package backup

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
)

// JSONFilenameSeparator defines the separator used between collection name and date in JSON filenames
// Change this to customize JSON filename format (e.g., "-", "_", ".")
const JSONFilenameSeparator = "_"

// ZIPFilenameSeparator defines the separator used between collection name and date in ZIP filenames
// Change this to customize ZIP filename format (e.g., "-", "_", ".")
const ZIPFilenameSeparator = "-"

// UseExternalCommands checks whether to use external command mode
func (e *BackupExecutor) UseExternalCommands() bool {
	// Can be controlled through environment variables
	if os.Getenv("USE_EXTERNAL_BACKUP") == "true" {
		return true
	}

	// Can also check available memory, automatically switch to external command mode if memory is insufficient
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	currentMB := float64(m.Alloc) / 1024 / 1024

	if currentMB > 2000 { // If Go process is already using more than 2GB, switch to external mode
		logrus.Warnf("[BackupExecutor] High memory usage detected (%.2fMB), switching to external command mode", currentMB)
		return true
	}

	return false
}

// logMemoryUsage logs memory usage information
func (e *BackupExecutor) logMemoryUsage(phase string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	logrus.Infof("[BackupExecutor] 📊 Go Memory [%s]: Alloc=%.2fMB, Sys=%.2fMB, NumGoroutines=%d",
		phase,
		float64(m.Alloc)/1024/1024,
		float64(m.Sys)/1024/1024,
		runtime.NumGoroutine())
}

// ExecuteExternalMongoBackup executes MongoDB backup using external commands
// Avoids Go memory management issues by directly calling system commands
func (e *BackupExecutor) ExecuteExternalMongoBackup(ctx context.Context, config ExecutorBackupConfig, tempDir string, task BackupTask, collection string) error {
	logrus.Infof("[BackupExecutor] 🚀 Using EXTERNAL COMMAND mode for collection: %s", collection)

	// Log Go process memory (should remain stable)
	e.logMemoryUsage("EXTERNAL_MODE_START")

	// Build connection string
	connStr := buildMongoDBConnectionString(config.Database.URL, config.Database.Username, config.Database.Password)

	// Clean collection name and generate file paths
	baseCollectionName := e.extractTablePrefix(collection)
	logrus.Infof("[BackupExecutor] 🔍 Original collection name: %s, extracted base name: %s", collection, baseCollectionName)

	dateStr := time.Now().AddDate(0, 0, -1).Format("2006-01-02")
	outputPath := fmt.Sprintf("%s/%s%s%s.json", tempDir, baseCollectionName, JSONFilenameSeparator, dateStr)
	zipPath := fmt.Sprintf("%s/%s%s%s.zip", tempDir, baseCollectionName, ZIPFilenameSeparator, dateStr)

	// Step 1: External mongoexport command
	logrus.Infof("[BackupExecutor] 📤 Step 1: External mongoexport")
	if err := e.executeExternalMongoExport(ctx, connStr, config.Database.Database, collection, outputPath); err != nil {
		return fmt.Errorf("external mongoexport failed: %w", err)
	}

	e.logMemoryUsage("AFTER_MONGOEXPORT")

	// Step 2: External zip command
	logrus.Infof("[BackupExecutor] 🗜️ Step 2: External zip compression")
	if err := e.executeExternalZip(ctx, tempDir, outputPath, zipPath); err != nil {
		return fmt.Errorf("external zip failed: %w", err)
	}

	e.logMemoryUsage("AFTER_ZIP")

	// Step 3: External gsutil upload (if GCS is configured)
	if config.Destination.GCSPath != "" {
		logrus.Infof("[BackupExecutor] ☁️ Step 3: External GCS upload")
		gcsPath := fmt.Sprintf("%s/%s%s%s.zip", config.Destination.GCSPath, baseCollectionName, ZIPFilenameSeparator, dateStr)
		if err := e.executeExternalGCSUpload(ctx, zipPath, gcsPath); err != nil {
			return fmt.Errorf("external GCS upload failed: %w", err)
		}
	}

	e.logMemoryUsage("EXTERNAL_MODE_COMPLETE")

	// Clean up temporary files
	if err := os.Remove(outputPath); err != nil {
		logrus.Warnf("[BackupExecutor] Failed to remove JSON file %s: %v", outputPath, err)
	} else {
		logrus.Debugf("[BackupExecutor] 🗑️  Cleaned up JSON file: %s", outputPath)
	}

	if err := os.Remove(zipPath); err != nil {
		logrus.Warnf("[BackupExecutor] Failed to remove ZIP file %s: %v", zipPath, err)
	} else {
		logrus.Debugf("[BackupExecutor] 🗑️  Cleaned up ZIP file: %s", zipPath)
	}

	logrus.Infof("[BackupExecutor] ✅ External backup completed for collection: %s", collection)
	return nil
}

// executeExternalMongoExport executes external mongoexport command
func (e *BackupExecutor) executeExternalMongoExport(ctx context.Context, connStr, database, collection, outputPath string) error {
	cmd := exec.CommandContext(ctx, "mongoexport",
		"--uri", connStr,
		"--db", database,
		"--collection", collection,
		"--out", outputPath,
		"--quiet")

	logrus.Infof("[BackupExecutor] Executing: mongoexport --db %s --collection %s --out %s", database, collection, outputPath)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mongoexport failed: %w, output: %s", err, string(output))
	}

	// Check output file
	if _, err := os.Stat(outputPath); err != nil {
		return fmt.Errorf("mongoexport output file not created: %w", err)
	}

	// Log file size
	if stat, err := os.Stat(outputPath); err == nil {
		logrus.Infof("[BackupExecutor] ✅ Mongoexport completed: %.2f MB", float64(stat.Size())/1024/1024)
	}

	return nil
}

// executeExternalMongoExportSimple complete external command backup: mongoexport -> zip -> GCS upload
func (e *BackupExecutor) executeExternalMongoExportSimple(ctx context.Context, connStr, database, collection, tempDir string, config ExecutorBackupConfig) error {
	logrus.Infof("[BackupExecutor] 🚀 Starting COMPLETE external command backup for collection: %s", collection)

	// Log Go process memory (should remain stable)
	e.logMemoryUsage("EXTERNAL_FULL_START")

	// Clean collection name and generate file paths
	baseCollectionName := e.extractTablePrefix(collection)
	logrus.Infof("[BackupExecutor] 🔍 Original collection name: %s, extracted base name: %s", collection, baseCollectionName)

	dateStr := time.Now().AddDate(0, 0, -1).Format("2006-01-02")
	outputPath := fmt.Sprintf("%s/%s%s%s.json", tempDir, baseCollectionName, JSONFilenameSeparator, dateStr)
	zipPath := fmt.Sprintf("%s/%s%s%s.zip", tempDir, baseCollectionName, ZIPFilenameSeparator, dateStr)

	// Step 1: Use mongoexport to export data
	logrus.Infof("[BackupExecutor] 📤 Step 1: External mongoexport")
	if err := e.executeExternalMongoExportWithOptions(ctx, connStr, database, collection, outputPath, config); err != nil {
		return fmt.Errorf("external mongoexport failed: %w", err)
	}

	e.logMemoryUsage("AFTER_EXTERNAL_EXPORT")

	// Step 2: External zip command
	logrus.Infof("[BackupExecutor] 🗜️ Step 2: External zip compression")
	if err := e.executeExternalZip(ctx, tempDir, outputPath, zipPath); err != nil {
		return fmt.Errorf("external zip failed: %w", err)
	}

	e.logMemoryUsage("AFTER_EXTERNAL_ZIP")

	// Step 3: External GCS upload
	zipFileName := fmt.Sprintf("%s%s%s.zip", baseCollectionName, ZIPFilenameSeparator, dateStr)
	gcsPath := fmt.Sprintf("%s/%s", config.Destination.GCSPath, zipFileName)
	logrus.Infof("[BackupExecutor] ☁️ Step 3: External GCS upload")
	if err := e.executeExternalGCSUpload(ctx, zipPath, gcsPath); err != nil {
		return fmt.Errorf("external GCS upload failed: %w", err)
	}

	e.logMemoryUsage("EXTERNAL_FULL_COMPLETE")

	// Clean up temporary files
	if err := os.Remove(outputPath); err != nil {
		logrus.Warnf("[BackupExecutor] Failed to remove JSON file %s: %v", outputPath, err)
	} else {
		logrus.Debugf("[BackupExecutor] 🗑️  Cleaned up JSON file: %s", outputPath)
	}

	if err := os.Remove(zipPath); err != nil {
		logrus.Warnf("[BackupExecutor] Failed to remove ZIP file %s: %v", zipPath, err)
	} else {
		logrus.Debugf("[BackupExecutor] 🗑️  Cleaned up ZIP file: %s", zipPath)
	}

	logrus.Infof("[BackupExecutor] ✅ COMPLETE external backup workflow completed for collection: %s", collection)

	return nil
}

// executeExternalMongoExportWithOptions executes external mongoexport command with support for query conditions and field selection
func (e *BackupExecutor) executeExternalMongoExportWithOptions(ctx context.Context, connStr, database, collection, outputPath string, config ExecutorBackupConfig) error {
	args := []string{
		"--uri", connStr,
		"--db", database,
		"--collection", collection,
		"--out", outputPath,
		"--quiet",
	}

	// Add query conditions
	if queryConditions, exists := config.Query[collection]; exists && len(queryConditions) > 0 {
		// Clean extra quotes in query conditions
		cleanedQuery := cleanQueryStringValues(queryConditions)

		// Convert dynamic time queries to specific MongoDB queries
		finalQuery := e.convertTimeRangeQuery(cleanedQuery)

		queryJSON, err := json.Marshal(finalQuery)
		if err != nil {
			logrus.Warnf("[BackupExecutor] Failed to marshal query for collection %s: %v", collection, err)
		} else {
			args = append(args, "--query", string(queryJSON))
			logrus.Infof("[BackupExecutor] Applied query for collection %s: %s", collection, string(queryJSON))
		}
	} else {
		// If no query conditions, export all data
		logrus.Infof("[BackupExecutor] No query conditions found for collection %s, exporting all data", collection)
	}

	// Add field selection
	if fields, exists := config.Database.Fields[collection]; exists && len(fields) > 0 && fields[0] != "all" {
		fieldsStr := strings.Join(fields, ",")
		args = append(args, "--fields", fieldsStr)
		logrus.Infof("[BackupExecutor] Applied field selection for collection %s: %s", collection, fieldsStr)
	}

	cmd := exec.CommandContext(ctx, "mongoexport", args...)

	// Display command line arguments with URI credentials masked
	logrus.Infof("[BackupExecutor] Executing: %s", e.maskSensitiveArgs(append([]string{"mongoexport"}, args...)))

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mongoexport failed: %w, output: %s", err, string(output))
	}

	// Check output file
	if _, err := os.Stat(outputPath); err != nil {
		return fmt.Errorf("mongoexport output file not created: %w", err)
	}

	// Count exported records and file size
	recordCount, fileSize, err := e.countRecordsInFile(outputPath)
	if err != nil {
		logrus.Warnf("[BackupExecutor] Failed to count records in %s: %v", outputPath, err)
		// Fall back to only showing file size
		if stat, err := os.Stat(outputPath); err == nil {
			logrus.Infof("[BackupExecutor] ✅ Mongoexport completed: %.2f MB", float64(stat.Size())/1024/1024)
		}
	} else {
		logrus.Infof("[BackupExecutor] ✅ Mongoexport completed: %d records, %.2f MB", recordCount, fileSize)
	}

	return nil
}

// exportMongoDBMergedTables performs multi-table merged backup using external commands
// Handles cross-month data export scenarios, supports merging multiple collections, following the implementation pattern of executeExternalMongoExportSimple
func (e *BackupExecutor) exportMongoDBMergedTables(ctx context.Context, connStr, database string, tables []string, tempDir string, config ExecutorBackupConfig) error {
	logrus.Infof("[BackupExecutor] 🚀 Starting multi-table merge backup for %d tables: %v", len(tables), tables)

	// Log Go process memory (should remain stable)
	e.logMemoryUsage("MERGED_TABLES_START")

	// Extract base name (remove date suffix)
	baseCollectionName := e.extractTablePrefix(tables[0])
	logrus.Infof("[BackupExecutor] 🔍 Original table name: %s, extracted base name: %s", tables[0], baseCollectionName)

	// Generate file names: use cleaned base collection name + date
	dateStr := time.Now().AddDate(0, 0, -1).Format("2006-01-02")
	jsonFileName := fmt.Sprintf("%s%s%s.json", baseCollectionName, JSONFilenameSeparator, dateStr)
	zipFileName := fmt.Sprintf("%s%s%s.zip", baseCollectionName, ZIPFilenameSeparator, dateStr)
	logrus.Infof("[BackupExecutor] 🔍 Generated file names - JSON: %s, ZIP: %s", jsonFileName, zipFileName)

	mergedJsonPath := filepath.Join(tempDir, jsonFileName)
	zipPath := filepath.Join(tempDir, zipFileName)

	// Step 1: Export each table separately and merge
	logrus.Infof("[BackupExecutor] 📤 Step 1: Exporting and merging %d tables", len(tables))

	// Create merged file
	mergedFile, err := os.Create(mergedJsonPath)
	if err != nil {
		return fmt.Errorf("failed to create merged file: %w", err)
	}
	defer mergedFile.Close()

	// No JSON array wrapper for JSONL format - each line is a separate JSON object

	for i, table := range tables {
		logrus.Infof("[BackupExecutor] 📄 Exporting table %d/%d: %s", i+1, len(tables), table)

		// Create temporary file for each table
		tempTablePath := fmt.Sprintf("%s/%s%s%s_temp.json", tempDir, table, JSONFilenameSeparator, dateStr) // _temp suffix for temporary files

		// Use mongoexport to export single table, apply query conditions and field selection
		if err := e.executeExternalMongoExportWithOptions(ctx, connStr, database, table, tempTablePath, config); err != nil {
			return fmt.Errorf("failed to export table %s: %w", table, err)
		}

		// Read temporary file and merge to main file
		tempFile, err := os.Open(tempTablePath)
		if err != nil {
			return fmt.Errorf("failed to open temp file for table %s: %w", table, err)
		}

		// Read JSONL format file content (mongoexport default output format)
		content, err := os.ReadFile(tempTablePath)
		if err != nil {
			tempFile.Close()
			return fmt.Errorf("failed to read temp file for table %s: %w", table, err)
		}

		// mongoexport outputs JSONL format (one JSON object per line)
		// Write directly in JSONL format, maintaining original format
		contentStr := strings.TrimSpace(string(content))

		if len(contentStr) > 0 {
			// Directly append JSONL content to merged file
			lines := strings.Split(contentStr, "\n")

			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line != "" && strings.HasPrefix(line, "{") {
					// Write each JSON object line directly, separated by newlines (JSONL format)
					if _, err := mergedFile.WriteString(line + "\n"); err != nil {
						tempFile.Close()
						return fmt.Errorf("failed to write line data for %s: %w", table, err)
					}
				}
			}
		}

		tempFile.Close()
		// Clean up temporary files
		if err := os.Remove(tempTablePath); err != nil {
			logrus.Warnf("[BackupExecutor] Failed to remove temp file %s: %v", tempTablePath, err)
		} else {
			logrus.Debugf("[BackupExecutor] 🗑️  Cleaned up temp file: %s", tempTablePath)
		}

		logrus.Infof("[BackupExecutor] ✅ Table %s merged successfully", table)
	}

	// No JSON array end needed for JSONL format
	mergedFile.Close()

	if stat, err := os.Stat(mergedJsonPath); err == nil {
		logrus.Infof("[BackupExecutor] ✅ Merge completed: %.2f MB", float64(stat.Size())/1024/1024)

		// Count records but don't output specific content
		if recordCount, fileSize, countErr := e.countRecordsInFile(mergedJsonPath); countErr == nil {
			logrus.Infof("[BackupExecutor] 🔍 Merged file contains %d records, %.2f MB", recordCount, fileSize)
		} else {
			logrus.Warnf("[BackupExecutor] ⚠️  Failed to count records: %v", countErr)
		}
	} else {
		logrus.Errorf("[BackupExecutor] ❌ Failed to stat merged file: %v", err)
	}

	e.logMemoryUsage("AFTER_MERGE")

	// Step 2: External zip command
	logrus.Infof("[BackupExecutor] 🗜️ Step 2: External zip compression")
	if err := e.executeExternalZip(ctx, tempDir, mergedJsonPath, zipPath); err != nil {
		return fmt.Errorf("external zip failed: %w", err)
	}

	e.logMemoryUsage("AFTER_EXTERNAL_ZIP")

	// Step 3: External GCS upload
	gcsPath := fmt.Sprintf("%s/%s", config.Destination.GCSPath, zipFileName)
	logrus.Infof("[BackupExecutor] ☁️ Step 3: External GCS upload")
	if err := e.executeExternalGCSUpload(ctx, zipPath, gcsPath); err != nil {
		return fmt.Errorf("external GCS upload failed: %w", err)
	}

	e.logMemoryUsage("MERGED_TABLES_COMPLETE")

	// Clean up temporary files
	if err := os.Remove(mergedJsonPath); err != nil {
		logrus.Warnf("[BackupExecutor] Failed to remove merged file %s: %v", mergedJsonPath, err)
	} else {
		logrus.Debugf("[BackupExecutor] 🗑️  Cleaned up merged file: %s", mergedJsonPath)
	}

	if err := os.Remove(zipPath); err != nil {
		logrus.Warnf("[BackupExecutor] Failed to remove ZIP file %s: %v", zipPath, err)
	} else {
		logrus.Debugf("[BackupExecutor] 🗑️  Cleaned up ZIP file: %s", zipPath)
	}

	logrus.Infof("[BackupExecutor] ✅ Multi-table merge backup completed successfully for %d tables", len(tables))
	return nil
}

// countRecordsInFile counts the number of records in JSONL file
func (e *BackupExecutor) countRecordsInFile(filePath string) (int, float64, error) {
	stat, err := os.Stat(filePath)
	if err != nil {
		return 0, 0, err
	}

	fileSize := float64(stat.Size()) / 1024 / 1024 // MB

	file, err := os.Open(filePath)
	if err != nil {
		return 0, fileSize, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// Increase buffer size to handle large JSON lines (default is 64KB, set to 1MB)
	const maxCapacity = 1024 * 1024 // 1MB
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	count := 0

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// For JSONL format, each non-empty line that starts with '{' is a record
		if line != "" && strings.HasPrefix(line, "{") {
			count++
		}
	}

	if err := scanner.Err(); err != nil {
		return 0, fileSize, err
	}

	return count, fileSize, nil
}
