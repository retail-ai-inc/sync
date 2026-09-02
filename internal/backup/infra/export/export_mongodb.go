package export

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

func (e *BackupExecutor) logMemoryUsage(phase string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	logrus.Infof("[BackupExecutor] 📊 Go Memory [%s]: Alloc=%.2fMB, Sys=%.2fMB, NumGoroutines=%d",
		phase,
		float64(m.Alloc)/1024/1024,
		float64(m.Sys)/1024/1024,
		runtime.NumGoroutine())
}

// executeExternalMongoExportSimple complete external command backup: mongoexport -> zip -> GCS upload
func (e *BackupExecutor) executeExternalMongoExportSimple(ctx context.Context, connStr, database, collection, tempDir string, config ExecutorBackupConfig) error {
	logrus.Infof("[BackupExecutor] 🚀 Starting COMPLETE external command backup for collection: %s", collection)

	e.logMemoryUsage("EXTERNAL_FULL_START")

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

	// The MongoDB paths compress regardless of the job's compressionType, which
	// is what they have always done.
	if _, err := e.compressAndUpload(ctx, tempDir, outputPath, zipPath, true, config); err != nil {
		return err
	}

	e.logMemoryUsage("EXTERNAL_FULL_COMPLETE")

	removeTemp("JSON file", outputPath)
	removeTemp("ZIP file", zipPath)

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

	if queryConditions, exists := config.Query[collection]; exists && len(queryConditions) > 0 {
		cleanedQuery := cleanQueryStringValues(queryConditions)

		// Convert dynamic time queries to specific MongoDB queries. A condition
		// that cannot be rendered is fatal: dropping it exports the whole
		// collection, which looks like a successful backup of the wrong thing.
		finalQuery, err := e.convertTimeRangeQuery(cleanedQuery)
		if err != nil {
			return fmt.Errorf("build the filter for %s: %w", collection, err)
		}

		queryJSON, err := json.Marshal(finalQuery)
		if err != nil {
			return fmt.Errorf("render the filter for %s: %w", collection, err)
		}
		args = append(args, "--query", string(queryJSON))
		logrus.Infof("[BackupExecutor] Applied query for collection %s: %s", collection, string(queryJSON))
	} else {
		// If no query conditions, export all data
		logrus.Infof("[BackupExecutor] No query conditions found for collection %s, exporting all data", collection)
	}

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

	if _, err := os.Stat(outputPath); err != nil {
		return fmt.Errorf("mongoexport output file not created: %w", err)
	}

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

	// The name of the output file is derived from the first table, so an empty
	// group would take the whole process down with an index out of range rather
	// than fail one backup.
	if len(tables) == 0 {
		return fmt.Errorf("no tables to back up")
	}

	e.logMemoryUsage("MERGED_TABLES_START")

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

	mergedFile, err := os.Create(mergedJsonPath)
	if err != nil {
		return fmt.Errorf("failed to create merged file: %w", err)
	}
	defer mergedFile.Close()

	// No JSON array wrapper for JSONL format - each line is a separate JSON object

	for i, table := range tables {
		logrus.Infof("[BackupExecutor] 📄 Exporting table %d/%d: %s", i+1, len(tables), table)

		tempTablePath := fmt.Sprintf("%s/%s%s%s_temp.json", tempDir, table, JSONFilenameSeparator, dateStr) // _temp suffix for temporary files

		// Use mongoexport to export single table, apply query conditions and field selection
		if err := e.executeExternalMongoExportWithOptions(ctx, connStr, database, table, tempTablePath, config); err != nil {
			return fmt.Errorf("failed to export table %s: %w", table, err)
		}

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

		if recordCount, fileSize, countErr := e.countRecordsInFile(mergedJsonPath); countErr == nil {
			logrus.Infof("[BackupExecutor] 🔍 Merged file contains %d records, %.2f MB", recordCount, fileSize)
		} else {
			logrus.Warnf("[BackupExecutor] ⚠️  Failed to count records: %v", countErr)
		}
	} else {
		logrus.Errorf("[BackupExecutor] ❌ Failed to stat merged file: %v", err)
	}

	e.logMemoryUsage("AFTER_MERGE")

	if _, err := e.compressAndUpload(ctx, tempDir, mergedJsonPath, zipPath, true, config); err != nil {
		return err
	}

	e.logMemoryUsage("MERGED_TABLES_COMPLETE")

	removeTemp("merged file", mergedJsonPath)
	removeTemp("ZIP file", zipPath)

	logrus.Infof("[BackupExecutor] ✅ Multi-table merge backup completed successfully for %d tables", len(tables))
	return nil
}

// countRecordsInFile counts the number of records in JSONL file.
//
// The scanner's buffer used to be a megabyte while a MongoDB document may be
// sixteen, so one large document turned the whole file's record count into an
// error rather than a number — and the count is what the caller reports as the
// size of the backup.
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
	// A BSON document may be 16 MB, and mongoexport writes one per line. The
	// buffer allows for that plus the expansion from BSON to JSON.
	const maxCapacity = 64 * 1024 * 1024
	scanner.Buffer(make([]byte, 0, 1024*1024), maxCapacity)

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
