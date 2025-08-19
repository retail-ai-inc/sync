package backup

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/retail-ai-inc/sync/pkg/utils"
	"github.com/sirupsen/logrus"
)

// BackupExecutor Backup executor
type BackupExecutor struct {
	db *sql.DB
}

// NewBackupExecutor Create a new backup executor
func NewBackupExecutor(db *sql.DB) *BackupExecutor {
	return &BackupExecutor{db: db}
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
			logrus.Debugf("[BackupExecutor] Cleaned string value for key %s: '%s' -> '%s'", key, v, cleanValue)
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

	logrus.Infof("[BackupExecutor] Processed file name pattern '%s' for table '%s' -> '%s'", pattern, tableName, result)
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
	var config struct {
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
		Format          string                            `json:"format"`
		BackupType      string                            `json:"backupType"`
		Query           map[string]map[string]interface{} `json:"query"`
		CompressionType string                            `json:"compressionType"`
	}

	if err := json.Unmarshal([]byte(task.ConfigJSON), &config); err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	logrus.Infof("[BackupExecutor] Starting backup for task %d (%s, type: %s)",
		taskID, config.Name, config.SourceType)

	// Process each table separately
	for _, table := range config.Database.Tables {
		logrus.Infof("[BackupExecutor] Processing table: %s", table)

		// Create temporary directory for this table
		tempDir, err := os.MkdirTemp("", fmt.Sprintf("backup_%d_%s_", taskID, table))
		if err != nil {
			logrus.Errorf("[BackupExecutor] Failed to create temp directory for table %s: %v", table, err)
			continue
		}

		logrus.Infof("[BackupExecutor] Temporary directory created for table %s: %s", table, tempDir)

		// Export single table
		var exportErr error
		switch config.SourceType {
		case "mongodb":
			exportErr = e.exportMongoDBSingleTable(ctx, config, tempDir, task, table)
		default:
			exportErr = fmt.Errorf("unsupported database type: %s", config.SourceType)
		}

		if exportErr != nil {
			logrus.Errorf("[BackupExecutor] Export failed for table %s: %v", table, exportErr)
			os.RemoveAll(tempDir) // Clean up failed export
			continue
		}

		// Create compressed file for this table using pattern
		tempParentDir := filepath.Dir(tempDir)
		archiveBaseName := processFileNamePattern(config.Destination.FileNamePattern, table)

		// Determine compression type and file extension
		compressionType := config.CompressionType
		if compressionType == "" {
			compressionType = "zip" // Default to zip
		}

		var fileExtension string
		switch compressionType {
		case "zip":
			fileExtension = ".zip"
		case "gzip":
			fileExtension = ".tar.gz"
		default:
			logrus.Warnf("[BackupExecutor] Unknown compression type '%s', defaulting to zip", compressionType)
			fileExtension = ".zip"
			compressionType = "zip"
		}

		// Ensure correct extension for archive based on compression type
		if !strings.HasSuffix(archiveBaseName, fileExtension) {
			// Remove existing extension if any and add correct extension
			ext := filepath.Ext(archiveBaseName)
			if ext != "" {
				archiveBaseName = strings.TrimSuffix(archiveBaseName, ext)
			}
			archiveBaseName += fileExtension
		}

		archivePath := filepath.Join(tempParentDir, archiveBaseName)

		logrus.Infof("[BackupExecutor] Starting compression for table %s: %s -> %s using %s", table, tempDir, archivePath, compressionType)
		if err := e.CompressDirectory(tempDir, archivePath, compressionType); err != nil {
			logrus.Errorf("[BackupExecutor] Compression failed for table %s: %v", table, err)
			os.RemoveAll(tempDir) // Clean up temp directory only
			continue
		}
		logrus.Infof("[BackupExecutor] Archive file created for table %s: %s", table, archivePath)

		// Upload to GCS
		logrus.Infof("[BackupExecutor] Starting upload to GCS for table %s", table)
		if err := utils.UploadToGCS(ctx, archivePath, config.Destination.GCSPath); err != nil {
			logrus.Errorf("[BackupExecutor] Upload to GCS failed for table %s: %v", table, err)
			logrus.Warnf("[BackupExecutor] Archive file preserved for debugging: %s", archivePath)
			// Clean up temp directory only, keep archive file for debugging
			os.RemoveAll(tempDir)
			continue
		}

		// Clean up temporary files after successful upload
		os.RemoveAll(tempDir)
		os.Remove(archivePath)
		logrus.Infof("[BackupExecutor] Backup completed successfully for table %s", table)
	}

	logrus.Infof("[BackupExecutor] All table backups completed for task %d", taskID)
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

// exportMongoDB Export MongoDB data
func (e *BackupExecutor) exportMongoDB(ctx context.Context, config struct {
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
	Format          string                            `json:"format"`
	BackupType      string                            `json:"backupType"`
	Query           map[string]map[string]interface{} `json:"query"`
	CompressionType string                            `json:"compressionType"`
}, tempDir string, task BackupTask) error {
	dateStr := utils.GetTodayDateString()

	// Build connection string
	connStr := config.Database.URL
	if config.Database.Username != "" && config.Database.Password != "" {
		connStr = fmt.Sprintf("mongodb://%s:%s@%s/?authSource=admin",
			config.Database.Username, config.Database.Password, config.Database.URL)
	} else {
		connStr = fmt.Sprintf("mongodb://%s", config.Database.URL)
	}

	// Choose mongodump or mongoexport based on format
	if config.Format == "bson" {
		// Use mongodump for BSON format
		return e.exportMongoDBWithDump(ctx, config, tempDir, task, connStr, dateStr)
	} else {
		// Use mongoexport for JSON or CSV format
		return e.exportMongoDBWithExport(ctx, config, tempDir, task, connStr, dateStr)
	}
}

// exportMongoDBWithDump Export BSON format using mongodump
func (e *BackupExecutor) exportMongoDBWithDump(ctx context.Context, config struct {
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
	Format          string                            `json:"format"`
	BackupType      string                            `json:"backupType"`
	Query           map[string]map[string]interface{} `json:"query"`
	CompressionType string                            `json:"compressionType"`
}, tempDir string, task BackupTask, connStr string, dateStr string) error {
	// Use full path to avoid command parsing issues
	mongodumpPath, err := exec.LookPath("mongodump")
	if err != nil {
		mongodumpPath = "mongodump" // If not found, use default command name
		logrus.Warnf("[BackupExecutor] mongodump command not found in PATH, using default name")
	}

	for _, collection := range config.Database.Tables {
		// Create separate output directory for each collection, directly under temp directory
		outputDir := filepath.Join(tempDir, fmt.Sprintf("%s-%s-dump", collection, dateStr))
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			return fmt.Errorf("failed to create output directory for collection %s: %w", collection, err)
		}

		// Basic command arguments
		cmd := exec.CommandContext(ctx, mongodumpPath,
			"--uri", connStr,
			"--db", config.Database.Database,
			"--collection", collection,
			"--out", outputDir)

		// Explicitly log output directory structure
		logrus.Infof("[BackupExecutor] MongoDB dump output directory for collection %s: %s",
			collection, outputDir)
		logrus.Infof("[BackupExecutor] Expected BSON file path: %s/%s/%s.bson",
			outputDir, config.Database.Database, collection)

		// Build query condition
		if queryObj, ok := config.Query[collection]; ok && len(queryObj) > 0 {
			// Clean query string values to remove extra escaping
			cleanedQueryObj := cleanQueryStringValues(queryObj)
			logrus.Infof("[BackupExecutor] Original query for collection %s: %+v", collection, queryObj)
			logrus.Infof("[BackupExecutor] Cleaned query for collection %s: %+v", collection, cleanedQueryObj)

			// Process time range query if needed
			processedQueryObj, err := utils.ProcessTimeRangeQuery(cleanedQueryObj)
			if err != nil {
				logrus.Warnf("[BackupExecutor] Failed to process time range query for collection %s: %v", collection, err)
				continue
			}

			// Ensure standard JSON format query
			queryBytes, err := json.Marshal(processedQueryObj)
			if err != nil {
				logrus.Warnf("[BackupExecutor] Failed to marshal query for collection %s: %v", collection, err)
				continue
			}

			// Add query condition
			jsonStr := string(queryBytes)
			cmd.Args = append(cmd.Args, "--query", jsonStr)
			logrus.Infof("[BackupExecutor] MongoDB query for collection %s: %s", collection, jsonStr)
		}

		// Handle incremental backup
		if config.BackupType == "incremental" && task.LastBackupTime.After(time.Time{}) {
			// If no explicit query conditions and there's a last backup time, add time-based query
			timeQuery := fmt.Sprintf(`{"createdAt":{"$gt":{"$date":"%s"}}}`,
				task.LastBackupTime.Format(time.RFC3339))
			cmd.Args = append(cmd.Args, "--query", timeQuery)
			logrus.Infof("[BackupExecutor] Adding incremental time filter: %s", timeQuery)
		}

		// Execute command
		if err := executeCommand(cmd, "mongodump"); err != nil {
			return err
		}

		// Verify file exists after command execution
		expectedBsonPath := filepath.Join(outputDir, config.Database.Database, collection+".bson")
		if _, err := os.Stat(expectedBsonPath); os.IsNotExist(err) {
			logrus.Warnf("[BackupExecutor] Expected BSON file not found at %s", expectedBsonPath)
			// List directory contents to help debugging
			files, _ := filepath.Glob(filepath.Join(outputDir, "*"))
			logrus.Infof("[BackupExecutor] Directory contents: %v", files)
		} else {
			logrus.Infof("[BackupExecutor] Successfully verified BSON file: %s", expectedBsonPath)
		}

		logrus.Infof("[BackupExecutor] Successfully dumped collection %s to %s", collection, outputDir)
	}

	return nil
}

// exportMongoDBWithExport Export JSON or CSV format using mongoexport
func (e *BackupExecutor) exportMongoDBWithExport(ctx context.Context, config struct {
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
	Format          string                            `json:"format"`
	BackupType      string                            `json:"backupType"`
	Query           map[string]map[string]interface{} `json:"query"`
	CompressionType string                            `json:"compressionType"`
}, tempDir string, task BackupTask, connStr string, dateStr string) error {
	// Use full path to avoid command parsing issues
	mongoexportPath, err := exec.LookPath("mongoexport")
	if err != nil {
		mongoexportPath = "mongoexport" // If not found, use default command name
		logrus.Warnf("[BackupExecutor] mongoexport command not found in PATH, using default name")
	}

	for _, collection := range config.Database.Tables {
		// Determine file extension
		fileExt := "json"
		if config.Format == "csv" {
			fileExt = "csv"
		}

		outputPath := filepath.Join(tempDir, fmt.Sprintf("%s_%s.%s",
			collection, dateStr, fileExt))

		// Build field projection
		var fields string
		if fieldsArr, ok := config.Database.Fields[collection]; ok && len(fieldsArr) > 0 {
			// Check if fields is set to "all" - if so, don't add fields parameter to export all fields
			if len(fieldsArr) == 1 && fieldsArr[0] == "all" {
				logrus.Infof("[BackupExecutor] Fields set to 'all' for collection %s, exporting all fields", collection)
				fields = ""
			} else {
				fields = "--fields=" + strings.Join(fieldsArr, ",")
			}
		}

		// Build query condition
		var query string
		if queryObj, ok := config.Query[collection]; ok && len(queryObj) > 0 {
			// Clean query string values to remove extra escaping
			cleanedQueryObj := cleanQueryStringValues(queryObj)
			logrus.Infof("[BackupExecutor] Original query for collection %s: %+v", collection, queryObj)
			logrus.Infof("[BackupExecutor] Cleaned query for collection %s: %+v", collection, cleanedQueryObj)

			// Process time range query if needed
			processedQueryObj, err := utils.ProcessTimeRangeQuery(cleanedQueryObj)
			if err != nil {
				logrus.Warnf("[BackupExecutor] Failed to process time range query for collection %s: %v", collection, err)
				continue
			}

			// Ensure standard JSON format query
			queryBytes, err := json.Marshal(processedQueryObj)
			if err != nil {
				logrus.Warnf("[BackupExecutor] Failed to marshal query for collection %s: %v", collection, err)
				continue
			}

			// Use JSON directly without any quotes
			jsonStr := string(queryBytes)
			query = fmt.Sprintf("--query=%s", jsonStr)
			logrus.Infof("[BackupExecutor] MongoDB query for collection %s: %s", collection, jsonStr)
		}

		// Build command
		cmd := exec.CommandContext(ctx, mongoexportPath,
			"--uri", connStr,
			"--db", config.Database.Database,
			"--collection", collection,
			"--out", outputPath)

		// Handle format parameters
		if config.Format == "csv" {
			cmd.Args = append(cmd.Args, "--type=csv")
			// CSV format requires fields parameter
			if fields == "" {
				// Check if this is due to "all" fields setting
				if fieldsArr, ok := config.Database.Fields[collection]; ok && len(fieldsArr) == 1 && fieldsArr[0] == "all" {
					logrus.Warnf("[BackupExecutor] CSV export for collection %s with 'all' fields is not supported - CSV format requires explicit field specification", collection)
				} else {
					logrus.Warnf("[BackupExecutor] CSV export for collection %s requires fields specification", collection)
				}
				continue
			}
		} else {
			// Use JSON format by default
			cmd.Args = append(cmd.Args, "--type=json")
		}

		// Add optional parameters
		if fields != "" {
			cmd.Args = append(cmd.Args, fields)
		}
		if query != "" {
			cmd.Args = append(cmd.Args, query)
		}

		// Handle backup type parameters (incremental/full)
		if config.BackupType == "incremental" {
			// MongoDB incremental backup is typically implemented via query conditions
			// Here we can add query conditions based on last backup time
			if query == "" && task.LastBackupTime.After(time.Time{}) {
				// If no explicit query conditions and there's a last backup time, add time-based query
				timeQuery := fmt.Sprintf(`{"createdAt":{"$gt":{"$date":"%s"}}}`,
					task.LastBackupTime.Format(time.RFC3339))
				cmd.Args = append(cmd.Args, fmt.Sprintf("--query=%s", timeQuery))
				logrus.Infof("[BackupExecutor] Adding incremental time filter: %s", timeQuery)
			}
		}

		// Execute command
		if err := executeCommand(cmd, "mongoexport"); err != nil {
			return err
		}
	}

	return nil
}

// CompressDirectory Compress directory with specified compression type
func (e *BackupExecutor) CompressDirectory(sourceDir, destFile, compressionType string) error {
	logrus.Infof("[BackupExecutor] Starting compression of directory: %s", sourceDir)

	// Set default compression type to zip if empty
	if compressionType == "" {
		compressionType = "zip"
	}

	logrus.Infof("[BackupExecutor] Using compression type: %s", compressionType)

	// Check if compression tool is available
	if err := e.checkCompressionToolAvailable(compressionType); err != nil {
		logrus.Warnf("[BackupExecutor] Preferred compression tool unavailable: %v, fallback to gzip", err)
		compressionType = "gzip"
	}

	// Recursively find all needed files
	var allFiles []string

	// Define find function
	findFiles := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			logrus.Warnf("[BackupExecutor] Error walking path %s: %v", path, err)
			return err
		}

		// Skip directories, only add files
		if !info.IsDir() {
			ext := filepath.Ext(path)
			// Support JSON, SQL and BSON files
			if ext == ".json" || ext == ".sql" || ext == ".bson" {
				allFiles = append(allFiles, path)
				logrus.Infof("[BackupExecutor] Found file to compress: %s (size: %.2f MB)",
					path, float64(info.Size())/1024/1024)
			} else {
				logrus.Debugf("[BackupExecutor] Skipping file with unsupported extension: %s", path)
			}
		}
		return nil
	}

	// Recursively traverse directory
	logrus.Infof("[BackupExecutor] Scanning directory for compressible files: %s", sourceDir)
	if err := filepath.Walk(sourceDir, findFiles); err != nil {
		logrus.Errorf("[BackupExecutor] Failed to walk directory %s: %v", sourceDir, err)
		return fmt.Errorf("failed to walk directory: %w", err)
	}

	if len(allFiles) == 0 {
		// Detailed logging to help debugging
		files, _ := filepath.Glob(filepath.Join(sourceDir, "*"))
		logrus.Errorf("[BackupExecutor] No compressible files found in %s. Directory contents: %v", sourceDir, files)

		// List all files recursively for debugging
		logrus.Infof("[BackupExecutor] Listing all files in directory for debugging:")
		filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() {
				logrus.Infof("[BackupExecutor] Found file: %s (ext: %s, size: %d)",
					path, filepath.Ext(path), info.Size())
			}
			return nil
		})

		return fmt.Errorf("no files to compress (checked for .json, .sql, .bson)")
	}

	// Calculate total size
	totalSize := int64(0)
	for _, file := range allFiles {
		if info, err := os.Stat(file); err == nil {
			totalSize += info.Size()
		}
	}

	logrus.Infof("[BackupExecutor] Compressing %d files (total size: %.2f MB) using %s compression",
		len(allFiles), float64(totalSize)/1024/1024, compressionType)

	// Execute compression based on type
	switch compressionType {
	case "zip":
		return e.compressWithZip(sourceDir, destFile, allFiles)
	case "gzip":
		return e.compressWithGzip(sourceDir, destFile, allFiles)
	default:
		return fmt.Errorf("unsupported compression type: %s", compressionType)
	}
}

// executeCommand General function to execute commands
func executeCommand(cmd *exec.Cmd, commandName string) error {
	// Print full command
	logrus.Infof("[BackupExecutor] Executing command: %s %s", filepath.Base(cmd.Path), strings.Join(cmd.Args[1:], " "))
	output, err := cmd.CombinedOutput()

	// Log full output to view export line count
	logrus.Infof("[BackupExecutor] Command output: %s", string(output))

	if err != nil {
		return fmt.Errorf("%s command failed: %w, output: %s",
			commandName, err, string(output))
	}

	// Parse and log export line count
	if strings.Contains(string(output), "exported") {
		lines := strings.Split(string(output), "\n")
		for _, line := range lines {
			if strings.Contains(line, "exported") {
				logrus.Infof("[BackupExecutor] %s", line)
				break
			}
		}
	}

	return nil
}

// checkCompressionToolAvailable Check if compression tool is available
func (e *BackupExecutor) checkCompressionToolAvailable(compressionType string) error {
	switch compressionType {
	case "zip":
		if _, err := exec.LookPath("zip"); err != nil {
			return fmt.Errorf("zip command not found: %w", err)
		}
	case "gzip":
		if _, err := exec.LookPath("tar"); err != nil {
			return fmt.Errorf("tar command not found: %w", err)
		}
	default:
		return fmt.Errorf("unsupported compression type: %s", compressionType)
	}
	return nil
}

// compressWithZip Compress files using zip
func (e *BackupExecutor) compressWithZip(sourceDir, destFile string, allFiles []string) error {
	// Use full path to avoid command parsing issues
	zipPath, err := exec.LookPath("zip")
	if err != nil {
		zipPath = "zip" // If not found, use default command name
		logrus.Warnf("[BackupExecutor] zip command not found in PATH, using default name")
	}

	// Make all file paths relative to source directory
	var relativeFiles []string
	for _, file := range allFiles {
		relFile, err := filepath.Rel(sourceDir, file)
		if err != nil {
			logrus.Warnf("[BackupExecutor] Failed to get relative path for %s: %v", file, err)
			continue
		}
		relativeFiles = append(relativeFiles, relFile)
	}

	// Build zip command: zip -r destFile files...
	args := []string{"-r", destFile}
	args = append(args, relativeFiles...)
	cmd := exec.Command(zipPath, args...)
	cmd.Dir = sourceDir // Set working directory to source directory

	logrus.Infof("[BackupExecutor] Executing zip command in directory: %s", sourceDir)

	// Execute command
	if err := executeCommand(cmd, "zip"); err != nil {
		logrus.Errorf("[BackupExecutor] ZIP compression failed: %v", err)
		return err
	}

	// Check if compressed file was created and get its size
	if info, err := os.Stat(destFile); err == nil {
		logrus.Infof("[BackupExecutor] ZIP compression completed successfully. Archive size: %.2f MB",
			float64(info.Size())/1024/1024)
	} else {
		logrus.Warnf("[BackupExecutor] Could not stat compressed file %s: %v", destFile, err)
	}

	return nil
}

// compressWithGzip Compress files using gzip (tar.gz)
func (e *BackupExecutor) compressWithGzip(sourceDir, destFile string, allFiles []string) error {
	// Use full path to avoid command parsing issues
	tarPath, err := exec.LookPath("tar")
	if err != nil {
		tarPath = "tar" // If not found, use default command name
		logrus.Warnf("[BackupExecutor] tar command not found in PATH, using default name")
	}

	// Make all file paths relative to source directory
	var relativeFiles []string
	for _, file := range allFiles {
		relFile, err := filepath.Rel(sourceDir, file)
		if err != nil {
			logrus.Warnf("[BackupExecutor] Failed to get relative path for %s: %v", file, err)
			continue
		}
		relativeFiles = append(relativeFiles, relFile)
	}

	// Build tar command: tar -czf destFile -C sourceDir files...
	args := []string{"-czf", destFile, "-C", sourceDir}
	args = append(args, relativeFiles...)
	cmd := exec.Command(tarPath, args...)

	logrus.Infof("[BackupExecutor] Executing tar command")

	// Execute command
	if err := executeCommand(cmd, "tar"); err != nil {
		logrus.Errorf("[BackupExecutor] GZIP compression failed: %v", err)
		return err
	}

	// Check if compressed file was created and get its size
	if info, err := os.Stat(destFile); err == nil {
		logrus.Infof("[BackupExecutor] GZIP compression completed successfully. Archive size: %.2f MB",
			float64(info.Size())/1024/1024)
	} else {
		logrus.Warnf("[BackupExecutor] Could not stat compressed file %s: %v", destFile, err)
	}

	return nil
}

// exportMongoDBSingleTable Export single MongoDB collection
func (e *BackupExecutor) exportMongoDBSingleTable(ctx context.Context, config struct {
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
	Format          string                            `json:"format"`
	BackupType      string                            `json:"backupType"`
	Query           map[string]map[string]interface{} `json:"query"`
	CompressionType string                            `json:"compressionType"`
}, tempDir string, task BackupTask, tableName string) error {
	// Use yesterday's date since we're typically backing up yesterday's data
	dateStr := time.Now().AddDate(0, 0, -1).Format("2006-01-02")

	// Build connection string
	connStr := config.Database.URL
	if config.Database.Username != "" && config.Database.Password != "" {
		connStr = fmt.Sprintf("mongodb://%s:%s@%s/?authSource=admin",
			config.Database.Username, config.Database.Password, config.Database.URL)
	} else {
		connStr = fmt.Sprintf("mongodb://%s", config.Database.URL)
	}

	// Choose mongodump or mongoexport based on format
	if config.Format == "bson" {
		// Use mongodump for BSON format
		return e.exportMongoDBSingleTableWithDump(ctx, config, tempDir, task, connStr, dateStr, tableName)
	} else {
		// Use mongoexport for JSON or CSV format
		return e.exportMongoDBSingleTableWithExport(ctx, config, tempDir, task, connStr, dateStr, tableName)
	}
}

// exportMongoDBSingleTableWithDump Export single table BSON format using mongodump
func (e *BackupExecutor) exportMongoDBSingleTableWithDump(ctx context.Context, config struct {
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
	Format          string                            `json:"format"`
	BackupType      string                            `json:"backupType"`
	Query           map[string]map[string]interface{} `json:"query"`
	CompressionType string                            `json:"compressionType"`
}, tempDir string, task BackupTask, connStr string, dateStr string, collection string) error {
	// Use full path to avoid command parsing issues
	mongodumpPath, err := exec.LookPath("mongodump")
	if err != nil {
		mongodumpPath = "mongodump" // If not found, use default command name
		logrus.Warnf("[BackupExecutor] mongodump command not found in PATH, using default name")
	}

	// Create separate output directory for the collection, directly under temp directory
	outputDir := filepath.Join(tempDir, fmt.Sprintf("%s-%s-dump", collection, dateStr))
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory for collection %s: %w", collection, err)
	}

	// Basic command arguments
	cmd := exec.CommandContext(ctx, mongodumpPath,
		"--uri", connStr,
		"--db", config.Database.Database,
		"--collection", collection,
		"--out", outputDir)

	// Explicitly log output directory structure
	logrus.Infof("[BackupExecutor] MongoDB dump output directory for collection %s: %s",
		collection, outputDir)
	logrus.Infof("[BackupExecutor] Expected BSON file path: %s/%s/%s.bson",
		outputDir, config.Database.Database, collection)

	// Build query condition
	if queryObj, ok := config.Query[collection]; ok && len(queryObj) > 0 {
		// Clean query string values to remove extra escaping
		cleanedQueryObj := cleanQueryStringValues(queryObj)
		logrus.Infof("[BackupExecutor] Original query for collection %s: %+v", collection, queryObj)
		logrus.Infof("[BackupExecutor] Cleaned query for collection %s: %+v", collection, cleanedQueryObj)

		// Process time range query if needed
		processedQueryObj, err := utils.ProcessTimeRangeQuery(cleanedQueryObj)
		if err != nil {
			logrus.Warnf("[BackupExecutor] Failed to process time range query for collection %s: %v", collection, err)
			return fmt.Errorf("failed to process time range query: %w", err)
		}

		// Ensure standard JSON format query
		queryBytes, err := json.Marshal(processedQueryObj)
		if err != nil {
			logrus.Warnf("[BackupExecutor] Failed to marshal query for collection %s: %v", collection, err)
			return fmt.Errorf("failed to marshal query: %w", err)
		}

		// Add query condition
		jsonStr := string(queryBytes)
		cmd.Args = append(cmd.Args, "--query", jsonStr)
		logrus.Infof("[BackupExecutor] MongoDB query for collection %s: %s", collection, jsonStr)
	}

	// Handle incremental backup
	if config.BackupType == "incremental" && task.LastBackupTime.After(time.Time{}) {
		// If no explicit query conditions and there's a last backup time, add time-based query
		timeQuery := fmt.Sprintf(`{"createdAt":{"$gt":{"$date":"%s"}}}`,
			task.LastBackupTime.Format(time.RFC3339))
		cmd.Args = append(cmd.Args, "--query", timeQuery)
		logrus.Infof("[BackupExecutor] Adding incremental time filter: %s", timeQuery)
	}

	// Execute command
	if err := executeCommand(cmd, "mongodump"); err != nil {
		return err
	}

	// Verify file exists after command execution
	expectedBsonPath := filepath.Join(outputDir, config.Database.Database, collection+".bson")
	if _, err := os.Stat(expectedBsonPath); os.IsNotExist(err) {
		logrus.Warnf("[BackupExecutor] Expected BSON file not found at %s", expectedBsonPath)
		// List directory contents to help debugging
		files, _ := filepath.Glob(filepath.Join(outputDir, "*"))
		logrus.Infof("[BackupExecutor] Directory contents: %v", files)
	} else {
		logrus.Infof("[BackupExecutor] Successfully verified BSON file: %s", expectedBsonPath)
	}

	logrus.Infof("[BackupExecutor] Successfully dumped collection %s to %s", collection, outputDir)
	return nil
}

// exportMongoDBSingleTableWithExport Export single table JSON or CSV format using mongoexport
func (e *BackupExecutor) exportMongoDBSingleTableWithExport(ctx context.Context, config struct {
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
	Format          string                            `json:"format"`
	BackupType      string                            `json:"backupType"`
	Query           map[string]map[string]interface{} `json:"query"`
	CompressionType string                            `json:"compressionType"`
}, tempDir string, task BackupTask, connStr string, dateStr string, collection string) error {
	// Use full path to avoid command parsing issues
	mongoexportPath, err := exec.LookPath("mongoexport")
	if err != nil {
		mongoexportPath = "mongoexport" // If not found, use default command name
		logrus.Warnf("[BackupExecutor] mongoexport command not found in PATH, using default name")
	}

	// Determine file extension
	fileExt := "json"
	if config.Format == "csv" {
		fileExt = "csv"
	}

	outputPath := filepath.Join(tempDir, fmt.Sprintf("%s_%s.%s",
		collection, dateStr, fileExt))

	// Build field projection
	var fields string
	if fieldsArr, ok := config.Database.Fields[collection]; ok && len(fieldsArr) > 0 {
		// Check if fields is set to "all" - if so, don't add fields parameter to export all fields
		if len(fieldsArr) == 1 && fieldsArr[0] == "all" {
			logrus.Infof("[BackupExecutor] Fields set to 'all' for collection %s, exporting all fields", collection)
			fields = ""
		} else {
			fields = "--fields=" + strings.Join(fieldsArr, ",")
		}
	}

	// Build query condition
	var query string
	if queryObj, ok := config.Query[collection]; ok && len(queryObj) > 0 {
		// Clean query string values to remove extra escaping
		cleanedQueryObj := cleanQueryStringValues(queryObj)
		logrus.Infof("[BackupExecutor] Original query for collection %s: %+v", collection, queryObj)
		logrus.Infof("[BackupExecutor] Cleaned query for collection %s: %+v", collection, cleanedQueryObj)

		// Process time range query if needed
		processedQueryObj, err := utils.ProcessTimeRangeQuery(cleanedQueryObj)
		if err != nil {
			logrus.Warnf("[BackupExecutor] Failed to process time range query for collection %s: %v", collection, err)
			return fmt.Errorf("failed to process time range query: %w", err)
		}

		// Ensure standard JSON format query
		queryBytes, err := json.Marshal(processedQueryObj)
		if err != nil {
			logrus.Warnf("[BackupExecutor] Failed to marshal query for collection %s: %v", collection, err)
			return fmt.Errorf("failed to marshal query: %w", err)
		}

		// Use JSON directly without any quotes
		jsonStr := string(queryBytes)
		query = fmt.Sprintf("--query=%s", jsonStr)
		logrus.Infof("[BackupExecutor] MongoDB query for collection %s: %s", collection, jsonStr)
	}

	// Build command
	cmd := exec.CommandContext(ctx, mongoexportPath,
		"--uri", connStr,
		"--db", config.Database.Database,
		"--collection", collection,
		"--out", outputPath)

	// Handle format parameters
	if config.Format == "csv" {
		cmd.Args = append(cmd.Args, "--type=csv")
		// CSV format requires fields parameter
		if fields == "" {
			// Check if this is due to "all" fields setting
			if fieldsArr, ok := config.Database.Fields[collection]; ok && len(fieldsArr) == 1 && fieldsArr[0] == "all" {
				logrus.Warnf("[BackupExecutor] CSV export for collection %s with 'all' fields is not supported - CSV format requires explicit field specification", collection)
				return fmt.Errorf("CSV export with 'all' fields is not supported - CSV format requires explicit field specification")
			} else {
				logrus.Warnf("[BackupExecutor] CSV export for collection %s requires fields specification", collection)
				return fmt.Errorf("CSV export requires fields specification")
			}
		}
	} else {
		// Use JSON format by default
		cmd.Args = append(cmd.Args, "--type=json")
	}

	// Add optional parameters
	if fields != "" {
		cmd.Args = append(cmd.Args, fields)
	}
	if query != "" {
		cmd.Args = append(cmd.Args, query)
	}

	// Handle backup type parameters (incremental/full)
	if config.BackupType == "incremental" {
		// MongoDB incremental backup is typically implemented via query conditions
		// Here we can add query conditions based on last backup time
		if query == "" && task.LastBackupTime.After(time.Time{}) {
			// If no explicit query conditions and there's a last backup time, add time-based query
			timeQuery := fmt.Sprintf(`{"createdAt":{"$gt":{"$date":"%s"}}}`,
				task.LastBackupTime.Format(time.RFC3339))
			cmd.Args = append(cmd.Args, fmt.Sprintf("--query=%s", timeQuery))
			logrus.Infof("[BackupExecutor] Adding incremental time filter: %s", timeQuery)
		}
	}

	// Execute command
	if err := executeCommand(cmd, "mongoexport"); err != nil {
		return err
	}

	return nil
}
