package backup

import (
	"archive/zip"
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
	"runtime/debug"
	"strconv"
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

		// Export table group (single table or merged tables)
		var exportErr error
		switch config.SourceType {
		case "mongodb":
			if len(tables) == 1 {
				// Single table export - convert config to old format
				oldConfig := struct {
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
				}{
					Name:            config.Name,
					SourceType:      config.SourceType,
					Database:        config.Database,
					Destination:     config.Destination,
					Format:          config.Format,
					BackupType:      config.BackupType,
					Query:           config.Query,
					CompressionType: config.CompressionType,
				}
				exportErr = e.exportMongoDBSingleTable(ctx, oldConfig, tempDir, task, tables[0])
			} else {
				// Merged table export
				exportErr = e.exportMongoDBMergedTables(ctx, &config, tempDir, task, tables, groupName)
			}
		default:
			exportErr = fmt.Errorf("unsupported database type: %s", config.SourceType)
		}

		if exportErr != nil {
			logrus.Errorf("[BackupExecutor] Export failed for table group %s: %v", groupName, exportErr)
			os.RemoveAll(tempDir) // Clean up failed export
			continue
		}

		// 🔍 CRITICAL: Log memory after export completes but BEFORE compression starts
		var memStats runtime.MemStats
		runtime.ReadMemStats(&memStats)
		logrus.Infof("[BackupExecutor] 📊 Memory after EXPORT completes, before compression: Alloc=%.2fMB, Sys=%.2fMB, Free=%.2fMB, NumGoroutines=%d", 
			float64(memStats.Alloc)/1024/1024, 
			float64(memStats.Sys)/1024/1024,
			float64(memStats.Sys-memStats.Alloc)/1024/1024, 
			runtime.NumGoroutine())

		// Create compressed file for this table group using pattern
		tempParentDir := filepath.Dir(tempDir)
		archiveBaseName := processFileNamePattern(config.Destination.FileNamePattern, groupName)

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

		// Log memory before compression phase
		runtime.ReadMemStats(&memStats)
		logrus.Infof("[BackupExecutor] Memory before compression phase - Alloc: %.2f MB, Sys: %.2f MB, Free: %.2f MB", 
			float64(memStats.Alloc)/1024/1024, 
			float64(memStats.Sys)/1024/1024,
			float64(memStats.Sys-memStats.Alloc)/1024/1024)

		if err := e.CompressDirectory(tempDir, archivePath, compressionType); err != nil {
			logrus.Errorf("[BackupExecutor] Compression failed for table group %s: %v", groupName, err)
			os.RemoveAll(tempDir) // Clean up temp directory only
			continue
		}

		// Log memory before GCS upload phase  
		runtime.ReadMemStats(&memStats)
		logrus.Infof("[BackupExecutor] Memory before GCS upload - Alloc: %.2f MB, Sys: %.2f MB, Free: %.2f MB", 
			float64(memStats.Alloc)/1024/1024, 
			float64(memStats.Sys)/1024/1024,
			float64(memStats.Sys-memStats.Alloc)/1024/1024)

		// Upload to GCS
		if err := utils.UploadToGCS(ctx, archivePath, config.Destination.GCSPath); err != nil {
			logrus.Errorf("[BackupExecutor] Upload to GCS failed for table group %s: %v", groupName, err)
			logrus.Warnf("[BackupExecutor] Archive file preserved for debugging: %s", archivePath)
			// Clean up temp directory only, keep archive file for debugging
			os.RemoveAll(tempDir)
			continue
		}

		// Log memory after GCS upload and before cleanup
		runtime.ReadMemStats(&memStats)
		logrus.Infof("[BackupExecutor] Memory after GCS upload - Alloc: %.2f MB, Sys: %.2f MB, Free: %.2f MB", 
			float64(memStats.Alloc)/1024/1024, 
			float64(memStats.Sys)/1024/1024,
			float64(memStats.Sys-memStats.Alloc)/1024/1024)

		// Clean up temporary files after successful upload
		os.RemoveAll(tempDir)
		os.Remove(archivePath)
		
		// Force garbage collection after cleanup
		runtime.GC()
		debug.FreeOSMemory()
		runtime.ReadMemStats(&memStats)
		logrus.Infof("[BackupExecutor] Memory after cleanup and GC - Alloc: %.2f MB, Sys: %.2f MB, Free: %.2f MB", 
			float64(memStats.Alloc)/1024/1024, 
			float64(memStats.Sys)/1024/1024,
			float64(memStats.Sys-memStats.Alloc)/1024/1024)
		
		logrus.Infof("[BackupExecutor] ✅ Backup completed successfully for table group: %s", groupName)
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
	connStr := buildMongoDBConnectionString(config.Database.URL, config.Database.Username, config.Database.Password)

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

		// Check collection size and use batch processing for large collections
		totalCount, err := e.getCollectionDocumentCount(ctx, connStr, config.Database.Database, collection, config.Query[collection])
		if err != nil {
			logrus.Warnf("[BackupExecutor] Failed to get document count for %s, using single export: %v", collection, err)
			if err := executeCommand(cmd, "mongoexport"); err != nil {
				return err
			}
			continue
		}

		const batchThreshold = 500000 // Use batch processing for collections larger than 500k documents
		const batchSize = 50000       // Reduced batch size to lower memory per batch

		if totalCount > batchThreshold {
			logrus.Infof("[BackupExecutor] Large collection %s detected (%d documents), using stream merge processing", collection, totalCount)
			if err := e.executeMongoExportWithStreamMerge(ctx, cmd, mongoexportPath, outputPath, totalCount, batchSize); err != nil {
				return err
			}
		} else {
			// Small collection, use single export
			if err := executeCommand(cmd, "mongoexport"); err != nil {
				return err
			}
		}
	}

	return nil
}

// CompressDirectory Compress directory with specified compression type
func (e *BackupExecutor) CompressDirectory(sourceDir, destFile, compressionType string) error {
	logrus.Infof("[BackupExecutor] 🗜️ STARTING CompressDirectory: sourceDir=%s, destFile=%s, type=%s", sourceDir, destFile, compressionType)
	
	// 🔍 Memory at the very start of compression
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	logrus.Infof("[BackupExecutor] 🗜️ Memory at START of CompressDirectory: Alloc=%.2fMB, Sys=%.2fMB, Free=%.2fMB, NumGoroutines=%d", 
		float64(memStats.Alloc)/1024/1024, 
		float64(memStats.Sys)/1024/1024,
		float64(memStats.Sys-memStats.Alloc)/1024/1024,
		runtime.NumGoroutine())

	// Set default compression type to zip if empty
	if compressionType == "" {
		compressionType = "zip"
	}

	logrus.Debugf("[BackupExecutor] Using compression type: %s", compressionType)

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
				logrus.Debugf("[BackupExecutor] Found file to compress: %s (size: %.2f MB)",
					path, float64(info.Size())/1024/1024)
			} else {
				logrus.Debugf("[BackupExecutor] Skipping file with unsupported extension: %s", path)
			}
		}
		return nil
	}

	// Recursively traverse directory
	logrus.Debugf("[BackupExecutor] Scanning directory for compressible files: %s", sourceDir)
	
	// 🔍 Memory before directory scanning
	runtime.ReadMemStats(&memStats)
	logrus.Infof("[BackupExecutor] 📁 Memory BEFORE directory scanning: Alloc=%.2fMB, Sys=%.2fMB, Free=%.2fMB", 
		float64(memStats.Alloc)/1024/1024, 
		float64(memStats.Sys)/1024/1024,
		float64(memStats.Sys-memStats.Alloc)/1024/1024)
	
	if err := filepath.Walk(sourceDir, findFiles); err != nil {
		logrus.Errorf("[BackupExecutor] Failed to walk directory %s: %v", sourceDir, err)
		return fmt.Errorf("failed to walk directory: %w", err)
	}
	
	// 🔍 Memory after directory scanning
	runtime.ReadMemStats(&memStats)
	logrus.Infof("[BackupExecutor] 📁 Memory AFTER directory scanning: Alloc=%.2fMB, Sys=%.2fMB, Free=%.2fMB, FoundFiles=%d", 
		float64(memStats.Alloc)/1024/1024, 
		float64(memStats.Sys)/1024/1024,
		float64(memStats.Sys-memStats.Alloc)/1024/1024,
		len(allFiles))

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

	logrus.Debugf("[BackupExecutor] Compressing %d files (total size: %.2f MB) using %s compression",
		len(allFiles), float64(totalSize)/1024/1024, compressionType)

	// 🔍 Memory BEFORE actual compression method call
	runtime.ReadMemStats(&memStats)
	logrus.Infof("[BackupExecutor] 🗜️ Memory BEFORE calling compression method (%s): Alloc=%.2fMB, Sys=%.2fMB, Free=%.2fMB", 
		compressionType,
		float64(memStats.Alloc)/1024/1024, 
		float64(memStats.Sys)/1024/1024,
		float64(memStats.Sys-memStats.Alloc)/1024/1024)

	// Execute compression based on type
	var compressionErr error
	switch compressionType {
	case "zip":
		compressionErr = e.compressWithZip(sourceDir, destFile, allFiles)
	case "gzip":
		compressionErr = e.compressWithGzip(sourceDir, destFile, allFiles)
	default:
		compressionErr = fmt.Errorf("unsupported compression type: %s", compressionType)
	}
	
	// 🔍 Memory AFTER compression method completes
	runtime.ReadMemStats(&memStats)
	logrus.Infof("[BackupExecutor] 🗜️ Memory AFTER compression method (%s): Alloc=%.2fMB, Sys=%.2fMB, Free=%.2fMB", 
		compressionType,
		float64(memStats.Alloc)/1024/1024, 
		float64(memStats.Sys)/1024/1024,
		float64(memStats.Sys-memStats.Alloc)/1024/1024)
	
	return compressionErr
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

	// Process stdout in streaming fashion, only keep key information
	var exportedCount string
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.Contains(line, "exported") && strings.Contains(line, "records") {
				exportedCount = strings.TrimSpace(line)
			}
			// Other output is discarded to save memory
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

// compressWithZip Compress files using memory-optimized streaming ZIP
func (e *BackupExecutor) compressWithZip(sourceDir, destFile string, allFiles []string) error {
	// Log memory before compression
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	logrus.Infof("[BackupExecutor] Memory before streaming ZIP compression - Alloc: %.2f MB, Sys: %.2f MB, Free: %.2f MB", 
		float64(memStats.Alloc)/1024/1024, 
		float64(memStats.Sys)/1024/1024,
		float64(memStats.Sys-memStats.Alloc)/1024/1024)

	// Create output ZIP file
	zipFile, err := os.Create(destFile)
	if err != nil {
		return fmt.Errorf("failed to create ZIP file: %w", err)
	}
	defer zipFile.Close()

	// Create ZIP writer with buffering
	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()

	logrus.Infof("[BackupExecutor] Starting streaming ZIP compression of %d files", len(allFiles))

	for i, file := range allFiles {
		if err := e.addFileToZipStreaming(zipWriter, sourceDir, file); err != nil {
			return fmt.Errorf("failed to add file %s to ZIP: %w", file, err)
		}

		// Log progress and force GC every 5 files to control memory
		if (i+1)%5 == 0 || i == len(allFiles)-1 {
			runtime.GC()
			runtime.ReadMemStats(&memStats)
			logrus.Debugf("[BackupExecutor] ZIP progress: %d/%d files, Memory: %.2f MB", 
				i+1, len(allFiles), float64(memStats.Alloc)/1024/1024)
		}
	}

	// Close ZIP writer to finalize compression
	if err := zipWriter.Close(); err != nil {
		return fmt.Errorf("failed to finalize ZIP file: %w", err)
	}

	// Force garbage collection and log memory after compression
	runtime.GC()
	debug.FreeOSMemory()
	runtime.ReadMemStats(&memStats)
	logrus.Infof("[BackupExecutor] Memory after streaming ZIP compression - Alloc: %.2f MB, Sys: %.2f MB, Free: %.2f MB", 
		float64(memStats.Alloc)/1024/1024, 
		float64(memStats.Sys)/1024/1024,
		float64(memStats.Sys-memStats.Alloc)/1024/1024)

	// Check compressed file size
	if info, err := os.Stat(destFile); err == nil {
		logrus.Infof("[BackupExecutor] Streaming ZIP compression completed. Archive size: %.2f MB",
			float64(info.Size())/1024/1024)
	} else {
		logrus.Warnf("[BackupExecutor] Could not stat compressed file %s: %v", destFile, err)
	}

	return nil
}

// addFileToZipStreaming Add a single file to ZIP archive using streaming I/O
func (e *BackupExecutor) addFileToZipStreaming(zipWriter *zip.Writer, sourceDir, filePath string) error {
	// Get relative path for ZIP entry
	relPath, err := filepath.Rel(sourceDir, filePath)
	if err != nil {
		return fmt.Errorf("failed to get relative path: %w", err)
	}

	// Open source file
	sourceFile, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer sourceFile.Close()

	// Get file info for ZIP header
	fileInfo, err := sourceFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat source file: %w", err)
	}

	// Create ZIP entry header
	header, err := zip.FileInfoHeader(fileInfo)
	if err != nil {
		return fmt.Errorf("failed to create ZIP header: %w", err)
	}
	header.Name = relPath
	header.Method = zip.Deflate // Use deflate compression

	// Create ZIP entry writer
	entryWriter, err := zipWriter.CreateHeader(header)
	if err != nil {
		return fmt.Errorf("failed to create ZIP entry: %w", err)
	}

	// Stream copy with limited buffer to control memory usage
	bufferSize := 32 * 1024 // 32KB buffer - much smaller than system zip
	buffer := make([]byte, bufferSize)
	
	totalBytes := int64(0)
	for {
		n, err := sourceFile.Read(buffer)
		if n > 0 {
			if _, writeErr := entryWriter.Write(buffer[:n]); writeErr != nil {
				return fmt.Errorf("failed to write to ZIP entry: %w", writeErr)
			}
			totalBytes += int64(n)
		}
		
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read source file: %w", err)
		}
		
		// Force GC every 10MB to prevent memory accumulation
		if totalBytes%10485760 == 0 { // 10MB
			runtime.GC()
		}
	}

	logrus.Debugf("[BackupExecutor] Added %s to ZIP (%.2f MB)", relPath, float64(totalBytes)/1024/1024)
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

	// Log memory before compression
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	logrus.Infof("[BackupExecutor] Memory before GZIP compression - Alloc: %.2f MB, Sys: %.2f MB, Free: %.2f MB", 
		float64(memStats.Alloc)/1024/1024, 
		float64(memStats.Sys)/1024/1024,
		float64(memStats.Sys-memStats.Alloc)/1024/1024)

	// Execute command
	if err := executeCommandStreaming(cmd, "tar"); err != nil {
		logrus.Errorf("[BackupExecutor] GZIP compression failed: %v", err)
		return err
	}

	// Force garbage collection and log memory after compression
	runtime.GC()
	debug.FreeOSMemory()
	runtime.ReadMemStats(&memStats)
	logrus.Infof("[BackupExecutor] Memory after GZIP compression - Alloc: %.2f MB, Sys: %.2f MB, Free: %.2f MB", 
		float64(memStats.Alloc)/1024/1024, 
		float64(memStats.Sys)/1024/1024,
		float64(memStats.Sys-memStats.Alloc)/1024/1024)

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
	connStr := buildMongoDBConnectionString(config.Database.URL, config.Database.Username, config.Database.Password)

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

	// Check collection size and use batch processing for large collections
	totalCount, err := e.getCollectionDocumentCount(ctx, connStr, config.Database.Database, collection, config.Query[collection])
	if err != nil {
		logrus.Warnf("[BackupExecutor] Failed to get document count, using single export: %v", err)
		return executeCommand(cmd, "mongoexport")
	}

	const batchThreshold = 500000 // Use batch processing for collections larger than 500k documents
	const batchSize = 50000       // Reduced batch size to lower memory per batch

	if totalCount > batchThreshold {
		logrus.Infof("[BackupExecutor] Large collection detected (%d documents), using stream merge processing", totalCount)
		
		// Execute stream merge and monitor memory immediately after
		streamErr := e.executeMongoExportWithStreamMerge(ctx, cmd, mongoexportPath, outputPath, totalCount, batchSize)
		
		// Log memory immediately after stream merge returns
		var memStats runtime.MemStats
		runtime.ReadMemStats(&memStats)
		logrus.Infof("[BackupExecutor] 🔍 Memory IMMEDIATELY after stream merge return: Alloc=%.2fMB, Sys=%.2fMB, Free=%.2fMB, NumGoroutines=%d", 
			float64(memStats.Alloc)/1024/1024, 
			float64(memStats.Sys)/1024/1024,
			float64(memStats.Sys-memStats.Alloc)/1024/1024, 
			runtime.NumGoroutine())
		
		return streamErr
	} else {
		// Small collection, use single export
		return executeCommand(cmd, "mongoexport")
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
		tableGroups = e.groupTablesByPrefix(actualTables)
		logrus.Debugf("[BackupExecutor] Regex mode enabled, found %d table groups from pattern %s",
			len(tableGroups), config.RegexPattern)
	} else {
		// Normal mode: each table is its own group
		for _, table := range config.Database.Tables {
			tableGroups[table] = []string{table}
		}
		logrus.Infof("[BackupExecutor] Normal mode enabled, processing %d individual tables",
			len(tableGroups))
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
	}

	for _, pattern := range patterns {
		re := regexp.MustCompile(pattern)
		if re.MatchString(tableName) {
			return re.ReplaceAllString(tableName, "")
		}
	}

	// If no pattern matches, return the original table name
	return tableName
}

// exportMongoDBMergedTables Export and merge multiple MongoDB collections
func (e *BackupExecutor) exportMongoDBMergedTables(ctx context.Context, config *ExecutorBackupConfig, tempDir string, task BackupTask, tables []string, groupName string) error {

	dateStr := time.Now().AddDate(0, 0, -1).Format("2006-01-02")
	var allDocuments []interface{}

	// Filter tables based on query time range if query conditions exist
	relevantTables := e.filterRelevantTables(tables, config.Query, groupName)
	if len(relevantTables) < len(tables) {
		logrus.Debugf("[BackupExecutor] Filtered %d tables to %d relevant tables based on time range", len(tables), len(relevantTables))
	}

	// Export each relevant table and collect documents
	for _, tableName := range relevantTables {

		// Create temporary config for single table export
		tempTableDir := filepath.Join(tempDir, tableName+"_temp")
		if err := os.MkdirAll(tempTableDir, 0755); err != nil {
			return fmt.Errorf("failed to create temp directory for table %s: %w", tableName, err)
		}

		// Export single table using existing method - convert config to old format
		oldConfig := struct {
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
		}{
			Name:            config.Name,
			SourceType:      config.SourceType,
			Database:        config.Database,
			Destination:     config.Destination,
			Format:          config.Format,
			BackupType:      config.BackupType,
			Query:           config.Query,
			CompressionType: config.CompressionType,
		}
		if err := e.exportMongoDBSingleTable(ctx, oldConfig, tempTableDir, task, tableName); err != nil {
			logrus.Warnf("[BackupExecutor] Failed to export table %s: %v", tableName, err)
			continue
		}

		// Read the exported file and collect documents
		documents, err := e.readJSONFile(tempTableDir, tableName, dateStr)
		if err != nil {
			logrus.Warnf("[BackupExecutor] Failed to read exported file for table %s: %v", tableName, err)
			continue
		}

		allDocuments = append(allDocuments, documents...)
		logrus.Debugf("[BackupExecutor] ✓ Collected %d documents from table: %s", len(documents), tableName)

		// Clean up temporary directory
		os.RemoveAll(tempTableDir)
	}

	// Write merged documents to final file
	if len(allDocuments) > 0 {
		finalFileName := fmt.Sprintf("%s_%s.json", groupName, dateStr)
		finalFilePath := filepath.Join(tempDir, finalFileName)

		if err := e.writeJSONFile(finalFilePath, allDocuments); err != nil {
			return fmt.Errorf("failed to write merged file: %w", err)
		}

		logrus.Infof("[BackupExecutor] ✅ Merged %d documents from %d tables into: %s",
			len(allDocuments), len(relevantTables), finalFileName)
	} else {
		logrus.Warnf("[BackupExecutor] No documents found in any of the tables: %v", relevantTables)
	}

	return nil
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
	if username != "" && password != "" {
		return fmt.Sprintf("mongodb://%s:%s@%s/?authSource=admin", username, password, url)
	}
	return fmt.Sprintf("mongodb://%s", url)
}

// getCollectionDocumentCount Get total document count for a collection with query conditions
func (e *BackupExecutor) getCollectionDocumentCount(ctx context.Context, connStr, database, collection string, queryObj map[string]interface{}) (int64, error) {
	// Connect to MongoDB with optimized connection settings for count operations
	clientOpts := options.Client().ApplyURI(connStr)
	// Optimize for minimal memory usage - single connection, no pooling for count operations
	clientOpts.SetMaxPoolSize(1)
	clientOpts.SetMaxConnIdleTime(time.Second * 10)
	
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return 0, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}
	defer func() {
		// Ensure connection is properly closed and resources released
		if disconnectErr := client.Disconnect(ctx); disconnectErr != nil {
			logrus.Warnf("[BackupExecutor] Failed to disconnect MongoDB client: %v", disconnectErr)
		}
	}()

	// Get database and collection
	db := client.Database(database)
	coll := db.Collection(collection)

	// Build query filter
	var filter bson.M
	if queryObj != nil && len(queryObj) > 0 {
		// Clean and process query
		cleanedQueryObj := cleanQueryStringValues(queryObj)
		processedQueryObj, err := utils.ProcessTimeRangeQuery(cleanedQueryObj)
		if err != nil {
			return 0, fmt.Errorf("failed to process time range query: %w", err)
		}

		// Convert to BSON
		queryBytes, err := json.Marshal(processedQueryObj)
		if err != nil {
			return 0, fmt.Errorf("failed to marshal query: %w", err)
		}

		if err := bson.UnmarshalExtJSON(queryBytes, false, &filter); err != nil {
			return 0, fmt.Errorf("failed to unmarshal query to BSON: %w", err)
		}
	} else {
		filter = bson.M{}
	}

	// Count documents with memory monitoring
	logrus.Debugf("[BackupExecutor] Counting documents in collection %s.%s", database, collection)
	
	count, err := coll.CountDocuments(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("failed to count documents: %w", err)
	}

	logrus.Infof("[BackupExecutor] Collection %s.%s contains %d documents", database, collection, count)
	
	// Force GC after count operation to ensure connection resources are freed
	runtime.GC()
	
	return count, nil
}

// executeMongoExportInBatches Execute mongoexport in batches to reduce memory usage
func (e *BackupExecutor) executeMongoExportInBatches(ctx context.Context, baseCmd *exec.Cmd, mongoexportPath, outputPath string, totalCount int64, batchSize int) error {
	var batchFiles []string
	batchCount := int((totalCount + int64(batchSize) - 1) / int64(batchSize)) // Ceiling division

	logrus.Infof("[BackupExecutor] Exporting %d documents in %d batches of %d", totalCount, batchCount, batchSize)

	for i := 0; i < batchCount; i++ {
		skip := i * batchSize
		limit := batchSize
		if i == batchCount-1 {
			// Last batch might be smaller
			remaining := int(totalCount) - skip
			if remaining < batchSize {
				limit = remaining
			}
		}

		// Create temp file for this batch
		tempFile := fmt.Sprintf("%s.batch_%d", outputPath, i)
		batchFiles = append(batchFiles, tempFile)

		// Export single batch
		if err := e.exportSingleBatch(ctx, baseCmd, mongoexportPath, i, skip, limit, tempFile); err != nil {
			e.cleanupTempFiles(batchFiles)
			return fmt.Errorf("batch %d failed: %w", i+1, err)
		}

		logrus.Infof("[BackupExecutor] Completed batch %d/%d (exported %d documents)", i+1, batchCount, limit)

		// Force garbage collection after every 5 batches to free memory
		if (i+1)%5 == 0 {
			logrus.Debugf("[BackupExecutor] Running garbage collection after batch %d", i+1)
			runtime.GC()
			runtime.GC() // Call twice for more aggressive cleanup
		}
	}

	// Final garbage collection before merge
	logrus.Debugf("[BackupExecutor] Running final GC before merging %d files", len(batchFiles))
	runtime.GC()
	runtime.GC()

	// Log memory stats before merge
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	logrus.Infof("[BackupExecutor] Memory before batch merge: Alloc=%.2fMB, Sys=%.2fMB, Free=%.2fMB, NumGoroutines=%d",
		float64(m.Alloc)/1024/1024, float64(m.Sys)/1024/1024, float64(m.Sys-m.Alloc)/1024/1024, runtime.NumGoroutine())

	// Merge all batch files
	if err := e.mergeBatchFiles(batchFiles, outputPath); err != nil {
		e.cleanupTempFiles(batchFiles)
		return fmt.Errorf("failed to merge batch files: %w", err)
	}

	// Clean up temporary files
	e.cleanupTempFiles(batchFiles)

	// Final cleanup and memory stats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&m)
	logrus.Infof("[BackupExecutor] Memory after batch cleanup: Alloc=%.2fMB, Sys=%.2fMB, Free=%.2fMB, NumGoroutines=%d",
		float64(m.Alloc)/1024/1024, float64(m.Sys)/1024/1024, float64(m.Sys-m.Alloc)/1024/1024, runtime.NumGoroutine())

	logrus.Infof("[BackupExecutor] Successfully exported %d documents in %d batches", totalCount, batchCount)
	return nil
}

// exportSingleBatch Execute single batch export
func (e *BackupExecutor) exportSingleBatch(ctx context.Context, baseCmd *exec.Cmd, mongoexportPath string, batchIndex, skip, limit int, tempFile string) error {
	// Create batch command by copying base command
	batchCmd := exec.CommandContext(ctx, mongoexportPath)
	batchCmd.Args = make([]string, len(baseCmd.Args))
	copy(batchCmd.Args, baseCmd.Args)

	// Update output path for this batch
	for j, arg := range batchCmd.Args {
		if arg == "--out" && j+1 < len(batchCmd.Args) {
			batchCmd.Args[j+1] = tempFile
			break
		}
	}

	// Add batch parameters
	batchCmd.Args = append(batchCmd.Args, "--limit", fmt.Sprintf("%d", limit))
	batchCmd.Args = append(batchCmd.Args, "--skip", fmt.Sprintf("%d", skip))

	// Execute batch command
	return executeCommand(batchCmd, "mongoexport")
}

// mergeBatchFiles Merge multiple batch files into final output file with streaming (MEMORY OPTIMIZED)
func (e *BackupExecutor) mergeBatchFiles(batchFiles []string, finalOutputPath string) error {
	finalFile, err := os.Create(finalOutputPath)
	if err != nil {
		return fmt.Errorf("failed to create final output file: %w", err)
	}
	defer finalFile.Close()

	logrus.Infof("[BackupExecutor] Streaming merge %d batch files into %s", len(batchFiles), finalOutputPath)

	// Use buffered writer for better performance
	bufWriter := bufio.NewWriter(finalFile)
	defer bufWriter.Flush()

	for i, batchFile := range batchFiles {
		batchData, err := os.Open(batchFile)
		if err != nil {
			logrus.Warnf("[BackupExecutor] Failed to open batch file %s: %v", batchFile, err)
			continue
		}

		// Stream copy with limited buffer to avoid memory accumulation
		scanner := bufio.NewScanner(batchData)
		// Set buffer size to 64KB (much smaller than ReadFrom's potential GB buffer)
		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 64*1024)
		
		lineCount := 0
		for scanner.Scan() {
			if _, err := bufWriter.Write(scanner.Bytes()); err != nil {
				batchData.Close()
				return fmt.Errorf("failed to write line from batch file %s: %w", batchFile, err)
			}
			if _, err := bufWriter.WriteString("\n"); err != nil {
				batchData.Close()
				return fmt.Errorf("failed to write newline: %w", err)
			}
			lineCount++
			
			// Flush buffer periodically to avoid memory accumulation
			if lineCount%1000 == 0 {
				if err := bufWriter.Flush(); err != nil {
					batchData.Close()
					return fmt.Errorf("failed to flush buffer: %w", err)
				}
			}
		}

		if err := scanner.Err(); err != nil {
			batchData.Close()
			return fmt.Errorf("error scanning batch file %s: %w", batchFile, err)
		}
		
		batchData.Close()
		
		// Force buffer flush after each file
		if err := bufWriter.Flush(); err != nil {
			return fmt.Errorf("failed to flush buffer after file %s: %w", batchFile, err)
		}
		
		logrus.Debugf("[BackupExecutor] Merged batch file %d/%d: %s (%d lines)", i+1, len(batchFiles), filepath.Base(batchFile), lineCount)
		
		// Force GC every few files to prevent memory accumulation
		if (i+1)%5 == 0 {
			runtime.GC()
			debug.FreeOSMemory()
		}
	}

	return nil
}

// cleanupTempFiles Remove temporary batch files
func (e *BackupExecutor) cleanupTempFiles(files []string) {
	for _, file := range files {
		if err := os.Remove(file); err != nil {
			logrus.Warnf("[BackupExecutor] Failed to remove temp file %s: %v", file, err)
		}
	}
}

// executeMongoExportWithStreamMerge Execute mongoexport with stream merge to minimize memory usage
func (e *BackupExecutor) executeMongoExportWithStreamMerge(ctx context.Context, baseCmd *exec.Cmd, mongoexportPath, outputPath string, totalCount int64, batchSize int) error {
	batchCount := int((totalCount + int64(batchSize) - 1) / int64(batchSize))

	logrus.Infof("[BackupExecutor] Starting stream merge export: %d documents in %d batches of %d", totalCount, batchCount, batchSize)

	// Create final output file
	finalFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create final output file: %w", err)
	}
	defer finalFile.Close()

	// Extract base arguments without --out parameter
	baseArgs := e.getBaseArgsWithoutOutput(baseCmd.Args)

	for i := 0; i < batchCount; i++ {
		skip := i * batchSize
		limit := batchSize
		if i == batchCount-1 {
			remaining := int(totalCount) - skip
			if remaining < batchSize {
				limit = remaining
			}
		}

		// Create temporary file for this batch
		tempFile := fmt.Sprintf("%s.stream_temp_%d", outputPath, i)

		// Export single batch with optimized parameters
		if err := e.exportSingleBatchOptimized(ctx, mongoexportPath, baseArgs, tempFile, skip, limit); err != nil {
			os.Remove(tempFile) // Clean up failed temp file
			return fmt.Errorf("batch %d failed: %w", i+1, err)
		}

		// Immediately stream append to final file and delete temp file
		if err := e.streamAppendAndCleanup(tempFile, finalFile); err != nil {
			return fmt.Errorf("failed to merge batch %d: %w", i+1, err)
		}

		logrus.Infof("[BackupExecutor] Completed batch %d/%d (merged %d documents)", i+1, batchCount, limit)

		// Force garbage collection every 3 batches to free memory
		if (i+1)%3 == 0 {
			runtime.GC()
			runtime.GC() // Call twice for more aggressive cleanup

			// Log detailed memory status for debugging
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			logrus.Infof("[BackupExecutor] Memory after batch %d: Alloc=%dMB, Sys=%dMB, HeapAlloc=%dMB, HeapSys=%dMB, NumGoroutines=%d", 
				i+1, m.Alloc/1024/1024, m.Sys/1024/1024, m.HeapAlloc/1024/1024, m.HeapSys/1024/1024, runtime.NumGoroutine())
		}
	}

	// Final cleanup with aggressive memory release
	runtime.GC()
	runtime.GC()
	
	// Force return memory to OS
	debug.FreeOSMemory()
	
	// Final memory stats
	var finalMem runtime.MemStats
	runtime.ReadMemStats(&finalMem)
	logrus.Infof("[BackupExecutor] Final memory stats: Alloc=%dMB, Sys=%dMB, NumGoroutines=%d", 
		finalMem.Alloc/1024/1024, finalMem.Sys/1024/1024, runtime.NumGoroutine())

	logrus.Infof("[BackupExecutor] Stream merge completed: %d documents exported", totalCount)
	return nil
}

// exportSingleBatchOptimized Export single batch with memory-optimized parameters
func (e *BackupExecutor) exportSingleBatchOptimized(ctx context.Context, mongoexportPath string, baseArgs []string, tempFile string, skip, limit int) error {
	// Build complete argument list, avoiding multiple memory allocations
	args := make([]string, 0, len(baseArgs)+6)
	args = append(args, baseArgs...)
	args = append(args, "--out", tempFile)
	args = append(args, "--limit", strconv.Itoa(limit))
	args = append(args, "--skip", strconv.Itoa(skip))

	cmd := exec.CommandContext(ctx, mongoexportPath, args...)

	// Use streaming output processing
	return executeCommandStreaming(cmd, "mongoexport")
}

// streamAppendAndCleanup Stream append temp file content to final file and delete temp file
func (e *BackupExecutor) streamAppendAndCleanup(tempFile string, finalFile *os.File) error {
	// Open temporary file
	temp, err := os.Open(tempFile)
	if err != nil {
		return fmt.Errorf("failed to open temp file %s: %w", tempFile, err)
	}
	defer temp.Close()

	// Stream copy to final file (no memory buffer)
	_, err = io.Copy(finalFile, temp)
	if err != nil {
		return fmt.Errorf("failed to copy temp file content: %w", err)
	}

	// Force flush to disk
	if err := finalFile.Sync(); err != nil {
		logrus.Warnf("[BackupExecutor] Failed to sync file: %v", err)
	}

	// Immediately delete temporary file
	if err := os.Remove(tempFile); err != nil {
		logrus.Warnf("[BackupExecutor] Failed to remove temp file %s: %v", tempFile, err)
	}

	return nil
}

// getBaseArgsWithoutOutput Extract base arguments excluding --out parameter for reuse
func (e *BackupExecutor) getBaseArgsWithoutOutput(args []string) []string {
	var baseArgs []string
	skipNext := false

	// Skip args[0] which is the executable path, start from args[1]
	for i := 1; i < len(args); i++ {
		arg := args[i]
		
		if skipNext {
			skipNext = false
			continue
		}

		if arg == "--out" {
			skipNext = true // Skip next argument (file path)
			continue
		}

		baseArgs = append(baseArgs, arg)
	}

	return baseArgs
}
