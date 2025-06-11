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
			GCSPath        string `json:"gcsPath"`
			Retention      int    `json:"retention"`
			ServiceAccount string `json:"serviceAccount"`
		} `json:"destination"`
		Format     string                            `json:"format"`
		BackupType string                            `json:"backupType"`
		Query      map[string]map[string]interface{} `json:"query"`
	}

	if err := json.Unmarshal([]byte(task.ConfigJSON), &config); err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	logrus.Infof("[BackupExecutor] Starting backup for task %d (%s, type: %s)",
		taskID, config.Name, config.SourceType)

	// Create temporary directory
	tempDir, err := os.MkdirTemp("", fmt.Sprintf("backup_%d_", taskID))
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	// Code to automatically delete temp directory is commented out
	// defer os.RemoveAll(tempDir)
	logrus.Infof("[BackupExecutor] Temporary directory created: %s (will not be automatically deleted, please check and remove manually)", tempDir)

	// Select export tool based on database type
	var exportErr error
	switch config.SourceType {
	case "mongodb":
		exportErr = e.exportMongoDB(ctx, config, tempDir, task)
	case "postgresql":
		exportErr = e.exportPostgreSQL(ctx, config, tempDir)
	// More database types can be added here
	default:
		exportErr = fmt.Errorf("unsupported database type: %s", config.SourceType)
	}

	if exportErr != nil {
		return fmt.Errorf("export failed: %w", exportErr)
	}

	// Compress exported files
	dateStr := time.Now().Format("2006-01-02")
	// Use concatenated table names as compression file prefix
	var filePrefix string
	if len(config.Database.Tables) == 1 {
		// If only one table, use table name directly
		filePrefix = config.Database.Tables[0]
	} else if len(config.Database.Tables) > 1 {
		// If multiple tables, concatenate all table names
		filePrefix = strings.Join(config.Database.Tables, "-")
	} else {
		// If no tables, use database name
		filePrefix = config.Database.Database
	}
	// Create compressed file outside of temp directory to avoid "Can't add archive to itself" error
	tempParentDir := filepath.Dir(tempDir)
	archivePath := filepath.Join(tempParentDir, fmt.Sprintf("%s-%s.tar.gz", filePrefix, dateStr))

	if err := e.compressDirectory(tempDir, archivePath); err != nil {
		return fmt.Errorf("compression failed: %w", err)
	}
	logrus.Infof("[BackupExecutor] Archive file created: %s", archivePath)

	// Upload to GCS
	if err := utils.UploadToGCS(ctx, archivePath, config.Destination.GCSPath); err != nil {
		return fmt.Errorf("upload to GCS failed: %w", err)
	}

	logrus.Infof("[BackupExecutor] Backup completed successfully for task %d", taskID)
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
		t, _ := time.Parse("2006-01-02 15:04:05", lastUpdateTime.String)
		task.LastUpdateTime = t
	}
	if lastBackupTime.Valid {
		t, _ := time.Parse("2006-01-02 15:04:05", lastBackupTime.String)
		task.LastBackupTime = t
	}
	if nextBackupTime.Valid {
		t, _ := time.Parse("2006-01-02 15:04:05", nextBackupTime.String)
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
		GCSPath        string `json:"gcsPath"`
		Retention      int    `json:"retention"`
		ServiceAccount string `json:"serviceAccount"`
	} `json:"destination"`
	Format     string                            `json:"format"`
	BackupType string                            `json:"backupType"`
	Query      map[string]map[string]interface{} `json:"query"`
}, tempDir string, task BackupTask) error {
	dateStr := time.Now().Format("2006-01-02")

	// Build connection string
	connStr := config.Database.URL
	if config.Database.Username != "" && config.Database.Password != "" {
		connStr = fmt.Sprintf("mongodb://%s:%s@%s",
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
		GCSPath        string `json:"gcsPath"`
		Retention      int    `json:"retention"`
		ServiceAccount string `json:"serviceAccount"`
	} `json:"destination"`
	Format     string                            `json:"format"`
	BackupType string                            `json:"backupType"`
	Query      map[string]map[string]interface{} `json:"query"`
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
			// Ensure standard JSON format query
			queryBytes, err := json.Marshal(queryObj)
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
		GCSPath        string `json:"gcsPath"`
		Retention      int    `json:"retention"`
		ServiceAccount string `json:"serviceAccount"`
	} `json:"destination"`
	Format     string                            `json:"format"`
	BackupType string                            `json:"backupType"`
	Query      map[string]map[string]interface{} `json:"query"`
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

		outputPath := filepath.Join(tempDir, fmt.Sprintf("%s-%s.%s",
			collection, dateStr, fileExt))

		// Build field projection
		var fields string
		if fieldsArr, ok := config.Database.Fields[collection]; ok && len(fieldsArr) > 0 {
			fields = "--fields=" + strings.Join(fieldsArr, ",")
		}

		// Build query condition
		var query string
		if queryObj, ok := config.Query[collection]; ok && len(queryObj) > 0 {
			// Ensure standard JSON format query
			queryBytes, err := json.Marshal(queryObj)
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
				logrus.Warnf("[BackupExecutor] CSV export for collection %s requires fields specification", collection)
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

// exportPostgreSQL Export PostgreSQL data
func (e *BackupExecutor) exportPostgreSQL(ctx context.Context, config struct {
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
		GCSPath        string `json:"gcsPath"`
		Retention      int    `json:"retention"`
		ServiceAccount string `json:"serviceAccount"`
	} `json:"destination"`
	Format     string                            `json:"format"`
	BackupType string                            `json:"backupType"`
	Query      map[string]map[string]interface{} `json:"query"`
}, tempDir string) error {
	dateStr := time.Now().Format("2006-01-02")

	// Set environment variables
	env := os.Environ()
	if config.Database.Password != "" {
		env = append(env, fmt.Sprintf("PGPASSWORD=%s", config.Database.Password))
	}

	for _, table := range config.Database.Tables {
		outputPath := filepath.Join(tempDir, fmt.Sprintf("%s-%s.sql",
			table, dateStr))

		// Build query condition (PostgreSQL-specific SQL WHERE clause)
		var whereClause string

		if queryObj, ok := config.Query[table]; ok && len(queryObj) > 0 {
			// Convert query object to SQL WHERE clause
			var conditions []string
			for field, value := range queryObj {
				// Process appropriately based on value type
				switch v := value.(type) {
				case int, int64, float64:
					// Numeric types use directly
					conditions = append(conditions, fmt.Sprintf("%s = %v", field, v))
				case string:
					// Strings need quotes
					conditions = append(conditions, fmt.Sprintf("%s = '%s'", field, v))
				case map[string]interface{}:
					// Object type, may be complex query condition
					for op, val := range v {
						// Handle common MongoDB operators
						switch op {
						case "$gt":
							conditions = append(conditions, fmt.Sprintf("%s > %v", field, val))
						case "$gte":
							conditions = append(conditions, fmt.Sprintf("%s >= %v", field, val))
						case "$lt":
							conditions = append(conditions, fmt.Sprintf("%s < %v", field, val))
						case "$lte":
							conditions = append(conditions, fmt.Sprintf("%s <= %v", field, val))
						case "$eq":
							if strVal, ok := val.(string); ok {
								conditions = append(conditions, fmt.Sprintf("%s = '%s'", field, strVal))
							} else {
								conditions = append(conditions, fmt.Sprintf("%s = %v", field, val))
							}
						default:
							logrus.Warnf("[BackupExecutor] Unsupported operator %s for field %s", op, field)
						}
					}
				default:
					logrus.Warnf("[BackupExecutor] Unsupported value type for field %s: %T", field, value)
				}
			}

			if len(conditions) > 0 {
				whereClause = fmt.Sprintf("--where=%s", strings.Join(conditions, " AND "))
				logrus.Infof("[BackupExecutor] PostgreSQL WHERE clause for table %s: %s", table, whereClause)
			}
		}

		// Build command
		// Use full path to avoid command parsing issues
		pgDumpPath, err := exec.LookPath("pg_dump")
		if err != nil {
			pgDumpPath = "pg_dump" // If not found, use default command name
			logrus.Warnf("[BackupExecutor] pg_dump command not found in PATH, using default name")
		}
		cmd := exec.CommandContext(ctx, pgDumpPath,
			"-h", config.Database.URL,
			"-U", config.Database.Username,
			"-d", config.Database.Database,
			"-t", table,
			"-f", outputPath)

		cmd.Env = env

		// Add optional parameters
		if whereClause != "" {
			cmd.Args = append(cmd.Args, whereClause)
		}

		// Execute command
		if err := executeCommand(cmd, "pg_dump"); err != nil {
			return err
		}
	}

	return nil
}

// compressDirectory Compress directory
func (e *BackupExecutor) compressDirectory(sourceDir, destFile string) error {
	// Use full path to avoid command parsing issues
	tarPath, err := exec.LookPath("tar")
	if err != nil {
		tarPath = "tar" // If not found, use default command name
		logrus.Warnf("[BackupExecutor] tar command not found in PATH, using default name")
	}

	// Recursively find all needed files
	var allFiles []string

	// Define find function
	findFiles := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories, only add files
		if !info.IsDir() {
			ext := filepath.Ext(path)
			// Support JSON, SQL and BSON files
			if ext == ".json" || ext == ".sql" || ext == ".bson" {
				allFiles = append(allFiles, path)
				logrus.Debugf("[BackupExecutor] Found file to compress: %s", path)
			}
		}
		return nil
	}

	// Recursively traverse directory
	if err := filepath.Walk(sourceDir, findFiles); err != nil {
		return fmt.Errorf("failed to walk directory: %w", err)
	}

	if len(allFiles) == 0 {
		// Detailed logging to help debugging
		files, _ := filepath.Glob(filepath.Join(sourceDir, "*"))
		logrus.Errorf("[BackupExecutor] No compressible files found. Directory contents: %v", files)
		return fmt.Errorf("no files to compress (checked for .json, .sql, .bson)")
	}

	// Make all file paths relative
	var relativeFiles []string
	for _, file := range allFiles {
		relFile, err := filepath.Rel(sourceDir, file)
		if err != nil {
			logrus.Warnf("[BackupExecutor] Failed to get relative path for %s: %v", file, err)
			continue
		}
		relativeFiles = append(relativeFiles, relFile)
	}

	// Build tar command, using -r option to include all subdirectories
	args := []string{"-czf", destFile, "-C", sourceDir}
	args = append(args, relativeFiles...)
	cmd := exec.Command(tarPath, args...)

	logrus.Infof("[BackupExecutor] Compressing %d files to %s", len(relativeFiles), destFile)

	// Execute command
	if err := executeCommand(cmd, "tar"); err != nil {
		return err
	}

	return nil
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
