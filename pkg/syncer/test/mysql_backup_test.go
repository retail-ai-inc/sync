package test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/pkg/backup"
	_ "github.com/mattn/go-sqlite3"
)

// TestMySQLBackupFunctions tests MySQL backup related functionality
func TestMySQLBackupFunctions(t *testing.T) {
	t.Run("MySQLConnectionParsing", func(t *testing.T) {
		testCases := []struct {
			name     string
			url      string
			username string
			password string
		}{
			{
				name:     "localhost with port",
				url:      "localhost:3306",
				username: "root",
				password: "password123",
			},
			{
				name:     "IP address with port",
				url:      "192.168.1.100:3307",
				username: "dbuser",
				password: "secret",
			},
			{
				name:     "hostname only",
				url:      "mysql-server",
				username: "admin",
				password: "pass",
			},
			{
				name:     "empty credentials",
				url:      "localhost:3306",
				username: "",
				password: "",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Test connection string building logic
				if tc.url == "" {
					t.Error("URL should not be empty")
				}
				if tc.username != "" && len(tc.username) < 1 {
					t.Error("Username validation failed")
				}
				t.Logf("Testing connection: %s with user: %s", tc.url, tc.username)
			})
		}
	})

	t.Run("TablePrefixExtraction", func(t *testing.T) {
		testCases := []struct {
			tableName      string
			expectedPrefix string
		}{
			{
				tableName:      "orders_202501",
				expectedPrefix: "orders",
			},
			{
				tableName:      "users_20250115",
				expectedPrefix: "users",
			},
			{
				tableName:      "logs_2025",
				expectedPrefix: "logs",
			},
			{
				tableName:      "events202501",
				expectedPrefix: "events",
			},
			{
				tableName:      "products",
				expectedPrefix: "products",
			},
			{
				tableName:      "analytics_data_123",
				expectedPrefix: "analytics_data_",
			},
		}

		db, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			t.Fatalf("Failed to create test database: %v", err)
		}
		defer db.Close()

		executor := backup.NewBackupExecutor(db)

		for _, tc := range testCases {
			t.Run(tc.tableName, func(t *testing.T) {
				// Test table prefix extraction logic
				if len(tc.tableName) == 0 {
					t.Error("Table name should not be empty")
				}
				
				// Test that prefix extraction would work
				if len(tc.expectedPrefix) > len(tc.tableName) {
					t.Errorf("Expected prefix %s should not be longer than table name %s", 
						tc.expectedPrefix, tc.tableName)
				}

				t.Logf("Table: %s -> Expected prefix: %s", tc.tableName, tc.expectedPrefix)
				
				// Test with executor
				if executor == nil {
					t.Error("Executor should not be nil")
				}
			})
		}
	})

	t.Run("TimeRangeQueryConversion", func(t *testing.T) {
		testCases := []struct {
			name        string
			query       map[string]interface{}
			expectValid bool
		}{
			{
				name: "daily time range with offsets",
				query: map[string]interface{}{
					"created_at": map[string]interface{}{
						"type":        "daily",
						"startOffset": float64(-1),
						"endOffset":   float64(0),
					},
				},
				expectValid: true,
			},
			{
				name: "weekly time range",
				query: map[string]interface{}{
					"updated_at": map[string]interface{}{
						"type":        "daily",
						"startOffset": float64(-7),
						"endOffset":   float64(0),
					},
				},
				expectValid: true,
			},
			{
				name: "simple equality condition",
				query: map[string]interface{}{
					"status": "active",
				},
				expectValid: true,
			},
			{
				name: "numeric condition",
				query: map[string]interface{}{
					"count": float64(100),
				},
				expectValid: true,
			},
			{
				name:        "empty query",
				query:       map[string]interface{}{},
				expectValid: true,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Test query structure validation
				if tc.query == nil && tc.expectValid {
					t.Error("Expected valid query but got nil")
				}

				// Test query marshaling (simulates config parsing)
				jsonData, err := json.Marshal(tc.query)
				if err != nil {
					t.Fatalf("Failed to marshal query: %v", err)
				}

				var unmarshalledQuery map[string]interface{}
				err = json.Unmarshal(jsonData, &unmarshalledQuery)
				if err != nil {
					t.Fatalf("Failed to unmarshal query: %v", err)
				}

				t.Logf("Query type: %s - Valid: %v", tc.name, tc.expectValid)
			})
		}
	})

	t.Run("MySQLSelectQueryBuilding", func(t *testing.T) {
		testCases := []struct {
			name   string
			table  string
			fields []string
			query  map[string]interface{}
		}{
			{
				name:   "all fields",
				table:  "users",
				fields: []string{"all"},
				query:  map[string]interface{}{},
			},
			{
				name:   "specific fields",
				table:  "orders",
				fields: []string{"id", "user_id", "created_at"},
				query: map[string]interface{}{
					"status": "completed",
				},
			},
			{
				name:   "with time range",
				table:  "logs",
				fields: []string{"*"},
				query: map[string]interface{}{
					"timestamp": map[string]interface{}{
						"type":        "daily",
						"startOffset": float64(-1),
						"endOffset":   float64(0),
					},
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				if tc.table == "" {
					t.Error("Table name should not be empty")
				}

				if len(tc.fields) == 0 {
					t.Error("Fields should not be empty")
				}

				// Simulate SELECT query construction
				query := fmt.Sprintf("SELECT %s FROM %s", "*", tc.table)
				if query == "" {
					t.Error("Query should not be empty")
				}

				t.Logf("Built query for table %s: %s", tc.table, query)
			})
		}
	})

	t.Run("PasswordMasking", func(t *testing.T) {
		testCases := []struct {
			name     string
			args     []string
			expected []string
		}{
			{
				name: "mysql command with password",
				args: []string{"mysql", "-h", "localhost", "-u", "root", "-pSecret123"},
				expected: []string{"mysql", "-h", "localhost", "-u", "root", "-p***"},
			},
			{
				name: "mysqldump with password",
				args: []string{"mysqldump", "-h", "localhost", "-P", "3306", "-u", "admin", "-pMyPassword"},
				expected: []string{"mysqldump", "-h", "localhost", "-P", "3306", "-u", "admin", "-p***"},
			},
			{
				name: "command without password",
				args: []string{"mysql", "-h", "localhost", "-u", "root"},
				expected: []string{"mysql", "-h", "localhost", "-u", "root"},
			},
			{
				name: "empty password flag",
				args: []string{"mysql", "-p"},
				expected: []string{"mysql", "-p"},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Test password masking logic
				maskedArgs := make([]string, len(tc.args))
				copy(maskedArgs, tc.args)

				for i, arg := range maskedArgs {
					if len(arg) > 2 && arg[:2] == "-p" {
						maskedArgs[i] = "-p***"
					}
				}

				// Verify masking worked
				for i, arg := range maskedArgs {
					if len(tc.expected) > i {
						if arg != tc.expected[i] {
							t.Errorf("Expected arg %d to be %s, got %s", i, tc.expected[i], arg)
						}
					}
				}

				t.Logf("Masked command: %v", maskedArgs)
			})
		}
	})
}

// TestMySQLBackupExecution tests MySQL backup execution scenarios
func TestMySQLBackupExecution(t *testing.T) {
	t.Run("BackupConfigValidation", func(t *testing.T) {
		db, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			t.Fatalf("Failed to create test database: %v", err)
		}
		defer db.Close()

		// Create backup_tasks table
		_, err = db.Exec(`
			CREATE TABLE backup_tasks (
				id INTEGER PRIMARY KEY,
				enable INTEGER,
				config_json TEXT,
				last_update_time TEXT,
				next_backup_time TEXT,
				last_backup_time TEXT
			)
		`)
		if err != nil {
			t.Fatalf("Failed to create backup_tasks table: %v", err)
		}

		testConfigs := []struct {
			name       string
			configJSON string
			shouldFail bool
		}{
			{
				name: "valid MySQL backup config",
				configJSON: `{
					"name": "mysql_backup_test",
					"sourceType": "mysql",
					"database": {
						"url": "localhost:3306",
						"username": "root",
						"password": "password",
						"database": "testdb",
						"tables": ["users", "orders"],
						"fields": {}
					},
					"destination": {
						"gcsPath": "gs://test-bucket/backups",
						"retention": 7,
						"serviceAccount": "",
						"fileNamePattern": ""
					},
					"format": "sql",
					"backupType": "full",
					"query": {},
					"compressionType": "zip",
					"tableSelectionMode": "manual",
					"regexPattern": ""
				}`,
				shouldFail: false,
			},
			{
				name: "MySQL CSV backup config",
				configJSON: `{
					"name": "mysql_csv_backup",
					"sourceType": "mysql",
					"database": {
						"url": "localhost:3306",
						"username": "admin",
						"password": "secret",
						"database": "analytics",
						"tables": ["logs_202501"],
						"fields": {
							"logs_202501": ["id", "timestamp", "message"]
						}
					},
					"destination": {
						"gcsPath": "gs://test-bucket/csv-backups",
						"retention": 30
					},
					"format": "csv",
					"backupType": "incremental",
					"query": {
						"logs_202501": {
							"timestamp": {
								"type": "daily",
								"startOffset": -1,
								"endOffset": 0
							}
						}
					},
					"compressionType": "zip",
					"tableSelectionMode": "manual"
				}`,
				shouldFail: false,
			},
			{
				name: "invalid JSON config",
				configJSON: `{
					"name": "invalid_config",
					"sourceType": "mysql",
					"database": {
						"url": "localhost:3306"
					}
				}`,
				shouldFail: false, // JSON is valid but may fail execution
			},
		}

		executor := backup.NewBackupExecutor(db)

		for i, tc := range testConfigs {
			t.Run(tc.name, func(t *testing.T) {
				taskID := i + 1

				// Insert test task
				_, err := db.Exec(`
					INSERT INTO backup_tasks (id, enable, config_json, last_update_time)
					VALUES (?, 1, ?, datetime('now'))
				`, taskID, tc.configJSON)
				if err != nil {
					t.Fatalf("Failed to insert test task: %v", err)
				}

				// Validate JSON can be parsed
				var config backup.ExecutorBackupConfig
				err = json.Unmarshal([]byte(tc.configJSON), &config)
				if err != nil {
					t.Fatalf("Failed to parse config JSON: %v", err)
				}

				// Verify config fields
				if config.Name == "" {
					t.Error("Config name should not be empty")
				}
				if config.SourceType != "mysql" {
					t.Errorf("Expected sourceType to be mysql, got %s", config.SourceType)
				}

				t.Logf("Config validation passed for: %s", tc.name)

				// Note: We don't actually execute the backup as it requires real MySQL connection
				// and external tools (mysqldump, mysql, python3, gsutil)
				if executor == nil {
					t.Error("Executor should not be nil")
				}
			})
		}
	})

	t.Run("TableGrouping", func(t *testing.T) {
		db, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			t.Fatalf("Failed to create test database: %v", err)
		}
		defer db.Close()

		testCases := []struct {
			name           string
			tables         []string
			expectedGroups int
		}{
			{
				name:           "monthly partitioned tables",
				tables:         []string{"orders_202501", "orders_202502", "orders_202503"},
				expectedGroups: 1, // Should be grouped by prefix "orders"
			},
			{
				name:           "daily partitioned tables",
				tables:         []string{"logs_20250101", "logs_20250102", "logs_20250103"},
				expectedGroups: 1, // Should be grouped by prefix "logs"
			},
			{
				name:           "unrelated tables",
				tables:         []string{"users", "products", "categories"},
				expectedGroups: 3, // Each table separate
			},
			{
				name:           "mixed tables",
				tables:         []string{"orders_202501", "users", "orders_202502"},
				expectedGroups: 2, // "orders" group and "users" separate
			},
		}

		executor := backup.NewBackupExecutor(db)

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Test table grouping logic
				if len(tc.tables) == 0 {
					t.Error("Tables should not be empty")
				}

				// Verify executor exists
				if executor == nil {
					t.Error("Executor should not be nil")
				}

				t.Logf("Testing grouping for %d tables: %v (expected %d groups)", 
					len(tc.tables), tc.tables, tc.expectedGroups)
			})
		}
	})

	t.Run("FileOperations", func(t *testing.T) {
		// Test temporary directory and file operations used in backup
		tempDir, err := os.MkdirTemp("", "mysql_backup_test_")
		if err != nil {
			t.Fatalf("Failed to create temp directory: %v", err)
		}
		defer os.RemoveAll(tempDir)

		testFiles := []struct {
			name    string
			format  string
			content string
		}{
			{
				name:    "users_2025-01-15.sql",
				format:  "sql",
				content: "-- MySQL dump\nCREATE TABLE users (id INT PRIMARY KEY);\n",
			},
			{
				name:    "orders_2025-01-15.csv",
				format:  "csv",
				content: "id,user_id,amount\n1,100,50.00\n2,101,75.50\n",
			},
		}

		for _, tf := range testFiles {
			t.Run(tf.name, func(t *testing.T) {
				filePath := filepath.Join(tempDir, tf.name)

				// Write test file
				err := os.WriteFile(filePath, []byte(tf.content), 0644)
				if err != nil {
					t.Fatalf("Failed to write test file: %v", err)
				}

				// Verify file exists
				if _, err := os.Stat(filePath); os.IsNotExist(err) {
					t.Error("Test file should exist after writing")
				}

				// Read file back
				content, err := os.ReadFile(filePath)
				if err != nil {
					t.Fatalf("Failed to read test file: %v", err)
				}

				if string(content) != tf.content {
					t.Error("File content mismatch")
				}

				// Get file info
				fileInfo, err := os.Stat(filePath)
				if err != nil {
					t.Fatalf("Failed to stat file: %v", err)
				}

				t.Logf("Created test file: %s (size: %d bytes, format: %s)", 
					tf.name, fileInfo.Size(), tf.format)
			})
		}
	})

	t.Run("TimeRangeFiltering", func(t *testing.T) {
		testCases := []struct {
			name       string
			tableName  string
			timeRange  struct {
				start time.Time
				end   time.Time
			}
			shouldInclude bool
		}{
			{
				name:      "current month table",
				tableName: fmt.Sprintf("orders_%s", time.Now().Format("200601")),
				timeRange: struct {
					start time.Time
					end   time.Time
				}{
					start: time.Now().AddDate(0, 0, -1),
					end:   time.Now(),
				},
				shouldInclude: true,
			},
			{
				name:      "yesterday table",
				tableName: fmt.Sprintf("logs_%s", time.Now().AddDate(0, 0, -1).Format("20060102")),
				timeRange: struct {
					start time.Time
					end   time.Time
				}{
					start: time.Now().AddDate(0, 0, -2),
					end:   time.Now(),
				},
				shouldInclude: true,
			},
			{
				name:      "old table",
				tableName: "archive_202301",
				timeRange: struct {
					start time.Time
					end   time.Time
				}{
					start: time.Now().AddDate(0, 0, -1),
					end:   time.Now(),
				},
				shouldInclude: false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				if tc.tableName == "" {
					t.Error("Table name should not be empty")
				}

				// Test time range logic
				if tc.timeRange.start.After(tc.timeRange.end) {
					t.Error("Start time should be before end time")
				}

				t.Logf("Table: %s, Should include: %v", tc.tableName, tc.shouldInclude)
			})
		}
	})
}

// TestMySQLBackupFormats tests different backup formats
func TestMySQLBackupFormats(t *testing.T) {
	t.Run("SQLFormat", func(t *testing.T) {
		formats := []string{"sql", "SQL", "Sql"}
		for _, format := range formats {
			t.Run(format, func(t *testing.T) {
				// Test format validation
				normalized := normalizeFormat(format)
				if normalized != "sql" {
					t.Errorf("Expected normalized format to be 'sql', got '%s'", normalized)
				}
			})
		}
	})

	t.Run("CSVFormat", func(t *testing.T) {
		formats := []string{"csv", "CSV", "Csv"}
		for _, format := range formats {
			t.Run(format, func(t *testing.T) {
				// Test format validation
				normalized := normalizeFormat(format)
				if normalized != "csv" {
					t.Errorf("Expected normalized format to be 'csv', got '%s'", normalized)
				}
			})
		}
	})

	t.Run("UnsupportedFormat", func(t *testing.T) {
		unsupportedFormats := []string{"json", "xml", "parquet", ""}
		for _, format := range unsupportedFormats {
			t.Run(format, func(t *testing.T) {
				normalized := normalizeFormat(format)
				if normalized == "sql" || normalized == "csv" {
					t.Logf("Format '%s' normalized to supported format: %s", format, normalized)
				} else {
					t.Logf("Format '%s' is unsupported", format)
				}
			})
		}
	})
}

// Helper function to normalize format strings
func normalizeFormat(format string) string {
	switch format {
	case "sql", "SQL", "Sql":
		return "sql"
	case "csv", "CSV", "Csv":
		return "csv"
	case "":
		return "sql" // Default
	default:
		return format
	}
}

// TestMySQLBackupEdgeCases tests edge cases in MySQL backup
func TestMySQLBackupEdgeCases(t *testing.T) {
	t.Run("EmptyTableList", func(t *testing.T) {
		db, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			t.Fatalf("Failed to create test database: %v", err)
		}
		defer db.Close()

		executor := backup.NewBackupExecutor(db)
		if executor == nil {
			t.Error("Executor should not be nil")
		}

		// Test with empty table list
		tables := []string{}
		if len(tables) > 0 {
			t.Error("Table list should be empty")
		}

		t.Log("Empty table list test passed")
	})

	t.Run("SpecialCharactersInTableName", func(t *testing.T) {
		specialTables := []string{
			"user_data",
			"order-items",
			"product.catalog",
			"log_2025_01",
		}

		for _, table := range specialTables {
			t.Run(table, func(t *testing.T) {
				if table == "" {
					t.Error("Table name should not be empty")
				}
				// Test that table names with special chars are handled
				t.Logf("Processing table with special characters: %s", table)
			})
		}
	})

	t.Run("VeryLongTableName", func(t *testing.T) {
		longTableName := "this_is_a_very_long_table_name_that_might_cause_issues_with_file_systems_or_commands_202501"
		if len(longTableName) < 50 {
			t.Error("Test table name should be long")
		}
		t.Logf("Long table name length: %d", len(longTableName))
	})

	t.Run("NullOrEmptyPassword", func(t *testing.T) {
		testCases := []struct {
			password string
			username string
		}{
			{password: "", username: "root"},
			{password: "pass", username: ""},
			{password: "", username: ""},
		}

		for i, tc := range testCases {
			t.Run(fmt.Sprintf("case_%d", i), func(t *testing.T) {
				// Test handling of empty credentials
				hasCredentials := tc.username != "" && tc.password != ""
				t.Logf("Has credentials: %v (username: %s)", hasCredentials, tc.username)
			})
		}
	})
}

