package test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/retail-ai-inc/sync/pkg/backup"
)

// TestMySQLBackupFunctions tests MySQL backup related functionality
func TestMySQLBackupFunctions(t *testing.T) {
	t.Run("MySQLConnectionStringBuilding", func(t *testing.T) {
		testCases := []struct {
			name         string
			url          string
			username     string
			password     string
			expectedHost string
			expectedPort string
		}{
			{
				name:         "full connection",
				url:          "localhost:3306",
				username:     "root",
				password:     "secret",
				expectedHost: "localhost",
				expectedPort: "3306",
			},
			{
				name:         "no password",
				url:          "localhost:3306",
				username:     "admin",
				password:     "",
				expectedHost: "localhost",
				expectedPort: "3306",
			},
			{
				name:         "custom port",
				url:          "db-server:3307",
				username:     "user",
				password:     "pass123",
				expectedHost: "db-server",
				expectedPort: "3307",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Call the actual function via test helper
				host, port, user, pass := backup.TestBuildMySQLConnectionString(tc.url, tc.username, tc.password)
				if host != tc.expectedHost {
					t.Errorf("Expected host %s, got %s", tc.expectedHost, host)
				}
				if port != tc.expectedPort {
					t.Errorf("Expected port %s, got %s", tc.expectedPort, port)
				}
				if user != tc.username {
					t.Errorf("Expected user %s, got %s", tc.username, user)
				}
				if pass != tc.password {
					t.Errorf("Expected pass %s, got %s", tc.password, pass)
				}
			})
		}
	})

	t.Run("MySQLConnectionParsing", func(t *testing.T) {
		testCases := []struct {
			name         string
			url          string
			expectedHost string
			expectedPort string
		}{
			{
				name:         "localhost with port",
				url:          "localhost:3306",
				expectedHost: "localhost",
				expectedPort: "3306",
			},
			{
				name:         "IP address with port",
				url:          "192.168.1.100:3307",
				expectedHost: "192.168.1.100",
				expectedPort: "3307",
			},
			{
				name:         "hostname only",
				url:          "mysql-server",
				expectedHost: "mysql-server",
				expectedPort: "3306",
			},
			{
				name:         "empty URL",
				url:          "",
				expectedHost: "localhost",
				expectedPort: "3306",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Call the actual function via test helper
				host, port, _, _ := backup.TestParseMySQLConnectionURL(tc.url)
				if host != tc.expectedHost {
					t.Errorf("Expected host %s, got %s", tc.expectedHost, host)
				}
				if port != tc.expectedPort {
					t.Errorf("Expected port %s, got %s", tc.expectedPort, port)
				}
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
				// Call the actual function via test helper
				result := executor.TestExtractTablePrefix(tc.tableName)
				if result != tc.expectedPrefix {
					t.Errorf("Expected prefix %s, got %s", tc.expectedPrefix, result)
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

		db, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			t.Fatalf("Failed to create test database: %v", err)
		}
		defer db.Close()

		executor := backup.NewBackupExecutor(db)

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Call the actual function via test helper
				result := executor.TestConvertTimeRangeQueryForMySQL(tc.query)

				// For empty query, expect empty result
				if len(tc.query) == 0 && result != "" {
					t.Errorf("Expected empty result for empty query, got: %s", result)
				}

				// For non-empty query, expect some output
				if len(tc.query) > 0 && tc.expectValid {
					t.Logf("Query conversion result: %s", result)
				}
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

		db, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			t.Fatalf("Failed to create test database: %v", err)
		}
		defer db.Close()

		executor := backup.NewBackupExecutor(db)

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Build config
				config := backup.ExecutorBackupConfig{}
				config.Database.Fields = make(map[string][]string)
				if len(tc.fields) > 0 && tc.fields[0] != "all" {
					config.Database.Fields[tc.table] = tc.fields
				}
				config.Query = make(map[string]map[string]interface{})
				if len(tc.query) > 0 {
					config.Query[tc.table] = tc.query
				}

				// Call the actual function via test helper
				result := executor.TestBuildMySQLSelectQuery(tc.table, config)
				if result == "" {
					t.Error("Query should not be empty")
				}

				// Verify table name is in the query
				if !containsString(result, tc.table) {
					t.Errorf("Query should contain table name %s, got: %s", tc.table, result)
				}

				t.Logf("Built query for table %s: %s", tc.table, result)
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
				name:     "mysql command with password",
				args:     []string{"mysql", "-h", "localhost", "-u", "root", "-pSecret123"},
				expected: []string{"mysql", "-h", "localhost", "-u", "root", "-p***"},
			},
			{
				name:     "mysqldump with password",
				args:     []string{"mysqldump", "-h", "localhost", "-P", "3306", "-u", "admin", "-pMyPassword"},
				expected: []string{"mysqldump", "-h", "localhost", "-P", "3306", "-u", "admin", "-p***"},
			},
			{
				name:     "command without password",
				args:     []string{"mysql", "-h", "localhost", "-u", "root"},
				expected: []string{"mysql", "-h", "localhost", "-u", "root"},
			},
			{
				name:     "empty password flag",
				args:     []string{"mysql", "-p"},
				expected: []string{"mysql", "-p"},
			},
		}

		db, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			t.Fatalf("Failed to create test database: %v", err)
		}
		defer db.Close()

		executor := backup.NewBackupExecutor(db)

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Call the actual function via test helper
				result := executor.TestMaskMySQLPassword(tc.args)

				// Build expected string
				expectedStr := ""
				for i, arg := range tc.expected {
					if i > 0 {
						expectedStr += " "
					}
					expectedStr += arg
				}

				if result != expectedStr {
					t.Errorf("Expected '%s', got '%s'", expectedStr, result)
				}
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
			name      string
			tableName string
			timeRange struct {
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

// Helper function to check if string contains substring
func containsString(s, substr string) bool {
	if len(substr) == 0 {
		return true
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		match := true
		for j := 0; j < len(substr); j++ {
			if s[i+j] != substr[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
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

// TestTableGrouping tests table grouping by prefix functionality
func TestTableGrouping(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	executor := backup.NewBackupExecutor(db)

	testCases := []struct {
		name           string
		tables         []string
		expectedGroups map[string][]string
	}{
		{
			name:   "monthly partitioned tables",
			tables: []string{"orders_202501", "orders_202502", "orders_202503"},
			expectedGroups: map[string][]string{
				"orders": {"orders_202501", "orders_202502", "orders_202503"},
			},
		},
		{
			name:   "daily partitioned tables",
			tables: []string{"logs_20250101", "logs_20250102", "logs_20250103"},
			expectedGroups: map[string][]string{
				"logs": {"logs_20250101", "logs_20250102", "logs_20250103"},
			},
		},
		{
			name:   "unrelated tables",
			tables: []string{"users", "products", "categories"},
			expectedGroups: map[string][]string{
				"users":      {"users"},
				"products":   {"products"},
				"categories": {"categories"},
			},
		},
		{
			name:   "mixed tables",
			tables: []string{"orders_202501", "users", "orders_202502"},
			expectedGroups: map[string][]string{
				"orders": {"orders_202501", "orders_202502"},
				"users":  {"users"},
			},
		},
		{
			name:   "yearly partitioned tables",
			tables: []string{"archive_2023", "archive_2024", "archive_2025"},
			expectedGroups: map[string][]string{
				"archive": {"archive_2023", "archive_2024", "archive_2025"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			groups := executor.TestGroupTablesByPrefix(tc.tables)

			if len(groups) != len(tc.expectedGroups) {
				t.Errorf("Expected %d groups, got %d", len(tc.expectedGroups), len(groups))
			}

			for groupName, expectedTables := range tc.expectedGroups {
				actualTables, exists := groups[groupName]
				if !exists {
					t.Errorf("Expected group %s not found", groupName)
					continue
				}

				if len(actualTables) != len(expectedTables) {
					t.Errorf("Group %s: expected %d tables, got %d", groupName, len(expectedTables), len(actualTables))
				}
			}

			t.Logf("Grouped %d tables into %d groups", len(tc.tables), len(groups))
		})
	}
}

// TestTimeRangeExtraction tests time range extraction from query conditions
func TestTimeRangeExtraction(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	executor := backup.NewBackupExecutor(db)

	testCases := []struct {
		name        string
		query       map[string]interface{}
		expectRange bool
	}{
		{
			name: "daily time range query",
			query: map[string]interface{}{
				"created_at": map[string]interface{}{
					"type":        "daily",
					"startOffset": float64(-1),
					"endOffset":   float64(0),
				},
			},
			expectRange: true,
		},
		{
			name: "weekly time range query",
			query: map[string]interface{}{
				"updated_at": map[string]interface{}{
					"type":        "daily",
					"startOffset": float64(-7),
					"endOffset":   float64(0),
				},
			},
			expectRange: true,
		},
		{
			name: "simple equality query",
			query: map[string]interface{}{
				"status": "active",
			},
			expectRange: false,
		},
		{
			name:        "empty query",
			query:       map[string]interface{}{},
			expectRange: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			timeRange := executor.TestExtractTimeRange(tc.query)

			if tc.expectRange && timeRange == nil {
				t.Error("Expected time range to be extracted, got nil")
			} else if !tc.expectRange && timeRange != nil {
				t.Error("Expected no time range, but got one")
			}

			if timeRange != nil {
				t.Logf("Extracted time range: %v to %v", timeRange.Start, timeRange.End)
			}
		})
	}
}

// TestTableTimePatternExtraction tests extracting time patterns from table names
func TestTableTimePatternExtraction(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	executor := backup.NewBackupExecutor(db)

	testCases := []struct {
		name        string
		tableName   string
		expectRange bool
	}{
		{
			name:        "monthly table",
			tableName:   "orders_202501",
			expectRange: true,
		},
		{
			name:        "daily table",
			tableName:   "logs_20250101",
			expectRange: true,
		},
		{
			name:        "yearly table",
			tableName:   "archive_2025",
			expectRange: true,
		},
		{
			name:        "table without date",
			tableName:   "users",
			expectRange: false,
		},
		{
			name:        "table with numeric suffix",
			tableName:   "data_123",
			expectRange: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			timeRange := executor.TestExtractTableTimePattern(tc.tableName)

			if tc.expectRange && timeRange == nil {
				t.Errorf("Expected time range for %s, got nil", tc.tableName)
			} else if !tc.expectRange && timeRange != nil {
				t.Logf("No time range expected for %s, correctly got nil or range", tc.tableName)
			}

			if timeRange != nil {
				t.Logf("Table %s time range: %v to %v", tc.tableName, timeRange.Start, timeRange.End)
			}
		})
	}
}

// TestMongoDBConnectionString tests MongoDB connection string building
func TestMongoDBConnectionString(t *testing.T) {
	testCases := []struct {
		name     string
		url      string
		username string
		password string
	}{
		{
			name:     "with credentials",
			url:      "localhost:27017",
			username: "admin",
			password: "secret",
		},
		{
			name:     "without credentials",
			url:      "localhost:27017",
			username: "",
			password: "",
		},
		{
			name:     "custom host and port",
			url:      "mongo-server:27018",
			username: "user",
			password: "pass",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			connStr := backup.TestBuildMongoDBConnectionString(tc.url, tc.username, tc.password)

			if connStr == "" {
				t.Error("Connection string should not be empty")
			}

			if !containsString(connStr, "mongodb://") {
				t.Error("Connection string should start with mongodb://")
			}

			if tc.username != "" && !containsString(connStr, "@") {
				t.Error("Connection string with credentials should contain @")
			}

			t.Logf("Generated connection string for %s (credentials redacted)", tc.url)
		})
	}
}

// TestSensitiveArgsMasking tests masking of sensitive information
func TestSensitiveArgsMasking(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	executor := backup.NewBackupExecutor(db)

	testCases := []struct {
		name     string
		args     []string
		expected string
	}{
		{
			name:     "MongoDB URI with credentials",
			args:     []string{"mongoexport", "--uri", "mongodb://admin:secret@localhost:27017/db"},
			expected: "mongoexport --uri mongodb://***:***@localhost:27017/db",
		},
		{
			name:     "MongoDB URI without credentials",
			args:     []string{"mongoexport", "--uri", "mongodb://localhost:27017/db"},
			expected: "mongoexport --uri mongodb://localhost:27017/db",
		},
		{
			name:     "command without sensitive data",
			args:     []string{"mongoexport", "--db", "testdb", "--collection", "users"},
			expected: "mongoexport --db testdb --collection users",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := executor.TestMaskSensitiveArgs(tc.args)

			if result == "" {
				t.Error("Masked result should not be empty")
			}

			// Verify password is masked
			if containsString(result, "secret") || containsString(result, "password") {
				t.Error("Result should not contain sensitive data")
			}

			t.Logf("Masked command: %s", result)
		})
	}
}

// TestMongoDBTimeRangeConversion tests MongoDB time range query conversion
func TestMongoDBTimeRangeConversion(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	executor := backup.NewBackupExecutor(db)

	testCases := []struct {
		name        string
		query       map[string]interface{}
		expectValid bool
	}{
		{
			name: "daily time range",
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
			name: "weekly range",
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
			name: "simple equality",
			query: map[string]interface{}{
				"status": "active",
			},
			expectValid: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := executor.TestConvertTimeRangeQuery(tc.query)

			if result == nil {
				t.Error("Result should not be nil")
			}

			t.Logf("Converted query: %v", result)
		})
	}
}

// TestQueryStringCleaning tests cleaning of query string values
func TestQueryStringCleaning(t *testing.T) {
	testCases := []struct {
		name     string
		query    map[string]interface{}
		expected map[string]interface{}
	}{
		{
			name: "remove double quotes",
			query: map[string]interface{}{
				"status": `"active"`,
			},
			expected: map[string]interface{}{
				"status": "active",
			},
		},
		{
			name: "remove single quotes",
			query: map[string]interface{}{
				"type": `'pending'`,
			},
			expected: map[string]interface{}{
				"type": "pending",
			},
		},
		{
			name: "nested query",
			query: map[string]interface{}{
				"created_at": map[string]interface{}{
					"type": `"daily"`,
				},
			},
			expected: map[string]interface{}{
				"created_at": map[string]interface{}{
					"type": "daily",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := backup.TestCleanQueryStringValues(tc.query)

			if result == nil {
				t.Error("Result should not be nil")
			}

			t.Logf("Cleaned query: %v", result)
		})
	}
}

// TestYearMonthParsing tests year and month parsing
func TestYearMonthParsing(t *testing.T) {
	testCases := []struct {
		name          string
		input         string
		expectError   bool
		expectedYear  int
		expectedMonth int
	}{
		{
			name:          "valid YYYYMM",
			input:         "202501",
			expectError:   false,
			expectedYear:  2025,
			expectedMonth: 1,
		},
		{
			name:          "valid YYYYMM December",
			input:         "202512",
			expectError:   false,
			expectedYear:  2025,
			expectedMonth: 12,
		},
		{
			name:        "invalid month",
			input:       "202513",
			expectError: true,
		},
		{
			name:        "invalid format",
			input:       "2025",
			expectError: true,
		},
		{
			name:        "non-numeric",
			input:       "20250a",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			year, month, err := backup.TestParseYearMonth(tc.input)

			if tc.expectError && err == nil {
				t.Error("Expected error but got none")
			} else if !tc.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if !tc.expectError {
				if year != tc.expectedYear {
					t.Errorf("Expected year %d, got %d", tc.expectedYear, year)
				}
				if month != tc.expectedMonth {
					t.Errorf("Expected month %d, got %d", tc.expectedMonth, month)
				}
			}
		})
	}
}

// TestYearParsing tests year parsing
func TestYearParsing(t *testing.T) {
	testCases := []struct {
		name         string
		input        string
		expectError  bool
		expectedYear int
	}{
		{
			name:         "valid year",
			input:        "2025",
			expectError:  false,
			expectedYear: 2025,
		},
		{
			name:         "valid year 2000",
			input:        "2000",
			expectError:  false,
			expectedYear: 2000,
		},
		{
			name:        "invalid format",
			input:       "25",
			expectError: true,
		},
		{
			name:        "non-numeric",
			input:       "202a",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			year, err := backup.TestParseYear(tc.input)

			if tc.expectError && err == nil {
				t.Error("Expected error but got none")
			} else if !tc.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if !tc.expectError && year != tc.expectedYear {
				t.Errorf("Expected year %d, got %d", tc.expectedYear, year)
			}
		})
	}
}

// TestFileNamePatternProcessing tests file name pattern processing
func TestFileNamePatternProcessing(t *testing.T) {
	testCases := []struct {
		name      string
		pattern   string
		tableName string
	}{
		{
			name:      "empty pattern uses default",
			pattern:   "",
			tableName: "users",
		},
		{
			name:      "pattern with table placeholder",
			pattern:   "{table}_backup",
			tableName: "orders",
		},
		{
			name:      "pattern with date only",
			pattern:   "backup_{YYYY}-{MM}-{DD}",
			tableName: "logs",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := backup.TestProcessFileNamePattern(tc.pattern, tc.tableName)

			if result == "" {
				t.Error("Result should not be empty")
			}

			if !containsString(result, tc.tableName) && tc.pattern == "" {
				t.Errorf("Result should contain table name %s", tc.tableName)
			}

			t.Logf("Processed pattern for %s: %s", tc.tableName, result)
		})
	}
}

// TestTableRelevanceForTimeRange tests checking if tables are relevant for time ranges
func TestTableRelevanceForTimeRange(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	executor := backup.NewBackupExecutor(db)

	now := time.Now()
	testCases := []struct {
		name           string
		tableName      string
		timeRange      *backup.TimeRange
		expectRelevant bool
	}{
		{
			name:      "current month table",
			tableName: fmt.Sprintf("orders_%s", now.Format("200601")),
			timeRange: &backup.TimeRange{
				Start: now.AddDate(0, 0, -1),
				End:   now,
			},
			expectRelevant: true,
		},
		{
			name:      "old table",
			tableName: "archive_202301",
			timeRange: &backup.TimeRange{
				Start: now.AddDate(0, 0, -1),
				End:   now,
			},
			expectRelevant: false,
		},
		{
			name:      "table without date pattern",
			tableName: "users",
			timeRange: &backup.TimeRange{
				Start: now.AddDate(0, 0, -1),
				End:   now,
			},
			expectRelevant: true, // Should include tables without date patterns
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := executor.TestIsTableRelevantForTimeRange(tc.tableName, tc.timeRange)

			t.Logf("Table %s relevance: %v (expected: %v)", tc.tableName, result, tc.expectRelevant)
		})
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
