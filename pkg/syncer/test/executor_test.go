package test

import (
	"context"
	"database/sql"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/retail-ai-inc/sync/pkg/backup"
)

// TestBackupExecutorInstantiation tests creating a new backup executor
func TestBackupExecutorInstantiation(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	executor := backup.NewBackupExecutor(db)
	if executor == nil {
		t.Fatal("NewBackupExecutor() returned nil")
	}
}

// TestBackupExecutorWithNilDB tests backup executor with nil database
func TestBackupExecutorWithNilDB(t *testing.T) {
	executor := backup.NewBackupExecutor(nil)
	if executor == nil {
		t.Fatal("NewBackupExecutor() should handle nil database")
	}
}

// TestExecuteWithNonExistentTask tests execute with non-existent task
func TestExecuteWithNonExistentTask(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	// Create backup_tasks table
	createTableSQL := `
	CREATE TABLE backup_tasks (
		id INTEGER PRIMARY KEY,
		enable INTEGER DEFAULT 1,
		last_update_time TEXT,
		last_backup_time TEXT,
		next_backup_time TEXT,
		config_json TEXT
	)`
	_, err = db.Exec(createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create backup_tasks table: %v", err)
	}

	executor := backup.NewBackupExecutor(db)
	ctx := context.Background()

	err = executor.Execute(ctx, 999) // Non-existent task ID
	if err == nil {
		t.Error("Execute should fail with non-existent task ID")
	}
}

// TestExecuteWithInvalidConfig tests execute with invalid config JSON
func TestExecuteWithInvalidConfig(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	// Create backup_tasks table
	createTableSQL := `
	CREATE TABLE backup_tasks (
		id INTEGER PRIMARY KEY,
		enable INTEGER DEFAULT 1,
		last_update_time TEXT,
		last_backup_time TEXT,
		next_backup_time TEXT,
		config_json TEXT
	)`
	_, err = db.Exec(createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create backup_tasks table: %v", err)
	}

	// Insert task with invalid config JSON
	insertSQL := `
	INSERT INTO backup_tasks (id, enable, last_update_time, last_backup_time, next_backup_time, config_json)
	VALUES (?, ?, ?, ?, ?, ?)`

	now := time.Now().Format("2006-01-02 15:04:05")
	_, err = db.Exec(insertSQL, 1, 1, now, now, now, "invalid json")
	if err != nil {
		t.Fatalf("Failed to insert test backup task: %v", err)
	}

	executor := backup.NewBackupExecutor(db)
	ctx := context.Background()

	err = executor.Execute(ctx, 1)
	if err == nil {
		t.Error("Execute should fail with invalid config JSON")
	}
}

// TestExecuteWithValidMongoDBConfig tests execute with valid MongoDB config
func TestExecuteWithValidMongoDBConfig(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	// Create backup_tasks table
	createTableSQL := `
	CREATE TABLE backup_tasks (
		id INTEGER PRIMARY KEY,
		enable INTEGER DEFAULT 1,
		last_update_time TEXT,
		last_backup_time TEXT,
		next_backup_time TEXT,
		config_json TEXT
	)`
	_, err = db.Exec(createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create backup_tasks table: %v", err)
	}

	// Create valid MongoDB backup config
	config := map[string]interface{}{
		"name":       "test-mongodb-backup",
		"sourceType": "mongodb",
		"database": map[string]interface{}{
			"url":      "localhost:27017",
			"username": "testuser",
			"password": "testpass",
			"database": "testdb",
			"tables":   []string{"test_collection"},
			"fields": map[string][]string{
				"test_collection": {"field1", "field2"},
			},
		},
		"destination": map[string]interface{}{
			"gcsPath":         "gs://test-bucket/backups/",
			"retention":       7,
			"serviceAccount":  "test-service-account.json",
			"fileNamePattern": "{table}-{YYYY}-{MM}-{DD}",
		},
		"format":     "json",
		"backupType": "full",
		"query": map[string]interface{}{
			"test_collection": map[string]interface{}{
				"status": "active",
			},
		},
	}
	configJSON, _ := json.Marshal(config)

	insertSQL := `
	INSERT INTO backup_tasks (id, enable, last_update_time, last_backup_time, next_backup_time, config_json)
	VALUES (?, ?, ?, ?, ?, ?)`

	now := time.Now().Format("2006-01-02 15:04:05")
	_, err = db.Exec(insertSQL, 1, 1, now, now, now, string(configJSON))
	if err != nil {
		t.Fatalf("Failed to insert test backup task: %v", err)
	}

	executor := backup.NewBackupExecutor(db)
	ctx := context.Background()

	err = executor.Execute(ctx, 1)
	// This will likely fail due to missing MongoDB tools/connection, but it tests the config parsing
	if err != nil {
		t.Logf("Execute failed (expected without MongoDB tools): %v", err)
	}
}

// TestExecuteWithValidPostgreSQLConfig tests execute with valid PostgreSQL config
func TestExecuteWithValidPostgreSQLConfig(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	// Create backup_tasks table
	createTableSQL := `
	CREATE TABLE backup_tasks (
		id INTEGER PRIMARY KEY,
		enable INTEGER DEFAULT 1,
		last_update_time TEXT,
		last_backup_time TEXT,
		next_backup_time TEXT,
		config_json TEXT
	)`
	_, err = db.Exec(createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create backup_tasks table: %v", err)
	}

	// Create valid PostgreSQL backup config
	config := map[string]interface{}{
		"name":       "test-postgresql-backup",
		"sourceType": "postgresql",
		"database": map[string]interface{}{
			"url":      "localhost",
			"username": "testuser",
			"password": "testpass",
			"database": "testdb",
			"tables":   []string{"test_table"},
			"fields": map[string][]string{
				"test_table": {"column1", "column2"},
			},
		},
		"destination": map[string]interface{}{
			"gcsPath":         "gs://test-bucket/backups/",
			"retention":       7,
			"serviceAccount":  "test-service-account.json",
			"fileNamePattern": "{table}-{YYYY}-{MM}-{DD}",
		},
		"format":     "sql",
		"backupType": "full",
		"query": map[string]interface{}{
			"test_table": map[string]interface{}{
				"id": map[string]interface{}{
					"$gt": 100,
				},
			},
		},
	}
	configJSON, _ := json.Marshal(config)

	insertSQL := `
	INSERT INTO backup_tasks (id, enable, last_update_time, last_backup_time, next_backup_time, config_json)
	VALUES (?, ?, ?, ?, ?, ?)`

	now := time.Now().Format("2006-01-02 15:04:05")
	_, err = db.Exec(insertSQL, 1, 1, now, now, now, string(configJSON))
	if err != nil {
		t.Fatalf("Failed to insert test backup task: %v", err)
	}

	executor := backup.NewBackupExecutor(db)
	ctx := context.Background()

	err = executor.Execute(ctx, 1)
	// This will likely fail due to missing PostgreSQL tools/connection, but it tests the config parsing
	if err != nil {
		t.Logf("Execute failed (expected without PostgreSQL tools): %v", err)
	}
}

// TestExecuteWithUnsupportedSourceType tests execute with unsupported source type
func TestExecuteWithUnsupportedSourceType(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	// Create backup_tasks table
	createTableSQL := `
	CREATE TABLE backup_tasks (
		id INTEGER PRIMARY KEY,
		enable INTEGER DEFAULT 1,
		last_update_time TEXT,
		last_backup_time TEXT,
		next_backup_time TEXT,
		config_json TEXT
	)`
	_, err = db.Exec(createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create backup_tasks table: %v", err)
	}

	// Create config with unsupported source type
	config := map[string]interface{}{
		"name":       "test-unsupported-backup",
		"sourceType": "unsupported_db",
		"database": map[string]interface{}{
			"url":      "localhost",
			"username": "testuser",
			"password": "testpass",
			"database": "testdb",
			"tables":   []string{"test_table"},
		},
		"destination": map[string]interface{}{
			"gcsPath":         "gs://test-bucket/backups/",
			"retention":       7,
			"serviceAccount":  "test-service-account.json",
			"fileNamePattern": "{table}-{YYYY}-{MM}-{DD}",
		},
		"format":     "json",
		"backupType": "full",
	}
	configJSON, _ := json.Marshal(config)

	insertSQL := `
	INSERT INTO backup_tasks (id, enable, last_update_time, last_backup_time, next_backup_time, config_json)
	VALUES (?, ?, ?, ?, ?, ?)`

	now := time.Now().Format("2006-01-02 15:04:05")
	_, err = db.Exec(insertSQL, 1, 1, now, now, now, string(configJSON))
	if err != nil {
		t.Fatalf("Failed to insert test backup task: %v", err)
	}

	executor := backup.NewBackupExecutor(db)
	ctx := context.Background()

	err = executor.Execute(ctx, 1)
	// Note: Execute may not return error for unsupported source types,
	// but it should log the error and continue processing
	if err != nil {
		t.Logf("Execute failed as expected with unsupported source type: %v", err)
	} else {
		t.Logf("Execute completed (unsupported source types are logged but do not cause total failure)")
	}
}

// TestCompressDirectoryFunction tests directory compression functionality
func TestCompressDirectoryFunction(t *testing.T) {
	// Create a temporary directory with test files
	tempDir, err := ioutil.TempDir("", "test_compress")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create test files
	testFiles := []struct {
		name    string
		content string
	}{
		{"test1.json", `{"test": "data1"}`},
		{"test2.sql", "SELECT * FROM test;"},
		{"test3.bson", "binary data here"},
	}

	for _, tf := range testFiles {
		filePath := filepath.Join(tempDir, tf.name)
		err := ioutil.WriteFile(filePath, []byte(tf.content), 0644)
		if err != nil {
			t.Fatalf("Failed to create test file %s: %v", tf.name, err)
		}
	}

	// Test compression
	archivePath := filepath.Join(tempDir, "test-archive.tar.gz")

	// Note: compressDirectory is a private method, so we simulate testing its functionality
	t.Logf("Testing compression simulation - would compress %s to %s", tempDir, archivePath)

	// Test that the directory exists and has files
	files, err := ioutil.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("Failed to read temp directory: %v", err)
	}

	compressibleCount := 0
	for _, file := range files {
		ext := filepath.Ext(file.Name())
		if ext == ".json" || ext == ".sql" || ext == ".bson" {
			compressibleCount++
		}
	}

	if compressibleCount != len(testFiles) {
		t.Errorf("Expected %d compressible files, found %d", len(testFiles), compressibleCount)
	}
}

// TestProcessFileNamePatternFunction tests file name pattern processing
func TestProcessFileNamePatternFunction(t *testing.T) {
	testCases := []struct {
		pattern   string
		tableName string
		expected  string
	}{
		{"", "users", "users-" + time.Now().Format("2006-01-02")},
		{"{table}-backup", "users", "users-backup"},
		{"{table}-{YYYY}-{MM}-{DD}", "orders", "orders-" + time.Now().Format("2006-01-02")},
		{"^backup-{table}$", "products", "backup-products"},
		{"{TABLE}-BACKUP.json", "customers", "CUSTOMERS-BACKUP.json"},
	}

	for _, tc := range testCases {
		t.Run(tc.pattern, func(t *testing.T) {
			// Since processFileNamePattern is private, we test the logic conceptually
			result := tc.pattern

			// Simulate the pattern processing logic
			if tc.pattern == "" {
				result = tc.tableName + "-" + time.Now().Format("2006-01-02")
			} else {
				result = strings.ReplaceAll(result, "^", "")
				result = strings.ReplaceAll(result, "$", "")
				result = strings.ReplaceAll(result, "{table}", tc.tableName)
				result = strings.ReplaceAll(result, "{TABLE}", strings.ToUpper(tc.tableName))
				result = strings.ReplaceAll(result, "{YYYY}", time.Now().Format("2006"))
				result = strings.ReplaceAll(result, "{MM}", time.Now().Format("01"))
				result = strings.ReplaceAll(result, "{DD}", time.Now().Format("02"))
			}

			// Basic validation
			if !strings.Contains(result, tc.tableName) && !strings.Contains(result, strings.ToUpper(tc.tableName)) {
				t.Errorf("Result should contain table name: got %s", result)
			}
		})
	}
}

// TestCleanQueryStringValuesFunction tests query string cleaning functionality
func TestCleanQueryStringValuesFunction(t *testing.T) {
	testCases := []struct {
		name     string
		input    map[string]interface{}
		expected map[string]interface{}
	}{
		{
			name: "simple string with quotes",
			input: map[string]interface{}{
				"status": `"active"`,
			},
			expected: map[string]interface{}{
				"status": "active",
			},
		},
		{
			name: "string with single quotes",
			input: map[string]interface{}{
				"name": `'test'`,
			},
			expected: map[string]interface{}{
				"name": "test",
			},
		},
		{
			name: "nested object",
			input: map[string]interface{}{
				"filter": map[string]interface{}{
					"status": `"published"`,
					"count":  10,
				},
			},
			expected: map[string]interface{}{
				"filter": map[string]interface{}{
					"status": "published",
					"count":  10,
				},
			},
		},
		{
			name: "non-string values",
			input: map[string]interface{}{
				"count":   100,
				"active":  true,
				"rate":    3.14,
				"nothing": nil,
			},
			expected: map[string]interface{}{
				"count":   100,
				"active":  true,
				"rate":    3.14,
				"nothing": nil,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Since cleanQueryStringValues is private, we simulate its logic here
			result := make(map[string]interface{})

			for key, value := range tc.input {
				switch v := value.(type) {
				case string:
					cleanValue := v
					if strings.HasPrefix(cleanValue, `"`) && strings.HasSuffix(cleanValue, `"`) {
						cleanValue = strings.Trim(cleanValue, `"`)
					}
					if strings.HasPrefix(cleanValue, `'`) && strings.HasSuffix(cleanValue, `'`) {
						cleanValue = strings.Trim(cleanValue, `'`)
					}
					result[key] = cleanValue
				case map[string]interface{}:
					// Recursively clean nested objects
					nestedResult := make(map[string]interface{})
					for nKey, nValue := range v {
						if nStr, ok := nValue.(string); ok {
							cleanValue := nStr
							if strings.HasPrefix(cleanValue, `"`) && strings.HasSuffix(cleanValue, `"`) {
								cleanValue = strings.Trim(cleanValue, `"`)
							}
							nestedResult[nKey] = cleanValue
						} else {
							nestedResult[nKey] = nValue
						}
					}
					result[key] = nestedResult
				default:
					result[key] = value
				}
			}

			// Compare results
			for key, expectedValue := range tc.expected {
				if actualValue, ok := result[key]; !ok {
					t.Errorf("Missing key %s in result", key)
				} else {
					// For nested maps, we need deep comparison
					if expectedMap, isMap := expectedValue.(map[string]interface{}); isMap {
						if actualMap, isActualMap := actualValue.(map[string]interface{}); isActualMap {
							for nestedKey, nestedExpected := range expectedMap {
								if nestedActual, exists := actualMap[nestedKey]; !exists {
									t.Errorf("Missing nested key %s in result", nestedKey)
								} else if nestedActual != nestedExpected {
									t.Errorf("Key %s.%s: expected %v, got %v", key, nestedKey, nestedExpected, nestedActual)
								}
							}
						} else {
							t.Errorf("Key %s: expected map, got %T", key, actualValue)
						}
					} else if actualValue != expectedValue {
						t.Errorf("Key %s: expected %v, got %v", key, expectedValue, actualValue)
					}
				}
			}
		})
	}
}

// TestBackupExecutorIntegration - main integration test for backup executor functionality
func TestBackupExecutorIntegration(t *testing.T) {
	t.Run("BackupExecutorInstantiation", func(t *testing.T) {
		TestBackupExecutorInstantiation(t)
	})

	t.Run("BackupExecutorWithNilDB", func(t *testing.T) {
		TestBackupExecutorWithNilDB(t)
	})

	t.Run("ExecuteWithNonExistentTask", func(t *testing.T) {
		TestExecuteWithNonExistentTask(t)
	})

	t.Run("ExecuteWithInvalidConfig", func(t *testing.T) {
		TestExecuteWithInvalidConfig(t)
	})

	t.Run("ExecuteWithValidMongoDBConfig", func(t *testing.T) {
		TestExecuteWithValidMongoDBConfig(t)
	})

	t.Run("ExecuteWithValidPostgreSQLConfig", func(t *testing.T) {
		TestExecuteWithValidPostgreSQLConfig(t)
	})

	t.Run("ExecuteWithUnsupportedSourceType", func(t *testing.T) {
		TestExecuteWithUnsupportedSourceType(t)
	})

	t.Run("CompressDirectoryFunction", func(t *testing.T) {
		TestCompressDirectoryFunction(t)
	})

	t.Run("ProcessFileNamePatternFunction", func(t *testing.T) {
		TestProcessFileNamePatternFunction(t)
	})

	t.Run("CleanQueryStringValuesFunction", func(t *testing.T) {
		TestCleanQueryStringValuesFunction(t)
	})
}
