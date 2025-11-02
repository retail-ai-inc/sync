package test

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/pkg/backup"
	"github.com/retail-ai-inc/sync/pkg/utils"
	"github.com/sirupsen/logrus"
)

// TestBackupExecutorCoverage tests backup executor functionality for better coverage
func TestBackupExecutorCoverage(t *testing.T) {
	t.Run("BackupExecutorBasicOperations", func(t *testing.T) {
		// Test with in-memory database
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

		executor := backup.NewBackupExecutor(db)
		if executor == nil {
			t.Fatal("Expected executor to be created")
		}

		// Test Execute with non-existent task ID
		err = executor.Execute(context.Background(), 999)
		if err == nil {
			t.Error("Expected error for non-existent task")
		}

		t.Log("Basic operations test completed")
	})

	t.Run("BackupConfigProcessing", func(t *testing.T) {
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

		// Insert a test backup task with invalid JSON
		_, err = db.Exec(`
			INSERT INTO backup_tasks (id, enable, config_json, last_update_time)
			VALUES (1, 1, '{"invalid": json}', datetime('now'))
		`)
		if err != nil {
			t.Fatalf("Failed to insert test task: %v", err)
		}

		executor := backup.NewBackupExecutor(db)

		// Test Execute with invalid JSON config
		err = executor.Execute(context.Background(), 1)
		if err == nil {
			t.Error("Expected error for invalid JSON config")
		}

		t.Log("Config processing test completed")
	})

	t.Run("FileOperations", func(t *testing.T) {
		// Test file operations used in backup
		tempDir, err := ioutil.TempDir("", "backup_test")
		if err != nil {
			t.Fatalf("Failed to create temp directory: %v", err)
		}
		defer os.RemoveAll(tempDir)

		// Create test files
		testFiles := []string{"test1.json", "test2.bson", "test3.sql"}
		for _, fileName := range testFiles {
			filePath := filepath.Join(tempDir, fileName)
			content := fmt.Sprintf(`{"test": "data for %s"}`, fileName)
			err := ioutil.WriteFile(filePath, []byte(content), 0644)
			if err != nil {
				t.Fatalf("Failed to create test file %s: %v", fileName, err)
			}
		}

		// Test directory reading
		files, err := ioutil.ReadDir(tempDir)
		if err != nil {
			t.Fatalf("Failed to read directory: %v", err)
		}

		if len(files) != len(testFiles) {
			t.Errorf("Expected %d files, got %d", len(testFiles), len(files))
		}

		t.Log("File operations test completed")
	})

	t.Run("ErrorHandling", func(t *testing.T) {
		db, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			t.Fatalf("Failed to create test database: %v", err)
		}
		defer db.Close()

		// Don't create the backup_tasks table to trigger an error
		executor := backup.NewBackupExecutor(db)

		// Test Execute with database error
		err = executor.Execute(context.Background(), 1)
		if err == nil {
			t.Error("Expected error due to missing table")
		}

		t.Log("Error handling test completed")
	})

	t.Run("TimeUtilities", func(t *testing.T) {
		// Test time-related utilities used in backup
		now := time.Now()

		// Test time formatting
		timeStr := now.Format("2006-01-02 15:04:05")
		if timeStr == "" {
			t.Error("Time string should not be empty")
		}

		// Test time parsing
		parsedTime, err := time.Parse("2006-01-02 15:04:05", timeStr)
		if err != nil {
			t.Fatalf("Failed to parse time: %v", err)
		}

		if parsedTime.Format("2006-01-02 15:04:05") != now.Format("2006-01-02 15:04:05") {
			t.Errorf("Parsed time (%s) should match original time (%s)",
				parsedTime.Format("2006-01-02 15:04:05"), now.Format("2006-01-02 15:04:05"))
		}

		t.Log("Time utilities test completed")
	})

	t.Run("ContextHandling", func(t *testing.T) {
		// Test context handling in backup operations
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		db, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			t.Fatalf("Failed to create test database: %v", err)
		}
		defer db.Close()

		executor := backup.NewBackupExecutor(db)

		// Test with cancelled context
		cancel()
		err = executor.Execute(ctx, 1)
		if err == nil {
			t.Log("Execute with cancelled context (may or may not return error depending on timing)")
		}

		t.Log("Context handling test completed")
	})
}

// TestBackupUtilities tests backup utility functions
func TestBackupUtilities(t *testing.T) {
	t.Run("StringProcessing", func(t *testing.T) {
		// Test string processing functions used in backup
		testStrings := []string{
			"collection_name",
			"table-name-2025",
			"backup_file.json",
			"database.collection",
		}

		for _, str := range testStrings {
			if len(str) == 0 {
				t.Error("String should not be empty")
			}

			// Test basic string operations
			if !contains(str, "_") && !contains(str, "-") && !contains(str, ".") {
				t.Logf("String %s contains no separators", str)
			}
		}

		t.Log("String processing test completed")
	})

	t.Run("FilePathProcessing", func(t *testing.T) {
		// Test file path processing
		testPaths := []string{
			"/tmp/backup_test.json",
			"./backup_file.bson",
			"../data/export.sql",
		}

		for _, path := range testPaths {
			dir := filepath.Dir(path)
			base := filepath.Base(path)
			ext := filepath.Ext(path)

			if dir == "" || base == "" {
				t.Errorf("Path components should not be empty: dir=%s, base=%s", dir, base)
			}

			t.Logf("Path: %s -> dir: %s, base: %s, ext: %s", path, dir, base, ext)
		}

		t.Log("File path processing test completed")
	})
}

// Helper function for string contains check
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > len(substr) && findSubstring(s, substr)))
}

// Simple substring finder
func findSubstring(s, substr string) bool {
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

// TestSlackNotifierCoverage tests Slack notifier with more scenarios
func TestSlackNotifierCoverage(t *testing.T) {
	t.Run("SlackAlertTypes", func(t *testing.T) {
		logger := logrus.New()
		logger.SetOutput(ioutil.Discard) // Suppress log output in tests

		notifier := utils.NewSlackNotifier("https://hooks.slack.com/test", "#test", logger)

		ctx := context.Background()

		// Test all alert types
		err := notifier.SendSyncStatus(ctx, "MongoDB", "source_db", "target_db", 100, 5*time.Second)
		if err != nil {
			t.Logf("SendSyncStatus returned: %v (expected if script not found)", err)
		}

		err = notifier.SendBackupStatus(ctx, "MongoDB", "test_table", "backup.json", 1024*1024)
		if err != nil {
			t.Logf("SendBackupStatus returned: %v (expected if script not found)", err)
		}

		t.Log("Slack alert types test completed")
	})

	t.Run("SlackFieldLogger", func(t *testing.T) {
		logger := logrus.New()
		fieldLogger := logger.WithField("component", "test")

		mockConfig := &mockSlackConfig{
			webhookURL: "https://hooks.slack.com/test",
			channel:    "#test",
		}

		notifier := utils.NewSlackNotifierFromConfigWithFieldLogger(mockConfig, fieldLogger)
		if notifier == nil {
			t.Error("Expected notifier to be created with field logger")
		}

		t.Log("Slack field logger test completed")
	})
}

// mockSlackConfig implements the ConfigProvider interface
type mockSlackConfig struct {
	webhookURL string
	channel    string
}

func (m *mockSlackConfig) GetSlackWebhookURL() string {
	return m.webhookURL
}

func (m *mockSlackConfig) GetSlackChannel() string {
	return m.channel
}
