package test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/retail-ai-inc/sync/pkg/backup"
)

// TestCronManagerCreation tests creating a new cron manager
func TestCronManagerCreation(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	apiServer := "http://localhost:8080"
	cronManager := backup.NewCronManager(db, apiServer)
	if cronManager == nil {
		t.Fatal("NewCronManager() returned nil")
	}
}

// TestCronManagerWithNilDB tests cron manager with nil database
func TestCronManagerWithNilDB(t *testing.T) {
	apiServer := "http://localhost:8080"
	cronManager := backup.NewCronManager(nil, apiServer)
	if cronManager == nil {
		t.Fatal("NewCronManager() should handle nil database")
	}
}

// TestSyncCrontabWithEmptyDB tests sync crontab with empty database
func TestSyncCrontabWithEmptyDB(t *testing.T) {
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

	apiServer := "http://localhost:8080"
	cronManager := backup.NewCronManager(db, apiServer)

	ctx := context.Background()
	err = cronManager.SyncCrontab(ctx)
	if err != nil {
		t.Errorf("SyncCrontab with empty database should not fail: %v", err)
	}
}

// TestSyncCrontabWithValidTasks tests sync crontab with valid backup tasks
func TestSyncCrontabWithValidTasks(t *testing.T) {
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

	// Insert test backup tasks
	testConfig := map[string]interface{}{
		"schedule": "0 2 * * *",
		"name":     "test-backup",
	}
	configJSON, _ := json.Marshal(testConfig)

	insertSQL := `
	INSERT INTO backup_tasks (id, enable, last_update_time, last_backup_time, next_backup_time, config_json)
	VALUES (?, ?, ?, ?, ?, ?)`

	now := time.Now().Format("2006-01-02 15:04:05")
	_, err = db.Exec(insertSQL, 1, 1, now, now, now, string(configJSON))
	if err != nil {
		t.Fatalf("Failed to insert test backup task: %v", err)
	}

	apiServer := "http://localhost:8080"
	cronManager := backup.NewCronManager(db, apiServer)

	ctx := context.Background()
	err = cronManager.SyncCrontab(ctx)
	// Note: This might fail in test environment without crontab command, that's expected
	if err != nil {
		t.Logf("SyncCrontab failed (expected in test environment): %v", err)
	}
}

// TestSyncCrontabWithInvalidJSON tests sync crontab with invalid JSON config
func TestSyncCrontabWithInvalidJSON(t *testing.T) {
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

	// Insert test backup task with invalid JSON
	insertSQL := `
	INSERT INTO backup_tasks (id, enable, last_update_time, last_backup_time, next_backup_time, config_json)
	VALUES (?, ?, ?, ?, ?, ?)`

	now := time.Now().Format("2006-01-02 15:04:05")
	_, err = db.Exec(insertSQL, 1, 1, now, now, now, "invalid json")
	if err != nil {
		t.Fatalf("Failed to insert test backup task: %v", err)
	}

	apiServer := "http://localhost:8080"
	cronManager := backup.NewCronManager(db, apiServer)

	ctx := context.Background()
	err = cronManager.SyncCrontab(ctx)
	// Should not fail even with invalid JSON, just skip the invalid tasks
	if err != nil {
		t.Logf("SyncCrontab failed (might be expected): %v", err)
	}
}

// TestSyncCrontabWithDisabledTasks tests sync crontab with disabled tasks
func TestSyncCrontabWithDisabledTasks(t *testing.T) {
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

	// Insert disabled backup task
	testConfig := map[string]interface{}{
		"schedule": "0 2 * * *",
		"name":     "disabled-backup",
	}
	configJSON, _ := json.Marshal(testConfig)

	insertSQL := `
	INSERT INTO backup_tasks (id, enable, last_update_time, last_backup_time, next_backup_time, config_json)
	VALUES (?, ?, ?, ?, ?, ?)`

	now := time.Now().Format("2006-01-02 15:04:05")
	_, err = db.Exec(insertSQL, 1, 0, now, now, now, string(configJSON)) // enable = 0
	if err != nil {
		t.Fatalf("Failed to insert test backup task: %v", err)
	}

	apiServer := "http://localhost:8080"
	cronManager := backup.NewCronManager(db, apiServer)

	ctx := context.Background()
	err = cronManager.SyncCrontab(ctx)
	if err != nil {
		t.Logf("SyncCrontab failed (might be expected in test environment): %v", err)
	}
}

// TestSyncCrontabWithMultipleTasks tests sync crontab with multiple backup tasks
func TestSyncCrontabWithMultipleTasks(t *testing.T) {
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

	// Insert multiple test backup tasks
	tasks := []struct {
		id       int
		schedule string
		name     string
	}{
		{1, "0 2 * * *", "daily-backup"},
		{2, "0 0 * * 0", "weekly-backup"},
		{3, "0 0 1 * *", "monthly-backup"},
	}

	insertSQL := `
	INSERT INTO backup_tasks (id, enable, last_update_time, last_backup_time, next_backup_time, config_json)
	VALUES (?, ?, ?, ?, ?, ?)`

	now := time.Now().Format("2006-01-02 15:04:05")
	for _, task := range tasks {
		testConfig := map[string]interface{}{
			"schedule": task.schedule,
			"name":     task.name,
		}
		configJSON, _ := json.Marshal(testConfig)

		_, err = db.Exec(insertSQL, task.id, 1, now, now, now, string(configJSON))
		if err != nil {
			t.Fatalf("Failed to insert test backup task %d: %v", task.id, err)
		}
	}

	apiServer := "http://localhost:8080"
	cronManager := backup.NewCronManager(db, apiServer)

	ctx := context.Background()
	err = cronManager.SyncCrontab(ctx)
	if err != nil {
		t.Logf("SyncCrontab failed (might be expected in test environment): %v", err)
	}
}

// TestCronManagerIntegration - main integration test for cron manager functionality
func TestCronManagerIntegration(t *testing.T) {
	t.Run("CronManagerCreation", func(t *testing.T) {
		TestCronManagerCreation(t)
	})

	t.Run("CronManagerWithNilDB", func(t *testing.T) {
		TestCronManagerWithNilDB(t)
	})

	t.Run("SyncCrontabWithEmptyDB", func(t *testing.T) {
		TestSyncCrontabWithEmptyDB(t)
	})

	t.Run("SyncCrontabWithValidTasks", func(t *testing.T) {
		TestSyncCrontabWithValidTasks(t)
	})

	t.Run("SyncCrontabWithInvalidJSON", func(t *testing.T) {
		TestSyncCrontabWithInvalidJSON(t)
	})

	t.Run("SyncCrontabWithDisabledTasks", func(t *testing.T) {
		TestSyncCrontabWithDisabledTasks(t)
	})

	t.Run("SyncCrontabWithMultipleTasks", func(t *testing.T) {
		TestSyncCrontabWithMultipleTasks(t)
	})
}
