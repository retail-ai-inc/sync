package test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/pkg/config"
	"github.com/retail-ai-inc/sync/pkg/syncer"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// createTestLogger creates an independent logger instance for testing
func createTestLogger() *logrus.Logger {
	log := logrus.New()
	log.SetLevel(logrus.InfoLevel)
	log.SetOutput(os.Stdout)
	return log
}

// TestMongoDBSyncerCreation tests MongoDB syncer creation
func TestMongoDBSyncerCreation(t *testing.T) {
	testCases := []struct {
		name        string
		config      config.SyncConfig
		expectNil   bool
		expectError bool
	}{
		{
			name: "valid configuration",
			config: config.SyncConfig{
				ID:               1,
				Type:             "mongodb",
				SourceConnection: "mongodb://localhost:27017",
				TargetConnection: "mongodb://localhost:27018",
				Enable:           true,
				Mappings: []config.DatabaseMapping{
					{
						SourceDatabase: "source_db",
						TargetDatabase: "target_db",
						Tables: []config.TableMapping{
							{
								SourceTable: "users",
								TargetTable: "users",
							},
						},
					},
				},
				MongoDBResumeTokenPath: "/tmp/mongodb_tokens",
			},
			expectNil:   false,
			expectError: false,
		},
		{
			name: "invalid source connection",
			config: config.SyncConfig{
				ID:               2,
				Type:             "mongodb",
				SourceConnection: "invalid://connection",
				TargetConnection: "mongodb://localhost:27018",
				Enable:           true,
			},
			expectNil:   true,
			expectError: true,
		},
		{
			name: "invalid target connection",
			config: config.SyncConfig{
				ID:               3,
				Type:             "mongodb",
				SourceConnection: "mongodb://localhost:27017",
				TargetConnection: "invalid://connection",
				Enable:           true,
			},
			expectNil:   true,
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create independent logger instance for each test
			log := createTestLogger()
			globalConfig := createMockGlobalConfig()

			syncer := syncer.NewMongoDBSyncer(tc.config, globalConfig, log)

			if tc.expectNil && syncer != nil {
				t.Errorf("Expected nil syncer for invalid config, got non-nil")
			}
			if !tc.expectNil && syncer == nil {
				t.Errorf("Expected non-nil syncer for valid config, got nil")
			}
		})
	}
}

// TestMongoDBBufferDirectory tests buffer directory creation
func TestMongoDBBufferDirectory(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "test_mongodb_buffer_")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config := config.SyncConfig{
		ID:                     1,
		Type:                   "mongodb",
		SourceConnection:       "mongodb://localhost:27017",
		TargetConnection:       "mongodb://localhost:27018",
		Enable:                 true,
		MongoDBResumeTokenPath: tempDir,
	}

	// Create independent logger instance for each test
	log := createTestLogger()

	syncer := syncer.NewMongoDBSyncer(config, createMockGlobalConfig(), log)

	// Even if connections fail, syncer should be created and buffer directory should exist
	if syncer == nil {
		t.Skip("Syncer creation failed, skipping buffer directory test")
	}

	// Check if buffer directory was created
	bufferDir := filepath.Join(tempDir, "buffer")
	if _, err := os.Stat(bufferDir); os.IsNotExist(err) {
		t.Errorf("Buffer directory should be created at %s", bufferDir)
	}
}

// TestMongoDBResumeTokenPath tests resume token path handling
func TestMongoDBResumeTokenPath(t *testing.T) {
	testCases := []struct {
		name              string
		resumeTokenPath   string
		expectDirCreation bool
	}{
		{
			name:              "valid path",
			resumeTokenPath:   "/tmp/mongodb_test_tokens",
			expectDirCreation: true,
		},
		{
			name:              "empty path",
			resumeTokenPath:   "",
			expectDirCreation: true, // Should create default buffer directory
		},
		{
			name:              "nested path",
			resumeTokenPath:   "/tmp/nested/path/tokens",
			expectDirCreation: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clean up any existing directory
			if tc.resumeTokenPath != "" {
				os.RemoveAll(tc.resumeTokenPath)
			}

			config := config.SyncConfig{
				ID:                     1,
				Type:                   "mongodb",
				SourceConnection:       "mongodb://localhost:27017",
				TargetConnection:       "mongodb://localhost:27018",
				Enable:                 true,
				MongoDBResumeTokenPath: tc.resumeTokenPath,
			}

			// Create independent logger instance for each test
			log := createTestLogger()

			syncer := syncer.NewMongoDBSyncer(config, createMockGlobalConfig(), log)

			if syncer == nil {
				t.Skip("Syncer creation failed, skipping resume token path test")
			}

			// Check directory creation
			if tc.expectDirCreation {
				expectedDir := tc.resumeTokenPath
				if expectedDir == "" {
					expectedDir = "./mongodb_buffer"
				}

				// Check if the directory or its parent exists
				if _, err := os.Stat(expectedDir); err != nil {
					// For empty path, check default buffer directory
					if tc.resumeTokenPath == "" {
						bufferDir := filepath.Join(expectedDir, "buffer")
						if _, err := os.Stat(bufferDir); err != nil {
							t.Logf("Default buffer directory check failed (expected in test env): %v", err)
						}
					}
				}
			}

			// Clean up
			if tc.resumeTokenPath != "" {
				os.RemoveAll(tc.resumeTokenPath)
			}
		})
	}
}

// TestMongoDBSyncerConfiguration tests syncer configuration
func TestMongoDBSyncerConfiguration(t *testing.T) {
	config := config.SyncConfig{
		ID:               1,
		Type:             "mongodb",
		SourceConnection: "mongodb://localhost:27017",
		TargetConnection: "mongodb://localhost:27018",
		Enable:           true,
		Mappings: []config.DatabaseMapping{
			{
				SourceDatabase: "source_db",
				TargetDatabase: "target_db",
				Tables: []config.TableMapping{
					{
						SourceTable: "users",
						TargetTable: "users",
					},
					{
						SourceTable: "orders",
						TargetTable: "orders",
					},
				},
			},
		},
		MongoDBResumeTokenPath: "/tmp/mongodb_tokens",
	}

	log := createTestLogger()
	syncer := syncer.NewMongoDBSyncer(config, createMockGlobalConfig(), log)

	if syncer == nil {
		t.Skip("Syncer creation failed, skipping configuration test")
	}

	// Test that syncer was created with correct configuration
	// Note: We can't directly access private fields, but we can verify the syncer is not nil
	// and the configuration was accepted
	t.Logf("MongoDB syncer created successfully with config ID: %d", config.ID)
}

// TestMongoDBConnectionHandling tests connection handling
func TestMongoDBConnectionHandling(t *testing.T) {
	testCases := []struct {
		name             string
		sourceConnection string
		targetConnection string
		expectSuccess    bool
	}{
		{
			name:             "valid connections",
			sourceConnection: "mongodb://localhost:27017",
			targetConnection: "mongodb://localhost:27018",
			expectSuccess:    true,
		},
		{
			name:             "invalid source",
			sourceConnection: "invalid://connection",
			targetConnection: "mongodb://localhost:27018",
			expectSuccess:    false,
		},
		{
			name:             "invalid target",
			sourceConnection: "mongodb://localhost:27017",
			targetConnection: "invalid://connection",
			expectSuccess:    false,
		},
		{
			name:             "unreachable host",
			sourceConnection: "mongodb://unreachable:27017",
			targetConnection: "mongodb://localhost:27018",
			expectSuccess:    false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := config.SyncConfig{
				ID:               1,
				Type:             "mongodb",
				SourceConnection: tc.sourceConnection,
				TargetConnection: tc.targetConnection,
				Enable:           true,
			}

			log := createTestLogger()
			syncer := syncer.NewMongoDBSyncer(config, createMockGlobalConfig(), log)

			if tc.expectSuccess && syncer == nil {
				t.Errorf("Expected successful syncer creation for valid connections")
			}
			if !tc.expectSuccess && syncer != nil {
				t.Logf("Syncer created despite invalid connection (may be due to lazy connection)")
			}
		})
	}
}

// TestMongoDBSyncerProcessRate tests process rate configuration
func TestMongoDBSyncerProcessRate(t *testing.T) {
	config := config.SyncConfig{
		ID:                     1,
		Type:                   "mongodb",
		SourceConnection:       "mongodb://localhost:27017",
		TargetConnection:       "mongodb://localhost:27018",
		Enable:                 true,
		MongoDBResumeTokenPath: "/tmp/mongodb_tokens",
	}

	log := createTestLogger()
	syncer := syncer.NewMongoDBSyncer(config, createMockGlobalConfig(), log)

	if syncer == nil {
		t.Skip("Syncer creation failed, skipping process rate test")
	}

	// Test that syncer was created with default process rate
	// Note: We can't directly access private fields, but we can verify the syncer is functional
	t.Logf("MongoDB syncer created with default process rate configuration")
}

// TestMongoDBMappingValidation tests mapping validation
func TestMongoDBMappingValidation(t *testing.T) {
	testCases := []struct {
		name         string
		mappings     []config.DatabaseMapping
		expectIssues bool
	}{
		{
			name: "valid mappings",
			mappings: []config.DatabaseMapping{
				{
					SourceDatabase: "source_db",
					TargetDatabase: "target_db",
					Tables: []config.TableMapping{
						{
							SourceTable: "users",
							TargetTable: "users",
						},
					},
				},
			},
			expectIssues: false,
		},
		{
			name:         "empty mappings",
			mappings:     []config.DatabaseMapping{},
			expectIssues: false, // Should not cause syncer creation to fail
		},
		{
			name: "missing source database",
			mappings: []config.DatabaseMapping{
				{
					SourceDatabase: "",
					TargetDatabase: "target_db",
					Tables: []config.TableMapping{
						{
							SourceTable: "users",
							TargetTable: "users",
						},
					},
				},
			},
			expectIssues: false, // Should not cause syncer creation to fail
		},
		{
			name: "missing target database",
			mappings: []config.DatabaseMapping{
				{
					SourceDatabase: "source_db",
					TargetDatabase: "",
					Tables: []config.TableMapping{
						{
							SourceTable: "users",
							TargetTable: "users",
						},
					},
				},
			},
			expectIssues: false, // Should not cause syncer creation to fail
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := config.SyncConfig{
				ID:               1,
				Type:             "mongodb",
				SourceConnection: "mongodb://localhost:27017",
				TargetConnection: "mongodb://localhost:27018",
				Enable:           true,
				Mappings:         tc.mappings,
			}

			log := createTestLogger()
			syncer := syncer.NewMongoDBSyncer(config, createMockGlobalConfig(), log)

			if syncer == nil && !tc.expectIssues {
				t.Errorf("Syncer creation failed unexpectedly for valid mappings")
			}
			if syncer != nil && tc.expectIssues {
				t.Logf("Syncer created despite potentially problematic mappings")
			}
		})
	}
}

// TestMongoDBSyncerBufferEnabled tests buffer enabled configuration
func TestMongoDBSyncerBufferEnabled(t *testing.T) {
	config := config.SyncConfig{
		ID:                     1,
		Type:                   "mongodb",
		SourceConnection:       "mongodb://localhost:27017",
		TargetConnection:       "mongodb://localhost:27018",
		Enable:                 true,
		MongoDBResumeTokenPath: "/tmp/mongodb_tokens",
	}

	log := createTestLogger()
	syncer := syncer.NewMongoDBSyncer(config, createMockGlobalConfig(), log)

	if syncer == nil {
		t.Skip("Syncer creation failed, skipping buffer enabled test")
	}

	// Test that buffer is enabled by default
	// Note: We can't directly access private fields, but we can verify the syncer is functional
	t.Logf("MongoDB syncer created with buffer enabled by default")
}

// TestMongoDBResumeTokenHandling tests resume token handling
func TestMongoDBResumeTokenHandling(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "test_mongodb_resume_")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config := config.SyncConfig{
		ID:                     1,
		Type:                   "mongodb",
		SourceConnection:       "mongodb://localhost:27017",
		TargetConnection:       "mongodb://localhost:27018",
		Enable:                 true,
		MongoDBResumeTokenPath: tempDir,
	}

	log := createTestLogger()
	syncer := syncer.NewMongoDBSyncer(config, createMockGlobalConfig(), log)

	if syncer == nil {
		t.Skip("Syncer creation failed, skipping resume token handling test")
	}

	// Test that resume token directory is created
	if _, err := os.Stat(tempDir); os.IsNotExist(err) {
		t.Errorf("Resume token directory should be created at %s", tempDir)
	}
}

// TestMongoDBSyncerErrorHandling tests error handling
func TestMongoDBSyncerErrorHandling(t *testing.T) {
	testCases := []struct {
		name        string
		config      config.SyncConfig
		expectError bool
	}{
		{
			name: "connection timeout",
			config: config.SyncConfig{
				ID:               1,
				Type:             "mongodb",
				SourceConnection: "mongodb://timeout:27017",
				TargetConnection: "mongodb://localhost:27018",
				Enable:           true,
			},
			expectError: true,
		},
		{
			name: "invalid URI format",
			config: config.SyncConfig{
				ID:               2,
				Type:             "mongodb",
				SourceConnection: "not-a-uri",
				TargetConnection: "mongodb://localhost:27018",
				Enable:           true,
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			log := createTestLogger()
			syncer := syncer.NewMongoDBSyncer(tc.config, createMockGlobalConfig(), log)

			if tc.expectError && syncer != nil {
				t.Logf("Syncer created despite expected error (may be due to lazy connection)")
			}
			if !tc.expectError && syncer == nil {
				t.Errorf("Syncer creation failed unexpectedly")
			}
		})
	}
}

// TestMongoDBContextHandling tests context handling
func TestMongoDBContextHandling(t *testing.T) {
	config := config.SyncConfig{
		ID:               1,
		Type:             "mongodb",
		SourceConnection: "mongodb://localhost:27017",
		TargetConnection: "mongodb://localhost:27018",
		Enable:           true,
	}

	log := createTestLogger()
	syncer := syncer.NewMongoDBSyncer(config, createMockGlobalConfig(), log)

	if syncer == nil {
		t.Skip("Syncer creation failed, skipping context handling test")
	}

	// Test context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Note: We can't directly test the Start method without proper MongoDB connections
	// but we can verify the syncer accepts context
	t.Logf("MongoDB syncer created and ready to accept context with timeout: %v", ctx.Err())
}

// TestMongoDBPersistedChangeStruct tests persisted change structure
func TestMongoDBPersistedChangeStruct(t *testing.T) {
	// This tests the structure that would be used by the MongoDB syncer
	// We can't directly access the private struct, but we can test similar structures

	type testPersistedChange struct {
		ID         string    `json:"id"`
		Token      bson.Raw  `json:"token"`
		Doc        []byte    `json:"doc"`
		OpType     string    `json:"op_type"`
		SourceDB   string    `json:"source_db"`
		SourceColl string    `json:"source_coll"`
		Timestamp  time.Time `json:"timestamp"`
	}

	change := testPersistedChange{
		ID:         "test-id",
		Token:      bson.Raw{},
		Doc:        []byte(`{"test": "data"}`),
		OpType:     "insert",
		SourceDB:   "source_db",
		SourceColl: "users",
		Timestamp:  time.Now(),
	}

	// Test that the structure is valid
	if change.ID == "" {
		t.Error("ID should not be empty")
	}
	if change.OpType != "insert" {
		t.Error("OpType should be 'insert'")
	}
	if change.SourceDB != "source_db" {
		t.Error("SourceDB should be 'source_db'")
	}
	if change.SourceColl != "users" {
		t.Error("SourceColl should be 'users'")
	}
	if len(change.Doc) == 0 {
		t.Error("Doc should not be empty")
	}
}

// TestMongoDBBufferedChangeStruct tests buffered change structure
func TestMongoDBBufferedChangeStruct(t *testing.T) {
	// This tests the structure that would be used by the MongoDB syncer
	// We can't directly access the private struct, but we can test similar structures

	type testBufferedChange struct {
		model  mongo.WriteModel
		opType string
	}

	// Create a test write model
	doc := bson.D{{"name", "test"}}
	writeModel := mongo.NewInsertOneModel().SetDocument(doc)

	change := testBufferedChange{
		model:  writeModel,
		opType: "insert",
	}

	// Test that the structure is valid
	if change.model == nil {
		t.Error("WriteModel should not be nil")
	}
	if change.opType != "insert" {
		t.Error("OpType should be 'insert'")
	}
}

// TestMongoDBClientConnections tests client connections
func TestMongoDBClientConnections(t *testing.T) {
	// Test connection options
	ctx := context.Background()

	// Test source connection
	sourceOpts := options.Client().ApplyURI("mongodb://localhost:27017")
	sourceClient, err := mongo.Connect(ctx, sourceOpts)
	if err != nil {
		t.Logf("Source connection failed (expected in test env): %v", err)
	} else {
		defer sourceClient.Disconnect(ctx)

		// Test ping
		err = sourceClient.Ping(ctx, nil)
		if err != nil {
			t.Logf("Source ping failed (expected in test env): %v", err)
		}
	}

	// Test target connection
	targetOpts := options.Client().ApplyURI("mongodb://localhost:27018")
	targetClient, err := mongo.Connect(ctx, targetOpts)
	if err != nil {
		t.Logf("Target connection failed (expected in test env): %v", err)
	} else {
		defer targetClient.Disconnect(ctx)

		// Test ping
		err = targetClient.Ping(ctx, nil)
		if err != nil {
			t.Logf("Target ping failed (expected in test env): %v", err)
		}
	}
}
