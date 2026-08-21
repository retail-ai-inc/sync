package mongodb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/retail-ai-inc/sync/pkg/config"
	"github.com/retail-ai-inc/sync/pkg/syncer/common"
	"github.com/retail-ai-inc/sync/pkg/utils"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDBSyncer struct {
	sourceClient  *mongo.Client
	targetClient  *mongo.Client
	cfg           config.SyncConfig
	logger        logrus.FieldLogger
	resumeTokens  map[string]bson.Raw
	resumeTokensM sync.RWMutex
	// New fields for persistent buffer
	bufferDir     string
	bufferEnabled bool
	// Fields for goroutine lifecycle management
	activeProcessors map[string]context.CancelFunc
	processorMutex   sync.RWMutex
	// New fields for async pipeline
	channelCapacity int
	// Smart batch controller configuration
	targetBatchSizeBytes int64
	maxFilesPerBatch     int
	minFilesPerBatch     int
	// Dead letter queue configuration
	deadLetterDir         string
	maxRetryAttempts      int
	retryInterval         time.Duration
	enableDeadLetterQueue bool
	// Global configuration for accessing Slack settings
	globalConfig *config.Config
}

func NewMongoDBSyncer(cfg config.SyncConfig, globalConfig *config.Config, logger *logrus.Logger) *MongoDBSyncer {
	var err error
	var sourceClient *mongo.Client

	// First attempt to connect to source without retry to check for immediate failures
	sourceClient, err = mongo.Connect(context.Background(), options.Client().ApplyURI(cfg.SourceConnection))
	if err != nil {
		// Check if it's a URI parsing error, if so, don't retry
		if strings.Contains(err.Error(), "scheme must be") || strings.Contains(err.Error(), "error parsing uri") {
			logger.Errorf("[MongoDB] Invalid source connection URI: %v", err)
			return nil
		}
		// For other errors, retry with exponential backoff
		err = utils.Retry(5, 2*time.Second, 2.0, func() error {
			var connErr error
			sourceClient, connErr = mongo.Connect(context.Background(), options.Client().ApplyURI(cfg.SourceConnection))
			return connErr
		})
		if err != nil {
			logger.Errorf("[MongoDB] Failed to connect to source after retries: %v", err)
			return nil
		}
	}

	var targetClient *mongo.Client

	// First attempt to connect to target without retry to check for immediate failures
	targetClient, err = mongo.Connect(context.Background(), options.Client().ApplyURI(cfg.TargetConnection))
	if err != nil {
		// Check if it's a URI parsing error, if so, don't retry
		if strings.Contains(err.Error(), "scheme must be") || strings.Contains(err.Error(), "error parsing uri") {
			logger.Errorf("[MongoDB] Invalid target connection URI: %v", err)
			return nil
		}
		// For other errors, retry with exponential backoff
		err = utils.Retry(5, 2*time.Second, 2.0, func() error {
			var connErr error
			targetClient, connErr = mongo.Connect(context.Background(), options.Client().ApplyURI(cfg.TargetConnection))
			return connErr
		})
		if err != nil {
			logger.Errorf("[MongoDB] Failed to connect to target after retries: %v", err)
			return nil
		}
	}

	resumeMap := make(map[string]bson.Raw)
	if cfg.MongoDBResumeTokenPath != "" {
		if e2 := os.MkdirAll(cfg.MongoDBResumeTokenPath, os.ModePerm); e2 != nil {
			logger.Warnf("[MongoDB] Failed to create resume token dir %s: %v", cfg.MongoDBResumeTokenPath, e2)
		}
	}

	// Create buffer directory for persistent storage
	bufferDir := cfg.MongoDBResumeTokenPath
	if bufferDir == "" {
		bufferDir = "./mongodb_buffer"
	} else {
		bufferDir = filepath.Join(bufferDir, "buffer")
	}

	if err := os.MkdirAll(bufferDir, os.ModePerm); err != nil {
		logger.Warnf("[MongoDB] Failed to create buffer directory %s: %v", bufferDir, err)
	}

	// Create dead letter directory for failed data
	deadLetterDir := cfg.MongoDBResumeTokenPath
	if deadLetterDir == "" {
		deadLetterDir = "./mongodb_dead_letter"
	} else {
		deadLetterDir = filepath.Join(deadLetterDir, "dead_letter")
	}

	if err := os.MkdirAll(deadLetterDir, os.ModePerm); err != nil {
		logger.Warnf("[MongoDB] Failed to create dead letter directory %s: %v", deadLetterDir, err)
	}

	return &MongoDBSyncer{
		sourceClient:     sourceClient,
		targetClient:     targetClient,
		cfg:              cfg,
		logger:           logger.WithField("sync_task_id", cfg.ID),
		resumeTokens:     resumeMap,
		bufferDir:        bufferDir,
		bufferEnabled:    true, // Enable persistent buffer by default
		activeProcessors: make(map[string]context.CancelFunc),
		// Initialize async pipeline components
		channelCapacity: 200, // A smaller, safer default to prevent OOM.
		// Initialize smart batch controller configuration
		targetBatchSizeBytes: 256 * 1024 * 1024, // 256MB - Increased batch size for higher throughput
		maxFilesPerBatch:     1000,
		minFilesPerBatch:     5,
		// Initialize dead letter queue configuration
		deadLetterDir:         deadLetterDir,
		maxRetryAttempts:      3,
		retryInterval:         time.Second * 5,
		enableDeadLetterQueue: true,
		globalConfig:          globalConfig,
	}
}

func (s *MongoDBSyncer) Start(ctx context.Context) {
	if s.sourceClient == nil || s.targetClient == nil {
		s.logger.Error("[MongoDB] MongoDBSyncer Start aborted: source/target client is nil.")
		return
	}
	s.logger.Info("[MongoDB] Starting synchronization...")

	var wg sync.WaitGroup
	sourceDBName := common.GetDatabaseName(s.cfg.Type, s.cfg.SourceConnection)
	targetDBName := common.GetDatabaseName(s.cfg.Type, s.cfg.TargetConnection)

	for _, mapping := range s.cfg.Mappings {
		if len(mapping.Tables) > 0 {
			wg.Add(1)
			go func(m config.DatabaseMapping) {
				defer wg.Done()
				s.syncDatabase(ctx, m, sourceDBName, targetDBName)
			}(mapping)
		} else {
			s.logger.Warn("[MongoDB] Table mappings are empty, skipping processing")
			continue
		}
	}
	wg.Wait()

	s.logger.Info("[MongoDB] All database mappings have been processed.")
}

func (s *MongoDBSyncer) syncDatabase(ctx context.Context, mapping config.DatabaseMapping, sourceDBName, targetDBName string) {
	sourceDB := s.sourceClient.Database(sourceDBName)
	targetDB := s.targetClient.Database(targetDBName)
	s.logger.Infof("[MongoDB] Processing database mapping: %s -> %s", sourceDBName, targetDBName)

	for _, tableMap := range mapping.Tables {
		srcColl := sourceDB.Collection(tableMap.SourceTable)
		tgtColl := targetDB.Collection(tableMap.TargetTable)
		s.logger.Infof("[MongoDB] Processing collection mapping: %s -> %s", tableMap.SourceTable, tableMap.TargetTable)

		// Ensure target collection exists
		if err := s.ensureCollectionExists(ctx, targetDB, tableMap.TargetTable); err != nil {
			s.logger.Errorf("[MongoDB] Failed to create target collection %s.%s: %v", targetDBName, tableMap.TargetTable, err)
			continue
		}

		// Copy indexes based on AdvancedSettings
		s.logger.Infof("[MongoDB] SyncIndexes: %v", tableMap.AdvancedSettings.SyncIndexes)
		if tableMap.AdvancedSettings.SyncIndexes {
			if errIdx := s.copyIndexes(ctx, srcColl, tgtColl); errIdx != nil {
				s.logger.Warnf("[MongoDB] Failed to copy indexes for %s -> %s: %v", tableMap.SourceTable, tableMap.TargetTable, errIdx)
			} else {
				s.logger.Infof("[MongoDB] Successfully copied indexes for %s -> %s", tableMap.SourceTable, tableMap.TargetTable)
			}
		} else {
			s.logger.Infof("[MongoDB] Index copying is disabled for %s -> %s (syncIndexes=false)", tableMap.SourceTable, tableMap.TargetTable)
		}

		// Perform initial sync if target has no data
		err := s.doInitialSync(ctx, srcColl, tgtColl, sourceDBName, targetDBName)
		if err != nil {
			s.logger.Errorf("[MongoDB] doInitialSync failed => %v", err)
			continue
		}

		// Start watching changes
		go s.watchChangesWithRetry(ctx, srcColl, tgtColl, sourceDBName, tableMap.SourceTable)
	}
}

func (s *MongoDBSyncer) ensureCollectionExists(ctx context.Context, db *mongo.Database, collName string) error {
	collections, err := db.ListCollectionNames(ctx, bson.M{"name": collName})
	if err != nil {
		s.logger.Errorf("[MongoDB] ListCollectionNames failed => %v", err)
		return err
	}
	if len(collections) == 0 {
		s.logger.Infof("[MongoDB] Creating collection => %s.%s", db.Name(), collName)
		if createErr := db.CreateCollection(ctx, collName); createErr != nil {
			return createErr
		}
	}
	return nil
}

func (s *MongoDBSyncer) copyIndexes(ctx context.Context, sourceColl, targetColl *mongo.Collection) error {
	cursor, err := sourceColl.Indexes().List(ctx)
	if err != nil {
		return fmt.Errorf("list source indexes fail: %w", err)
	}
	defer cursor.Close(ctx)

	var indexDocs []bson.M
	if err2 := cursor.All(ctx, &indexDocs); err2 != nil {
		return fmt.Errorf("read indexes fail: %w", err2)
	}

	targetCursor, err := targetColl.Indexes().List(ctx)
	if err != nil {
		s.logger.Warnf("[MongoDB] Failed to list existing indexes: %v", err)
	}

	existingIndexes := make(map[string]bool)
	if targetCursor != nil {
		var targetIdxDocs []bson.M
		if err := targetCursor.All(ctx, &targetIdxDocs); err == nil {
			for _, idx := range targetIdxDocs {
				if name, ok := idx["name"].(string); ok {
					existingIndexes[name] = true
				}
			}
		}
		targetCursor.Close(ctx)
	}

	indexesCreated := 0
	indexesSkipped := 0

	for _, idx := range indexDocs {
		if name, ok := idx["name"].(string); ok && name == "_id_" {
			continue
		}

		var name string
		if nameStr, ok := idx["name"].(string); ok {
			name = nameStr
			if existingIndexes[name] {
				s.logger.Debugf("[MongoDB] Index %s already exists, skipping", name)
				indexesSkipped++
				continue
			}
		}

		keyDoc := bson.D{}
		if keys, ok := idx["key"].(bson.M); ok {
			for field, direction := range keys {
				fixedDirection := direction
				if strVal, isString := direction.(string); isString {
					if strVal == "1" {
						fixedDirection = int32(1)
					} else if strVal == "-1" {
						fixedDirection = int32(-1)
					}
				} else if floatVal, isFloat := direction.(float64); isFloat {
					fixedDirection = int32(floatVal)
				}

				keyDoc = append(keyDoc, bson.E{Key: field, Value: fixedDirection})
			}
		} else {
			s.logger.Warnf("[MongoDB] Invalid index key format: %v", idx["key"])
			continue
		}

		indexOptions := options.Index()

		if uniqueVal, hasUnique := idx["unique"]; hasUnique {
			if uv, isBool := uniqueVal.(bool); isBool && uv {
				indexOptions.SetUnique(true)
			}
		}

		if nameVal, hasName := idx["name"]; hasName {
			if nameStr, ok := nameVal.(string); ok {
				name = nameStr
				indexOptions.SetName(nameStr)
			}
		}

		indexModel := mongo.IndexModel{
			Keys:    keyDoc,
			Options: indexOptions,
		}

		_, errC := targetColl.Indexes().CreateOne(ctx, indexModel)
		if errC != nil {
			if strings.Contains(errC.Error(), "already exists") {
				indexesSkipped++
			} else {
				s.logger.Warnf("[MongoDB] Create index %s fail: %v", name, errC)
			}
		} else {
			indexesCreated++
		}
	}

	s.logger.Infof("[MongoDB] Index creation summary for %s: created=%d, skipped=%d",
		targetColl.Name(), indexesCreated, indexesSkipped)

	return nil
}

func (s *MongoDBSyncer) findTableAdvancedSettings(collName string) config.AdvancedSettings {
	for _, m := range s.cfg.Mappings {
		for _, t := range m.Tables {
			if t.SourceTable == collName {
				return t.AdvancedSettings
			}
		}
	}
	return config.AdvancedSettings{}
}
