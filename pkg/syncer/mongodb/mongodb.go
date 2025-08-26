package mongodb

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
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

// Smart Batch Controller Implementation
//
// This implementation replaces the fixed file count processing (maxFilesPerRun = 5)
// with an intelligent batch controller that limits memory usage to a target size (default: 256MB).
//
// Key Features:
// - Dynamic batch size based on actual file sizes
// - Memory usage control to prevent OOM
// - Configurable target batch size (targetBatchSizeBytes)
// - Automatic optimization and monitoring
// - Detailed logging for performance analysis
//
// Performance Benefits:
// - Processes large numbers of small files efficiently
// - Handles large files safely without OOM
// - Maximizes memory utilization up to the target limit
// - Provides throughput metrics for monitoring
//
// Configuration:
// - targetBatchSizeBytes: 256MB (default) - maximum memory per batch
// - maxFilesPerBatch: 1000 (default) - safety limit for file count
// - minFilesPerBatch: 5 (default) - minimum files to process
//
// Usage Example:
// To adjust batch size at runtime:
//   syncer.updateBatchSizeConfig(512*1024*1024, 2000, 10) // 512MB, max 2000 files, min 10 files

// bsonRawSeparator is a unique byte sequence used to separate BSON documents in a buffer file.
var bsonRawSeparator = []byte{0xDE, 0xAD, 0xBE, 0xEF, 0xDE, 0xAD, 0xBE, 0xEF}

// generateBatchID generates a unique batch ID for tracking performance
func generateBatchID() string {
	bytes := make([]byte, 8)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// BatchMetrics holds performance metrics for a batch processing operation
type BatchMetrics struct {
	BatchID           string
	StartTime         time.Time
	FileSelectionTime time.Duration
	FileParsingTime   time.Duration
	DatabaseWriteTime time.Duration
	FileDeletionTime  time.Duration
	TotalTime         time.Duration
	FileCount         int
	TotalSizeBytes    int64
	WriteModelCount   int
	RemainingFiles    int
}

// FileParseJob represents a file parsing job for parallel processing
type FileParseJob struct {
	FilePath       string
	FileIndex      int
	TotalFiles     int
	SourceDB       string
	CollectionName string
}

// FileParseResult represents the result of parsing a file
type FileParseResult struct {
	FilePath    string
	FileIndex   int
	WriteModels []mongo.WriteModel
	ParseTime   time.Duration
	Error       error
}

// streamEvent represents the data passed from the Change Stream reader to the disk writer.
// It contains the raw BSON data and the corresponding resume token.
type streamEvent struct {
	RawData     bson.Raw
	ResumeToken bson.Raw
}

// persistedChange represents a change that will be saved to disk
type persistedChange struct {
	ID         string    `bson:"id"`
	Token      bson.Raw  `bson:"token"`
	RawData    bson.Raw  `bson:"raw_data"` // Store the raw BSON event directly
	SourceDB   string    `bson:"source_db"`
	SourceColl string    `bson:"source_coll"`
	Timestamp  time.Time `bson:"timestamp"`
}

// FailedOperation represents a failed database operation for dead letter queue
type FailedOperation struct {
	ID         string          `json:"id"`
	WriteModel json.RawMessage `json:"write_model"` // Serialized WriteModel
	Error      string          `json:"error"`       // Error message
	ErrorCode  int             `json:"error_code"`  // MongoDB error code
	OpType     string          `json:"op_type"`     // insert, update, replace, delete
	SourceDB   string          `json:"source_db"`
	SourceColl string          `json:"source_coll"`
	Timestamp  time.Time       `json:"timestamp"`
	RetryCount int             `json:"retry_count"`
	BatchIndex int             `json:"batch_index"` // Index in the original batch
}

// DeadLetterBatch represents a batch of failed operations
type DeadLetterBatch struct {
	BatchID       string            `json:"batch_id"`
	FailedOps     []FailedOperation `json:"failed_operations"`
	TotalOps      int               `json:"total_operations"`
	SuccessfulOps int               `json:"successful_operations"`
	Timestamp     time.Time         `json:"timestamp"`
	SourceDB      string            `json:"source_db"`
	SourceColl    string            `json:"source_coll"`
}

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

func (s *MongoDBSyncer) doInitialSync(ctx context.Context, sourceColl, targetColl *mongo.Collection, sourceDB, targetDB string) error {
	count, err := targetColl.EstimatedDocumentCount(ctx)
	if err != nil {
		return fmt.Errorf("check target collection %s.%s fail: %v", targetDB, targetColl.Name(), err)
	}
	if count > 0 {
		s.logger.Infof("[MongoDB] %s.%s has data => skip initial sync", targetDB, targetColl.Name())
		return nil
	}

	s.logger.Infof("[MongoDB] Starting initial sync for %s.%s -> %s.%s", sourceDB, sourceColl.Name(), targetDB, targetColl.Name())

	cursor, err := sourceColl.Find(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("source find fail => %v", err)
	}
	defer cursor.Close(ctx)

	batchSize := 100
	var batch []interface{}
	inserted := 0

	for cursor.Next(ctx) {
		var doc bson.M
		if errD := cursor.Decode(&doc); errD != nil {
			return fmt.Errorf("decode doc fail => %v", errD)
		}
		batch = append(batch, doc)
		if len(batch) >= batchSize {
			err := utils.RetryMongoOperation(ctx, s.logger,
				fmt.Sprintf("InsertMany to %s.%s", targetDB, targetColl.Name()),
				func() error {
					res, err := targetColl.InsertMany(ctx, batch)
					if err != nil {
						return err
					}
					inserted += len(res.InsertedIDs)
					return nil
				})

			if err != nil {
				return fmt.Errorf("insertMany fail => %v", err)
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		err := utils.RetryMongoOperation(ctx, s.logger,
			fmt.Sprintf("InsertMany final batch to %s.%s", targetDB, targetColl.Name()),
			func() error {
				res, err := targetColl.InsertMany(ctx, batch)
				if err != nil {
					return err
				}
				inserted += len(res.InsertedIDs)
				return nil
			})

		if err != nil {
			return fmt.Errorf("insertMany fail => %v", err)
		}
	}

	s.logger.Infof("[MongoDB] doInitialSync => %s.%s => %s.%s inserted=%d docs",
		sourceDB, sourceColl.Name(), targetDB, targetColl.Name(), inserted)
	return nil
}

func (s *MongoDBSyncer) watchChanges(ctx context.Context, sourceColl, targetColl *mongo.Collection, sourceDB, collectionName string) {
	key := fmt.Sprintf("%s.%s", sourceDB, collectionName)

	s.processorMutex.Lock()
	if cancelFunc, exists := s.activeProcessors[key]; exists {
		cancelFunc()
	}
	processorCtx, cancel := context.WithCancel(ctx)
	s.activeProcessors[key] = cancel
	s.processorMutex.Unlock()

	defer func() {
		s.processorMutex.Lock()
		delete(s.activeProcessors, key)
		s.processorMutex.Unlock()
		cancel()
	}()

	eventChannel := make(chan streamEvent, s.channelCapacity)

	go s.diskWriter(processorCtx, eventChannel, sourceDB, collectionName)
	go s.processPersistentBuffer(processorCtx, sourceDB, collectionName, targetColl.Database().Name(), targetColl.Name())

	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "ns.db", Value: sourceDB},
			{Key: "ns.coll", Value: collectionName},
			{Key: "operationType", Value: bson.M{"$in": []string{"insert", "update", "replace", "delete"}}},
		}}},
	}
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	resumeToken := s.loadMongoDBResumeToken(sourceDB, collectionName)
	if resumeToken != nil {
		opts.SetResumeAfter(resumeToken)
	}

	cs, err := sourceColl.Watch(ctx, pipeline, opts)
	if err != nil {
		s.logger.Errorf("[MongoDB] Failed to establish change stream for %s.%s: %v", sourceDB, collectionName, err)
		return
	}
	defer cs.Close(ctx)
	s.logger.Infof("[MongoDB] Watching changes => %s.%s", sourceDB, collectionName)

	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("[MongoDB] Context cancelled, stopping change stream for %s.%s", sourceDB, collectionName)
			return
		default:
			if !cs.Next(ctx) {
				if err := cs.Err(); err != nil {
					s.logger.Errorf("[MongoDB] Change stream error for %s.%s: %v", sourceDB, collectionName, err)

					// Check if this is a recoverable error
					if isRecoverableError(err) {
						s.logger.Warnf("[MongoDB] Recoverable error detected for %s.%s, will be retried by guardian", sourceDB, collectionName)
					} else {
						s.logger.Errorf("[MongoDB] Non-recoverable error for %s.%s, stopping", sourceDB, collectionName)
					}
				} else {
					s.logger.Infof("[MongoDB] Change stream ended normally for %s.%s", sourceDB, collectionName)
				}
				// Stream closed or an error occurred, exit and let the guardian restart.
				return
			}

			event := streamEvent{
				RawData:     cs.Current,
				ResumeToken: cs.ResumeToken(),
			}

			select {
			case eventChannel <- event:
				// Event successfully sent
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
				s.logger.Errorf("[MongoDB] Channel full for 10 seconds, potential deadlock or severe bottleneck for %s.%s. Stopping.", sourceDB, collectionName)
				return
			}
		}
	}
}

// watchChangesWithRetry is a guardian wrapper around watchChanges that provides automatic retry and recovery
func (s *MongoDBSyncer) watchChangesWithRetry(ctx context.Context, sourceColl, targetColl *mongo.Collection, sourceDB, collectionName string) {
	// Get retry settings from configuration
	advancedSettings := s.findTableAdvancedSettings(collectionName)
	maxRetries := advancedSettings.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 10 // Default value
	}

	baseDelay := advancedSettings.BaseRetryDelay
	if baseDelay <= 0 {
		baseDelay = 5 * time.Second // Default value
	}

	maxDelay := advancedSettings.MaxRetryDelay
	if maxDelay <= 0 {
		maxDelay = 5 * time.Minute // Default value
	}

	currentDelay := baseDelay
	retryCount := 0

	s.logger.Infof("[MongoDB] Starting guardian loop for %s.%s", sourceDB, collectionName)

	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("[MongoDB] Guardian loop cancelled for %s.%s", sourceDB, collectionName)
			return
		default:
			s.logger.Infof("[MongoDB] Attempting to start watchChanges for %s.%s (attempt %d)", sourceDB, collectionName, retryCount+1)

			// Start the actual watchChanges in a separate goroutine
			watchCtx, watchCancel := context.WithCancel(ctx)
			watchDone := make(chan struct{})

			go func() {
				defer close(watchDone)
				s.watchChanges(watchCtx, sourceColl, targetColl, sourceDB, collectionName)
			}()

			// Wait for watchChanges to complete or context to be cancelled
			select {
			case <-watchDone:
				// watchChanges has exited, check if it was due to context cancellation
				select {
				case <-ctx.Done():
					s.logger.Infof("[MongoDB] Guardian loop stopping due to context cancellation for %s.%s", sourceDB, collectionName)
					return
				default:
					// watchChanges exited due to error, retry
					retryCount++
					if retryCount > maxRetries {
						s.logger.Errorf("[MongoDB] Max retries (%d) exceeded for %s.%s, stopping guardian loop", maxRetries, sourceDB, collectionName)
						return
					}

					s.logger.Warnf("[MongoDB] watchChanges exited for %s.%s, retrying in %v (attempt %d/%d)",
						sourceDB, collectionName, currentDelay, retryCount, maxRetries)

					// Exponential backoff with jitter
					select {
					case <-ctx.Done():
						return
					case <-time.After(currentDelay):
						// Increase delay for next retry
						currentDelay = time.Duration(float64(currentDelay) * 1.5)
						if currentDelay > maxDelay {
							currentDelay = maxDelay
						}
					}
				}
			case <-ctx.Done():
				watchCancel()
				<-watchDone // Wait for watchChanges to clean up
				return
			}
		}
	}
}

func (s *MongoDBSyncer) diskWriter(ctx context.Context, eventChannel <-chan streamEvent, sourceDB, collectionName string) {
	s.logger.Infof("[MongoDB] Starting disk writer for %s.%s", sourceDB, collectionName)
	defer s.logger.Infof("[MongoDB] Stopping disk writer for %s.%s", sourceDB, collectionName)

	var buffer []streamEvent
	const batchSize = 100
	flushInterval := 2 * time.Second
	timer := time.NewTimer(flushInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			if len(buffer) > 0 {
				s.flushBufferToDisk(ctx, &buffer, sourceDB, collectionName)
			}
			return
		case event, ok := <-eventChannel:
			if !ok {
				if len(buffer) > 0 {
					s.flushBufferToDisk(ctx, &buffer, sourceDB, collectionName)
				}
				return
			}
			buffer = append(buffer, event)
			if len(buffer) >= batchSize {
				s.flushBufferToDisk(ctx, &buffer, sourceDB, collectionName)
				timer.Reset(flushInterval)
			}
		case <-timer.C:
			if len(buffer) > 0 {
				s.flushBufferToDisk(ctx, &buffer, sourceDB, collectionName)
			}
			timer.Reset(flushInterval)
		}
	}
}

func (s *MongoDBSyncer) flushBufferToDisk(ctx context.Context, buffer *[]streamEvent, sourceDB, collectionName string) {
	if len(*buffer) == 0 {
		return
	}

	bufferPath := s.getBufferPath(sourceDB, collectionName)
	if err := os.MkdirAll(bufferPath, os.ModePerm); err != nil {
		s.logger.Errorf("[MongoDB] Failed to create buffer directory %s: %v", bufferPath, err)
		return
	}

	timestamp := time.Now().UnixNano()
	fileName := fmt.Sprintf("batch_%d.bsonstream", timestamp)
	filePath := filepath.Join(bufferPath, fileName)

	file, err := os.Create(filePath)
	if err != nil {
		s.logger.Errorf("[MongoDB] Failed to create buffer file %s: %v", filePath, err)
		return
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	var lastToken bson.Raw
	for _, event := range *buffer {
		if _, err := writer.Write(event.RawData); err != nil {
			s.logger.Errorf("[MongoDB] Failed to write event to buffer file %s: %v", filePath, err)
			return // Stop on first error
		}
		if _, err := writer.Write(bsonRawSeparator); err != nil {
			s.logger.Errorf("[MongoDB] Failed to write separator to buffer file %s: %v", filePath, err)
			return // Stop on first error
		}
		lastToken = event.ResumeToken
	}

	if err := writer.Flush(); err != nil {
		s.logger.Errorf("[MongoDB] Failed to flush buffer file %s: %v", filePath, err)
		return
	}

	// If we successfully wrote the entire buffer to a file, we can save the last token.
	if lastToken != nil {
		s.saveMongoDBResumeToken(sourceDB, collectionName, lastToken)
	}

	// Clear the buffer now that it's been persisted.
	*buffer = (*buffer)[:0]
}

func (s *MongoDBSyncer) processPersistentBuffer(ctx context.Context, sourceDB, collectionName, targetDBName, targetCollectionName string) {
	ticker := time.NewTicker(100 * time.Millisecond) // Check for files every 100ms for better responsiveness
	defer ticker.Stop()

	// Add optimization ticker to periodically analyze and optimize batch size
	optimizationTicker := time.NewTicker(5 * time.Minute) // Optimize every 5 minutes
	defer optimizationTicker.Stop()

	// Add dead letter queue retry ticker
	deadLetterTicker := time.NewTicker(s.retryInterval) // Retry dead letter queue
	defer deadLetterTicker.Stop()

	s.logger.Infof("[MongoDB] Started persistent buffer processor for %s.%s with smart batch control (target: %.2f MB)",
		sourceDB, collectionName, float64(s.targetBatchSizeBytes)/(1024*1024))

	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("[MongoDB] Stopping persistent buffer processor for %s.%s", sourceDB, collectionName)
			return
		case <-ticker.C:
			s.processBufferedChanges(ctx, sourceDB, collectionName, targetDBName, targetCollectionName)
		case <-optimizationTicker.C:
			// Periodically analyze buffer and optimize batch size
			bufferPath := s.getBufferPath(sourceDB, collectionName)
			s.estimateOptimalBatchSize(bufferPath)
		case <-deadLetterTicker.C:
			// Periodically retry dead letter queue
			if s.enableDeadLetterQueue {
				s.processDeadLetterQueue(ctx, sourceDB, collectionName, targetDBName, targetCollectionName)
			}
		}
	}
}

func (s *MongoDBSyncer) processBufferedChanges(ctx context.Context, sourceDB, collectionName, targetDBName, targetCollectionName string) {
	// Generate unique batch ID for tracking
	batchID := generateBatchID()
	batchStartTime := time.Now()

	targetColl := s.targetClient.Database(targetDBName).Collection(targetCollectionName)
	bufferPath := s.getBufferPath(sourceDB, collectionName)

	if _, err := os.Stat(bufferPath); os.IsNotExist(err) {
		return
	}

	// === STEP 1: File Selection ===
	step1StartTime := time.Now()
	s.logger.Debugf("[MongoDB] [BatchID:%s] Starting file selection for %s.%s",
		batchID, sourceDB, collectionName)

	selectedFiles, batchSize := s.buildSmartBatch(bufferPath)
	if len(selectedFiles) == 0 {
		return
	}

	step1Duration := time.Since(step1StartTime)
	s.logger.Debugf("[MongoDB] [BatchID:%s] File selection completed - selected %d files (%.2f MB) in %v",
		batchID, len(selectedFiles), float64(batchSize)/(1024*1024), step1Duration)

	var processedFiles []string
	var allWriteModels []mongo.WriteModel
	var writeSuccess = true

	// === STEP 2: File Parsing (Parallel) ===
	step2StartTime := time.Now()
	s.logger.Debugf("[MongoDB] [BatchID:%s] Starting parallel file parsing - %d files to process",
		batchID, len(selectedFiles))

	// Use parallel parsing for better performance
	allWriteModels, processedFiles, writeSuccess = s.parseFilesParallel(ctx, selectedFiles, sourceDB, collectionName, batchID)

	step2Duration := time.Since(step2StartTime)
	avgParseTime := time.Duration(0)
	if len(processedFiles) > 0 {
		avgParseTime = time.Duration(int64(step2Duration) / int64(len(processedFiles)))
	}
	s.logger.Debugf("[MongoDB] [BatchID:%s] Parallel file parsing completed - processed %d files, collected %d write models in %v (avg: %v/file)",
		batchID, len(processedFiles), len(allWriteModels), step2Duration, avgParseTime)

	// === STEP 3: Memory Accumulation ===
	estimatedMemoryMB := float64(len(allWriteModels)*1024) / (1024 * 1024) // Assume ~1KB per WriteModel
	s.logger.Debugf("[MongoDB] [BatchID:%s] Memory accumulation completed - %d WriteModels (estimated %.2f MB in memory)",
		batchID, len(allWriteModels), estimatedMemoryMB)

	// === STEP 4: Database Write ===
	var step4Duration time.Duration
	if writeSuccess && len(allWriteModels) > 0 {
		step4StartTime := time.Now()
		s.logger.Debugf("[MongoDB] [BatchID:%s] Starting database bulk write - %d operations to %s.%s",
			batchID, len(allWriteModels), targetDBName, targetCollectionName)

		err := s.flushWriteModels(ctx, targetColl, allWriteModels, sourceDB, collectionName)
		if err != nil {
			s.logger.Errorf("[MongoDB] [BatchID:%s] Database bulk write failed: %v", batchID, err)
			writeSuccess = false
		} else {
			step4Duration = time.Since(step4StartTime)
			opsPerSecond := float64(len(allWriteModels)) / step4Duration.Seconds()
			s.logger.Debugf("[MongoDB] [BatchID:%s] Database bulk write completed - %d operations in %v (%.2f ops/sec)",
				batchID, len(allWriteModels), step4Duration, opsPerSecond)
		}
	}

	// === STEP 5: File Cleanup ===
	var step5Duration time.Duration
	if writeSuccess && len(processedFiles) > 0 {
		step5StartTime := time.Now()
		s.logger.Debugf("[MongoDB] [BatchID:%s] Starting file cleanup - %d files to delete",
			batchID, len(processedFiles))

		deletedCount := 0
		failedDeletes := 0
		for _, filePath := range processedFiles {
			if err := os.Remove(filePath); err != nil {
				s.logger.Warnf("[MongoDB] [BatchID:%s] Failed to delete file %s: %v",
					batchID, filepath.Base(filePath), err)
				failedDeletes++
			} else {
				deletedCount++
			}
		}

		step5Duration = time.Since(step5StartTime)
		s.logger.Debugf("[MongoDB] [BatchID:%s] File cleanup completed - deleted %d files, failed %d in %v",
			batchID, deletedCount, failedDeletes, step5Duration)
	}

	// === BATCH SUMMARY ===
	totalDuration := time.Since(batchStartTime)

	// Calculate remaining files
	remainingFiles := 0
	if remainingFileList, err := os.ReadDir(bufferPath); err == nil {
		remainingFiles = len(remainingFileList)
	}

	// Performance metrics
	throughputMBps := float64(batchSize) / totalDuration.Seconds() / (1024 * 1024)
	operationsPerSecond := float64(len(allWriteModels)) / totalDuration.Seconds()

	if writeSuccess {
		s.logger.Debugf("[MongoDB] [BatchID:%s] Batch completed successfully - processed %d files, %d operations in %v (%.2f MB/s, %.2f ops/sec)",
			batchID, len(processedFiles), len(allWriteModels), totalDuration, throughputMBps, operationsPerSecond)
		s.logger.Debugf("[MongoDB] [BatchID:%s] Remaining files: %d", batchID, remainingFiles)
	} else {
		s.logger.Errorf("[MongoDB] [BatchID:%s] Batch failed - keeping %d files for retry (total time: %v)",
			batchID, len(selectedFiles), totalDuration)
	}

}

// parseFilesParallel parses multiple files in parallel using worker goroutines
func (s *MongoDBSyncer) parseFilesParallel(ctx context.Context, selectedFiles []string, sourceDB, collectionName, batchID string) ([]mongo.WriteModel, []string, bool) {
	// Determine optimal worker count based on CPU cores and file count
	workerCount := runtime.NumCPU()
	if workerCount > 8 {
		workerCount = 8 // Cap at 8 workers to avoid too much contention
	}
	if len(selectedFiles) < workerCount {
		workerCount = len(selectedFiles) // Don't create more workers than files
	}

	s.logger.Debugf("[MongoDB] [BatchID:%s] Using %d parallel workers to parse %d files",
		batchID, workerCount, len(selectedFiles))

	// Create channels for job distribution and result collection
	jobs := make(chan FileParseJob, len(selectedFiles))
	results := make(chan FileParseResult, len(selectedFiles))

	// Start worker goroutines
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			s.fileParseWorker(ctx, workerID, jobs, results, batchID)
		}(i)
	}

	// Send jobs to workers
	for i, filePath := range selectedFiles {
		jobs <- FileParseJob{
			FilePath:       filePath,
			FileIndex:      i,
			TotalFiles:     len(selectedFiles),
			SourceDB:       sourceDB,
			CollectionName: collectionName,
		}
	}
	close(jobs)

	// Wait for all workers to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	fileResults := make([]FileParseResult, len(selectedFiles))
	var allWriteModels []mongo.WriteModel
	var processedFiles []string
	writeSuccess := true
	totalParseTime := time.Duration(0)
	maxParseTime := time.Duration(0)
	minParseTime := time.Duration(1<<63 - 1)

	for result := range results {
		fileResults[result.FileIndex] = result

		if result.Error != nil {
			s.logger.Errorf("[MongoDB] [BatchID:%s] Parallel parsing failed for file %s (index %d): %v",
				batchID, filepath.Base(result.FilePath), result.FileIndex, result.Error)
			writeSuccess = false
		} else {
			allWriteModels = append(allWriteModels, result.WriteModels...)
			processedFiles = append(processedFiles, result.FilePath)
			totalParseTime += result.ParseTime

			if result.ParseTime > maxParseTime {
				maxParseTime = result.ParseTime
			}
			if result.ParseTime < minParseTime {
				minParseTime = result.ParseTime
			}

			// Log individual file parsing time
			s.logger.Debugf("[MongoDB] [BatchID:%s] Parsed file %s (%d/%d) - %d models in %v",
				batchID, filepath.Base(result.FilePath), result.FileIndex+1, len(selectedFiles),
				len(result.WriteModels), result.ParseTime)
		}
	}

	// Calculate statistics
	avgParseTime := time.Duration(0)
	if len(processedFiles) > 0 {
		avgParseTime = totalParseTime / time.Duration(len(processedFiles))
	}

	s.logger.Debugf("[MongoDB] [BatchID:%s] Parallel parsing stats: processed=%d, avg=%v, min=%v, max=%v",
		batchID, len(processedFiles), avgParseTime, minParseTime, maxParseTime)

	return allWriteModels, processedFiles, writeSuccess
}

// fileParseWorker is a worker goroutine that processes file parsing jobs
func (s *MongoDBSyncer) fileParseWorker(ctx context.Context, workerID int, jobs <-chan FileParseJob, results chan<- FileParseResult, batchID string) {
	s.logger.Debugf("[MongoDB] [BatchID:%s] Starting file parse worker %d", batchID, workerID)

	for job := range jobs {
		select {
		case <-ctx.Done():
			// Context cancelled, send error result
			results <- FileParseResult{
				FilePath:  job.FilePath,
				FileIndex: job.FileIndex,
				Error:     ctx.Err(),
			}
			return
		default:
			// Process the file
			startTime := time.Now()
			writeModels, err := s.parseFileToWriteModels(ctx, job.FilePath, job.SourceDB, job.CollectionName)
			parseTime := time.Since(startTime)

			result := FileParseResult{
				FilePath:    job.FilePath,
				FileIndex:   job.FileIndex,
				WriteModels: writeModels,
				ParseTime:   parseTime,
				Error:       err,
			}

			results <- result
		}
	}

	s.logger.Debugf("[MongoDB] [BatchID:%s] File parse worker %d completed", batchID, workerID)
}

// parseFileToWriteModels parses a file and returns all WriteModels without executing them
func (s *MongoDBSyncer) parseFileToWriteModels(ctx context.Context, filePath string, sourceDB, collectionName string) ([]mongo.WriteModel, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// Set buffer size to 100MB to handle large BSON documents
	bufferSize := 100 * 1024 * 1024 // 100MB
	scanner.Buffer(make([]byte, bufferSize), bufferSize)
	// Set the scanner to use our custom split function.
	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}
		if i := bytes.Index(data, bsonRawSeparator); i >= 0 {
			// We have a full event followed by a separator.
			return i + len(bsonRawSeparator), data[0:i], nil
		}
		// If we're at EOF, we have a final, non-terminated event. Return it.
		if atEOF {
			return len(data), data, nil
		}
		// Request more data.
		return 0, nil, nil
	})

	var writeModels []mongo.WriteModel

	for scanner.Scan() {
		eventData := scanner.Bytes()
		if len(eventData) == 0 {
			continue
		}

		model := s.convertRawBSONToWriteModel(eventData, sourceDB, collectionName)
		if model != nil {
			writeModels = append(writeModels, model)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading from file stream: %w", err)
	}

	s.logger.Debugf("[MongoDB] Parsed file %s: extracted %d write models",
		filepath.Base(filePath), len(writeModels))

	return writeModels, nil
}

// Legacy method for backward compatibility - now deprecated
func (s *MongoDBSyncer) processFileAsStream(ctx context.Context, filePath string, targetColl *mongo.Collection, sourceDB, collectionName string) error {
	s.logger.Warnf("[MongoDB] DEPRECATED: processFileAsStream is deprecated, use parseFileToWriteModels + flushWriteModels instead")

	writeModels, err := s.parseFileToWriteModels(ctx, filePath, sourceDB, collectionName)
	if err != nil {
		return err
	}

	if len(writeModels) > 0 {
		return s.flushWriteModels(ctx, targetColl, writeModels, sourceDB, collectionName)
	}

	return nil
}

func (s *MongoDBSyncer) convertRawBSONToWriteModel(rawData bson.Raw, sourceDB, collectionName string) mongo.WriteModel {
	var event bson.M
	if err := bson.Unmarshal(rawData, &event); err != nil {
		s.logger.Errorf("[MongoDB] Failed to unmarshal raw BSON event: %v", err)
		return nil
	}

	opType, _ := event["operationType"].(string)
	if opType == "" {
		s.logger.Warnf("[MongoDB] Operation type is missing from BSON event")
		return nil
	}

	// This is a simplified conversion. A full implementation would need the logic from the old `convertToWriteModel`.
	// For now, we'll just handle insert as an example.
	switch opType {
	case "insert":
		if fullDoc, ok := event["fullDocument"]; ok {
			return mongo.NewInsertOneModel().SetDocument(fullDoc)
		}
	case "update", "replace":
		var docID interface{}
		if dk, ok := event["documentKey"].(bson.M); ok {
			docID = dk["_id"]
		} else {
			return nil
		}
		if fullDoc, ok := event["fullDocument"]; ok {
			return mongo.NewReplaceOneModel().SetFilter(bson.M{"_id": docID}).SetReplacement(fullDoc).SetUpsert(true)
		}
	case "delete":
		var docID interface{}
		if dk, ok := event["documentKey"].(bson.M); ok {
			docID = dk["_id"]
		} else {
			return nil
		}
		return mongo.NewDeleteOneModel().SetFilter(bson.M{"_id": docID})
	}

	s.logger.Warnf("[MongoDB] Unhandled operation type '%s' in convertRawBSONToWriteModel", opType)
	return nil
}

func (s *MongoDBSyncer) flushWriteModels(ctx context.Context, targetColl *mongo.Collection, models []mongo.WriteModel, sourceDB, collectionName string) error {
	if len(models) == 0 {
		return nil
	}

	// First attempt: Try bulk write with unordered operations
	err := utils.RetryMongoOperation(ctx, s.logger, fmt.Sprintf("BulkWrite to %s.%s",
		targetColl.Database().Name(), targetColl.Name()),
		func() error {
			res, err := targetColl.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
			if err != nil {
				return err
			}
			s.logger.Debugf(
				"[MongoDB] BulkWrite => table=%s.%s inserted=%d matched=%d modified=%d upserted=%d deleted=%d",
				targetColl.Database().Name(),
				targetColl.Name(),
				res.InsertedCount,
				res.MatchedCount,
				res.ModifiedCount,
				res.UpsertedCount,
				res.DeletedCount,
			)
			return nil
		})

	if err == nil {
		// Bulk write succeeded completely
		return nil
	}

	// Check if it's a bulk write error with write errors
	if bulkWriteErr, ok := err.(mongo.BulkWriteException); ok {
		s.logger.Warnf("[MongoDB] BulkWrite partially failed for %s.%s: %d write errors out of %d operations",
			sourceDB, collectionName, len(bulkWriteErr.WriteErrors), len(models))

		// Log detailed error information
		for _, writeErr := range bulkWriteErr.WriteErrors {
			s.logger.Warnf("[MongoDB] Write error at index %d: code=%d, message=%s",
				writeErr.Index, writeErr.Code, writeErr.Message)
		}

		// Handle specific error types
		return s.handleBulkWriteErrors(ctx, targetColl, models, bulkWriteErr, sourceDB, collectionName)
	}

	// For other types of errors, try individual operations
	s.logger.Warnf("[MongoDB] BulkWrite failed with non-bulk error for %s.%s: %v, falling back to individual operations",
		sourceDB, collectionName, err)

	return s.handleBulkWriteWithIndividualOps(ctx, targetColl, models, sourceDB, collectionName)
}

// handleBulkWriteErrors handles specific bulk write errors with intelligent recovery
func (s *MongoDBSyncer) handleBulkWriteErrors(ctx context.Context, targetColl *mongo.Collection, models []mongo.WriteModel, bulkErr mongo.BulkWriteException, sourceDB, collectionName string) error {
	// Create a map of failed indices for quick lookup
	failedIndices := make(map[int]bool)
	errorMap := make(map[int]mongo.BulkWriteError)

	for _, writeErr := range bulkErr.WriteErrors {
		failedIndices[writeErr.Index] = true
		errorMap[writeErr.Index] = writeErr
	}

	// Separate successful and failed operations
	var successfulModels []mongo.WriteModel
	var failedModels []mongo.WriteModel
	var failedErrors []mongo.BulkWriteError

	for i, model := range models {
		if failedIndices[i] {
			failedModels = append(failedModels, model)
			if err, exists := errorMap[i]; exists {
				failedErrors = append(failedErrors, err)
			}
		} else {
			successfulModels = append(successfulModels, model)
		}
	}

	s.logger.Infof("[MongoDB] Bulk write analysis for %s.%s: %d successful, %d failed",
		sourceDB, collectionName, len(successfulModels), len(failedModels))

	// Try to retry failed operations individually
	var stillFailedModels []mongo.WriteModel
	var stillFailedErrors []mongo.BulkWriteError

	if len(failedModels) > 0 {
		s.logger.Infof("[MongoDB] Attempting to retry %d failed operations individually for %s.%s",
			len(failedModels), sourceDB, collectionName)

		retrySuccess, retryFailedModels, retryFailedErrors := s.retryFailedOperationsWithDetails(ctx, targetColl, failedModels, failedErrors, sourceDB, collectionName)

		if retrySuccess > 0 {
			s.logger.Infof("[MongoDB] Successfully retried %d operations for %s.%s",
				retrySuccess, sourceDB, collectionName)
		}

		stillFailedModels = retryFailedModels
		stillFailedErrors = retryFailedErrors

		if len(stillFailedModels) > 0 {
			s.logger.Warnf("[MongoDB] %d operations still failed after retry for %s.%s",
				len(stillFailedModels), sourceDB, collectionName)
		}
	}

	// Move still failed operations to dead letter queue
	if len(stillFailedModels) > 0 && s.enableDeadLetterQueue {
		err := s.storeToDeadLetterQueue(stillFailedModels, stillFailedErrors, len(models), len(successfulModels), sourceDB, collectionName)
		if err != nil {
			s.logger.Errorf("[MongoDB] Failed to store failed operations to dead letter queue: %v", err)
		} else {
			s.logger.Infof("[MongoDB] Moved %d failed operations to dead letter queue for %s.%s",
				len(stillFailedModels), sourceDB, collectionName)
		}
	}

	// Always consider the operation successful if we processed the data
	// Failed operations are safely stored in dead letter queue
	s.logger.Infof("[MongoDB] Bulk write completed for %s.%s: %d successful, %d moved to dead letter queue",
		sourceDB, collectionName, len(successfulModels), len(stillFailedModels))

	return nil // Always return success - failed data is in dead letter queue
}

// storeToDeadLetterQueue stores failed operations to dead letter queue
func (s *MongoDBSyncer) storeToDeadLetterQueue(failedModels []mongo.WriteModel, failedErrors []mongo.BulkWriteError, totalOps, successfulOps int, sourceDB, collectionName string) error {
	if !s.enableDeadLetterQueue {
		return nil
	}

	// Create dead letter directory for this collection
	collectionDir := filepath.Join(s.deadLetterDir, fmt.Sprintf("%s_%s", sourceDB, collectionName))
	if err := os.MkdirAll(collectionDir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create dead letter collection directory: %w", err)
	}

	// Convert failed operations to serializable format
	var failedOps []FailedOperation
	for i, model := range failedModels {
		// Serialize WriteModel to JSON
		modelBytes, err := s.serializeWriteModel(model)
		if err != nil {
			s.logger.Warnf("[MongoDB] Failed to serialize WriteModel: %v", err)
			continue
		}

		// Get error information
		var errorMsg string
		var errorCode int
		if i < len(failedErrors) {
			errorMsg = failedErrors[i].Message
			errorCode = failedErrors[i].Code
		}

		// Determine operation type
		opType := s.getOperationType(model)

		failedOp := FailedOperation{
			ID:         fmt.Sprintf("%s_%d_%d", sourceDB, time.Now().UnixNano(), i),
			WriteModel: modelBytes,
			Error:      errorMsg,
			ErrorCode:  errorCode,
			OpType:     opType,
			SourceDB:   sourceDB,
			SourceColl: collectionName,
			Timestamp:  time.Now(),
			RetryCount: 0,
			BatchIndex: i,
		}

		failedOps = append(failedOps, failedOp)
	}

	// Create dead letter batch
	batchID := fmt.Sprintf("batch_%s_%s_%d", sourceDB, collectionName, time.Now().UnixNano())
	deadLetterBatch := DeadLetterBatch{
		BatchID:       batchID,
		FailedOps:     failedOps,
		TotalOps:      totalOps,
		SuccessfulOps: successfulOps,
		Timestamp:     time.Now(),
		SourceDB:      sourceDB,
		SourceColl:    collectionName,
	}

	// Save to file
	fileName := fmt.Sprintf("%s.json", batchID)
	filePath := filepath.Join(collectionDir, fileName)

	batchBytes, err := json.MarshalIndent(deadLetterBatch, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal dead letter batch: %w", err)
	}

	if err := os.WriteFile(filePath, batchBytes, 0644); err != nil {
		return fmt.Errorf("failed to write dead letter batch file: %w", err)
	}

	s.logger.Infof("[MongoDB] Stored %d failed operations to dead letter queue: %s", len(failedOps), filePath)

	// Send Slack notification for dead letter queue storage
	if s.globalConfig != nil {
		slackNotifier := utils.NewSlackNotifierFromConfigWithFieldLogger(s.globalConfig, s.logger)
		if slackNotifier.IsConfigured() {
			message := fmt.Sprintf("🚨 MongoDB Operations Failed - Dead Letter Queue\n\nDatabase: %s.%s\nFailed Operations: %d\nTotal Operations: %d\nSuccess Rate: %.1f%%\n\nFile: %s",
				sourceDB, collectionName, len(failedOps), totalOps,
				float64(successfulOps)/float64(totalOps)*100, fileName)

			// Send notification asynchronously to avoid blocking main process
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				err := slackNotifier.SendError(ctx, "MongoDB Dead Letter Queue", message)
				if err != nil {
					s.logger.Warnf("[MongoDB] Failed to send dead letter queue Slack notification: %v", err)
				}
			}()
		}
	}

	return nil
}

// serializeWriteModel converts a WriteModel to JSON for storage
func (s *MongoDBSyncer) serializeWriteModel(model mongo.WriteModel) (json.RawMessage, error) {
	switch m := model.(type) {
	case *mongo.InsertOneModel:
		data := map[string]interface{}{
			"type":     "insert",
			"document": m.Document,
		}
		return json.Marshal(data)
	case *mongo.UpdateOneModel:
		data := map[string]interface{}{
			"type":   "update",
			"filter": m.Filter,
			"update": m.Update,
			"upsert": true, // Default to upsert for safety
		}
		return json.Marshal(data)
	case *mongo.ReplaceOneModel:
		data := map[string]interface{}{
			"type":        "replace",
			"filter":      m.Filter,
			"replacement": m.Replacement,
			"upsert":      true, // Default to upsert for safety
		}
		return json.Marshal(data)
	case *mongo.DeleteOneModel:
		data := map[string]interface{}{
			"type":   "delete",
			"filter": m.Filter,
		}
		return json.Marshal(data)
	default:
		return nil, fmt.Errorf("unsupported WriteModel type: %T", model)
	}
}

// getOperationType returns the operation type string for a WriteModel
func (s *MongoDBSyncer) getOperationType(model mongo.WriteModel) string {
	switch model.(type) {
	case *mongo.InsertOneModel:
		return "insert"
	case *mongo.UpdateOneModel:
		return "update"
	case *mongo.ReplaceOneModel:
		return "replace"
	case *mongo.DeleteOneModel:
		return "delete"
	default:
		return "unknown"
	}
}

// retryFailedOperationsWithDetails retries failed operations individually and returns detailed results
func (s *MongoDBSyncer) retryFailedOperationsWithDetails(ctx context.Context, targetColl *mongo.Collection, failedModels []mongo.WriteModel, failedErrors []mongo.BulkWriteError, sourceDB, collectionName string) (int, []mongo.WriteModel, []mongo.BulkWriteError) {
	successCount := 0
	var stillFailedModels []mongo.WriteModel
	var stillFailedErrors []mongo.BulkWriteError

	for i, model := range failedModels {
		opType := s.getOperationType(model)
		err := s.executeIndividualOperation(ctx, targetColl, model)

		if err != nil {
			stillFailedModels = append(stillFailedModels, model)
			if i < len(failedErrors) {
				stillFailedErrors = append(stillFailedErrors, failedErrors[i])
			}

			// Log specific error information
			if i < len(failedErrors) {
				s.logger.Warnf("[MongoDB] Individual retry failed for %s operation: code=%d, message=%s, error=%v",
					opType, failedErrors[i].Code, failedErrors[i].Message, err)
			} else {
				s.logger.Warnf("[MongoDB] Individual retry failed for %s operation: %v", opType, err)
			}
		} else {
			successCount++
			s.logger.Debugf("[MongoDB] Individual retry succeeded for %s operation", opType)
		}
	}

	return successCount, stillFailedModels, stillFailedErrors
}

// executeIndividualOperation executes a single WriteModel operation
func (s *MongoDBSyncer) executeIndividualOperation(ctx context.Context, targetColl *mongo.Collection, model mongo.WriteModel) error {
	switch m := model.(type) {
	case *mongo.InsertOneModel:
		_, err := targetColl.InsertOne(ctx, m.Document)
		return err
	case *mongo.UpdateOneModel:
		_, err := targetColl.UpdateOne(ctx, m.Filter, m.Update, options.Update().SetUpsert(true))
		return err
	case *mongo.ReplaceOneModel:
		_, err := targetColl.ReplaceOne(ctx, m.Filter, m.Replacement, options.Replace().SetUpsert(true))
		return err
	case *mongo.DeleteOneModel:
		_, err := targetColl.DeleteOne(ctx, m.Filter)
		return err
	default:
		return fmt.Errorf("unsupported WriteModel type: %T", model)
	}
}

// retryFailedOperations retries failed operations individually with specific error handling
func (s *MongoDBSyncer) retryFailedOperations(ctx context.Context, targetColl *mongo.Collection, failedModels []mongo.WriteModel, failedErrors []mongo.BulkWriteError, sourceDB, collectionName string) (int, int) {
	successCount, stillFailedModels, _ := s.retryFailedOperationsWithDetails(ctx, targetColl, failedModels, failedErrors, sourceDB, collectionName)
	return successCount, len(stillFailedModels)
}

// handleBulkWriteWithIndividualOps handles cases where bulk write fails completely
func (s *MongoDBSyncer) handleBulkWriteWithIndividualOps(ctx context.Context, targetColl *mongo.Collection, models []mongo.WriteModel, sourceDB, collectionName string) error {
	s.logger.Infof("[MongoDB] Falling back to individual operations for %s.%s: %d operations",
		sourceDB, collectionName, len(models))

	successCount := 0
	var failedModels []mongo.WriteModel

	for i, model := range models {
		opType := s.getOperationType(model)
		err := s.executeIndividualOperation(ctx, targetColl, model)

		if err != nil {
			failedModels = append(failedModels, model)
			s.logger.Warnf("[MongoDB] Individual operation failed for %s operation at index %d: %v",
				opType, i, err)
		} else {
			successCount++
		}
	}

	s.logger.Infof("[MongoDB] Individual operations completed for %s.%s: %d successful, %d failed",
		sourceDB, collectionName, successCount, len(failedModels))

	// Store failed operations to dead letter queue
	if len(failedModels) > 0 && s.enableDeadLetterQueue {
		// Create empty error slice for failed operations (no specific bulk write errors)
		failedErrors := make([]mongo.BulkWriteError, len(failedModels))
		err := s.storeToDeadLetterQueue(failedModels, failedErrors, len(models), successCount, sourceDB, collectionName)
		if err != nil {
			s.logger.Errorf("[MongoDB] Failed to store failed operations to dead letter queue: %v", err)
		} else {
			s.logger.Infof("[MongoDB] Moved %d failed operations to dead letter queue for %s.%s",
				len(failedModels), sourceDB, collectionName)
		}
	}

	// Always consider successful - failed operations are in dead letter queue
	return nil
}

func (s *MongoDBSyncer) loadMongoDBResumeToken(db, coll string) bson.Raw {
	key := fmt.Sprintf("%s.%s", db, coll)

	s.resumeTokensM.RLock()
	if token, exists := s.resumeTokens[key]; exists {
		s.resumeTokensM.RUnlock()
		return token
	}
	s.resumeTokensM.RUnlock()

	if s.cfg.MongoDBResumeTokenPath == "" {
		return nil
	}

	path := s.getResumeTokenPath(db, coll)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	if len(data) <= 1 {
		return nil
	}

	var token bson.Raw
	if errU := json.Unmarshal(data, &token); errU != nil {
		s.logger.Errorf("[MongoDB] unmarshal resume token fail => %v", errU)
		s.removeMongoDBResumeToken(db, coll)
		return nil
	}

	s.resumeTokensM.Lock()
	s.resumeTokens[key] = token
	s.resumeTokensM.Unlock()

	return token
}

func (s *MongoDBSyncer) saveMongoDBResumeToken(db, coll string, token bson.Raw) {
	if token == nil {
		return
	}

	key := fmt.Sprintf("%s.%s", db, coll)
	s.resumeTokensM.Lock()
	s.resumeTokens[key] = token
	s.resumeTokensM.Unlock()

	if s.cfg.MongoDBResumeTokenPath == "" {
		return
	}

	path := s.getResumeTokenPath(db, coll)
	data, err := json.Marshal(token)
	if err != nil {
		s.logger.Errorf("[MongoDB] marshal resume token fail => %v", err)
		return
	}
	if errW := os.WriteFile(path, data, 0644); errW != nil {
		s.logger.Errorf("[MongoDB] write resume token file => %v", errW)
	}
}

func (s *MongoDBSyncer) removeMongoDBResumeToken(db, coll string) {
	if s.cfg.MongoDBResumeTokenPath == "" {
		return
	}
	path := s.getResumeTokenPath(db, coll)
	_ = os.Remove(path)
	s.resumeTokensM.Lock()
	delete(s.resumeTokens, fmt.Sprintf("%s.%s", db, coll))
	s.resumeTokensM.Unlock()
	s.logger.Infof("[MongoDB] removed invalid resume token => %s", path)
}

func (s *MongoDBSyncer) getResumeTokenPath(db, coll string) string {
	fileName := fmt.Sprintf("%s_%s.json", db, coll)
	return filepath.Join(s.cfg.MongoDBResumeTokenPath, fileName)
}

func (s *MongoDBSyncer) getBufferPath(db, coll string) string {
	return filepath.Join(s.bufferDir, fmt.Sprintf("%s_%s", db, coll))
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

// buildSmartBatch builds a batch of files that doesn't exceed the target memory size
func (s *MongoDBSyncer) buildSmartBatch(bufferPath string) ([]string, int64) {
	files, err := os.ReadDir(bufferPath)
	if err != nil {
		s.logger.Errorf("[MongoDB] Failed to read buffer directory %s: %v", bufferPath, err)
		return nil, 0
	}

	if len(files) == 0 {
		return nil, 0
	}

	var selectedFiles []string
	currentBatchSize := int64(0)

	s.logger.Debugf("[MongoDB] Building smart batch from %d files, target size: %.2f MB",
		len(files), float64(s.targetBatchSizeBytes)/(1024*1024))

	for _, file := range files {
		filePath := filepath.Join(bufferPath, file.Name())
		info, err := os.Stat(filePath)
		if err != nil {
			s.logger.Warnf("[MongoDB] Failed to stat file %s: %v", filePath, err)
			continue
		}

		fileSize := info.Size()

		// Check if adding this file would exceed the target size
		if currentBatchSize+fileSize > s.targetBatchSizeBytes && len(selectedFiles) >= s.minFilesPerBatch {
			s.logger.Debugf("[MongoDB] Stopping batch construction: adding file would exceed target size (current: %.2f MB + file: %.2f MB > target: %.2f MB)",
				float64(currentBatchSize)/(1024*1024), float64(fileSize)/(1024*1024), float64(s.targetBatchSizeBytes)/(1024*1024))
			break
		}

		// Check if we've reached the maximum file count limit
		if len(selectedFiles) >= s.maxFilesPerBatch {
			s.logger.Debugf("[MongoDB] Stopping batch construction: reached max files per batch (%d)", s.maxFilesPerBatch)
			break
		}

		selectedFiles = append(selectedFiles, filePath)
		currentBatchSize += fileSize

		s.logger.Debugf("[MongoDB] Added file %s (%.2f KB) to batch, total: %d files, %.2f MB",
			file.Name(), float64(fileSize)/1024, len(selectedFiles), float64(currentBatchSize)/(1024*1024))
	}

	if len(selectedFiles) > 0 {
		s.logger.Debugf("[MongoDB] Built smart batch: %d files, %.2f MB (%.1f%% of target)",
			len(selectedFiles), float64(currentBatchSize)/(1024*1024),
			float64(currentBatchSize)/float64(s.targetBatchSizeBytes)*100)
	} else {
		s.logger.Debugf("[MongoDB] No files selected for batch")
	}

	return selectedFiles, currentBatchSize
}

// calculateAverageFileSize calculates the average file size in the buffer directory
func (s *MongoDBSyncer) calculateAverageFileSize(bufferPath string) int64 {
	files, err := os.ReadDir(bufferPath)
	if err != nil || len(files) == 0 {
		return 0
	}

	// Intelligent sampling: take first 20 files or all files (if less than 20)
	sampleSize := 20
	if sampleSize > len(files) {
		sampleSize = len(files)
	}

	totalSize := int64(0)
	validFiles := 0

	for i := 0; i < sampleSize; i++ {
		filePath := filepath.Join(bufferPath, files[i].Name())
		if info, err := os.Stat(filePath); err == nil {
			totalSize += info.Size()
			validFiles++
		}
	}

	if validFiles == 0 {
		return 0
	}

	avgSize := totalSize / int64(validFiles)
	s.logger.Debugf("[MongoDB] Calculated average file size: %.2f KB (sampled %d files)",
		float64(avgSize)/1024, validFiles)

	return avgSize
}

// updateBatchSizeConfig allows dynamic adjustment of batch size parameters
func (s *MongoDBSyncer) updateBatchSizeConfig(targetSizeBytes int64, maxFiles, minFiles int) {
	if targetSizeBytes > 0 {
		s.targetBatchSizeBytes = targetSizeBytes
	}
	if maxFiles > 0 {
		s.maxFilesPerBatch = maxFiles
	}
	if minFiles > 0 {
		s.minFilesPerBatch = minFiles
	}

	s.logger.Infof("[MongoDB] Updated batch size config: target=%.2f MB, maxFiles=%d, minFiles=%d",
		float64(s.targetBatchSizeBytes)/(1024*1024), s.maxFilesPerBatch, s.minFilesPerBatch)
}

// getBatchSizeConfig returns current batch size configuration
func (s *MongoDBSyncer) getBatchSizeConfig() (int64, int, int) {
	return s.targetBatchSizeBytes, s.maxFilesPerBatch, s.minFilesPerBatch
}

// estimateOptimalBatchSize estimates optimal batch size based on buffer directory statistics
func (s *MongoDBSyncer) estimateOptimalBatchSize(bufferPath string) {
	avgFileSize := s.calculateAverageFileSize(bufferPath)
	if avgFileSize == 0 {
		return
	}

	// Estimate how many files would fit in target batch size
	estimatedFileCount := s.targetBatchSizeBytes / avgFileSize

	s.logger.Debugf("[MongoDB] Batch size estimation: avgFileSize=%.2f KB, estimatedFileCount=%d, target=%.2f MB",
		float64(avgFileSize)/1024, estimatedFileCount, float64(s.targetBatchSizeBytes)/(1024*1024))

	// Adjust max files per batch if estimation is reasonable
	if estimatedFileCount > int64(s.maxFilesPerBatch) {
		s.logger.Infof("[MongoDB] Current maxFilesPerBatch (%d) might be limiting, estimated optimal: %d",
			s.maxFilesPerBatch, estimatedFileCount)
	} else if estimatedFileCount < int64(s.minFilesPerBatch) {
		s.logger.Warnf("[MongoDB] Files are very large (avg %.2f MB), might need to reduce target batch size",
			float64(avgFileSize)/(1024*1024))
	}
}

// isRecoverableError determines if a MongoDB error is recoverable and should trigger a retry
func isRecoverableError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()

	// Network-related errors that are typically recoverable
	recoverablePatterns := []string{
		"server selection timeout",
		"connection refused",
		"network timeout",
		"no reachable servers",
		"connection pool exhausted",
		"write concern timeout",
		"read concern timeout",
		"cursor not found",
		"interrupted at shutdown",
		"host unreachable",
		"connection reset",
		"broken pipe",
		"i/o timeout",
	}

	for _, pattern := range recoverablePatterns {
		if strings.Contains(strings.ToLower(errStr), strings.ToLower(pattern)) {
			return true
		}
	}

	// Check for specific MongoDB error codes that are recoverable
	if strings.Contains(errStr, "error code 11600") || // InterruptedAtShutdown
		strings.Contains(errStr, "error code 11602") || // InterruptedDueToReplStateChange
		strings.Contains(errStr, "error code 10107") || // NotMaster
		strings.Contains(errStr, "error code 189") { // PrimarySteppedDown
		return true
	}

	return false
}

// processDeadLetterQueue processes failed operations from dead letter queue
func (s *MongoDBSyncer) processDeadLetterQueue(ctx context.Context, sourceDB, collectionName, targetDBName, targetCollectionName string) {
	collectionDir := filepath.Join(s.deadLetterDir, fmt.Sprintf("%s_%s", sourceDB, collectionName))

	if _, err := os.Stat(collectionDir); os.IsNotExist(err) {
		return // No dead letter queue for this collection
	}

	files, err := os.ReadDir(collectionDir)
	if err != nil {
		s.logger.Errorf("[MongoDB] Failed to read dead letter queue directory %s: %v", collectionDir, err)
		return
	}

	if len(files) == 0 {
		return
	}

	targetColl := s.targetClient.Database(targetDBName).Collection(targetCollectionName)
	processedFiles := 0

	s.logger.Debugf("[MongoDB] Processing dead letter queue for %s.%s: %d files", sourceDB, collectionName, len(files))

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".json") {
			continue
		}

		filePath := filepath.Join(collectionDir, file.Name())
		processed := s.processDeadLetterBatch(ctx, filePath, targetColl, sourceDB, collectionName)
		if processed {
			processedFiles++
		}
	}

	if processedFiles > 0 {
		s.logger.Infof("[MongoDB] Processed %d dead letter batches for %s.%s", processedFiles, sourceDB, collectionName)
	}
}

// processDeadLetterBatch processes a single dead letter batch file
func (s *MongoDBSyncer) processDeadLetterBatch(ctx context.Context, filePath string, targetColl *mongo.Collection, sourceDB, collectionName string) bool {
	// Read and parse the dead letter batch
	batchData, err := os.ReadFile(filePath)
	if err != nil {
		s.logger.Errorf("[MongoDB] Failed to read dead letter batch file %s: %v", filePath, err)
		return false
	}

	var batch DeadLetterBatch
	if err := json.Unmarshal(batchData, &batch); err != nil {
		s.logger.Errorf("[MongoDB] Failed to unmarshal dead letter batch %s: %v", filePath, err)
		return false
	}

	// Check if any operations need retry
	var operationsToRetry []FailedOperation
	for _, op := range batch.FailedOps {
		if op.RetryCount < s.maxRetryAttempts {
			operationsToRetry = append(operationsToRetry, op)
		}
	}

	if len(operationsToRetry) == 0 {
		s.logger.Debugf("[MongoDB] No operations to retry in batch %s (all exceeded max retries)", batch.BatchID)
		return false
	}

	s.logger.Infof("[MongoDB] Retrying %d operations from dead letter batch %s", len(operationsToRetry), batch.BatchID)

	// Convert back to WriteModels and retry
	var retryModels []mongo.WriteModel
	var stillFailedOps []FailedOperation

	for _, op := range operationsToRetry {
		writeModel, err := s.deserializeWriteModel(op.WriteModel)
		if err != nil {
			s.logger.Warnf("[MongoDB] Failed to deserialize WriteModel for operation %s: %v", op.ID, err)
			op.RetryCount++
			stillFailedOps = append(stillFailedOps, op)
			continue
		}

		retryModels = append(retryModels, writeModel)
	}

	// Execute retry operations
	successCount := 0
	for i, model := range retryModels {
		err := s.executeIndividualOperation(ctx, targetColl, model)
		if err != nil {
			// Increment retry count and keep in failed list
			operationsToRetry[i].RetryCount++
			operationsToRetry[i].Error = err.Error()
			stillFailedOps = append(stillFailedOps, operationsToRetry[i])
			s.logger.Warnf("[MongoDB] Retry failed for operation %s (attempt %d/%d): %v",
				operationsToRetry[i].ID, operationsToRetry[i].RetryCount, s.maxRetryAttempts, err)
		} else {
			successCount++
			s.logger.Debugf("[MongoDB] Retry succeeded for operation %s", operationsToRetry[i].ID)
		}
	}

	// Update the batch with remaining failed operations
	batch.FailedOps = stillFailedOps

	if len(stillFailedOps) == 0 {
		// All operations succeeded, delete the file
		if err := os.Remove(filePath); err != nil {
			s.logger.Warnf("[MongoDB] Failed to delete completed dead letter batch %s: %v", filePath, err)
		} else {
			s.logger.Infof("[MongoDB] Successfully processed and deleted dead letter batch %s", batch.BatchID)
		}
		return true
	} else {
		// Update the file with remaining failed operations
		updatedData, err := json.MarshalIndent(batch, "", "  ")
		if err != nil {
			s.logger.Errorf("[MongoDB] Failed to marshal updated dead letter batch %s: %v", batch.BatchID, err)
			return false
		}

		if err := os.WriteFile(filePath, updatedData, 0644); err != nil {
			s.logger.Errorf("[MongoDB] Failed to update dead letter batch file %s: %v", filePath, err)
			return false
		}

		s.logger.Infof("[MongoDB] Updated dead letter batch %s: %d succeeded, %d still failed",
			batch.BatchID, successCount, len(stillFailedOps))
		return true
	}
}

// deserializeWriteModel converts JSON back to WriteModel
func (s *MongoDBSyncer) deserializeWriteModel(data json.RawMessage) (mongo.WriteModel, error) {
	var modelData map[string]interface{}
	if err := json.Unmarshal(data, &modelData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal model data: %w", err)
	}

	opType, ok := modelData["type"].(string)
	if !ok {
		return nil, fmt.Errorf("missing or invalid operation type")
	}

	switch opType {
	case "insert":
		document, ok := modelData["document"]
		if !ok {
			return nil, fmt.Errorf("missing document for insert operation")
		}
		return mongo.NewInsertOneModel().SetDocument(document), nil

	case "update":
		filter, ok := modelData["filter"]
		if !ok {
			return nil, fmt.Errorf("missing filter for update operation")
		}
		update, ok := modelData["update"]
		if !ok {
			return nil, fmt.Errorf("missing update for update operation")
		}
		upsert, _ := modelData["upsert"].(bool)

		updateModel := mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update)
		if upsert {
			updateModel.SetUpsert(true)
		}
		return updateModel, nil

	case "replace":
		filter, ok := modelData["filter"]
		if !ok {
			return nil, fmt.Errorf("missing filter for replace operation")
		}
		replacement, ok := modelData["replacement"]
		if !ok {
			return nil, fmt.Errorf("missing replacement for replace operation")
		}
		upsert, _ := modelData["upsert"].(bool)

		replaceModel := mongo.NewReplaceOneModel().SetFilter(filter).SetReplacement(replacement)
		if upsert {
			replaceModel.SetUpsert(true)
		}
		return replaceModel, nil

	case "delete":
		filter, ok := modelData["filter"]
		if !ok {
			return nil, fmt.Errorf("missing filter for delete operation")
		}
		return mongo.NewDeleteOneModel().SetFilter(filter), nil

	default:
		return nil, fmt.Errorf("unsupported operation type: %s", opType)
	}
}

// getDeadLetterQueueStats returns statistics about the dead letter queue
func (s *MongoDBSyncer) getDeadLetterQueueStats(sourceDB, collectionName string) (int, int, error) {
	collectionDir := filepath.Join(s.deadLetterDir, fmt.Sprintf("%s_%s", sourceDB, collectionName))

	if _, err := os.Stat(collectionDir); os.IsNotExist(err) {
		return 0, 0, nil
	}

	files, err := os.ReadDir(collectionDir)
	if err != nil {
		return 0, 0, err
	}

	totalBatches := 0
	totalFailedOps := 0

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".json") {
			continue
		}

		filePath := filepath.Join(collectionDir, file.Name())
		batchData, err := os.ReadFile(filePath)
		if err != nil {
			continue
		}

		var batch DeadLetterBatch
		if err := json.Unmarshal(batchData, &batch); err != nil {
			continue
		}

		totalBatches++
		totalFailedOps += len(batch.FailedOps)
	}

	return totalBatches, totalFailedOps, nil
}
