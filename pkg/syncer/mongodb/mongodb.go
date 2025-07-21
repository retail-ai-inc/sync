package mongodb

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/retail-ai-inc/sync/pkg/config"
	"github.com/retail-ai-inc/sync/pkg/syncer/common"
	"github.com/retail-ai-inc/sync/pkg/syncer/security"
	"github.com/retail-ai-inc/sync/pkg/utils"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type bufferedChange struct {
	model  mongo.WriteModel
	opType string
}

// rawChangeEvent represents the raw BSON data from Change Stream
type rawChangeEvent struct {
	FullDoc       []byte    // Raw BSON data from fullDocument
	DocKey        []byte    // Raw BSON data from documentKey  
	UpdateDesc    []byte    // Raw BSON data from updateDescription
	OpType        string    // Operation type
	Timestamp     time.Time // Event timestamp
}

// persistedChange represents a change that will be saved to disk
type persistedChange struct {
	ID         string    `bson:"id"`
	Token      bson.Raw  `bson:"token"`
	FullDoc    []byte    `bson:"full_doc"`    // Raw BSON data from fullDocument
	DocKey     []byte    `bson:"doc_key"`     // Raw BSON data from documentKey
	UpdateDesc []byte    `bson:"update_desc"` // Raw BSON data from updateDescription (for updates)
	OpType     string    `bson:"op_type"`
	SourceDB   string    `bson:"source_db"`
	SourceColl string    `bson:"source_coll"`
	Timestamp  time.Time `bson:"timestamp"`
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
	processRate   int // Changes per second to process
	// Fields for goroutine lifecycle management
	activeProcessors map[string]context.CancelFunc
	processorMutex   sync.RWMutex
}

func NewMongoDBSyncer(cfg config.SyncConfig, logger *logrus.Logger) *MongoDBSyncer {
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
		logger.Warnf("[MongoDB] Failed to create buffer dir %s: %v", bufferDir, err)
	}

	// Reduced processing rate to prevent memory pressure
	processRate := 3000 // Reduced from 10000 to prevent OOM
	// Custom configured rate can be implemented in the future by expanding the SyncConfig struct

	return &MongoDBSyncer{
		sourceClient:     sourceClient,
		targetClient:     targetClient,
		cfg:              cfg,
		logger:           logger.WithField("sync_task_id", cfg.ID),
		resumeTokens:     resumeMap,
		bufferDir:        bufferDir,
		bufferEnabled:    true, // Enable by default
		processRate:      processRate,
		activeProcessors: make(map[string]context.CancelFunc),
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
		go s.watchChanges(ctx, srcColl, tgtColl, sourceDBName, tableMap.SourceTable)
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
						s.logger.Infof("[MongoDB] Converting string direction '1' to int32(1) for field %s", field)
					} else if strVal == "-1" {
						fixedDirection = int32(-1)
						s.logger.Infof("[MongoDB] Converting string direction '-1' to int32(-1) for field %s", field)
					}
				} else if floatVal, isFloat := direction.(float64); isFloat {
					fixedDirection = int32(floatVal)
					s.logger.Debugf("[MongoDB] Converting float64 direction %f to int32(%d) for field %s",
						floatVal, int32(floatVal), field)
				}

				keyDoc = append(keyDoc, bson.E{Key: field, Value: fixedDirection})
				s.logger.Debugf("[MongoDB] Index key field=%s, value=%v, type=%T",
					field, fixedDirection, fixedDirection)
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

		s.logger.Infof("[MongoDB] Creating index => collection=%s, keys=%v, options=%+v",
			targetColl.Name(), keyDoc, indexOptions)

		s.logger.Debugf("[MongoDB] Source index document: %+v", idx)

		indexModel := mongo.IndexModel{
			Keys:    keyDoc,
			Options: indexOptions,
		}

		_, errC := targetColl.Indexes().CreateOne(ctx, indexModel)
		if errC != nil {
			if strings.Contains(errC.Error(), "already exists") {
				s.logger.Debugf("[MongoDB] Index %s already exists", name)
				indexesSkipped++
			} else {
				s.logger.Warnf("[MongoDB] Create index %s fail: %v, keys=%v, options=%+v",
					name, errC, keyDoc, indexOptions)
				s.logger.Warnf("[MongoDB] Failed index details: name=%s, source_document=%+v",
					name, idx)
			}
		} else {
			s.logger.Debugf("[MongoDB] Successfully created index %s for %s", name, targetColl.Name())
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
	// Ensure only one buffer processor is running for this collection
	key := fmt.Sprintf("%s.%s", sourceDB, collectionName)

	s.processorMutex.Lock()
	// Stop existing processor if any
	if cancelFunc, exists := s.activeProcessors[key]; exists {
		cancelFunc()
		delete(s.activeProcessors, key)
	}

	// Create new context for this processor
	processorCtx, cancel := context.WithCancel(ctx)
	s.activeProcessors[key] = cancel
	s.processorMutex.Unlock()

	// Ensure processor is cleaned up when function exits
	defer func() {
		s.processorMutex.Lock()
		if _, exists := s.activeProcessors[key]; exists {
			delete(s.activeProcessors, key)
		}
		s.processorMutex.Unlock()
		cancel()
	}()

	// Add connection health check timer
	connCheckTicker := time.NewTicker(5 * time.Minute)
	defer connCheckTicker.Stop()

	diagnosticTicker := time.NewTicker(1 * time.Hour)
	defer diagnosticTicker.Stop()

	// Register new ChangeStream
	utils.RegisterChangeStream(s.cfg.ID, sourceDB, collectionName)
	s.logger.Debugf("[MongoDB] Registered ChangeStream tracker for %s.%s with sync_task_id=%d", sourceDB, collectionName, s.cfg.ID)

	// Ensure ChangeStream is deregistered when function exits
	defer utils.DeactivateChangeStream(sourceDB, collectionName)

	filteredTypes := make(map[string]int)
	var previousSourceCount, previousTargetCount int64
	var previousEventGap int
	var monitorStartTime = time.Now()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-connCheckTicker.C:
				// Check target connection
				if err := utils.CheckMongoConnection(ctx, s.targetClient); err != nil {
					s.logger.Warnf("[MongoDB] Target connection check failed: %v", err)
					// Try to reconnect
					if newClient, err := utils.ReopenMongoConnection(ctx, s.logger, s.cfg.TargetConnection); err == nil {
						if s.targetClient != nil {
							_ = s.targetClient.Disconnect(ctx)
						}
						s.targetClient = newClient
						s.logger.Info("[MongoDB] Successfully reconnected to target database")
					}
				}

				// Check source connection (optional, as source connection is handled by change stream automatically)
				if err := utils.CheckMongoConnection(ctx, s.sourceClient); err != nil {
					s.logger.Warnf("[MongoDB] Source connection check failed: %v", err)
					if newClient, err := utils.ReopenMongoConnection(ctx, s.logger, s.cfg.SourceConnection); err == nil {
						if s.sourceClient != nil {
							_ = s.sourceClient.Disconnect(ctx)
						}
						s.sourceClient = newClient
						s.logger.Info("[MongoDB] Successfully reconnected to source database")
						// Note: After reconnection, change stream may need to be restarted
						return // Exit current goroutine to let the main process restart the change stream
					}
				}
			case <-diagnosticTicker.C:
				s.logger.Debugf("[MongoDB] =========== DIAGNOSTIC LOG BEGIN ===========")
				s.logger.Debugf("[MongoDB] ChangeStream monitoring %s.%s for %s", sourceDB, collectionName, time.Since(monitorStartTime))

				var sourceCount, targetCount int64
				var srcCountErr, tgtCountErr error

				sourceCount, srcCountErr = sourceColl.EstimatedDocumentCount(ctx)
				targetCount, tgtCountErr = targetColl.EstimatedDocumentCount(ctx)

				if srcCountErr != nil || tgtCountErr != nil {
					s.logger.Warnf("[MongoDB] Count error - source: %v, target: %v", srcCountErr, tgtCountErr)
				} else {
					currentGap := sourceCount - targetCount
					sourceGrowth := sourceCount - previousSourceCount
					targetGrowth := targetCount - previousTargetCount
					gapChange := currentGap - int64(previousEventGap)

					s.logger.Debugf("[MongoDB] Count stats: source=%d, target=%d, gap=%d",
						sourceCount, targetCount, currentGap)
					s.logger.Debugf("[MongoDB] Change since last check: source_growth=%d, target_growth=%d, gap_change=%d",
						sourceGrowth, targetGrowth, gapChange)

					if gapChange > 100000 && previousEventGap > 0 {
						s.logger.Warnf("[MongoDB] WARNING: Gap is growing rapidly! Increased by %d records", gapChange)
					}

					previousSourceCount = sourceCount
					previousTargetCount = targetCount
				}

				bufferPath := s.getBufferPath(sourceDB, collectionName)
				files, err := os.ReadDir(bufferPath)
				if err == nil {
					bufferSize := len(files)
					s.logger.Debugf("[MongoDB] Buffer status: files=%d", bufferSize)

					if bufferSize > 10000 {
						s.logger.Warnf("[MongoDB] Buffer files exceeding 10000, possible processing backlog")
					}

					if len(files) > 0 {
						sampleSize := 5
						if sampleSize > len(files) {
							sampleSize = len(files)
						}

						totalSize := int64(0)
						for i := 0; i < sampleSize; i++ {
							filePath := filepath.Join(bufferPath, files[i].Name())
							info, err := os.Stat(filePath)
							if err == nil {
								totalSize += info.Size()
							}
						}

						avgSize := totalSize / int64(sampleSize)
						s.logger.Debugf("[MongoDB] Buffer file sampling: avg_size=%d bytes from %d files", avgSize, sampleSize)
					}
				} else {
					s.logger.Debugf("[MongoDB] Buffer directory not found or empty: %v", err)
				}

				// Event statistics are now tracked in ChangeStreamInfo via utils package
				s.logger.Debugf("[MongoDB] Event statistics are tracked in ChangeStreamInfo and database")
				s.logger.Debugf("[MongoDB] Event types: %v", filteredTypes)

				currentProcessRate := s.processRate
				s.logger.Debugf("[MongoDB] Processing rate: %d changes/sec", currentProcessRate)

				// Gap tracking logic removed - statistics now tracked in database

				s.logger.Debugf("[MongoDB] =========== DIAGNOSTIC LOG END ===========")
			}
		}
	}()

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
		s.logger.Infof("[MongoDB] Resume token found => %s.%s", sourceDB, collectionName)
	} else {
		s.logger.Warnf("[MongoDB] No resume token for %s.%s, starting from current position", sourceDB, collectionName)
	}

	// Process any existing buffered changes before starting the change stream
	// Note: These changes were already counted as "received" when originally buffered
	s.processBufferedChanges(ctx, sourceDB, collectionName, targetColl.Database().Name(), targetColl.Name())

	s.logger.Infof("[MongoDB] Creating ChangeStream pipeline for %s.%s: %v", sourceDB, collectionName, pipeline)

	cs, err := sourceColl.Watch(ctx, pipeline, opts)
	if err != nil {
		s.logger.Errorf("[MongoDB] Failed to establish change stream for %s.%s: %v", sourceDB, collectionName, err)
		// Record ChangeStream creation failure
		utils.RecordChangeStreamError(sourceDB, collectionName, fmt.Sprintf("Failed to establish: %v", err))
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
			return
		}
	}
	s.logger.Infof("[MongoDB] Watching changes => %s.%s", sourceDB, collectionName)

	// Reduce buffer size to prevent OOM issues with large datasets
	var buffer []rawChangeEvent
	var latestToken bson.Raw // Track the latest resume token separately
	var tokenMutex sync.RWMutex
	const batchSize = 500            // Reduced from 2000 to prevent OOM
	flushInterval := time.Second * 2 // Increased flush frequency
	timer := time.NewTimer(flushInterval)
	var bufferMutex sync.Mutex

	changeStreamActive := true

	lastFlushTime := time.Now()
	var totalProcessed int

	// Separate goroutine for buffer flushing
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				bufferMutex.Lock()
				if len(buffer) > 0 {
					batchSize := len(buffer)
					s.logger.Debugf("[MongoDB] Starting buffer flush: %d items for %s.%s",
						batchSize, sourceDB, collectionName)

					flushStart := time.Now()

					if s.bufferEnabled {
						// Store to persistent buffer instead of directly flushing
						// Note: Events already counted as "received" when added to buffer
						tokenMutex.RLock()
						currentToken := latestToken
						tokenMutex.RUnlock()
						s.storeToBuffer(ctx, &buffer, sourceDB, collectionName, currentToken)
					} else {
						s.logger.Debugf("[MongoDB] flush timer => %s.%s => flushing %d ops", sourceDB, collectionName, len(buffer))
						// Note: Events already counted as "received" when added to buffer
						tokenMutex.RLock()
						currentToken := latestToken
						tokenMutex.RUnlock()
						s.flushBuffer(ctx, targetColl, &buffer, sourceDB, collectionName, currentToken)
					}

					flushDuration := time.Since(flushStart)
					totalProcessed += batchSize
					timeSinceLastFlush := time.Since(lastFlushTime)
					rate := float64(totalProcessed) / timeSinceLastFlush.Seconds()

					s.logger.Debugf("[MongoDB] Buffer flush completed in %v, rate: %.2f items/sec",
						flushDuration, rate)

					if timeSinceLastFlush > time.Minute {
						s.logger.Debugf("[MongoDB] Processing rate over last minute: %.2f items/sec", rate)
						lastFlushTime = time.Now()
						totalProcessed = 0
					}
				}
				bufferMutex.Unlock()
				timer.Reset(flushInterval)
			}
		}
	}()

	// Start a separate goroutine for processing the persistent buffer
	if s.bufferEnabled {
		go s.processPersistentBuffer(processorCtx, sourceDB, collectionName, targetColl.Database().Name(), targetColl.Name())
	}

	lastLogTime := time.Now()
	eventsSinceLastLog := 0

	for changeStreamActive {
		select {
		case <-ctx.Done():
			bufferMutex.Lock()
			if len(buffer) > 0 {
				if s.bufferEnabled {
					s.logger.Infof("[MongoDB] context done => storing %d ops to buffer for %s.%s", len(buffer), sourceDB, collectionName)
					// Note: Events already counted as "received" when added to buffer
					tokenMutex.RLock()
					currentToken := latestToken
					tokenMutex.RUnlock()
					s.storeToBuffer(ctx, &buffer, sourceDB, collectionName, currentToken)
				} else {
					s.logger.Infof("[MongoDB] context done => flush %d ops for %s.%s", len(buffer), sourceDB, collectionName)
					// Note: Events already counted as "received" when added to buffer
					tokenMutex.RLock()
					currentToken := latestToken
					tokenMutex.RUnlock()
					s.flushBuffer(ctx, targetColl, &buffer, sourceDB, collectionName, currentToken)
				}
			}
			bufferMutex.Unlock()
			cs.Close(ctx)
			return
		default:
			if cs.Next(ctx) {
				var changeEvent bson.M
				if errDec := cs.Decode(&changeEvent); errDec != nil {
					s.logger.Errorf("[MongoDB] decode event fail => %v", errDec)
					utils.RecordChangeStreamError(sourceDB, collectionName, fmt.Sprintf("decode error: %v", errDec))
					continue
				}

				// Event received - will be tracked in ChangeStreamInfo

				opType, _ := changeEvent["operationType"].(string)

				filteredTypes[opType]++
				eventsSinceLastLog++

				if time.Since(lastLogTime) > time.Hour {
					s.logger.Infof("[MongoDB] Received %d events in last hour for %s.%s, types: %v",
						eventsSinceLastLog, sourceDB, collectionName, filteredTypes)
					lastLogTime = time.Now()
					eventsSinceLastLog = 0
				}

				token := cs.ResumeToken()
				
				// Extract raw BSON data directly from change event
				rawEvent := s.extractRawBSONData(changeEvent, opType, collectionName)
				if rawEvent != nil {
					// Update latest token
					tokenMutex.Lock()
					latestToken = token
					tokenMutex.Unlock()

					bufferMutex.Lock()
					buffer = append(buffer, *rawEvent)
					bufferMutex.Unlock()

					// Count this event as received immediately when added to buffer (avoid duplicate counting)
					utils.AccumulateChangeStreamActivity(sourceDB, collectionName, 0, 1, 0, 0, 0, 0)

					s.logger.Debugf("[MongoDB][%s] table=%s.%s event extracted",
						strings.ToUpper(rawEvent.OpType),
						targetColl.Database().Name(),
						targetColl.Name(),
					)

					bufferMutex.Lock()
					buffSize := len(buffer)

					if buffSize%100 == 0 && buffSize > 0 {
						s.logger.Debugf("[MongoDB] Current buffer size: %d for %s.%s",
							buffSize, sourceDB, collectionName)

						// Memory pressure warning
						if buffSize > batchSize*1.5 {
							s.logger.Warnf("[MongoDB] High memory pressure detected: buffer size %d exceeds threshold for %s.%s",
								buffSize, sourceDB, collectionName)
						}
					}

					if buffSize >= batchSize {
						if s.bufferEnabled {
							s.logger.Debugf("[MongoDB] buffer reached %d => storing to persistent buffer => %s.%s", batchSize, sourceDB, collectionName)
							tokenMutex.RLock()
							currentToken := latestToken
							tokenMutex.RUnlock()
							s.storeToBuffer(ctx, &buffer, sourceDB, collectionName, currentToken)
						} else {
							s.logger.Debugf("[MongoDB] buffer reached %d => flush now => %s.%s", batchSize, sourceDB, collectionName)
							tokenMutex.RLock()
							currentToken := latestToken
							tokenMutex.RUnlock()
							s.flushBuffer(ctx, targetColl, &buffer, sourceDB, collectionName, currentToken)
						}
						timer.Reset(flushInterval)
					}

					// Emergency memory management: force flush if buffer gets too large
					if buffSize >= batchSize*2 {
						s.logger.Warnf("[MongoDB] Emergency buffer flush triggered: %d items for %s.%s", buffSize, sourceDB, collectionName)
						if s.bufferEnabled {
							tokenMutex.RLock()
							currentToken := latestToken
							tokenMutex.RUnlock()
							s.storeToBuffer(ctx, &buffer, sourceDB, collectionName, currentToken)
						} else {
							tokenMutex.RLock()
							currentToken := latestToken
							tokenMutex.RUnlock()
							s.flushBuffer(ctx, targetColl, &buffer, sourceDB, collectionName, currentToken)
						}
					}
					bufferMutex.Unlock()
				} else {
					s.logger.Debugf("[MongoDB] Failed to extract raw BSON data for event type %s, skipping", opType)
				}
			} else {
				if errCS := cs.Err(); errCS != nil {
					s.logger.Errorf("[MongoDB] changeStream error => %v", errCS)
					// Record ChangeStream error
					errorMsg := errCS.Error()
					utils.RecordChangeStreamError(sourceDB, collectionName, errorMsg)

					// Check for specific error types, such as ChangeStreamHistoryLost
					if strings.Contains(errorMsg, "ChangeStreamHistoryLost") {
						s.logger.Warnf("[MongoDB] History lost for %s.%s, removing resume token and rebuilding stream", sourceDB, collectionName)

						s.logger.Errorf("[MongoDB] ChangeStreamHistoryLost error details:")
						s.logger.Errorf("[MongoDB] - Operation type distribution: %v", filteredTypes)

						bufferPath := s.getBufferPath(sourceDB, collectionName)
						files, err := os.ReadDir(bufferPath)
						if err == nil {
							s.logger.Errorf("[MongoDB] - Buffer files count: %d", len(files))
						}

						var sourceCount, targetCount int64
						var srcErr, tgtErr error

						sourceCount, srcErr = sourceColl.EstimatedDocumentCount(ctx)
						targetCount, tgtErr = targetColl.EstimatedDocumentCount(ctx)

						if srcErr == nil && tgtErr == nil {
							s.logger.Errorf("[MongoDB] - Source count: %d, Target count: %d, Gap: %d",
								sourceCount, targetCount, sourceCount-targetCount)
						}

						s.removeMongoDBResumeToken(sourceDB, collectionName)

						// Close the current ChangeStream
						cs.Close(ctx)

						// Directly rebuild ChangeStream without using resumeToken
						newOpts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
						// Start monitoring from current time point rather than trying to resume
						newOpts.SetStartAtOperationTime(nil)

						newCs, newErr := sourceColl.Watch(ctx, pipeline, newOpts)
						if newErr != nil {
							s.logger.Errorf("[MongoDB] Failed to rebuild change stream after history lost: %v", newErr)
							changeStreamActive = false
						} else {
							s.logger.Infof("[MongoDB] Successfully rebuilt change stream for %s.%s after history lost", sourceDB, collectionName)
							cs = newCs
							utils.UpdateChangeStreamActivity(sourceDB, collectionName, 0, 0, 0) // Reset activity status
							// Continue monitoring without exiting the loop
							continue
						}
					} else {
						// Other types of errors, handle as usual
						changeStreamActive = false
					}
				} else {
					changeStreamActive = false
				}
			}
		}
	}

	cs.Close(ctx)
	s.logger.Infof("[MongoDB] Change stream closed, will re-establish after delay.")

	select {
	case <-ctx.Done():
		return
	case <-time.After(5 * time.Second):
	}

	// Add retry mechanism for source database restart issues
	// Retry up to 5 times with 10 second intervals
	for retry := 1; retry <= 5; retry++ {
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
		}

		s.logger.Infof("[MongoDB] Attempting to re-establish ChangeStream for %s.%s (attempt %d/5)",
			sourceDB, collectionName, retry)

		// Check source connection before attempting to create ChangeStream
		if err := utils.CheckMongoConnection(ctx, s.sourceClient); err != nil {
			s.logger.Warnf("[MongoDB] Source connection check failed on retry %d: %v", retry, err)
			// Try to reconnect source client
			if newClient, err := utils.ReopenMongoConnection(ctx, s.logger, s.cfg.SourceConnection); err == nil {
				if s.sourceClient != nil {
					_ = s.sourceClient.Disconnect(ctx)
				}
				s.sourceClient = newClient
				sourceColl = s.sourceClient.Database(sourceDB).Collection(collectionName)
				s.logger.Infof("[MongoDB] Successfully reconnected source client on retry %d", retry)
			} else {
				s.logger.Errorf("[MongoDB] Failed to reconnect source client on retry %d: %v", retry, err)
				continue // Try next retry
			}
		}

		// Attempt to re-establish ChangeStream
		opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
		resumeToken := s.loadMongoDBResumeToken(sourceDB, collectionName)
		if resumeToken != nil {
			opts.SetResumeAfter(resumeToken)
			s.logger.Infof("[MongoDB] Re-establishing ChangeStream with resume token (attempt %d/5)", retry)
		} else {
			s.logger.Warnf("[MongoDB] No resume token available, starting from current position (attempt %d/5)", retry)
		}

		pipeline := mongo.Pipeline{
			{{Key: "$match", Value: bson.D{
				{Key: "ns.db", Value: sourceDB},
				{Key: "ns.coll", Value: collectionName},
				{Key: "operationType", Value: bson.M{"$in": []string{"insert", "update", "replace", "delete"}}},
			}}},
		}

		newCs, newErr := sourceColl.Watch(ctx, pipeline, opts)
		if newErr != nil {
			s.logger.Errorf("[MongoDB] Failed to re-establish ChangeStream on attempt %d/5: %v", retry, newErr)

			// If resume token fails, try without it
			if resumeToken != nil {
				s.logger.Warnf("[MongoDB] Retrying without resume token (attempt %d/5)", retry)
				s.removeMongoDBResumeToken(sourceDB, collectionName)
				opts = options.ChangeStream().SetFullDocument(options.UpdateLookup)
				newCs, newErr = sourceColl.Watch(ctx, pipeline, opts)

				if newErr != nil {
					s.logger.Errorf("[MongoDB] Failed to re-establish ChangeStream without resume token (attempt %d/5): %v", retry, newErr)
					continue // Try next retry
				}
			} else {
				continue // Try next retry
			}
		}

		// Success - close test stream and restart monitoring
		s.logger.Infof("[MongoDB] Successfully re-established ChangeStream for %s.%s on attempt %d/5",
			sourceDB, collectionName, retry)
		newCs.Close(ctx)

		// Restart the entire watchChanges function
		s.watchChanges(ctx, sourceColl, targetColl, sourceDB, collectionName)
		return
	}

	// If all retries failed, log error and exit
	s.logger.Errorf("[MongoDB] Failed to re-establish ChangeStream for %s.%s after 5 attempts, giving up",
		sourceDB, collectionName)
}

func (s *MongoDBSyncer) prepareWriteModel(doc bson.M, collName, opType string) mongo.WriteModel {
	tableSecurity := security.FindTableSecurityFromMappings(collName, s.cfg.Mappings)
	advancedSettings := s.findTableAdvancedSettings(collName)

	switch opType {
	case "insert":
		if fullDoc, ok := doc["fullDocument"].(bson.M); ok {
			if tableSecurity.SecurityEnabled && len(tableSecurity.FieldSecurity) > 0 {
				for field, value := range fullDoc {
					processedValue := security.ProcessValue(value, field, tableSecurity)
					fullDoc[field] = processedValue
				}
			}
			return mongo.NewInsertOneModel().SetDocument(fullDoc)
		}
		s.logger.Errorf("[MongoDB] Insert operation failed: fullDocument not found in change event for %s", collName)

	case "update", "replace":
		var docID interface{}
		if id, ok := doc["documentKey"].(bson.M)["_id"]; ok {
			docID = id
		} else {
			s.logger.Warnf("[MongoDB] cannot find _id in documentKey => %v", doc)
			return nil
		}

		s.logger.Debugf("[MongoDB] Processing update operation for %s, docID=%v", collName, docID)

		// Check if we have both fullDocument and updateDescription
		fullDoc, hasFullDoc := doc["fullDocument"].(bson.M)
		updateDesc, hasUpdateDesc := doc["updateDescription"].(bson.M)

		// Try incremental update if we have updateDescription
		if hasUpdateDesc && hasFullDoc {
			// Check for array index out of bounds issues
			if s.hasArrayIndexOutOfBounds(updateDesc, fullDoc) {
				s.logger.Debugf("[MongoDB] Array index out of bounds detected, using full replacement for docID=%v", docID)
			} else {
				// Use incremental update
				incrementalModel := s.buildIncrementalUpdate(docID, updateDesc, fullDoc, tableSecurity)
				if incrementalModel != nil {
					s.logger.Debugf("[MongoDB] Using incremental update for docID=%v", docID)
					return incrementalModel
				}
			}
		}

		// Fallback to full document replacement
		if hasFullDoc {
			if tableSecurity.SecurityEnabled && len(tableSecurity.FieldSecurity) > 0 {
				for field, value := range fullDoc {
					processedValue := security.ProcessValue(value, field, tableSecurity)
					fullDoc[field] = processedValue
				}
			}
			s.logger.Debugf("[MongoDB] Using full document replacement for update operation, docID=%v", docID)
			return mongo.NewReplaceOneModel().
				SetFilter(bson.M{"_id": docID}).
				SetReplacement(fullDoc).
				SetUpsert(true)
		} else {
			s.logger.Warnf("[MongoDB] fullDocument not available for update operation, skipping, docID=%v", docID)
			return nil
		}

	case "delete":
		// Use table-specific ignoreDeleteOps setting instead of global enableDeleteOps
		if advancedSettings.IgnoreDeleteOps {
			s.logger.Debugf("[MongoDB] Delete operation skipped (ignoreDeleteOps=true) for table %s, document: %v", collName, doc["documentKey"])
			return nil
		}

		if docID, ok := doc["documentKey"].(bson.M)["_id"]; ok {
			s.logger.Debugf("[MongoDB] Delete operation prepared for %s, docID=%v", collName, docID)
			return mongo.NewDeleteOneModel().SetFilter(bson.M{"_id": docID})
		}
		s.logger.Errorf("[MongoDB] Delete operation failed: _id not found in documentKey for %s", collName)
	}

	s.logger.Warnf("[MongoDB] No write model created for opType=%s, collName=%s", opType, collName)
	return nil
}

// hasArrayIndexOutOfBounds checks if any updated fields contain array indices that exceed the current array length
func (s *MongoDBSyncer) hasArrayIndexOutOfBounds(updateDesc bson.M, fullDoc bson.M) bool {
	updatedFields, hasUpdatedFields := updateDesc["updatedFields"].(bson.M)
	if !hasUpdatedFields {
		return false
	}

	for fieldPath := range updatedFields {
		// Check if this is an array index update (e.g., "Prices.2.Price")
		parts := strings.Split(fieldPath, ".")
		if len(parts) < 3 {
			continue // Not an array index update
		}

		// Look for numeric indices in the field path
		for i := 1; i < len(parts)-1; i++ {
			if s.isNumericString(parts[i]) {
				// This is an array index, check if it's out of bounds
				arrayFieldPath := strings.Join(parts[:i], ".")
				arrayField := s.getNestedField(fullDoc, arrayFieldPath)

				if arraySlice, ok := arrayField.([]interface{}); ok {
					indexValue := s.parseIndex(parts[i])
					if indexValue >= len(arraySlice) {
						s.logger.Debugf("[MongoDB] Array index %d exceeds current length %d for field %s",
							indexValue, len(arraySlice), arrayFieldPath)
						return true
					}
				}
			}
		}
	}

	return false
}

// buildIncrementalUpdate creates an incremental update operation
func (s *MongoDBSyncer) buildIncrementalUpdate(docID interface{}, updateDesc bson.M, fullDoc bson.M, tableSecurity security.TableSecurity) mongo.WriteModel {
	updateDoc := bson.M{}

	// Handle updated fields
	if updatedFields, ok := updateDesc["updatedFields"].(bson.M); ok && len(updatedFields) > 0 {
		setDoc := bson.M{}
		for fieldPath, newValue := range updatedFields {
			// Apply security processing if needed
			if tableSecurity.SecurityEnabled && len(tableSecurity.FieldSecurity) > 0 {
				// Extract the field name (handle nested paths)
				fieldName := strings.Split(fieldPath, ".")[0]
				processedValue := security.ProcessValue(newValue, fieldName, tableSecurity)
				setDoc[fieldPath] = processedValue
			} else {
				setDoc[fieldPath] = newValue
			}
		}
		if len(setDoc) > 0 {
			updateDoc["$set"] = setDoc
		}
	}

	// Handle removed fields
	if removedFields, ok := updateDesc["removedFields"].([]interface{}); ok && len(removedFields) > 0 {
		unsetDoc := bson.M{}
		for _, field := range removedFields {
			if fieldStr, ok := field.(string); ok {
				unsetDoc[fieldStr] = ""
			}
		}
		if len(unsetDoc) > 0 {
			updateDoc["$unset"] = unsetDoc
		}
	}

	if len(updateDoc) == 0 {
		return nil
	}

	s.logger.Debugf("[MongoDB] Built incremental update: %v", updateDoc)
	return mongo.NewUpdateOneModel().
		SetFilter(bson.M{"_id": docID}).
		SetUpdate(updateDoc).
		SetUpsert(true)
}

// Helper functions
func (s *MongoDBSyncer) isNumericString(str string) bool {
	_, err := strconv.Atoi(str)
	return err == nil
}

func (s *MongoDBSyncer) parseIndex(str string) int {
	index, _ := strconv.Atoi(str)
	return index
}

func (s *MongoDBSyncer) getNestedField(doc bson.M, fieldPath string) interface{} {
	parts := strings.Split(fieldPath, ".")
	current := doc

	for i, part := range parts {
		if i == len(parts)-1 {
			return current[part]
		}

		if next, ok := current[part].(bson.M); ok {
			current = next
		} else {
			return nil
		}
	}

	return nil
}

func describeWriteModel(m mongo.WriteModel, opType string) string {
	switch opType {
	case "insert":
		if ins, ok := m.(*mongo.InsertOneModel); ok {
			return fmt.Sprintf("INSERT doc=%s", toJSONString(ins.Document))
		}
	case "update", "replace":
		if rep, ok := m.(*mongo.ReplaceOneModel); ok {
			return fmt.Sprintf("UPDATE filter=%s, doc=%s", toJSONString(rep.Filter), toJSONString(rep.Replacement))
		}
		if upd, ok := m.(*mongo.UpdateOneModel); ok {
			return fmt.Sprintf("UPDATE filter=%s, update=%s", toJSONString(upd.Filter), toJSONString(upd.Update))
		}
	case "delete":
		if del, ok := m.(*mongo.DeleteOneModel); ok {
			return fmt.Sprintf("DELETE filter=%s", toJSONString(del.Filter))
		}
	}
	return "(unknown)"
}

func toJSONString(doc interface{}) string {
	if doc == nil {
		return "null"
	}
	data, err := json.Marshal(doc)
	if err != nil {
		return fmt.Sprintf("json_error:%v", err)
	}
	return string(data)
}

// flushBuffer processes batched write operations to the target collection
func (s *MongoDBSyncer) flushBuffer(ctx context.Context, targetColl *mongo.Collection, buffer *[]rawChangeEvent, sourceDB, collectionName string, currentToken bson.Raw) {
	if len(*buffer) == 0 {
		return
	}

	writeModels := make([]mongo.WriteModel, 0, len(*buffer))

	var insertCount, updateCount, deleteCount int
	for _, bc := range *buffer {
		// Convert rawChangeEvent to WriteModel
		model := s.convertRawEventToWriteModel(bc, sourceDB, collectionName)
		if model != nil {
			writeModels = append(writeModels, model)

			switch bc.OpType {
			case "insert":
				insertCount++
			case "update", "replace":
				updateCount++
			case "delete":
				deleteCount++
			}
		}
	}

	// Use RetryMongoOperation for retries
	err := utils.RetryMongoOperation(ctx, s.logger, fmt.Sprintf("BulkWrite to %s.%s",
		targetColl.Database().Name(), targetColl.Name()),
		func() error {
			res, err := targetColl.BulkWrite(ctx, writeModels, options.BulkWrite().SetOrdered(false))
			if err != nil {
				// Don't retry resumeToken errors
				if strings.Contains(err.Error(), "resume token") {
					s.removeMongoDBResumeToken(sourceDB, collectionName)
					return err // Return error, won't retry
				}
				return err // If it's a connection error, RetryMongoOperation will retry
			}

			// Operation results tracking now handled by ChangeStreamInfo
			successCount := int(res.InsertedCount + res.ModifiedCount + res.DeletedCount)

			// Accumulate ChangeStream activity with execution statistics only
			// Do NOT add to received count here to avoid duplicate counting
			// (received is already counted when events are added to buffer)
			utils.AccumulateChangeStreamActivity(sourceDB, collectionName, successCount, 0, successCount, int(res.InsertedCount), int(res.ModifiedCount), int(res.DeletedCount))

			s.logger.Debugf(
				"[MongoDB] BulkWrite => table=%s.%s inserted=%d matched=%d modified=%d upserted=%d deleted=%d (opTotals=>insert=%d, update=%d, delete=%d)",
				targetColl.Database().Name(),
				targetColl.Name(),
				res.InsertedCount,
				res.MatchedCount,
				res.ModifiedCount,
				res.UpsertedCount,
				res.DeletedCount,
				insertCount,
				updateCount,
				deleteCount,
			)

			if currentToken != nil {
				s.saveMongoDBResumeToken(sourceDB, collectionName, currentToken)
			}
			return nil
		})

	if err != nil {
		s.logger.Errorf("[MongoDB] BulkWrite failed after retries: %v", err)

		if strings.Contains(err.Error(), "Document failed validation") {
			s.logger.Errorf("[MongoDB] Document validation error detected for %s.%s. Error details: %v", sourceDB, collectionName, err)

			var errDetails string
			if strings.Contains(err.Error(), "failingDocumentId") {
				parts := strings.Split(err.Error(), "Document failed validation:")
				if len(parts) > 1 {
					errDetails = parts[1]
				}
				s.logger.Errorf("[MongoDB] Validation failure details: %s", errDetails)
			}

			s.logger.Errorf("[MongoDB] === Source documents that caused validation errors: ===")
			for i, bc := range *buffer {
				var docContent string
				switch bc.OpType {
				case "insert":
					// Convert rawChangeEvent to WriteModel for logging
					model := s.convertRawEventToWriteModel(bc, sourceDB, collectionName)
					if ins, ok := model.(*mongo.InsertOneModel); ok {
						docContent = toJSONString(ins.Document)
					}
				case "update", "replace":
					model := s.convertRawEventToWriteModel(bc, sourceDB, collectionName)
					if rep, ok := model.(*mongo.ReplaceOneModel); ok {
						docContent = fmt.Sprintf("filter=%s, replacement=%s", toJSONString(rep.Filter), toJSONString(rep.Replacement))
					} else if upd, ok := model.(*mongo.UpdateOneModel); ok {
						docContent = fmt.Sprintf("filter=%s, update=%s", toJSONString(upd.Filter), toJSONString(upd.Update))
					}
				case "delete":
					model := s.convertRawEventToWriteModel(bc, sourceDB, collectionName)
					if del, ok := model.(*mongo.DeleteOneModel); ok {
						docContent = fmt.Sprintf("filter=%s", toJSONString(del.Filter))
					}
				}
				s.logger.Errorf("[MongoDB] Source doc [%d], type=%s: %s", i, bc.OpType, docContent)

				if bc.OpType == "insert" || bc.OpType == "replace" {
					var doc bson.M
					model := s.convertRawEventToWriteModel(bc, sourceDB, collectionName)
					if ins, ok := model.(*mongo.InsertOneModel); ok {
						doc, _ = ins.Document.(bson.M)
					} else if rep, ok := model.(*mongo.ReplaceOneModel); ok {
						doc, _ = rep.Replacement.(bson.M)
					}

					if doc != nil {
						missingFields := []string{}
						for _, fieldName := range []string{"reg_date", "created_at", "updated_at"} {
							if _, exists := doc[fieldName]; !exists {
								missingFields = append(missingFields, fieldName)
							}
						}

						if len(missingFields) > 0 {
							s.logger.Errorf("[MongoDB] Document [%d] missing common required fields: %v", i, missingFields)
						}

						if regDate, exists := doc["reg_date"]; exists {
							s.logger.Errorf("[MongoDB] Document [%d] reg_date value: %v (type: %T)", i, regDate, regDate)
						}
					}
				}
			}
			s.logger.Errorf("[MongoDB] === End of source documents ===")
		}

		if strings.Contains(err.Error(), "Cannot create field") && strings.Contains(err.Error(), "in element") {
			s.logger.Errorf("[MongoDB] Cannot create field error detected for %s.%s", sourceDB, collectionName)
			s.logger.Errorf("[MongoDB] Error details: %v", err)

			for _, model := range writeModels {
				switch m := model.(type) {
				case *mongo.UpdateOneModel:
					if m.Filter != nil && m.Update != nil {
						// Extract _id from filter
						var docID interface{}
						if filter, ok := m.Filter.(bson.M); ok {
							docID = filter["_id"]
						}

						s.logger.Errorf("[MongoDB] Problem document: database=%s, collection=%s, _id=%v",
							sourceDB, collectionName, docID)

						if updateDoc, ok := m.Update.(bson.M); ok {
							if setDoc, hasSet := updateDoc["$set"]; hasSet {
								if setFields, ok := setDoc.(bson.M); ok {
									for fieldName, fieldValue := range setFields {
										s.logger.Errorf("[MongoDB] Field update: field='%s', value=%v, value_type=%T",
											fieldName, fieldValue, fieldValue)
									}
								}
								s.logger.Errorf("[MongoDB] Full $set operation: %s", toJSONString(setDoc))
							}
							if unsetDoc, hasUnset := updateDoc["$unset"]; hasUnset {
								s.logger.Errorf("[MongoDB] $unset operation: %s", toJSONString(unsetDoc))
							}
						}

						s.logger.Errorf("[MongoDB] Complete update model: filter=%s, update=%s",
							toJSONString(m.Filter), toJSONString(m.Update))
					}
				case *mongo.ReplaceOneModel:
					var docID interface{}
					if filter, ok := m.Filter.(bson.M); ok {
						docID = filter["_id"]
					}
					s.logger.Errorf("[MongoDB] Problem document (replace): database=%s, collection=%s, _id=%v",
						sourceDB, collectionName, docID)
					s.logger.Errorf("[MongoDB] Replace operation: filter=%s, replacement=%s",
						toJSONString(m.Filter), toJSONString(m.Replacement))
				case *mongo.InsertOneModel:
					var docID interface{}
					if doc, ok := m.Document.(bson.M); ok {
						docID = doc["_id"]
					}
					s.logger.Errorf("[MongoDB] Problem document (insert): database=%s, collection=%s, _id=%v",
						sourceDB, collectionName, docID)
					s.logger.Errorf("[MongoDB] Insert document: %s", toJSONString(m.Document))
				}
			}
		}
	}

	// Save latest resume token before clearing buffer
	if currentToken != nil {
		s.saveMongoDBResumeToken(sourceDB, collectionName, currentToken)
	}
	// Clear in-memory buffer
	*buffer = (*buffer)[:0]
}

// flushBufferWithResult processes batched write operations and returns write success status
func (s *MongoDBSyncer) flushBufferWithResult(ctx context.Context, targetColl *mongo.Collection, buffer *[]rawChangeEvent, sourceDB, collectionName string, currentToken bson.Raw) bool {
	if len(*buffer) == 0 {
		return true // No data to write is considered successful
	}

	writeModels := make([]mongo.WriteModel, 0, len(*buffer))

	var insertCount, updateCount, deleteCount int
	for _, bc := range *buffer {
		// Convert rawChangeEvent to WriteModel
		model := s.convertRawEventToWriteModel(bc, sourceDB, collectionName)
		if model != nil {
			writeModels = append(writeModels, model)

			switch bc.OpType {
			case "insert":
				insertCount++
			case "update", "replace":
				updateCount++
			case "delete":
				deleteCount++
			}
		}
	}

	// Use RetryMongoOperation for retries
	err := utils.RetryMongoOperation(ctx, s.logger, fmt.Sprintf("BulkWrite to %s.%s",
		targetColl.Database().Name(), targetColl.Name()),
		func() error {
			res, err := targetColl.BulkWrite(ctx, writeModels, options.BulkWrite().SetOrdered(false))
			if err != nil {
				// Handle duplicate key errors specially
				if strings.Contains(err.Error(), "E11000 duplicate key error") {
					s.logger.Warnf("[MongoDB] BulkWrite encountered duplicate key error, switching to individual operations for %s.%s", sourceDB, collectionName)
					// Try individual operations to isolate problematic records
					return s.handleBatchWithDuplicateKeys(ctx, targetColl, writeModels, sourceDB, collectionName)
				}
				// Don't retry resumeToken errors
				if strings.Contains(err.Error(), "resume token") {
					s.removeMongoDBResumeToken(sourceDB, collectionName)
					return err // Return error, won't retry
				}
				return err // If it's a connection error, RetryMongoOperation will retry
			}

			// Operation results tracking now handled by ChangeStreamInfo
			successCount := int(res.InsertedCount + res.ModifiedCount + res.DeletedCount)

			// Accumulate ChangeStream activity with execution statistics only
			// Do NOT add to received count here to avoid duplicate counting
			// (received is already counted when events are added to buffer)
			utils.AccumulateChangeStreamActivity(sourceDB, collectionName, successCount, 0, successCount, int(res.InsertedCount), int(res.ModifiedCount), int(res.DeletedCount))

			s.logger.Debugf(
				"[MongoDB] BulkWrite => table=%s.%s inserted=%d matched=%d modified=%d upserted=%d deleted=%d (opTotals=>insert=%d, update=%d, delete=%d)",
				targetColl.Database().Name(),
				targetColl.Name(),
				res.InsertedCount,
				res.MatchedCount,
				res.ModifiedCount,
				res.UpsertedCount,
				res.DeletedCount,
				insertCount,
				updateCount,
				deleteCount,
			)

			if currentToken != nil {
				s.saveMongoDBResumeToken(sourceDB, collectionName, currentToken)
			}
			return nil
		})

	if err != nil {
		s.logger.Errorf("[MongoDB] BulkWrite failed after retries: %v", err)
		// Don't clear buffer on failure - return false to indicate failure
		return false
	}

	// Save latest resume token and clear buffer only on success
	if currentToken != nil {
		s.saveMongoDBResumeToken(sourceDB, collectionName, currentToken)
	}
	// Clear in-memory buffer
	*buffer = (*buffer)[:0]

	// Return true to indicate successful write
	return true
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
		s.logger.Debugf("[MongoDB] no resume token file => %s => %v", path, err)
		return nil
	}
	if len(data) <= 1 {
		s.logger.Debugf("[MongoDB] empty resume token file => %s", path)
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

// storeToBuffer persists the buffered changes to disk
func (s *MongoDBSyncer) storeToBuffer(ctx context.Context, buffer *[]rawChangeEvent, sourceDB, collectionName string, latestToken bson.Raw) {
	if len(*buffer) == 0 {
		return
	}

	bufferPath := s.getBufferPath(sourceDB, collectionName)
	startTime := time.Now()

	// Ensure buffer directory exists
	if err := os.MkdirAll(bufferPath, os.ModePerm); err != nil {
		s.logger.Errorf("[MongoDB] Failed to create buffer directory %s: %v", bufferPath, err)
		return
	}

	// Batch file writing optimization
	batchSize := 100 // Number of files per batch
	timestamp := time.Now().UnixNano()

	s.logger.Debugf("[MongoDB] Starting batch buffer write: %d changes for %s.%s",
		len(*buffer), sourceDB, collectionName)

	// Track write success
	writeSuccess := true

	for i := 0; i < len(*buffer); i += batchSize {
		end := i + batchSize
		if end > len(*buffer) {
			end = len(*buffer)
		}

		batchChanges := make([]persistedChange, 0, end-i)

		// Prepare batch changes
		for j := i; j < end; j++ {
			change := (*buffer)[j]

			// Create persisted change with raw BSON data
			persistedChange := persistedChange{
				ID:         fmt.Sprintf("%d_%d", timestamp, j),
				Token:      latestToken,
				FullDoc:    change.FullDoc,
				DocKey:     change.DocKey,
				UpdateDesc: change.UpdateDesc,
				OpType:     change.OpType,
				SourceDB:   sourceDB,
				SourceColl: collectionName,
				Timestamp:  change.Timestamp,
			}

			batchChanges = append(batchChanges, persistedChange)
		}

		// Write batch to single file using BSON instead of JSON
		batchFilePath := filepath.Join(bufferPath, fmt.Sprintf("batch_%d_%d.bson", timestamp, i))
		tempFilePath := filepath.Join(bufferPath, fmt.Sprintf("temp_%d_%d.bson", timestamp, i))
		
		// Wrap the batch in a structure for proper BSON serialization
		batchWrapper := struct {
			Changes []persistedChange `bson:"changes"`
			Count   int               `bson:"count"`
		}{
			Changes: batchChanges,
			Count:   len(batchChanges),
		}
		
		batchBytes, err := bson.Marshal(batchWrapper)
		if err != nil {
			s.logger.Errorf("[MongoDB] Failed to marshal batch changes: %v", err)
			writeSuccess = false
			continue
		}

		// Write to temporary file first
		if err := os.WriteFile(tempFilePath, batchBytes, 0644); err != nil {
			s.logger.Errorf("[MongoDB] Failed to write temp batch file %s: %v", tempFilePath, err)
			writeSuccess = false
		} else {
			// Atomic rename to final filename
			if err := os.Rename(tempFilePath, batchFilePath); err != nil {
				s.logger.Errorf("[MongoDB] Failed to rename temp file %s to %s: %v", tempFilePath, batchFilePath, err)
				_ = os.Remove(tempFilePath) // Clean up temp file
				writeSuccess = false
			} else {
				// Validate the written file immediately
				if !s.validateBatchFile(batchFilePath) {
					s.logger.Warnf("[MongoDB] Batch file validation failed, retrying: %s", batchFilePath)
					_ = os.Remove(batchFilePath) // Remove corrupted file
					writeSuccess = false         // Keep data in buffer for retry
				}
			}
		}
	}

	duration := time.Since(startTime)
	rate := float64(len(*buffer)) / duration.Seconds()
	s.logger.Debugf("[MongoDB] Completed batch buffer write in %v (%.2f items/sec) for %s.%s",
		duration, rate, sourceDB, collectionName)

	s.logger.Debugf("[MongoDB] Stored %d changes to buffer for %s.%s using batch optimization",
		len(*buffer), sourceDB, collectionName)

	// Only clear buffer and update token if all writes succeeded
	if writeSuccess {
		// Clear in-memory buffer
		*buffer = (*buffer)[:0]

		// Save latest token
		if latestToken != nil {
			s.saveMongoDBResumeToken(sourceDB, collectionName, latestToken)
		}
		s.logger.Debugf("[MongoDB] All writes successful, buffer cleared and resume token updated")
	} else {
		s.logger.Warnf("[MongoDB] Some writes failed, keeping data in buffer and NOT updating resume token")
	}
}

// processPersistentBuffer reads from the persistent buffer and applies changes at a controlled rate
func (s *MongoDBSyncer) processPersistentBuffer(ctx context.Context, sourceDB, collectionName, targetDBName, targetCollectionName string) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	diagnosticTicker := time.NewTicker(1 * time.Minute)
	defer diagnosticTicker.Stop()

	var totalProcessed int64
	var lastProcessTime = time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Process up to processRate files per second
			processed := s.processBufferedChanges(ctx, sourceDB, collectionName, targetDBName, targetCollectionName)
			totalProcessed += int64(processed)
		case <-diagnosticTicker.C:
			elapsed := time.Since(lastProcessTime)
			if elapsed > 0 && totalProcessed > 0 {
				rate := float64(totalProcessed) / elapsed.Seconds()

				bufferPath := s.getBufferPath(sourceDB, collectionName)
				files, err := os.ReadDir(bufferPath)
				var backlogCount int
				if err == nil {
					backlogCount = len(files)
				}

				s.logger.Debugf("[MongoDB] Buffer processing stats: processed=%d, rate=%.2f/sec, backlog=%d files",
					totalProcessed, rate, backlogCount)

				if backlogCount > 5000 && rate < float64(s.processRate)/2 {
					s.logger.Warnf("[MongoDB] Processing rate (%.2f/sec) significantly below target (%d/sec) with high backlog (%d)",
						rate, s.processRate, backlogCount)
				}

				totalProcessed = 0
				lastProcessTime = time.Now()
			}
		}
	}
}

// processBufferedChanges processes a batch of buffered changes with data safety
func (s *MongoDBSyncer) processBufferedChanges(ctx context.Context, sourceDB, collectionName, targetDBName, targetCollectionName string) int {
	// Get fresh target collection from current client to ensure we use the latest connection
	targetColl := s.targetClient.Database(targetDBName).Collection(targetCollectionName)

	bufferPath := s.getBufferPath(sourceDB, collectionName)
	processedCount := 0

	// Ensure buffer directory exists
	if _, err := os.Stat(bufferPath); os.IsNotExist(err) {
		return 0 // No buffer directory yet
	}

	// List files in buffer directory
	files, err := os.ReadDir(bufferPath)
	if err != nil {
		s.logger.Errorf("[MongoDB] Failed to read buffer directory %s: %v", bufferPath, err)
		return 0
	}

	if len(files) == 0 {
		return 0 // No files to process
	}

	// Process limit increased to twice the original for faster buffer emptying
	filesToProcess := s.processRate * 2
	if filesToProcess > len(files) {
		filesToProcess = len(files)
	}

	fileCountStat := ""
	if len(files) > 1000 {
		fileCountStat = fmt.Sprintf(", buffer backlog: %d files", len(files))
	}
	s.logger.Debugf("[MongoDB] Processing %d/%d buffered changes for %s.%s%s",
		filesToProcess, len(files), sourceDB, collectionName, fileCountStat)

	var batch []rawChangeEvent
	var processedToken bson.Raw
	var processedFiles []string // Track successfully processed files for deletion
	startTime := time.Now()

	// Track if we encounter any corrupted files
	corruptedFileEncountered := false

	// Step 1: Read and process files (DO NOT DELETE YET)
	for i := 0; i < filesToProcess; i++ {
		if i >= len(files) {
			break
		}

		fileName := files[i].Name()

		// Skip temporary files, wait for them to be renamed to final files
		if strings.HasPrefix(fileName, "temp_") {
			continue
		}

		filePath := filepath.Join(bufferPath, fileName)
		fileData, err := os.ReadFile(filePath)
		if err != nil {
			if os.IsNotExist(err) {
				s.logger.Debugf("[MongoDB] Buffer file %s already processed by another goroutine", filePath)
			} else {
				s.logger.Errorf("[MongoDB] Failed to read buffer file %s: %v", filePath, err)
			}
			continue
		}

		// Check if it's a batch processing file
		if strings.HasPrefix(filepath.Base(filePath), "batch_") {
			// Parse batch changes using BSON
			var batchWrapper struct {
				Changes []persistedChange `bson:"changes"`
				Count   int               `bson:"count"`
			}
			if err := bson.Unmarshal(fileData, &batchWrapper); err != nil {
				s.logger.Errorf("[MongoDB] Failed to unmarshal batch changes: %v", err)
				s.logger.Errorf("[MongoDB] Corrupted file: %s, size: %d bytes", filePath, len(fileData))
				// Move corrupted file to quarantine directory instead of deleting
				if quarantineErr := s.quarantineCorruptedFile(filePath, sourceDB, collectionName); quarantineErr != nil {
					s.logger.Errorf("[MongoDB] Failed to quarantine corrupted file %s: %v", filePath, quarantineErr)
				} else {
					s.logger.Infof("[MongoDB] Quarantined corrupted file: %s", filePath)
				}
				corruptedFileEncountered = true
				continue // Continue processing other files
			}

			// Process each change in the batch
			for _, persistedChange := range batchWrapper.Changes {
				// Convert to WriteModel
				model := s.convertToWriteModel(persistedChange)
				if model != nil {
					// Convert persistedChange back to rawChangeEvent for consistency
					rawEvent := rawChangeEvent{
						FullDoc:    persistedChange.FullDoc,
						DocKey:     persistedChange.DocKey,
						UpdateDesc: persistedChange.UpdateDesc,
						OpType:     persistedChange.OpType,
						Timestamp:  persistedChange.Timestamp,
					}
					batch = append(batch, rawEvent)
					processedToken = persistedChange.Token
					processedCount++
				}
			}
		} else {
			// Process old format single change file using BSON
			var persistedChange persistedChange
			if err := bson.Unmarshal(fileData, &persistedChange); err != nil {
				s.logger.Errorf("[MongoDB] Failed to unmarshal persisted change: %v", err)
				// Move corrupted file to quarantine directory instead of deleting
				if quarantineErr := s.quarantineCorruptedFile(filePath, sourceDB, collectionName); quarantineErr != nil {
					s.logger.Errorf("[MongoDB] Failed to quarantine corrupted file %s: %v", filePath, quarantineErr)
				} else {
					s.logger.Infof("[MongoDB] Quarantined corrupted file: %s", filePath)
				}
				corruptedFileEncountered = true
				continue // Continue processing other files
			}

			model := s.convertToWriteModel(persistedChange)
			if model != nil {
				// Convert persistedChange back to rawChangeEvent for consistency
				rawEvent := rawChangeEvent{
					FullDoc:    persistedChange.FullDoc,
					DocKey:     persistedChange.DocKey,
					UpdateDesc: persistedChange.UpdateDesc,
					OpType:     persistedChange.OpType,
					Timestamp:  persistedChange.Timestamp,
				}
				batch = append(batch, rawEvent)
				processedToken = persistedChange.Token
				processedCount++
			}
		}

		// Add to processed files list (will be deleted only after successful write)
		processedFiles = append(processedFiles, filePath)
	}

	// Step 2: Try to write to database
	writeSuccess := false
	if len(batch) > 0 {
		deleteOps := countDeleteOps(batch)
		s.logger.Debugf("[MongoDB] Attempting to flush %d operations (%d deletes) for %s.%s",
			len(batch), deleteOps, sourceDB, collectionName)

		// Create a custom flush operation that returns success status
		writeSuccess = s.flushBufferWithResult(ctx, targetColl, &batch, sourceDB, collectionName, processedToken)

		if writeSuccess {
			s.logger.Debugf("[MongoDB] Successfully flushed %d operations to database", len(batch))
		} else {
			s.logger.Errorf("[MongoDB] Failed to flush operations to database - keeping buffer files for retry")
		}
	}

	// Step 3: Only delete files if write was successful
	if writeSuccess && len(processedFiles) > 0 {
		// Check if uploadToGcs is enabled for any table
		uploadToGcs := false
		gcsAddress := ""

		// Get table advanced settings to check if GCS upload is enabled
		for _, mapping := range s.cfg.Mappings {
			for _, table := range mapping.Tables {
				if table.SourceTable == collectionName || table.TargetTable == collectionName {
					if table.AdvancedSettings.UploadToGcs {
						uploadToGcs = true
						gcsAddress = table.AdvancedSettings.GcsAddress
						s.logger.Debugf("[MongoDB] GCS upload enabled for %s.%s, address: %s",
							sourceDB, collectionName, gcsAddress)
					}
					break
				}
			}
			if uploadToGcs {
				break
			}
		}

		if uploadToGcs && gcsAddress != "" {
			// Upload files to GCS instead of deleting them
			uploadedCount := 0
			for _, filePath := range processedFiles {
				if err := utils.UploadToGCS(ctx, filePath, gcsAddress); err != nil {
					s.logger.Errorf("[MongoDB] Failed to upload buffer file %s to GCS: %v", filePath, err)
					// Keep file on failure
				} else {
					uploadedCount++
					// Delete file after successful upload
					if err := os.Remove(filePath); err != nil {
						if !os.IsNotExist(err) {
							s.logger.Warnf("[MongoDB] Failed to remove uploaded buffer file %s: %v", filePath, err)
						}
					} else {
						s.logger.Debugf("[MongoDB] Deleted buffer file after GCS upload: %s", filepath.Base(filePath))
					}
				}
			}
			s.logger.Debugf("[MongoDB] Uploaded %d/%d buffer files to GCS for %s.%s",
				uploadedCount, len(processedFiles), sourceDB, collectionName)
		} else {
			// Original behavior: delete files directly
			deletedCount := 0
			for _, filePath := range processedFiles {
				if err := os.Remove(filePath); err != nil {
					if !os.IsNotExist(err) {
						s.logger.Warnf("[MongoDB] Failed to remove processed buffer file %s: %v", filePath, err)
					}
				} else {
					deletedCount++
					s.logger.Debugf("[MongoDB] Deleted processed buffer file: %s", filepath.Base(filePath))
				}
			}
			s.logger.Debugf("[MongoDB] Deleted %d/%d buffer files after successful database write", deletedCount, len(processedFiles))
		}

		// Only save resume token if write was successful and no corrupted files were encountered
		if !corruptedFileEncountered && processedToken != nil {
			s.saveMongoDBResumeToken(sourceDB, collectionName, processedToken)
			s.logger.Debugf("[MongoDB] Resume token updated successfully")
		}
	} else if len(processedFiles) > 0 {
		s.logger.Warnf("[MongoDB] Database write failed - keeping %d buffer files for retry: %v",
			len(processedFiles), processedFiles)
	}

	if processedCount > 0 {
		duration := time.Since(startTime)
		rate := float64(processedCount) / duration.Seconds()
		s.logger.Debugf("[MongoDB] Processed %d changes in %v (%.2f/sec) for %s.%s, write_success=%v",
			processedCount, duration, rate, sourceDB, collectionName, writeSuccess)
	}

	return processedCount
}

// Helper function to count delete operations in a batch
func countDeleteOps(batch []rawChangeEvent) int {
	count := 0
	for _, change := range batch {
		if change.OpType == "delete" {
			count++
		}
	}
	return count
}

// getBufferPath returns the path to the buffer directory for a collection
func (s *MongoDBSyncer) getBufferPath(db, coll string) string {
	return filepath.Join(s.bufferDir, fmt.Sprintf("%s_%s", db, coll))
}

// validateBatchFile performs complete BSON validation to check file integrity
func (s *MongoDBSyncer) validateBatchFile(filePath string) bool {
	// 1. Check file size
	stat, err := os.Stat(filePath)
	if err != nil || stat.Size() < 10 {
		s.logger.Debugf("[MongoDB] File validation failed: invalid size for %s", filePath)
		return false
	}

	// 2. Complete BSON validation by attempting to unmarshal
	fileData, err := os.ReadFile(filePath)
	if err != nil {
		s.logger.Debugf("[MongoDB] File validation failed: cannot read file %s: %v", filePath, err)
		return false
	}

	var batchWrapper struct {
		Changes []persistedChange `bson:"changes"`
		Count   int               `bson:"count"`
	}
	if err := bson.Unmarshal(fileData, &batchWrapper); err != nil {
		s.logger.Debugf("[MongoDB] File validation failed: invalid BSON structure for %s: %v", filePath, err)
		return false
	}

	s.logger.Debugf("[MongoDB] File validation passed for %s", filePath)
	return true
}

// convertToWriteModel converts persistedChange to WriteModel using raw BSON data
func (s *MongoDBSyncer) convertToWriteModel(persistedChange persistedChange) mongo.WriteModel {
	var model mongo.WriteModel

	s.logger.Debugf("[MongoDB] convertToWriteModel: opType=%s, id=%s", persistedChange.OpType, persistedChange.ID)

	switch persistedChange.OpType {
	case "insert":
		var doc bson.M
		if err := bson.Unmarshal(persistedChange.FullDoc, &doc); err != nil {
			s.logger.Errorf("[MongoDB] Failed to unmarshal insert document from BSON: %v", err)
			return nil
		}
		model = mongo.NewInsertOneModel().SetDocument(doc)

	case "update":
		// For updates, we need to check if we have updateDescription or fullDocument
		if len(persistedChange.UpdateDesc) > 0 {
			// Incremental update
			var updateDoc bson.M
			if err := bson.Unmarshal(persistedChange.UpdateDesc, &updateDoc); err != nil {
				s.logger.Errorf("[MongoDB] Failed to unmarshal update document from BSON: %v", err)
				return nil
			}

			var docKey bson.M
			if err := bson.Unmarshal(persistedChange.DocKey, &docKey); err != nil {
				s.logger.Errorf("[MongoDB] Failed to unmarshal document key from BSON: %v", err)
				return nil
			}

			model = mongo.NewUpdateOneModel().
				SetFilter(docKey).
				SetUpdate(updateDoc).
				SetUpsert(true)
		} else if len(persistedChange.FullDoc) > 0 {
			// Full document replacement
			var fullDoc bson.M
			if err := bson.Unmarshal(persistedChange.FullDoc, &fullDoc); err != nil {
				s.logger.Errorf("[MongoDB] Failed to unmarshal full document from BSON: %v", err)
				return nil
			}

			var docKey bson.M
			if err := bson.Unmarshal(persistedChange.DocKey, &docKey); err != nil {
				s.logger.Errorf("[MongoDB] Failed to unmarshal document key from BSON: %v", err)
				return nil
			}

			model = mongo.NewReplaceOneModel().
				SetFilter(docKey).
				SetReplacement(fullDoc).
				SetUpsert(true)
		} else {
			s.logger.Errorf("[MongoDB] Invalid update data: missing both update description and full document")
			return nil
		}

	case "replace":
		var fullDoc bson.M
		if err := bson.Unmarshal(persistedChange.FullDoc, &fullDoc); err != nil {
			s.logger.Errorf("[MongoDB] Failed to unmarshal replace document from BSON: %v", err)
			return nil
		}

		var docKey bson.M
		if err := bson.Unmarshal(persistedChange.DocKey, &docKey); err != nil {
			s.logger.Errorf("[MongoDB] Failed to unmarshal document key from BSON: %v", err)
			return nil
		}

		model = mongo.NewReplaceOneModel().
			SetFilter(docKey).
			SetReplacement(fullDoc).
			SetUpsert(true)

	case "delete":
		var docKey bson.M
		if err := bson.Unmarshal(persistedChange.DocKey, &docKey); err != nil {
			s.logger.Errorf("[MongoDB] Failed to unmarshal delete document key from BSON: %v", err)
			return nil
		}

		model = mongo.NewDeleteOneModel().SetFilter(docKey)

	default:
		s.logger.Errorf("[MongoDB] Unknown operation type in persisted change: %s", persistedChange.OpType)
		return nil
	}

	if model != nil {
		s.logger.Debugf("[MongoDB] Successfully converted persisted change %s to WriteModel", persistedChange.ID)
	}

	return model
}

// findTableAdvancedSettings returns the AdvancedSettings for a given table name
func (s *MongoDBSyncer) findTableAdvancedSettings(tableName string) config.AdvancedSettings {
	for _, mapping := range s.cfg.Mappings {
		for _, table := range mapping.Tables {
			if table.SourceTable == tableName || table.TargetTable == tableName {
				return table.AdvancedSettings
			}
		}
	}
	// Return default settings if table not found
	return config.AdvancedSettings{
		SyncIndexes:     false,
		IgnoreDeleteOps: false, // Default to ignore delete operations
		UploadToGcs:     false, // Default to not upload to GCS
		GcsAddress:      "",    // Default empty GCS address
	}
}

// handleBatchWithDuplicateKeys processes writeModels individually when bulk operation fails due to duplicate keys
func (s *MongoDBSyncer) handleBatchWithDuplicateKeys(ctx context.Context, targetColl *mongo.Collection, writeModels []mongo.WriteModel, sourceDB, collectionName string) error {
	var successCount, skipCount int
	var lastErr error

	s.logger.Infof("[MongoDB] Processing %d operations individually due to duplicate key errors for %s.%s",
		len(writeModels), sourceDB, collectionName)

	for i, model := range writeModels {
		var err error
		var opType string
		var docID interface{}

		// Extract operation type and document ID for logging
		switch m := model.(type) {
		case *mongo.InsertOneModel:
			opType = "insert"
			if doc, ok := m.Document.(bson.M); ok {
				docID = doc["_id"]
			}
			_, err = targetColl.InsertOne(ctx, m.Document)
		case *mongo.UpdateOneModel:
			opType = "update"
			if filter, ok := m.Filter.(bson.M); ok {
				docID = filter["_id"]
			}
			_, err = targetColl.UpdateOne(ctx, m.Filter, m.Update, options.Update().SetUpsert(true))
		case *mongo.ReplaceOneModel:
			opType = "replace"
			if filter, ok := m.Filter.(bson.M); ok {
				docID = filter["_id"]
			}
			_, err = targetColl.ReplaceOne(ctx, m.Filter, m.Replacement, options.Replace().SetUpsert(true))
		case *mongo.DeleteOneModel:
			opType = "delete"
			if filter, ok := m.Filter.(bson.M); ok {
				docID = filter["_id"]
			}
			_, err = targetColl.DeleteOne(ctx, m.Filter)
		default:
			s.logger.Warnf("[MongoDB] Unknown write model type, skipping operation %d", i)
			continue
		}

		if err != nil {
			if strings.Contains(err.Error(), "E11000 duplicate key error") {
				s.logger.Warnf("[MongoDB] Skipping duplicate key error for %s operation with _id=%v: %v",
					opType, docID, err)
				skipCount++
			} else {
				s.logger.Errorf("[MongoDB] Failed %s operation with _id=%v: %v",
					opType, docID, err)
				lastErr = err
			}
		} else {
			successCount++
		}
	}

	s.logger.Infof("[MongoDB] Individual operation results for %s.%s: success=%d, skipped=%d, total=%d",
		sourceDB, collectionName, successCount, skipCount, len(writeModels))

	// Consider operation successful if we processed all operations (even with some skips)
	// Only fail if there were non-duplicate-key errors
	if lastErr != nil && !strings.Contains(lastErr.Error(), "E11000 duplicate key error") {
		return lastErr
	}

	return nil
}

// quarantineCorruptedFile moves a corrupted file to a quarantine directory for later analysis
func (s *MongoDBSyncer) quarantineCorruptedFile(filePath, sourceDB, collectionName string) error {
	// Create quarantine directory
	quarantineDir := filepath.Join(s.bufferDir, "quarantine", fmt.Sprintf("%s_%s", sourceDB, collectionName))
	if err := os.MkdirAll(quarantineDir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create quarantine directory: %w", err)
	}

	// Generate unique filename with timestamp
	fileName := filepath.Base(filePath)
	timestamp := time.Now().Format("20060102_150405")
	quarantinePath := filepath.Join(quarantineDir, fmt.Sprintf("corrupted_%s_%s", timestamp, fileName))

	// Move file to quarantine directory
	if err := os.Rename(filePath, quarantinePath); err != nil {
		return fmt.Errorf("failed to move file to quarantine: %w", err)
	}

	s.logger.Infof("[MongoDB] Corrupted file moved to quarantine: %s -> %s", filePath, quarantinePath)
	return nil
}

// extractRawBSONData extracts raw BSON data directly from Change Stream event
func (s *MongoDBSyncer) extractRawBSONData(changeEvent bson.M, opType, collectionName string) *rawChangeEvent {
	tableSecurity := security.FindTableSecurityFromMappings(collectionName, s.cfg.Mappings)
	advancedSettings := s.findTableAdvancedSettings(collectionName)

	// Extract timestamp
	timestamp := time.Now()
	if ts, ok := changeEvent["_ts"].(time.Time); ok {
		timestamp = ts
	}

	// Extract documentKey (always present)
	docKeyBytes, err := bson.Marshal(changeEvent["documentKey"])
	if err != nil {
		s.logger.Errorf("[MongoDB] Failed to marshal documentKey: %v", err)
		return nil
	}

	switch opType {
	case "insert":
		if fullDoc, ok := changeEvent["fullDocument"].(bson.M); ok {
			// Apply security processing if needed
			if tableSecurity.SecurityEnabled && len(tableSecurity.FieldSecurity) > 0 {
				for field, value := range fullDoc {
					processedValue := security.ProcessValue(value, field, tableSecurity)
					fullDoc[field] = processedValue
				}
			}
			
			fullDocBytes, err := bson.Marshal(fullDoc)
			if err != nil {
				s.logger.Errorf("[MongoDB] Failed to marshal fullDocument: %v", err)
				return nil
			}

			return &rawChangeEvent{
				FullDoc:    fullDocBytes,
				DocKey:     docKeyBytes,
				UpdateDesc: nil,
				OpType:     "insert",
				Timestamp:  timestamp,
			}
		}
		s.logger.Errorf("[MongoDB] Insert operation failed: fullDocument not found in change event for %s", collectionName)

	case "update", "replace":
		var docID interface{}
		if id, ok := changeEvent["documentKey"].(bson.M)["_id"]; ok {
			docID = id
		} else {
			s.logger.Warnf("[MongoDB] cannot find _id in documentKey => %v", changeEvent)
			return nil
		}

		s.logger.Debugf("[MongoDB] Processing update operation for %s, docID=%v", collectionName, docID)

		// Check if we have both fullDocument and updateDescription
		fullDoc, hasFullDoc := changeEvent["fullDocument"].(bson.M)
		updateDesc, hasUpdateDesc := changeEvent["updateDescription"].(bson.M)

		// Try incremental update if we have updateDescription
		if hasUpdateDesc && hasFullDoc {
			// Check for array index out of bounds issues
			if s.hasArrayIndexOutOfBounds(updateDesc, fullDoc) {
				s.logger.Debugf("[MongoDB] Array index out of bounds detected, using full replacement for docID=%v", docID)
				// Fall through to full document replacement
			} else {
				// Use incremental update - store updateDescription
				updateDescBytes, err := bson.Marshal(updateDesc)
				if err != nil {
					s.logger.Errorf("[MongoDB] Failed to marshal updateDescription: %v", err)
					return nil
				}

				return &rawChangeEvent{
					FullDoc:    nil,
					DocKey:     docKeyBytes,
					UpdateDesc: updateDescBytes,
					OpType:     "update",
					Timestamp:  timestamp,
				}
			}
		}

		// Fallback to full document replacement
		if hasFullDoc {
			// Apply security processing if needed
			if tableSecurity.SecurityEnabled && len(tableSecurity.FieldSecurity) > 0 {
				for field, value := range fullDoc {
					processedValue := security.ProcessValue(value, field, tableSecurity)
					fullDoc[field] = processedValue
				}
			}

			fullDocBytes, err := bson.Marshal(fullDoc)
			if err != nil {
				s.logger.Errorf("[MongoDB] Failed to marshal fullDocument: %v", err)
				return nil
			}

			s.logger.Debugf("[MongoDB] Using full document replacement for update operation, docID=%v", docID)
			return &rawChangeEvent{
				FullDoc:    fullDocBytes,
				DocKey:     docKeyBytes,
				UpdateDesc: nil,
				OpType:     "replace",
				Timestamp:  timestamp,
			}
		} else {
			s.logger.Warnf("[MongoDB] fullDocument not available for update operation, skipping, docID=%v", docID)
			return nil
		}

	case "delete":
		// Use table-specific ignoreDeleteOps setting instead of global enableDeleteOps
		if advancedSettings.IgnoreDeleteOps {
			s.logger.Debugf("[MongoDB] Delete operation skipped (ignoreDeleteOps=true) for table %s, document: %v", collectionName, changeEvent["documentKey"])
			return nil
		}

		s.logger.Debugf("[MongoDB] Delete operation prepared for %s", collectionName)
		return &rawChangeEvent{
			FullDoc:    nil,
			DocKey:     docKeyBytes,
			UpdateDesc: nil,
			OpType:     "delete",
			Timestamp:  timestamp,
		}
	}

	s.logger.Warnf("[MongoDB] No raw BSON data extracted for opType=%s, collName=%s", opType, collectionName)
	return nil
}

// convertRawEventToWriteModel converts rawChangeEvent to WriteModel
func (s *MongoDBSyncer) convertRawEventToWriteModel(rawEvent rawChangeEvent, sourceDB, collectionName string) mongo.WriteModel {
	switch rawEvent.OpType {
	case "insert":
		var doc bson.M
		if err := bson.Unmarshal(rawEvent.FullDoc, &doc); err != nil {
			s.logger.Errorf("[MongoDB] Failed to unmarshal insert document from BSON: %v", err)
			return nil
		}
		return mongo.NewInsertOneModel().SetDocument(doc)

	case "update":
		// For updates, we need to check if we have updateDescription or fullDocument
		if len(rawEvent.UpdateDesc) > 0 {
			// Incremental update
			var updateDesc bson.M
			if err := bson.Unmarshal(rawEvent.UpdateDesc, &updateDesc); err != nil {
				s.logger.Errorf("[MongoDB] Failed to unmarshal update description from BSON: %v", err)
				return nil
			}

			var docKey bson.M
			if err := bson.Unmarshal(rawEvent.DocKey, &docKey); err != nil {
				s.logger.Errorf("[MongoDB] Failed to unmarshal document key from BSON: %v", err)
				return nil
			}

			// Extract document ID
			var docID interface{}
			if id, ok := docKey["_id"]; ok {
				docID = id
			} else {
				s.logger.Errorf("[MongoDB] Cannot find _id in documentKey")
				return nil
			}

			// Build incremental update using the same logic as prepareWriteModel
			tableSecurity := security.FindTableSecurityFromMappings(collectionName, s.cfg.Mappings)
			
			// Get full document for array bounds checking (if available)
			var fullDoc bson.M
			if len(rawEvent.FullDoc) > 0 {
				if err := bson.Unmarshal(rawEvent.FullDoc, &fullDoc); err != nil {
					s.logger.Warnf("[MongoDB] Failed to unmarshal full document for bounds checking: %v", err)
				}
			}

			// Check for array index out of bounds issues
			if len(fullDoc) > 0 && s.hasArrayIndexOutOfBounds(updateDesc, fullDoc) {
				s.logger.Debugf("[MongoDB] Array index out of bounds detected, using full replacement")
				// Fall through to full document replacement
			} else {
				// Use incremental update
				incrementalModel := s.buildIncrementalUpdate(docID, updateDesc, fullDoc, tableSecurity)
				if incrementalModel != nil {
					s.logger.Debugf("[MongoDB] Using incremental update for docID=%v", docID)
					return incrementalModel
				}
			}
		}

		// Fallback to full document replacement
		if len(rawEvent.FullDoc) > 0 {
			var fullDoc bson.M
			if err := bson.Unmarshal(rawEvent.FullDoc, &fullDoc); err != nil {
				s.logger.Errorf("[MongoDB] Failed to unmarshal full document from BSON: %v", err)
				return nil
			}

			var docKey bson.M
			if err := bson.Unmarshal(rawEvent.DocKey, &docKey); err != nil {
				s.logger.Errorf("[MongoDB] Failed to unmarshal document key from BSON: %v", err)
				return nil
			}

			return mongo.NewReplaceOneModel().
				SetFilter(docKey).
				SetReplacement(fullDoc).
				SetUpsert(true)
		} else {
			s.logger.Errorf("[MongoDB] Invalid update data: missing both update description and full document")
			return nil
		}

	case "replace":
		var fullDoc bson.M
		if err := bson.Unmarshal(rawEvent.FullDoc, &fullDoc); err != nil {
			s.logger.Errorf("[MongoDB] Failed to unmarshal replace document from BSON: %v", err)
			return nil
		}

		var docKey bson.M
		if err := bson.Unmarshal(rawEvent.DocKey, &docKey); err != nil {
			s.logger.Errorf("[MongoDB] Failed to unmarshal document key from BSON: %v", err)
			return nil
		}

		return mongo.NewReplaceOneModel().
			SetFilter(docKey).
			SetReplacement(fullDoc).
			SetUpsert(true)

	case "delete":
		var docKey bson.M
		if err := bson.Unmarshal(rawEvent.DocKey, &docKey); err != nil {
			s.logger.Errorf("[MongoDB] Failed to unmarshal delete document key from BSON: %v", err)
			return nil
		}

		return mongo.NewDeleteOneModel().SetFilter(docKey)

	default:
		s.logger.Errorf("[MongoDB] Unknown operation type in raw change event: %s", rawEvent.OpType)
		return nil
	}
}
