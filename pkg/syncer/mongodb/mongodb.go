package mongodb

import (
	"context"
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

// persistedChange represents a change that will be saved to disk
type persistedChange struct {
	ID         string    `json:"id"`
	Token      bson.Raw  `json:"token"`
	Doc        []byte    `json:"doc"` // Changed to []byte to store raw BSON data
	OpType     string    `json:"op_type"`
	SourceDB   string    `json:"source_db"`
	SourceColl string    `json:"source_coll"`
	Timestamp  time.Time `json:"timestamp"`
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
}

func NewMongoDBSyncer(cfg config.SyncConfig, logger *logrus.Logger) *MongoDBSyncer {
	var err error
	var sourceClient *mongo.Client
	err = utils.Retry(5, 2*time.Second, 2.0, func() error {
		var connErr error
		sourceClient, connErr = mongo.Connect(context.Background(), options.Client().ApplyURI(cfg.SourceConnection))
		return connErr
	})
	if err != nil {
		logger.Errorf("[MongoDB] Failed to connect to source after retries: %v", err)
		return nil
	}

	var targetClient *mongo.Client
	err = utils.Retry(5, 2*time.Second, 2.0, func() error {
		var connErr error
		targetClient, connErr = mongo.Connect(context.Background(), options.Client().ApplyURI(cfg.TargetConnection))
		return connErr
	})
	if err != nil {
		logger.Errorf("[MongoDB] Failed to connect to target after retries: %v", err)
		return nil
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
		sourceClient:  sourceClient,
		targetClient:  targetClient,
		cfg:           cfg,
		logger:        logger.WithField("sync_task_id", cfg.ID),
		resumeTokens:  resumeMap,
		bufferDir:     bufferDir,
		bufferEnabled: true, // Enable by default
		processRate:   processRate,
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
		go s.watchChanges(ctx, srcColl, tgtColl, sourceDBName, tableMap.TargetTable)
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

	s.logMemoryUsage("WATCH_START", collectionName, 0)

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
	s.processBufferedChanges(ctx, targetColl, sourceDB, collectionName)

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
	var buffer []bufferedChange
	var latestToken bson.Raw
	var tokenMutex sync.RWMutex
	const batchSize = 500
	flushInterval := time.Second * 2
	timer := time.NewTimer(flushInterval)
	var bufferMutex sync.Mutex

	var eventsReceived int64
	var diskWriteFailures int
	var lastMemoryLogTime = time.Now()
	var lastBufferGrowthLogTime = time.Now()

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
						tokenMutex.RLock()
						currentToken := latestToken
						tokenMutex.RUnlock()
						s.storeToBufferWithLogging(ctx, &buffer, sourceDB, collectionName, currentToken)
					} else {
						s.logger.Debugf("[MongoDB] flush timer => %s.%s => flushing %d ops", sourceDB, collectionName, len(buffer))
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
		go s.processPersistentBuffer(ctx, targetColl, sourceDB, collectionName)
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
					tokenMutex.RLock()
					currentToken := latestToken
					tokenMutex.RUnlock()
					s.storeToBufferWithLogging(ctx, &buffer, sourceDB, collectionName, currentToken)
				} else {
					s.logger.Infof("[MongoDB] context done => flush %d ops for %s.%s", len(buffer), sourceDB, collectionName)
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
				model := s.prepareWriteModel(changeEvent, collectionName, opType)
				if model != nil {
					// Update latest token
					tokenMutex.Lock()
					latestToken = token
					tokenMutex.Unlock()

					// Determine the actual operation type based on the model type
					actualOpType := opType
					if opType == "update" {
						if _, isReplace := model.(*mongo.ReplaceOneModel); isReplace {
							actualOpType = "replace"
							s.logger.Debugf("[MongoDB] Changed opType from 'update' to 'replace' for full document replacement")
						}
					}

					bufferMutex.Lock()
					buffer = append(buffer, bufferedChange{
						model:  model,
						opType: actualOpType,
					})
					buffSize := len(buffer)
					bufferMutex.Unlock()

					// Activity will be tracked through ChangeStreamInfo updates in utils package

					queryStr := describeWriteModel(model, actualOpType)
					s.logger.Debugf("[MongoDB][%s] table=%s.%s query=%s",
						strings.ToUpper(actualOpType),
						targetColl.Database().Name(),
						targetColl.Name(),
						queryStr,
					)
					s.logger.Debugf("[MongoDB][%s] table=%s.%s rowsAffected=1",
						strings.ToUpper(actualOpType),
						targetColl.Database().Name(),
						targetColl.Name(),
					)

					eventsReceived++

					// Smart memory logging: only log when significant changes occur
					now := time.Now()
					shouldLogMemory := false
					shouldLogBufferGrowth := false

					// Log memory every 50K events or every 2 minutes
					if eventsReceived%50000 == 0 || now.Sub(lastMemoryLogTime) > 2*time.Minute {
						shouldLogMemory = true
						lastMemoryLogTime = now
					}

					// Log buffer growth only when significant or time-based
					if (buffSize%100 == 0 && buffSize > 100) || now.Sub(lastBufferGrowthLogTime) > time.Minute {
						shouldLogBufferGrowth = true
						lastBufferGrowthLogTime = now
					}

					if shouldLogMemory {
						s.logMemoryUsage("PERIODIC_EVENT", collectionName, buffSize)
					}

					if shouldLogBufferGrowth {
						s.logger.Warnf("[BUFFER_STATUS] Collection=%s | BufferSize=%d | EventsReceived=%d | DiskFailures=%d",
							collectionName, buffSize, eventsReceived, diskWriteFailures)
					}

					if buffSize >= batchSize {
						// Only log when hitting batch size threshold, not every 50 items
						s.logger.Warnf("[BUFFER_FLUSH_TRIGGER] Collection=%s | BufferSize=%d | Attempting disk write",
							collectionName, buffSize)

						if s.bufferEnabled {
							tokenMutex.RLock()
							currentToken := latestToken
							tokenMutex.RUnlock()

							// Only log memory before/after for large buffers or failures
							logMemoryForDisk := buffSize > 200 || diskWriteFailures > 0
							if logMemoryForDisk {
								s.logMemoryUsage("BEFORE_DISK_WRITE", collectionName, buffSize)
							}

							writeSuccess := s.storeToBufferWithLogging(ctx, &buffer, sourceDB, collectionName, currentToken)
							if !writeSuccess {
								diskWriteFailures++
								s.logger.Errorf("[DISK_WRITE_FAILURE] Collection=%s | BufferSize=%d | TotalFailures=%d",
									collectionName, len(buffer), diskWriteFailures)
								// Always log memory after failure for debugging
								s.logMemoryUsage("AFTER_DISK_WRITE_FAILURE", collectionName, len(buffer))
							} else if logMemoryForDisk {
								s.logMemoryUsage("AFTER_DISK_WRITE", collectionName, len(buffer))
							}
						}
					}

					if buffSize >= batchSize*2 {
						s.logger.Errorf("[EMERGENCY_FLUSH] Collection=%s | BufferSize=%d | CRITICAL MEMORY SITUATION",
							collectionName, buffSize)
						s.logMemoryUsage("EMERGENCY_FLUSH", collectionName, buffSize)

						runtime.GC()
						s.logMemoryUsage("AFTER_EMERGENCY_GC", collectionName, buffSize)
					}
				} else {
					s.logger.Debugf("[MongoDB] Failed to prepare write model for event type %s, skipping", opType)
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

		// Use full document replacement for all updates to ensure data consistency
		if fullDoc, ok := doc["fullDocument"].(bson.M); ok {
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
func (s *MongoDBSyncer) flushBuffer(ctx context.Context, targetColl *mongo.Collection, buffer *[]bufferedChange, sourceDB, collectionName string, currentToken bson.Raw) {
	if len(*buffer) == 0 {
		return
	}

	writeModels := make([]mongo.WriteModel, 0, len(*buffer))

	var insertCount, updateCount, deleteCount int
	for _, bc := range *buffer {
		writeModels = append(writeModels, bc.model)

		switch bc.opType {
		case "insert":
			insertCount++
		case "update", "replace":
			updateCount++
		case "delete":
			deleteCount++
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
			bufferSize := len(*buffer)

			// Accumulate ChangeStream activity with correct statistics
			// received = number of events that were buffered (batch size)
			// executed = number of operations that were successfully applied
			utils.AccumulateChangeStreamActivity(sourceDB, collectionName, successCount, bufferSize, successCount, int(res.InsertedCount), int(res.ModifiedCount), int(res.DeletedCount))

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
				switch bc.opType {
				case "insert":
					if ins, ok := bc.model.(*mongo.InsertOneModel); ok {
						docContent = toJSONString(ins.Document)
					}
				case "update", "replace":
					if rep, ok := bc.model.(*mongo.ReplaceOneModel); ok {
						docContent = fmt.Sprintf("filter=%s, replacement=%s", toJSONString(rep.Filter), toJSONString(rep.Replacement))
					} else if upd, ok := bc.model.(*mongo.UpdateOneModel); ok {
						docContent = fmt.Sprintf("filter=%s, update=%s", toJSONString(upd.Filter), toJSONString(upd.Update))
					}
				case "delete":
					if del, ok := bc.model.(*mongo.DeleteOneModel); ok {
						docContent = fmt.Sprintf("filter=%s", toJSONString(del.Filter))
					}
				}
				s.logger.Errorf("[MongoDB] Source doc [%d], type=%s: %s", i, bc.opType, docContent)

				if bc.opType == "insert" || bc.opType == "replace" {
					var doc bson.M
					if ins, ok := bc.model.(*mongo.InsertOneModel); ok {
						doc, _ = ins.Document.(bson.M)
					} else if rep, ok := bc.model.(*mongo.ReplaceOneModel); ok {
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

// storeToBufferWithLogging persists the buffered changes to disk
func (s *MongoDBSyncer) storeToBufferWithLogging(ctx context.Context, buffer *[]bufferedChange, sourceDB, collectionName string, latestToken bson.Raw) bool {
	if len(*buffer) == 0 {
		return true
	}

	bufferPath := s.getBufferPath(sourceDB, collectionName)
	startTime := time.Now()
	initialBufferSize := len(*buffer)

	// Only log detailed start info for large buffers or when there have been failures
	logDetailed := initialBufferSize > 200
	if logDetailed {
		s.logger.Infof("[DISK_WRITE_START] Collection=%s | BufferSize=%d",
			collectionName, initialBufferSize)
	}

	if err := os.MkdirAll(bufferPath, os.ModePerm); err != nil {
		s.logger.Errorf("[DISK_WRITE_MKDIR_FAIL] Collection=%s | Path=%s | Error=%v",
			collectionName, bufferPath, err)
		return false
	}

	totalDataSize := 0
	batchSize := 100
	timestamp := time.Now().UnixNano()
	writeSuccess := true

	for i := 0; i < len(*buffer); i += batchSize {
		end := i + batchSize
		if end > len(*buffer) {
			end = len(*buffer)
		}

		batchChanges := make([]persistedChange, 0, end-i)

		for j := i; j < end; j++ {
			change := (*buffer)[j]

			var docBytes []byte
			var err error

			switch change.opType {
			case "insert":
				if ins, ok := change.model.(*mongo.InsertOneModel); ok {
					docBytes, err = bson.Marshal(ins.Document)
				}
			case "update", "replace":
				if rep, ok := change.model.(*mongo.ReplaceOneModel); ok {
					docBytes, err = bson.Marshal(map[string]interface{}{
						"filter":      rep.Filter,
						"replacement": rep.Replacement,
					})
				}
			case "delete":
				if del, ok := change.model.(*mongo.DeleteOneModel); ok {
					docBytes, err = bson.Marshal(map[string]interface{}{
						"filter": del.Filter,
					})
				}
			}

			if err != nil {
				s.logger.Errorf("[DISK_WRITE_MARSHAL_FAIL] Collection=%s | OpType=%s | Error=%v",
					collectionName, change.opType, err)
				writeSuccess = false
				continue
			}

			totalDataSize += len(docBytes)

			persistedChange := persistedChange{
				ID:         fmt.Sprintf("%d_%d", timestamp, j),
				Token:      latestToken,
				Doc:        docBytes,
				OpType:     change.opType,
				SourceDB:   sourceDB,
				SourceColl: collectionName,
				Timestamp:  time.Now(),
			}

			batchChanges = append(batchChanges, persistedChange)
		}

		batchFilePath := filepath.Join(bufferPath, fmt.Sprintf("batch_%d_%d.json", timestamp, i))
		batchBytes, err := json.Marshal(batchChanges)
		if err != nil {
			s.logger.Errorf("[DISK_WRITE_JSON_FAIL] Collection=%s | BatchFile=%s | Error=%v",
				collectionName, batchFilePath, err)
			writeSuccess = false
			continue
		}

		// Only log individual file writes for large batches or debugging
		if logDetailed {
			s.logger.Infof("[DISK_WRITE_FILE] Collection=%s | File=%s | Size=%dKB | Changes=%d",
				collectionName, filepath.Base(batchFilePath), len(batchBytes)/1024, len(batchChanges))
		}

		if err := os.WriteFile(batchFilePath, batchBytes, 0644); err != nil {
			s.logger.Errorf("[DISK_WRITE_FILE_FAIL] Collection=%s | File=%s | Error=%v",
				collectionName, batchFilePath, err)
			writeSuccess = false
		}
	}

	duration := time.Since(startTime)

	// Always log completion summary
	s.logger.Infof("[DISK_WRITE_COMPLETE] Collection=%s | Items=%d | DataMB=%.1f | Duration=%v | Success=%v",
		collectionName, initialBufferSize, float64(totalDataSize)/1024/1024, duration, writeSuccess)

	if writeSuccess {
		*buffer = (*buffer)[:0]
		if latestToken != nil {
			s.saveMongoDBResumeToken(sourceDB, collectionName, latestToken)
		}
		if logDetailed {
			s.logger.Infof("[MEMORY_CLEARED] Collection=%s | Buffer cleared and resume token updated", collectionName)
		}
	} else {
		s.logger.Errorf("[MEMORY_RETAINED] Collection=%s | WriteSuccess=%v | Buffer size remains=%d | DATA ACCUMULATING IN MEMORY!",
			collectionName, writeSuccess, len(*buffer))
	}

	return writeSuccess
}

// processPersistentBuffer reads from the persistent buffer and applies changes at a controlled rate
func (s *MongoDBSyncer) processPersistentBuffer(ctx context.Context, targetColl *mongo.Collection, sourceDB, collectionName string) {
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
			processed := s.processBufferedChanges(ctx, targetColl, sourceDB, collectionName)
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

// processBufferedChanges processes a batch of buffered changes
func (s *MongoDBSyncer) processBufferedChanges(ctx context.Context, targetColl *mongo.Collection, sourceDB, collectionName string) int {
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

	var batch []bufferedChange
	var processedToken bson.Raw
	startTime := time.Now()

	// Track if we encounter any corrupted files
	corruptedFileEncountered := false

	// Read and process files
	for i := 0; i < filesToProcess; i++ {
		if i >= len(files) {
			break
		}

		filePath := filepath.Join(bufferPath, files[i].Name())
		fileData, err := os.ReadFile(filePath)
		if err != nil {
			s.logger.Errorf("[MongoDB] Failed to read buffer file %s: %v", filePath, err)
			continue
		}

		// Check if it's a batch processing file
		if strings.HasPrefix(filepath.Base(filePath), "batch_") {
			// Parse batch changes
			var batchChanges []persistedChange
			if err := json.Unmarshal(fileData, &batchChanges); err != nil {
				s.logger.Errorf("[MongoDB] Failed to unmarshal batch changes: %v", err)
				s.logger.Errorf("[MongoDB] Corrupted file: %s, size: %d bytes", filePath, len(fileData))
				_ = os.Remove(filePath)
				corruptedFileEncountered = true
				s.logger.Warnf("[MongoDB] Stopping processing due to corrupted file to prevent data loss")
				break // Stop processing to prevent skipping data
			}

			// Process each change in the batch
			for _, persistedChange := range batchChanges {
				// Convert to WriteModel
				model := s.convertToWriteModel(persistedChange)
				if model != nil {
					batch = append(batch, bufferedChange{
						model:  model,
						opType: persistedChange.OpType,
					})
					processedToken = persistedChange.Token
					processedCount++
				}
			}
		} else {
			// Process old format single change file
			var persistedChange persistedChange
			if err := json.Unmarshal(fileData, &persistedChange); err != nil {
				s.logger.Errorf("[MongoDB] Failed to unmarshal persisted change: %v", err)
				_ = os.Remove(filePath)
				corruptedFileEncountered = true
				s.logger.Warnf("[MongoDB] Stopping processing due to corrupted file to prevent data loss")
				break // Stop processing to prevent skipping data
			}

			model := s.convertToWriteModel(persistedChange)
			if model != nil {
				batch = append(batch, bufferedChange{
					model:  model,
					opType: persistedChange.OpType,
				})
				processedToken = persistedChange.Token
				processedCount++
			}
		}

		// Delete processed file
		if err := os.Remove(filePath); err != nil {
			s.logger.Warnf("[MongoDB] Failed to remove processed buffer file %s: %v", filePath, err)
		}
	}

	// Apply batch to target collection
	if len(batch) > 0 {
		deleteOps := countDeleteOps(batch)
		s.logger.Debugf("[MongoDB] Flushing %d operations (%d deletes) for %s.%s",
			len(batch), deleteOps, sourceDB, collectionName)
		s.flushBuffer(ctx, targetColl, &batch, sourceDB, collectionName, processedToken)

		// Only save resume token if no corrupted files were encountered
		if !corruptedFileEncountered && processedToken != nil {
			s.saveMongoDBResumeToken(sourceDB, collectionName, processedToken)
			s.logger.Debugf("[MongoDB] Resume token updated successfully")
		} else if corruptedFileEncountered {
			s.logger.Warnf("[MongoDB] Resume token NOT updated due to corrupted file encounter - will reprocess from last known good position")
		}
	}

	if processedCount > 0 {
		duration := time.Since(startTime)
		rate := float64(processedCount) / duration.Seconds()
		s.logger.Debugf("[MongoDB] Processed %d changes in %v (%.2f/sec) for %s.%s",
			processedCount, duration, rate, sourceDB, collectionName)
	}

	return processedCount
}

// Helper function to count delete operations in a batch
func countDeleteOps(batch []bufferedChange) int {
	count := 0
	for _, change := range batch {
		if change.opType == "delete" {
			count++
		}
	}
	return count
}

// getBufferPath returns the path to the buffer directory for a collection
func (s *MongoDBSyncer) getBufferPath(db, coll string) string {
	return filepath.Join(s.bufferDir, fmt.Sprintf("%s_%s", db, coll))
}

// convertToWriteModel converts persistedChange to WriteModel
func (s *MongoDBSyncer) convertToWriteModel(persistedChange persistedChange) mongo.WriteModel {
	var model mongo.WriteModel

	s.logger.Debugf("[MongoDB] convertToWriteModel: opType=%s, id=%s", persistedChange.OpType, persistedChange.ID)

	switch persistedChange.OpType {
	case "insert":
		var doc bson.D
		if err := bson.Unmarshal(persistedChange.Doc, &doc); err != nil {
			s.logger.Errorf("[MongoDB] Failed to unmarshal insert document from BSON: %v", err)
			return nil
		}
		model = mongo.NewInsertOneModel().SetDocument(doc)

	case "update":
		var updateData bson.M
		if err := bson.Unmarshal(persistedChange.Doc, &updateData); err != nil {
			s.logger.Errorf("[MongoDB] Failed to unmarshal update document from BSON: %v", err)
			return nil
		}

		filter, ok := updateData["filter"].(bson.M)
		if !ok {
			s.logger.Errorf("[MongoDB] Invalid update data structure: missing filter field")
			s.logger.Errorf("[MongoDB] updateData content: %v", updateData)
			return nil
		}

		// Check for new format (replacement) - this is what we want
		if replacement, hasReplacement := updateData["replacement"]; hasReplacement && replacement != nil {
			// New format: use ReplaceOneModel for full document replacement
			s.logger.Debugf("[MongoDB] Processing update as full document replacement")
			model = mongo.NewReplaceOneModel().
				SetFilter(filter).
				SetReplacement(replacement).
				SetUpsert(true)
		} else if updateData["update"] != nil {
			// Legacy format detected - skip to avoid array index issues
			s.logger.Warnf("[MongoDB] Skipping legacy update operation (partial update) for document %v to avoid array index issues. Please restart sync for full document replacement.", filter["_id"])
			return nil
		} else {
			s.logger.Errorf("[MongoDB] Invalid update data structure: missing both replacement and update fields")
			s.logger.Errorf("[MongoDB] updateData content: %v", updateData)
			return nil
		}

	case "replace":
		var updateData bson.M
		if err := bson.Unmarshal(persistedChange.Doc, &updateData); err != nil {
			s.logger.Errorf("[MongoDB] Failed to unmarshal replace document from BSON: %v", err)
			return nil
		}

		filter, ok := updateData["filter"].(bson.M)
		if !ok || updateData["replacement"] == nil {
			s.logger.Errorf("[MongoDB] Invalid replace data structure: missing filter or replacement field")
			s.logger.Errorf("[MongoDB] updateData content: %v", updateData)
			return nil
		}

		model = mongo.NewReplaceOneModel().
			SetFilter(filter).
			SetReplacement(updateData["replacement"]).
			SetUpsert(true)

	case "delete":
		var deleteData bson.M
		if err := bson.Unmarshal(persistedChange.Doc, &deleteData); err != nil {
			s.logger.Errorf("[MongoDB] Failed to unmarshal delete document from BSON: %v", err)
			return nil
		}

		filter, ok := deleteData["filter"].(bson.M)
		if !ok {
			s.logger.Errorf("[MongoDB] Invalid delete filter structure")
			return nil
		}

		model = mongo.NewDeleteOneModel().SetFilter(filter)

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
	}
}

func (s *MongoDBSyncer) logMemoryUsage(location, collectionName string, bufferSize int) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	allocMB := m.Alloc / 1024 / 1024
	sysMB := m.Sys / 1024 / 1024
	heapMB := m.HeapAlloc / 1024 / 1024
	heapSysMB := m.HeapSys / 1024 / 1024

	s.logger.Warnf("[MEMORY] %s | Collection=%s | BufferSize=%d | AllocMB=%d | SysMB=%d | HeapMB=%d | HeapSysMB=%d | NumGC=%d | HeapObjects=%d",
		location, collectionName, bufferSize, allocMB, sysMB, heapMB, heapSysMB, m.NumGC, m.HeapObjects)
}
