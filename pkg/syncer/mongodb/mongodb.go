package mongodb

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
	token  bson.Raw
	model  mongo.WriteModel
	opType string
}

type MongoDBSyncer struct {
	sourceClient  *mongo.Client
	targetClient  *mongo.Client
	cfg           config.SyncConfig
	logger        logrus.FieldLogger
	resumeTokens  map[string]bson.Raw
	resumeTokensM sync.RWMutex
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

	return &MongoDBSyncer{
		sourceClient: sourceClient,
		targetClient: targetClient,
		cfg:          cfg,
		logger:       logger.WithField("sync_task_id", cfg.ID),
		resumeTokens: resumeMap,
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

		// Copy indexes from source to target
		if errIdx := s.copyIndexes(ctx, srcColl, tgtColl); errIdx != nil {
			s.logger.Warnf("[MongoDB] Failed to copy indexes for %s -> %s: %v", tableMap.SourceTable, tableMap.TargetTable, errIdx)
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

		if name, ok := idx["name"].(string); ok && existingIndexes[name] {
			s.logger.Debugf("[MongoDB] Index %s already exists, skipping", name)
			indexesSkipped++
			continue
		}

		keyDoc := bson.D{}
		if keys, ok := idx["key"].(bson.M); ok {
			for field, direction := range keys {
				keyDoc = append(keyDoc, bson.E{Key: field, Value: direction})
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

		indexModel := mongo.IndexModel{
			Keys:    keyDoc,
			Options: indexOptions,
		}

		name := ""
		if nameVal, hasName := idx["name"]; hasName {
			if nameStr, ok := nameVal.(string); ok {
				name = nameStr
			}
		}

		_, errC := targetColl.Indexes().CreateOne(ctx, indexModel)
		if errC != nil {
			if strings.Contains(errC.Error(), "already exists") {
				s.logger.Debugf("[MongoDB] Index %s already exists", name)
				indexesSkipped++
			} else {
				s.logger.Warnf("[MongoDB] Create index %s fail: %v", name, errC)
			}
		} else {
			s.logger.Infof("[MongoDB] Successfully created index %s for %s", name, targetColl.Name())
			indexesCreated++
		}
	}

	s.logger.Infof("[MongoDB] Index creation summary for %s: created=%d, skipped=%d",
		targetColl.Name(), indexesCreated, indexesSkipped)

	return nil
}

// CreateIndex creates a single field index on the collection
func createIndex(ctx context.Context, collection *mongo.Collection, field string, unique bool) error {
	indexModel := mongo.IndexModel{
		Keys:    bson.D{{Key: field, Value: 1}},
		Options: options.Index().SetUnique(unique),
	}

	_, err := collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return fmt.Errorf("create index fail: %w", err)
	}

	return nil
}

// CreateCompositeIndex creates a multi-field index on the collection
// fields is a map of field names to index direction (1 for ascending, -1 for descending)
func createCompositeIndex(ctx context.Context, collection *mongo.Collection, fields map[string]int, unique bool) error {
	// Convert map to bson.D for ordered keys
	indexKeys := bson.D{}
	for field, direction := range fields {
		indexKeys = append(indexKeys, bson.E{Key: field, Value: direction})
	}

	indexModel := mongo.IndexModel{
		Keys:    indexKeys,
		Options: options.Index().SetUnique(unique),
	}

	_, err := collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return fmt.Errorf("create composite index fail: %w", err)
	}

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
			res, errI := targetColl.InsertMany(ctx, batch)
			if errI != nil {
				return fmt.Errorf("insertMany fail => %v", errI)
			}
			inserted += len(res.InsertedIDs)
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		res, errI := targetColl.InsertMany(ctx, batch)
		if errI != nil {
			return fmt.Errorf("insertMany fail => %v", errI)
		}
		inserted += len(res.InsertedIDs)
	}
	s.logger.Infof("[MongoDB] doInitialSync => %s.%s => %s.%s inserted=%d docs",
		sourceDB, sourceColl.Name(), targetDB, targetColl.Name(), inserted)
	return nil
}

func (s *MongoDBSyncer) watchChanges(ctx context.Context, sourceColl, targetColl *mongo.Collection, sourceDB, collectionName string) {
	// Outer loop to re-establish the change stream if needed
	for {
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
		}

		cs, err := sourceColl.Watch(ctx, pipeline, opts)
		if err != nil {
			s.logger.Errorf("[MongoDB] Failed to establish change stream for %s.%s: %v", sourceDB, collectionName, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
				continue
			}
		}
		s.logger.Infof("[MongoDB] Watching changes => %s.%s", sourceDB, collectionName)

		var buffer []bufferedChange
		const batchSize = 200
		flushInterval := time.Second * 1
		timer := time.NewTimer(flushInterval)
		var bufferMutex sync.Mutex
		done := false

		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-timer.C:
					bufferMutex.Lock()
					if len(buffer) > 0 {
						s.logger.Debugf("[MongoDB] flush timer => %s.%s => flushing %d ops", sourceDB, collectionName, len(buffer))
						s.flushBuffer(ctx, targetColl, &buffer, sourceDB, collectionName)
					}
					bufferMutex.Unlock()
					timer.Reset(flushInterval)
				}
			}
		}()

		for {
			select {
			case <-ctx.Done():
				bufferMutex.Lock()
				if len(buffer) > 0 {
					s.logger.Infof("[MongoDB] context done => flush %d ops for %s.%s", len(buffer), sourceDB, collectionName)
					s.flushBuffer(ctx, targetColl, &buffer, sourceDB, collectionName)
				}
				bufferMutex.Unlock()
				cs.Close(ctx)
				return
			default:
				if cs.Next(ctx) {
					var changeEvent bson.M
					if errDec := cs.Decode(&changeEvent); errDec != nil {
						s.logger.Errorf("[MongoDB] decode event fail => %v", errDec)
						continue
					}
					opType, _ := changeEvent["operationType"].(string)
					token := cs.ResumeToken()
					model := s.prepareWriteModel(changeEvent, sourceDB, collectionName, opType)
					if model != nil {
						bufferMutex.Lock()
						buffer = append(buffer, bufferedChange{
							token:  token,
							model:  model,
							opType: opType,
						})
						bufferMutex.Unlock()

						queryStr := describeWriteModel(model, opType)
						s.logger.Debugf("[MongoDB][%s] table=%s.%s query=%s",
							strings.ToUpper(opType),
							targetColl.Database().Name(),
							targetColl.Name(),
							queryStr,
						)
						s.logger.Infof("[MongoDB][%s] table=%s.%s rowsAffected=1",
							strings.ToUpper(opType),
							targetColl.Database().Name(),
							targetColl.Name(),
						)

						bufferMutex.Lock()
						buffSize := len(buffer)
						if buffSize >= batchSize {
							s.logger.Infof("[MongoDB] buffer reached %d => flush now => %s.%s", batchSize, sourceDB, collectionName)
							s.flushBuffer(ctx, targetColl, &buffer, sourceDB, collectionName)
							timer.Reset(flushInterval)
						}
						bufferMutex.Unlock()
					}
				} else {
					if errCS := cs.Err(); errCS != nil {
						s.logger.Errorf("[MongoDB] changeStream error => %v", errCS)
						done = true
						break
					}
					done = true
					break
				}
			}
			if done {
				break
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
}

func (s *MongoDBSyncer) prepareWriteModel(doc bson.M, sourceDB, collName, opType string) mongo.WriteModel {
	tableSecurity := security.FindTableSecurityFromMappings(collName, s.cfg.Mappings)

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

	case "update", "replace":
		var docID interface{}
		if id, ok := doc["documentKey"].(bson.M)["_id"]; ok {
			docID = id
		} else {
			s.logger.Warnf("[MongoDB] cannot find _id in documentKey => %v", doc)
			return nil
		}

		if opType == "replace" {
			if fullDoc, ok := doc["fullDocument"].(bson.M); ok {
				if tableSecurity.SecurityEnabled && len(tableSecurity.FieldSecurity) > 0 {
					for field, value := range fullDoc {
						processedValue := security.ProcessValue(value, field, tableSecurity)
						fullDoc[field] = processedValue
					}
				}
				return mongo.NewReplaceOneModel().
					SetFilter(bson.M{"_id": docID}).
					SetReplacement(fullDoc).
					SetUpsert(true)
			}
		} else { // "update"
			if updateDesc, ok := doc["updateDescription"].(bson.M); ok {
				var updateDoc bson.M = make(bson.M)

				if updatedFields, ok := updateDesc["updatedFields"].(bson.M); ok && len(updatedFields) > 0 {
					if tableSecurity.SecurityEnabled && len(tableSecurity.FieldSecurity) > 0 {
						for field, value := range updatedFields {
							processedValue := security.ProcessValue(value, field, tableSecurity)
							updatedFields[field] = processedValue
						}
					}
					updateDoc["$set"] = updatedFields
				}

				if removedFields, ok := updateDesc["removedFields"].(bson.A); ok && len(removedFields) > 0 {
					unsetDoc := make(bson.M)
					for _, field := range removedFields {
						if fieldStr, ok := field.(string); ok {
							unsetDoc[fieldStr] = ""
						}
					}
					if len(unsetDoc) > 0 {
						updateDoc["$unset"] = unsetDoc
					}
				}

				if len(updateDoc) > 0 {
					return mongo.NewUpdateOneModel().
						SetFilter(bson.M{"_id": docID}).
						SetUpdate(updateDoc).
						SetUpsert(true)
				}
			}
		}

	case "delete":
		if docID, ok := doc["documentKey"].(bson.M)["_id"]; ok {
			return mongo.NewDeleteOneModel().SetFilter(bson.M{"_id": docID})
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

func (s *MongoDBSyncer) flushBuffer(ctx context.Context, targetColl *mongo.Collection, buffer *[]bufferedChange, sourceDB, collectionName string) {
	if len(*buffer) == 0 {
		return
	}

	writeModels := make([]mongo.WriteModel, 0, len(*buffer))
	var lastToken bson.Raw

	var insertCount, updateCount, deleteCount int

	for _, bc := range *buffer {
		writeModels = append(writeModels, bc.model)
		lastToken = bc.token

		switch bc.opType {
		case "insert":
			insertCount++
		case "update", "replace":
			updateCount++
		case "delete":
			deleteCount++
		}
	}

	res, err := targetColl.BulkWrite(ctx, writeModels, options.BulkWrite().SetOrdered(false))
	if err != nil {
		s.logger.Errorf("[MongoDB] BulkWrite fail => %v", err)
	} else {
		s.logger.Infof(
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
		if lastToken != nil {
			s.saveMongoDBResumeToken(sourceDB, collectionName, lastToken)
		}
	}
	*buffer = (*buffer)[:0]
}

func (s *MongoDBSyncer) loadMongoDBResumeToken(db, coll string) bson.Raw {
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
		return nil
	}
	return token
}

func (s *MongoDBSyncer) saveMongoDBResumeToken(db, coll string, token bson.Raw) {
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

func (s *MongoDBSyncer) handleStreamDocument(
	doc bson.M,
	buffer *[]bufferedChange,
	token bson.Raw,
	targetColl *mongo.Collection,
	sourceColl *mongo.Collection,
	sourceDB, collName string,
) {
	if doc == nil {
		return
	}

	tableSecurity := security.FindTableSecurityFromMappings(collName, s.cfg.Mappings)

	s.logger.Debugf("[MongoDB] Collection=%s security config: enabled=%v, rules=%d",
		collName, tableSecurity.SecurityEnabled, len(tableSecurity.FieldSecurity))

	operationType, ok := doc["operationType"].(string)
	if !ok {
		s.logger.Warnf("[MongoDB] Missing operationType in stream doc: %v", doc)
		return
	}

	if tableSecurity.SecurityEnabled && len(tableSecurity.FieldSecurity) > 0 {
		switch operationType {
		case "insert", "replace", "update":
			var documentData bson.M
			if operationType == "insert" || operationType == "replace" {
				if fullDoc, ok := doc["fullDocument"].(bson.M); ok {
					documentData = fullDoc
				}
			} else if operationType == "update" {
				if updateDesc, ok := doc["updateDescription"].(bson.M); ok {
					if updatedFields, ok := updateDesc["updatedFields"].(bson.M); ok {
						documentData = updatedFields
					}
				}
			}

			s.logger.Debugf("[MongoDB][%s] Data before processing: %v", operationType, documentData)

			if documentData != nil {
				for field, value := range documentData {
					processedValue := security.ProcessValue(value, field, tableSecurity)
					documentData[field] = processedValue
				}
			}

			s.logger.Debugf("[MongoDB][%s] Data after processing: %v", operationType, documentData)
		}
	}

	model := s.prepareWriteModel(doc, sourceDB, collName, operationType)
	if model != nil {
		*buffer = append(*buffer, bufferedChange{
			token:  token,
			model:  model,
			opType: operationType,
		})
	}
}
