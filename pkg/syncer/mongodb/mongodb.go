package mongodb

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/retail-ai-inc/sync/pkg/config"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strings"
)

type bufferedChange struct {
	token bson.Raw
	model mongo.WriteModel
}

type MongoDBSyncer struct {
	sourceClient  *mongo.Client
	targetClient  *mongo.Client
	syncConfig    config.SyncConfig
	logger        *logrus.Logger
	lastSyncedAt  time.Time
	resumeTokens  map[string]bson.Raw // Key: "database.collection"
	resumeTokensM sync.RWMutex
}

func NewMongoDBSyncer(syncCfg config.SyncConfig, logger *logrus.Logger) *MongoDBSyncer {
	sourceClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI(syncCfg.SourceConnection))
	if err != nil {
		logger.Fatalf("[MongoDB] Failed to connect to source: %v", err)
	}

	targetClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI(syncCfg.TargetConnection))
	if err != nil {
		logger.Fatalf("[MongoDB] Failed to connect to target: %v", err)
	}

	resumeTokens := make(map[string]bson.Raw)
	if syncCfg.MongoDBResumeTokenPath != "" {
		err := os.MkdirAll(syncCfg.MongoDBResumeTokenPath, os.ModePerm)
		if err != nil {
			logger.Fatalf("[MongoDB] Failed to create resume token directory %s: %v", syncCfg.MongoDBResumeTokenPath, err)
		}
	}

	return &MongoDBSyncer{
		sourceClient: sourceClient,
		targetClient: targetClient,
		syncConfig:   syncCfg,
		logger:       logger,
		lastSyncedAt: time.Now().Add(-10 * time.Minute),
		resumeTokens: resumeTokens,
	}
}

// getResumeTokenPath constructs the resume token file path based on database and collection
func (s *MongoDBSyncer) getResumeTokenPath(db string, collection string) string {
	if s.syncConfig.MongoDBResumeTokenPath == "" {
		return ""
	}
	fileName := fmt.Sprintf("%s_%s.json", db, collection)
	return filepath.Join(s.syncConfig.MongoDBResumeTokenPath, fileName)
}

func (s *MongoDBSyncer) loadMongoDBResumeToken(db string, collection string) bson.Raw {
	path := s.getResumeTokenPath(db, collection)
	data, err := os.ReadFile(path)
	if err != nil {
		s.logger.Infof("[MongoDB] No previous resume token for %s.%s at %s: %v", db, collection, path, err)
		return nil
	}
	if len(data) <= 1 {
		s.logger.Infof("[MongoDB] Resume token file for %s.%s is empty: %s", db, collection, path)
		return nil
	}
	var token bson.Raw
	if err := json.Unmarshal(data, &token); err != nil {
		s.logger.Errorf("[MongoDB] Failed to unmarshal resume token for %s.%s: %v", db, collection, err)
		return nil
	}
	s.logger.Infof("[MongoDB] Loaded resume token for %s.%s from %s", db, collection, path)
	return token
}

func (s *MongoDBSyncer) saveMongoDBResumeToken(db string, collection string, token bson.Raw) {
	if s.syncConfig.MongoDBResumeTokenPath == "" {
		return
	}
	path := s.getResumeTokenPath(db, collection)
	data, err := json.Marshal(token)
	if err != nil {
		s.logger.Errorf("[MongoDB] Failed to marshal resume token for %s.%s: %v", db, collection, err)
		return
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		s.logger.Errorf("[MongoDB] Failed to write resume token for %s.%s to file %s: %v", db, collection, path, err)
	} else {
		s.logger.Debugf("[MongoDB] Saved resume token for %s.%s to %s", db, collection, path)
	}
}

func (s *MongoDBSyncer) removeMongoDBResumeToken(db string, collection string) {
	if s.syncConfig.MongoDBResumeTokenPath == "" {
		return
	}
	path := s.getResumeTokenPath(db, collection)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		s.logger.Errorf("[MongoDB] Failed to remove invalid resume token file %s for %s.%s: %v", path, db, collection, err)
	} else {
		s.logger.Infof("[MongoDB] Removed invalid resume token file %s for %s.%s", path, db, collection)
	}
	s.resumeTokensM.Lock()
	delete(s.resumeTokens, fmt.Sprintf("%s.%s", db, collection))
	s.resumeTokensM.Unlock()
}

func (s *MongoDBSyncer) Start(ctx context.Context) {
	var wg sync.WaitGroup

	for _, mapping := range s.syncConfig.Mappings {
		wg.Add(1)
		go func(mapping config.DatabaseMapping) {
			defer wg.Done()
			s.syncDatabase(ctx, mapping)
		}(mapping)
	}

	wg.Wait()
	s.logger.Info("[MongoDB] Synchronization completed.")
}

func (s *MongoDBSyncer) syncDatabase(ctx context.Context, mapping config.DatabaseMapping) {
	sourceDB := s.sourceClient.Database(mapping.SourceDatabase)
	targetDB := s.targetClient.Database(mapping.TargetDatabase)
	s.logger.Infof("[MongoDB] Processing database mapping: %s -> %s", mapping.SourceDatabase, mapping.TargetDatabase)

	for _, tableMap := range mapping.Tables {
		sourceColl := sourceDB.Collection(tableMap.SourceTable)
		targetColl := targetDB.Collection(tableMap.TargetTable)
		s.logger.Infof("[MongoDB] Processing collection mapping: %s -> %s", tableMap.SourceTable, tableMap.TargetTable)

		// If the target collection does not exist, create it first (MongoDB write operations can automatically create collections, but we explicitly do it here)
		if err := s.createCollectionIfNotExists(ctx, targetDB, tableMap.TargetTable); err != nil {
			s.logger.Errorf("[MongoDB] Failed to create target collection %s.%s: %v", mapping.TargetDatabase, tableMap.TargetTable, err)
			continue
		}

		if err := s.syncIndexes(ctx, sourceColl, targetColl); err != nil {
			s.logger.Errorf("[MongoDB] Failed to sync indexes for %s.%s: %v", mapping.TargetDatabase, tableMap.TargetTable, err)
		}

		err := s.initialSync(ctx, sourceColl, targetColl, mapping.SourceDatabase, mapping.TargetDatabase)
		if err != nil {
			s.logger.Errorf("[MongoDB] Initial sync failed for %s.%s: %v", mapping.SourceDatabase, tableMap.SourceTable, err)
			continue
		}

		go s.watchChangesForCollection(ctx, sourceColl, targetColl, mapping.SourceDatabase, mapping.TargetDatabase)
	}
}

// If the target collection does not exist, create it
func (s *MongoDBSyncer) createCollectionIfNotExists(ctx context.Context, db *mongo.Database, collName string) error {
	collections, err := db.ListCollectionNames(ctx, bson.M{"name": collName})
	if err != nil {
		return err
	}
	if len(collections) == 0 {
		s.logger.Infof("[MongoDB] Target collection not found, creating => %s.%s", db.Name(), collName)
		if err2 := db.CreateCollection(ctx, collName); err2 != nil {
			return err2
		}
	}
	return nil
}

// Sync indexes: Copy all indexes from sourceColl to targetColl
func (s *MongoDBSyncer) syncIndexes(ctx context.Context, sourceColl, targetColl *mongo.Collection) error {
	cursor, err := sourceColl.Indexes().List(ctx)
	if err != nil {
		return fmt.Errorf("list source indexes fail: %w", err)
	}
	defer cursor.Close(ctx)

	var indexDocs []bson.M
	if err = cursor.All(ctx, &indexDocs); err != nil {
		return fmt.Errorf("read indexes fail: %w", err)
	}

	for _, idx := range indexDocs {
		keys, ok := idx["key"].(bson.M)
		if !ok {
			continue
		}
		indexModel := mongo.IndexModel{
			Keys: keys,
		}
		if uniqueVal, hasUnique := idx["unique"]; hasUnique {
			if uv, isBool := uniqueVal.(bool); isBool && uv {
				indexModel.Options = options.Index().SetUnique(true)
			}
		}
		_, errCreate := targetColl.Indexes().CreateOne(ctx, indexModel)
		if errCreate != nil && !strings.Contains(errCreate.Error(), "already exists") {
			s.logger.Warnf("[MongoDB] create index fail => %v", errCreate)
		}
	}
	return nil
}

func (s *MongoDBSyncer) initialSync(ctx context.Context, sourceColl, targetColl *mongo.Collection, sourceDB, targetDB string) error {
	count, err := targetColl.EstimatedDocumentCount(ctx)
	if err != nil {
		return fmt.Errorf("[MongoDB] Failed to check target collection %s.%s: %v", targetDB, targetColl.Name(), err)
	}

	if count > 0 {
		s.logger.Infof("[MongoDB] Skipping initial sync for %s.%s -> %s.%s, target has data", sourceDB, sourceColl.Name(), targetDB, targetColl.Name())
		return nil
	}

	s.logger.Infof("[MongoDB] Starting initial sync for collection %s.%s -> %s.%s", sourceDB, sourceColl.Name(), targetDB, targetColl.Name())
	cursor, err := sourceColl.Find(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("[MongoDB] Failed to query source %s.%s for initial sync: %v", sourceDB, sourceColl.Name(), err)
	}
	defer cursor.Close(ctx)

	batchSize := 100
	var batch []interface{}

	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return fmt.Errorf("Failed to decode document during initial sync: %v", err)
		}
		batch = append(batch, doc)

		if len(batch) >= batchSize {
			_, err := targetColl.InsertMany(ctx, batch)
			if err != nil {
				return fmt.Errorf("[MongoDB] Failed to insert documents into %s.%s: %v", targetDB, targetColl.Name(), err)
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		_, err := targetColl.InsertMany(ctx, batch)
		if err != nil {
			return fmt.Errorf("[MongoDB] Failed to insert remaining documents into %s.%s: %v", targetDB, targetColl.Name(), err)
		}
	}

	s.logger.Infof("[MongoDB] Initial sync completed for %s.%s -> %s.%s", sourceDB, sourceColl.Name(), targetDB, targetColl.Name())
	return nil
}

func (s *MongoDBSyncer) watchChangesForCollection(ctx context.Context, sourceColl, targetColl *mongo.Collection, sourceDB, targetDB string) {
	collectionName := sourceColl.Name()
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
		s.logger.Infof("[MongoDB] Resuming change stream for %s.%s from saved resume token", sourceDB, collectionName)
	}

	cs, err := sourceColl.Watch(ctx, pipeline, opts)
	if err != nil && resumeToken != nil {
		s.logger.Errorf("[MongoDB] Failed to resume with token for %s.%s: %v, retrying without token", sourceDB, collectionName, err)
		s.removeMongoDBResumeToken(sourceDB, collectionName)
		opts = options.ChangeStream().SetFullDocument(options.UpdateLookup)
		cs, err = sourceColl.Watch(ctx, pipeline, opts)
	}

	if err != nil {
		s.logger.Errorf("[MongoDB] Failed to watch changes for %s.%s: %v", sourceDB, collectionName, err)
		return
	}
	defer cs.Close(ctx)

	s.logger.Infof("[MongoDB] Watching changes in %s.%s", sourceDB, collectionName)

	var buffer []bufferedChange
	const batchSize = 200
	flushInterval := time.Second * 1
	timer := time.NewTimer(flushInterval)
	defer timer.Stop()

	var bufferMutex sync.Mutex

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				bufferMutex.Lock()
				if len(buffer) > 0 {
					s.logger.Debugf("[MongoDB] Flush timer for %s.%s triggered, flushing %d ops", sourceDB, collectionName, len(buffer))
					s.flushBuffer(ctx, targetColl, &buffer, targetDB, sourceDB, collectionName)
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
				s.logger.Infof("[MongoDB] Context done, flushing remaining %d ops for %s.%s", len(buffer), sourceDB, collectionName)
				s.flushBuffer(ctx, targetColl, &buffer, targetDB, sourceDB, collectionName)
			}
			bufferMutex.Unlock()
			return
		default:
			if cs.Next(ctx) {
				var changeEvent bson.M
				if err := cs.Decode(&changeEvent); err != nil {
					s.logger.Errorf("[MongoDB] Failed to decode change event for %s.%s: %v", sourceDB, collectionName, err)
					continue
				}

				operationType, _ := changeEvent["operationType"].(string)
				s.logger.Debugf("[MongoDB] [OP: %s] {db: %s, coll: %s} Event: %+v", operationType, sourceDB, collectionName, changeEvent)

				token := cs.ResumeToken()
				model := s.prepareWriteModel(changeEvent, sourceDB, collectionName)
				if model != nil {
					bufferMutex.Lock()
					buffer = append(buffer, bufferedChange{
						token: token,
						model: model,
					})
					bufferSize := len(buffer)
					bufferMutex.Unlock()

					if bufferSize >= batchSize {
						bufferMutex.Lock()
						s.logger.Infof("[MongoDB] Buffer reached %d for %s.%s, flushing", batchSize, sourceDB, collectionName)
						s.flushBuffer(ctx, targetColl, &buffer, targetDB, sourceDB, collectionName)
						bufferMutex.Unlock()
						timer.Reset(flushInterval)
					}
				}
			} else {
				if err := cs.Err(); err != nil {
					s.logger.Errorf("[MongoDB] ChangeStream error for %s.%s: %v", sourceDB, collectionName, err)
					return
				}
			}
		}
	}
}

func (s *MongoDBSyncer) prepareWriteModel(changeEvent bson.M, sourceDB, collectionName string) mongo.WriteModel {
	operationType, _ := changeEvent["operationType"].(string)
	fullDocument, _ := changeEvent["fullDocument"].(bson.M)
	documentKey, _ := changeEvent["documentKey"].(bson.M)
	filter := documentKey

	switch operationType {
	case "insert":
		if fullDocument != nil {
			s.logger.Debugf("[MongoDB] [INSERT] {db: %s, coll: %s} Doc: %+v", sourceDB, collectionName, fullDocument)
			return mongo.NewInsertOneModel().SetDocument(fullDocument)
		}
	case "update", "replace":
		if fullDocument != nil {
			s.logger.Debugf("[MongoDB] [UPDATE] {db: %s, coll: %s} Filter: %+v, Doc: %+v", sourceDB, collectionName, filter, fullDocument)
			return mongo.NewReplaceOneModel().SetFilter(filter).SetReplacement(fullDocument).SetUpsert(true)
		}
	case "delete":
		s.logger.Debugf("[MongoDB] [DELETE] {db: %s, coll: %s} Filter: %+v", sourceDB, collectionName, filter)
		return mongo.NewDeleteOneModel().SetFilter(filter)
	default:
		s.logger.Warnf("[MongoDB] Unhandled operation type: %s for %s.%s", operationType, sourceDB, collectionName)
	}
	return nil
}

func (s *MongoDBSyncer) flushBuffer(
	ctx context.Context,
	targetColl *mongo.Collection,
	buffer *[]bufferedChange,
	targetDB, sourceDB, collectionName string,
) {
	if len(*buffer) == 0 {
		return
	}

	writeModels := make([]mongo.WriteModel, len(*buffer))
	var lastToken bson.Raw
	for i, bc := range *buffer {
		writeModels[i] = bc.model
		lastToken = bc.token
	}

	opts := options.BulkWrite().SetOrdered(false)
	result, err := targetColl.BulkWrite(ctx, writeModels, opts)
	if err != nil {
		s.logger.Errorf("[MongoDB] Bulk write failed for %s.%s: %v", targetDB, targetColl.Name(), err)
	} else {
		s.logger.Debugf("[MongoDB] Bulk write result for %s.%s => Inserted: %d, Matched: %d, Modified: %d, Upserted: %d, Deleted: %d",
			targetDB, targetColl.Name(),
			result.InsertedCount,
			result.MatchedCount,
			result.ModifiedCount,
			result.UpsertedCount,
			result.DeletedCount,
		)
		if lastToken != nil {
			s.saveMongoDBResumeToken(sourceDB, collectionName, lastToken)
		}
	}

	*buffer = (*buffer)[:0]
	s.logger.Debugf("[MongoDB] Cleared buffer for %s.%s after flush", sourceDB, collectionName)
}
