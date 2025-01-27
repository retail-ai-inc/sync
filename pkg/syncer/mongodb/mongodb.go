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
	logger        logrus.FieldLogger
	resumeTokens  map[string]bson.Raw
	resumeTokensM sync.RWMutex
}

func NewMongoDBSyncer(syncCfg config.SyncConfig, logger *logrus.Logger) *MongoDBSyncer {
	sourceClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI(syncCfg.SourceConnection))
	if err != nil {
		logger.Errorf("[MongoDB] Failed to connect to source: %v", err)
		return nil
	}

	targetClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI(syncCfg.TargetConnection))
	if err != nil {
		logger.Errorf("[MongoDB] Failed to connect to target: %v", err)
		return nil
	}

	resumeMap := make(map[string]bson.Raw)
	if syncCfg.MongoDBResumeTokenPath != "" {
		if e2 := os.MkdirAll(syncCfg.MongoDBResumeTokenPath, os.ModePerm); e2 != nil {
			logger.Warnf("[MongoDB] Failed to create resume token dir %s: %v", syncCfg.MongoDBResumeTokenPath, e2)
		}
	}

	return &MongoDBSyncer{
		sourceClient: sourceClient,
		targetClient: targetClient,
		syncConfig:   syncCfg,
		logger:       logger.WithField("sync_task_id", syncCfg.ID),
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
	for _, mapping := range s.syncConfig.Mappings {
		wg.Add(1)
		go func(m config.DatabaseMapping) {
			defer wg.Done()
			s.syncDatabase(ctx, m)
		}(mapping)
	}
	wg.Wait()

	s.logger.Info("[MongoDB] All database mappings have been processed.")
}

func (s *MongoDBSyncer) syncDatabase(ctx context.Context, mapping config.DatabaseMapping) {
	sourceDB := s.sourceClient.Database(mapping.SourceDatabase)
	targetDB := s.targetClient.Database(mapping.TargetDatabase)
	s.logger.Infof("[MongoDB] Processing database mapping: %s -> %s", mapping.SourceDatabase, mapping.TargetDatabase)

	for _, tableMap := range mapping.Tables {
		srcColl := sourceDB.Collection(tableMap.SourceTable)
		tgtColl := targetDB.Collection(tableMap.TargetTable)
		s.logger.Infof("[MongoDB] Processing collection mapping: %s -> %s", tableMap.SourceTable, tableMap.TargetTable)

		if errCreate := s.createCollectionIfNotExists(ctx, targetDB, tableMap.TargetTable); errCreate != nil {
			s.logger.Errorf("[MongoDB] Failed to create target collection %s.%s: %v",
				mapping.TargetDatabase, tableMap.TargetTable, errCreate)
			continue
		}

		if errIdx := s.syncIndexes(ctx, srcColl, tgtColl); errIdx != nil {
			s.logger.Warnf("[MongoDB] Failed to sync indexes for %s -> %s: %v", tableMap.SourceTable, tableMap.TargetTable, errIdx)
		}

		if errInit := s.initialSync(ctx, srcColl, tgtColl, mapping.SourceDatabase, mapping.TargetDatabase); errInit != nil {
			s.logger.Errorf("[MongoDB] initialSync fail => %v", errInit)
			continue
		}

		go s.watchChangesForCollection(ctx, srcColl, tgtColl, mapping.SourceDatabase, mapping.TargetDatabase)
	}
}

func (s *MongoDBSyncer) createCollectionIfNotExists(ctx context.Context, db *mongo.Database, collName string) error {
	collections, err := db.ListCollectionNames(ctx, bson.M{"name": collName})
	if err != nil {
		return err
	}
	if len(collections) == 0 {
		s.logger.Infof("[MongoDB] Creating collection => %s.%s", db.Name(), collName)
		return db.CreateCollection(ctx, collName)
	}
	return nil
}

func (s *MongoDBSyncer) syncIndexes(ctx context.Context, sourceColl, targetColl *mongo.Collection) error {
	cursor, err := sourceColl.Indexes().List(ctx)
	if err != nil {
		return fmt.Errorf("list source indexes fail: %w", err)
	}
	defer cursor.Close(ctx)

	var indexDocs []bson.M
	if err2 := cursor.All(ctx, &indexDocs); err2 != nil {
		return fmt.Errorf("read indexes fail: %w", err2)
	}

	for _, idx := range indexDocs {
		keys, ok := idx["key"].(bson.M)
		if !ok {
			continue
		}
		indexModel := mongo.IndexModel{Keys: keys}
		if uniqueVal, hasUnique := idx["unique"]; hasUnique {
			if uv, isBool := uniqueVal.(bool); isBool && uv {
				indexModel.Options = options.Index().SetUnique(true)
			}
		}
		_, errC := targetColl.Indexes().CreateOne(ctx, indexModel)
		if errC != nil && !strings.Contains(errC.Error(), "already exists") {
			s.logger.Warnf("[MongoDB] create index fail => %v", errC)
		}
	}
	return nil
}

func (s *MongoDBSyncer) initialSync(ctx context.Context, sourceColl, targetColl *mongo.Collection, sourceDB, targetDB string) error {
	count, err := targetColl.EstimatedDocumentCount(ctx)
	if err != nil {
		return fmt.Errorf("check target collection %s.%s fail: %v", targetDB, targetColl.Name(), err)
	}
	if count > 0 {
		s.logger.Infof("[MongoDB] %s.%s has data => skip initial sync", targetDB, targetColl.Name())
		return nil
	}

	s.logger.Infof("[MongoDB] Starting initial sync for %s.%s -> %s.%s", sourceDB, sourceColl.Name(), targetDB, targetColl.Name())

	cursor, errQ := sourceColl.Find(ctx, bson.M{})
	if errQ != nil {
		return fmt.Errorf("source find fail => %v", errQ)
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
	s.logger.Infof("[MongoDB] initialSync => %s.%s => %s.%s inserted=%d docs",
		sourceDB, sourceColl.Name(), targetDB, targetColl.Name(), inserted)
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
		s.logger.Infof("[MongoDB] Resume token found => %s.%s", sourceDB, collectionName)
	}

	cs, err := sourceColl.Watch(ctx, pipeline, opts)
	if err != nil && resumeToken != nil {
		s.logger.Warnf("[MongoDB] Watch with token fail => retry without token. error=%v", err)
		s.removeMongoDBResumeToken(sourceDB, collectionName)
		opts = options.ChangeStream().SetFullDocument(options.UpdateLookup)
		cs, err = sourceColl.Watch(ctx, pipeline, opts)
	}
	if err != nil {
		s.logger.Errorf("[MongoDB] watchChanges fail => %s.%s => %v", sourceDB, collectionName, err)
		return
	}
	defer cs.Close(ctx)

	s.logger.Infof("[MongoDB] watching changes => %s.%s", sourceDB, collectionName)

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
						token: token,
						model: model,
					})
					s.logger.Debugf("[MongoDB][%s] queue doc => %s.%s totalQueue=%d", opType, sourceDB, collectionName, len(buffer))
					buffSize := len(buffer)
					bufferMutex.Unlock()

					if buffSize >= batchSize {
						bufferMutex.Lock()
						s.logger.Infof("[MongoDB] buffer reached %d => flush now => %s.%s", batchSize, sourceDB, collectionName)
						s.flushBuffer(ctx, targetColl, &buffer, sourceDB, collectionName)
						bufferMutex.Unlock()
						timer.Reset(flushInterval)
					}
				}
			} else {
				if errCS := cs.Err(); errCS != nil {
					s.logger.Errorf("[MongoDB] changeStream error => %v", errCS)
					return
				}
			}
		}
	}
}

func (s *MongoDBSyncer) prepareWriteModel(changeEvent bson.M, sourceDB, collectionName, opType string) mongo.WriteModel {
	fullDoc, _ := changeEvent["fullDocument"].(bson.M)
	docKey, _ := changeEvent["documentKey"].(bson.M)
	switch opType {
	case "insert":
		if fullDoc != nil {
			return mongo.NewInsertOneModel().SetDocument(fullDoc)
		}
	case "update", "replace":
		if fullDoc != nil && docKey != nil {
			return mongo.NewReplaceOneModel().
				SetFilter(docKey).
				SetReplacement(fullDoc).
				SetUpsert(true)
		}
	case "delete":
		if docKey != nil {
			return mongo.NewDeleteOneModel().SetFilter(docKey)
		}
	default:
		s.logger.Debugf("[MongoDB] Unhandled opType=%s => skip", opType)
	}
	return nil
}

func (s *MongoDBSyncer) flushBuffer(
	ctx context.Context,
	targetColl *mongo.Collection,
	buffer *[]bufferedChange,
	sourceDB, collectionName string,
) {
	if len(*buffer) == 0 {
		return
	}
	writeModels := make([]mongo.WriteModel, 0, len(*buffer))
	var lastToken bson.Raw
	for _, bc := range *buffer {
		writeModels = append(writeModels, bc.model)
		lastToken = bc.token
	}

	res, err := targetColl.BulkWrite(ctx, writeModels, options.BulkWrite().SetOrdered(false))
	if err != nil {
		s.logger.Errorf("[MongoDB] BulkWrite fail => %v", err)
	} else {
		s.logger.Infof("[MongoDB] BulkWrite => %s.%s inserted=%d matched=%d modified=%d upserted=%d deleted=%d",
			targetColl.Database().Name(), targetColl.Name(),
			res.InsertedCount, res.MatchedCount, res.ModifiedCount, res.UpsertedCount, res.DeletedCount)
		if lastToken != nil {
			s.saveMongoDBResumeToken(sourceDB, collectionName, lastToken)
		}
	}
	*buffer = (*buffer)[:0]
}

func (s *MongoDBSyncer) loadMongoDBResumeToken(db, coll string) bson.Raw {
	if s.syncConfig.MongoDBResumeTokenPath == "" {
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
	if s.syncConfig.MongoDBResumeTokenPath == "" {
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
	if s.syncConfig.MongoDBResumeTokenPath == "" {
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
	return filepath.Join(s.syncConfig.MongoDBResumeTokenPath, fileName)
}
