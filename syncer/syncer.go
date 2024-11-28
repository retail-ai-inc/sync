package syncer

import (
    "context"
    "fmt"
    "sync"
    "time"

    "github.com/sirupsen/logrus"
    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"

    "mongodb_sync/config"
)

type Syncer struct {
    clientA      *mongo.Client
    clientB      *mongo.Client
    syncMappings []config.SyncMapping
    logger       *logrus.Logger
    lastSyncedAt time.Time
}

func NewSyncer(clientA, clientB *mongo.Client, syncMappings []config.SyncMapping, logger *logrus.Logger) *Syncer {
    return &Syncer{
        clientA:      clientA,
        clientB:      clientB,
        syncMappings: syncMappings,
        logger:       logger,
        lastSyncedAt: time.Now().Add(-10 * time.Minute), // Initial synchronization time
    }
}

func (s *Syncer) Start(ctx context.Context) {
    var wg sync.WaitGroup

    for _, mapping := range s.syncMappings {
        wg.Add(1)
        go func(mapping config.SyncMapping) {
            defer wg.Done()
            s.syncDatabase(ctx, mapping)
        }(mapping)
    }

    go func() {
        wg.Wait()
        s.logger.Info("Synchronization of all databases completed")
    }()
}

func (s *Syncer) syncDatabase(ctx context.Context, mapping config.SyncMapping) {
    dbA := s.clientA.Database(mapping.SourceDatabase)
    dbB := s.clientB.Database(mapping.TargetDatabase)
    s.logger.Infof("Processing mapping: %+v", mapping)

    for _, collMap := range mapping.Collections {
        collA := dbA.Collection(collMap.SourceCollection)
        collB := dbB.Collection(collMap.TargetCollection)
        s.logger.Infof("Processing collection mapping: %+v", collMap)

        // Initial sync
        err := s.initialSync(ctx, collA, collB, mapping.SourceDatabase, mapping.TargetDatabase)
        if err != nil {
            s.logger.Errorf("Initial sync of collection %s.%s to %s.%s failed: %v",
                mapping.SourceDatabase, collA.Name(), mapping.TargetDatabase, collB.Name(), err)
            continue
        }

        // Incremental sync
        // err = s.incrementalSync(ctx, collA, collB)
        // if err != nil {
        //     s.logger.Errorf("Incremental sync of collection %s.%s to %s.%s failed: %v",
        //         mapping.SourceDatabase, collMap.SourceCollection,
        //         mapping.TargetDatabase, collMap.TargetCollection, err)
        //     continue
        // }

        go s.watchChangesForCollection(ctx, collA, collB, mapping.SourceDatabase, mapping.TargetDatabase)
    }
}

func (s *Syncer) initialSync(ctx context.Context, collA, collB *mongo.Collection, sourceDatabase, targetDatabase string) error {
    count, err := collB.EstimatedDocumentCount(ctx)
    if err != nil {
        return fmt.Errorf("Failed to check target collection %s.%s document count: %v", targetDatabase, collB.Name(), err)
    }

    if count > 0 {
        s.logger.Infof("Skipping initial sync for %s.%s to %s.%s as target collection already contains data",
            sourceDatabase, collA.Name(), targetDatabase, collB.Name())
        return nil
    }

    s.logger.Infof("Starting initial sync for collection %s.%s to %s.%s", sourceDatabase, collA.Name(), targetDatabase, collB.Name())

    cursor, err := collA.Find(ctx, bson.M{})
    if err != nil {
        return fmt.Errorf("Failed to query source collection %s.%s for initial sync: %v", sourceDatabase, collA.Name(), err)
    }
    defer cursor.Close(ctx)

    batchSize := 200
    var batch []interface{}

    for cursor.Next(ctx) {
        var doc bson.M
        if err := cursor.Decode(&doc); err != nil {
            return fmt.Errorf("Failed to decode document during initial sync: %v", err)
        }
        batch = append(batch, doc)

        if len(batch) >= batchSize {
            _, err := collB.InsertMany(ctx, batch)
            if err != nil {
                return fmt.Errorf("Failed to insert documents into target collection %s.%s during initial sync: %v", targetDatabase, collB.Name(), err)
            }
            batch = batch[:0]
        }
    }

    if len(batch) > 0 {
        _, err := collB.InsertMany(ctx, batch)
        if err != nil {
            return fmt.Errorf("Failed to insert remaining documents into target collection %s.%s during initial sync: %v", targetDatabase, collB.Name(), err)
        }
    }

    s.logger.Infof("Initial sync of collection %s.%s to %s.%s completed", sourceDatabase, collA.Name(), targetDatabase, collB.Name())
    return nil
}

func (s *Syncer) incrementalSync(ctx context.Context, collA, collB *mongo.Collection) error {
    filter := bson.M{"updatedAt": bson.M{"$gt": s.lastSyncedAt}}

    s.logger.Infof("Performing incremental sync for collection: %s, filter: %v, lastSyncedAt: %v", collA.Name(), filter, s.lastSyncedAt)

    cursor, err := collA.Find(ctx, filter)
    if err != nil {
        return fmt.Errorf("Failed to query source collection for incremental sync: %v", err)
    }
    defer cursor.Close(ctx)

    var models []mongo.WriteModel
    batchSize := 200  

    for cursor.Next(ctx) {
        var doc bson.M
        if err := cursor.Decode(&doc); err != nil {
            return fmt.Errorf("Failed to decode document during incremental sync: %v", err)
        }

        id := doc["_id"]
        filter := bson.M{"_id": id}
        update := bson.M{"$set": doc}  

        // Upsert
        model := mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true)
        models = append(models, model)

        // Perform batch write operations when batch size is reached
        if len(models) >= batchSize {
            if err := s.executeBulkWrite(ctx, collB, models); err != nil {
                return err
            }
            models = models[:0] // Clear batch
        }
    }

    // Insert remaining documents
    if len(models) > 0 {
        if err := s.executeBulkWrite(ctx, collB, models); err != nil {
            return err
        }
    }

    s.logger.Infof("Incremental sync completed.")
    return nil
}

// Helper function for executing batch write operations
func (s *Syncer) executeBulkWrite(ctx context.Context, coll *mongo.Collection, models []mongo.WriteModel) error {
    if len(models) == 0 {
        return nil
    }

    opts := options.BulkWrite().SetOrdered(false)
    result, err := coll.BulkWrite(ctx, models, opts)
    if err != nil {
        return fmt.Errorf("Failed to execute bulk write: %v", err)
    }

    s.logger.Infof("Bulk write result - Matched: %d, Modified: %d, Upserted: %d",
        result.MatchedCount, result.ModifiedCount, result.UpsertedCount)

    return nil
}

func (s *Syncer) watchChangesForCollection(ctx context.Context, collA, collB *mongo.Collection, sourceDatabase, targetDatabase string) {
    // Set up Change Stream pipeline, filtering operation types
    pipeline := mongo.Pipeline{
        {{Key: "$match", Value: bson.D{
            {Key: "ns.db", Value: sourceDatabase},
            {Key: "ns.coll", Value: collA.Name()},
            {Key: "operationType", Value: bson.M{"$in": []string{"insert", "update", "replace", "delete"}}},
        }}},
    }
    opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)

    cs, err := collA.Watch(ctx, pipeline, opts)
    if err != nil {
        s.logger.Errorf("Failed to watch Change Stream for collection %s.%s: %v", sourceDatabase, collA.Name(), err)
        return
    }
    defer cs.Close(ctx)

    s.logger.Infof("Started watching changes in collection %s.%s", sourceDatabase, collA.Name())

    // Introduce batch processing
    var buffer []mongo.WriteModel
    const batchSize = 200
    flushInterval := time.Second * 1 // Periodic flush
    timer := time.NewTimer(flushInterval)
    defer timer.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        default:
            if cs.TryNext(ctx) {
                var changeEvent bson.M
                if err := cs.Decode(&changeEvent); err != nil {
                    s.logger.Errorf("Failed to decode change event for %s.%s: %v", sourceDatabase, collA.Name(), err)
                    continue
                }

                // Build batch operations
                writeModel := s.prepareWriteModel(changeEvent)
                if writeModel != nil {
                    buffer = append(buffer, writeModel)
                }

                // Perform batch write when batch size is reached
                if len(buffer) >= batchSize {
                    s.flushBuffer(ctx, collB, &buffer, targetDatabase)
                    timer.Reset(flushInterval)
                }
            } else {
                // Check for errors
                if err := cs.Err(); err != nil {
                    s.logger.Errorf("Change Stream error for collection %s.%s: %v", sourceDatabase, collA.Name(), err)
                    return
                }
            }

            
            select {
            case <-timer.C:
                if len(buffer) > 0 {
                    s.flushBuffer(ctx, collB, &buffer, targetDatabase)
                }
                timer.Reset(flushInterval)
            default:
                // Continue watching for changes
            }
        }
    }
}

func (s *Syncer) prepareWriteModel(changeEvent bson.M) mongo.WriteModel {
    operationType, _ := changeEvent["operationType"].(string)
    fullDocument, _ := changeEvent["fullDocument"].(bson.M)
    documentKey, _ := changeEvent["documentKey"].(bson.M)
    filter := documentKey

    switch operationType {
    case "insert":
        if fullDocument != nil {
            return mongo.NewInsertOneModel().SetDocument(fullDocument)
        }
    case "update", "replace":
        if fullDocument != nil {
            return mongo.NewReplaceOneModel().SetFilter(filter).SetReplacement(fullDocument).SetUpsert(true)
        }
    case "delete":
        return mongo.NewDeleteOneModel().SetFilter(filter)
    default:
        s.logger.Warnf("Unhandled operation type: %s", operationType)
    }
    return nil
}

func (s *Syncer) flushBuffer(ctx context.Context, collB *mongo.Collection, buffer *[]mongo.WriteModel, targetDatabase string) {
    if len(*buffer) == 0 {
        return
    }

    opts := options.BulkWrite().SetOrdered(false)
    result, err := collB.BulkWrite(ctx, *buffer, opts)
    if err != nil {
        s.logger.Errorf("Bulk write failed for collection %s.%s: %v", targetDatabase, collB.Name(), err)
    } else {
        s.logger.Infof("Bulk write result for collection %s.%s - Matched: %d, Modified: %d, Upserted: %d",
            targetDatabase, collB.Name(), result.MatchedCount, result.ModifiedCount, result.UpsertedCount)
    }

    // Clear buffer
    *buffer = (*buffer)[:0]
}