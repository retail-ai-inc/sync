package mongodb

import (
	"context"
	"fmt"

	"github.com/retail-ai-inc/sync/internal/platform/resilience"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// clusterTime reports the source's current cluster time.
//
// It is read before the snapshot copies a document, and the change stream is
// started from it afterwards. Starting the stream after the copy instead —
// which is what happens with no start point at all — loses every write made
// while the copy was running, and a first copy of a payment collection runs
// for as long as it runs.
func (s *MongoDBSyncer) clusterTime(ctx context.Context) (primitive.Timestamp, error) {
	raw, err := s.sourceClient.Database("admin").
		RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Raw()
	if err != nil {
		return primitive.Timestamp{}, fmt.Errorf("read cluster time: %w", err)
	}
	return clusterTimeFrom(raw)
}

// clusterTimeFrom picks the timestamp out of a hello reply. A replica set
// answers with both $clusterTime and operationTime; a standalone with neither,
// and a standalone cannot serve a change stream anyway.
func clusterTimeFrom(raw bson.Raw) (primitive.Timestamp, error) {
	if v, err := raw.LookupErr("$clusterTime", "clusterTime"); err == nil {
		if t, i, ok := v.TimestampOK(); ok {
			return primitive.Timestamp{T: t, I: i}, nil
		}
	}
	if v, err := raw.LookupErr("operationTime"); err == nil {
		if t, i, ok := v.TimestampOK(); ok {
			return primitive.Timestamp{T: t, I: i}, nil
		}
	}
	return primitive.Timestamp{}, fmt.Errorf(
		"the source reported no cluster time: change streams need a replica set")
}

func (s *MongoDBSyncer) doInitialSync(ctx context.Context, sourceColl, targetColl *mongo.Collection, sourceDB, targetDB string) error {
	// A target that already holds documents is not evidence the copy finished;
	// it is what an interrupted copy leaves. The checkpoint is the authority,
	// and with no checkpoint path there is nothing to remember between runs, so
	// the document count is all there is to go on.
	if s.cfg.MongoDBResumeTokenPath == "" {
		count, err := targetColl.EstimatedDocumentCount(ctx)
		if err != nil {
			return fmt.Errorf("check target collection %s.%s fail: %v", targetDB, targetColl.Name(), err)
		}
		if count > 0 {
			s.logger.Infof("[MongoDB] %s.%s has data and no checkpoint path is configured => skip initial sync", targetDB, targetColl.Name())
			return nil
		}
	}

	s.logger.Infof("[MongoDB] Starting initial sync for %s.%s -> %s.%s", sourceDB, sourceColl.Name(), targetDB, targetColl.Name())

	cursor, err := sourceColl.Find(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("source find fail => %v", err)
	}
	defer cursor.Close(ctx)

	batchSize := 100
	var batch []bson.M
	inserted := 0

	for cursor.Next(ctx) {
		var doc bson.M
		if errD := cursor.Decode(&doc); errD != nil {
			return fmt.Errorf("decode doc fail => %v", errD)
		}
		batch = append(batch, doc)
		if len(batch) >= batchSize {
			written, err := s.copyBatch(ctx, targetColl, batch, targetDB)
			if err != nil {
				return err
			}
			inserted += written
			batch = batch[:0]
		}
	}
	if err := cursor.Err(); err != nil {
		return fmt.Errorf("read source collection %s.%s: %w", sourceDB, sourceColl.Name(), err)
	}

	if len(batch) > 0 {
		written, err := s.copyBatch(ctx, targetColl, batch, targetDB)
		if err != nil {
			return err
		}
		inserted += written
	}

	s.logger.Infof("[MongoDB] doInitialSync => %s.%s => %s.%s inserted=%d docs",
		sourceDB, sourceColl.Name(), targetDB, targetColl.Name(), inserted)
	return nil
}

// copyBatch writes one batch of the snapshot.
//
// The writes are upserts rather than inserts because a copy that is interrupted
// and resumed re-reads documents it already wrote, and because the change
// stream that resumes from the pinned cluster time replays the writes made
// while the copy was running. Neither may fail on a document that is already
// there.
func (s *MongoDBSyncer) copyBatch(ctx context.Context, targetColl *mongo.Collection, batch []bson.M, targetDB string) (int, error) {
	models := make([]mongo.WriteModel, 0, len(batch))
	for _, doc := range batch {
		id, ok := doc["_id"]
		if !ok {
			// Nothing to address it by; an insert is the only option and a
			// resumed copy will duplicate it.
			models = append(models, mongo.NewInsertOneModel().SetDocument(doc))
			continue
		}
		models = append(models, mongo.NewReplaceOneModel().
			SetFilter(bson.M{"_id": id}).
			SetReplacement(doc).
			SetUpsert(true))
	}

	err := resilience.RetryMongoOperation(ctx, s.logger,
		fmt.Sprintf("copy %d documents to %s.%s", len(models), targetDB, targetColl.Name()),
		func() error {
			_, err := targetColl.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
			return err
		})
	if err != nil {
		return 0, fmt.Errorf("copy batch to %s.%s: %w", targetDB, targetColl.Name(), err)
	}
	return len(models), nil
}
