package mongodb

import (
	"context"
	"fmt"

	"github.com/retail-ai-inc/sync/internal/platform/resilience"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// clusterTime reports the source's current cluster time. It is read before the
// snapshot copies a document, and the change stream is started from it
// afterwards.
func (s *MongoDBSyncer) clusterTime(ctx context.Context) (bson.Timestamp, error) {
	raw, err := s.sourceClient.Database("admin").
		RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Raw()
	if err != nil {
		return bson.Timestamp{}, fmt.Errorf("read cluster time: %w", err)
	}
	return clusterTimeFrom(raw)
}

// clusterTimeFrom picks the timestamp out of a hello reply. A replica set
// answers with both $clusterTime and operationTime; a standalone with neither,
// and a standalone cannot serve a change stream anyway.
func clusterTimeFrom(raw bson.Raw) (bson.Timestamp, error) {
	if v, err := raw.LookupErr("$clusterTime", "clusterTime"); err == nil {
		if t, i, ok := v.TimestampOK(); ok {
			return bson.Timestamp{T: t, I: i}, nil
		}
	}
	if v, err := raw.LookupErr("operationTime"); err == nil {
		if t, i, ok := v.TimestampOK(); ok {
			return bson.Timestamp{T: t, I: i}, nil
		}
	}
	return bson.Timestamp{}, fmt.Errorf(
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

	// How the target addresses these documents. A copy has no documentKey to
	// take it from, so the shard key is read once here and its values are taken
	// out of each document; without them a sharded target refuses every upsert.
	address, err := addressOf(ctx, s.sourceClient, sourceDB+"."+sourceColl.Name())
	if err != nil {
		return fmt.Errorf("read how %s.%s is partitioned: %w", sourceDB, sourceColl.Name(), err)
	}

	// A target that is still empty can be filled with plain inserts, which is
	// the difference between one round trip per batch and a keyed write per
	// document. It is asked once, here: nothing else writes to the target while
	// the copy runs, because the change stream only opens once it has finished.
	fresh, err := collectionIsEmpty(ctx, targetColl)
	if err != nil {
		return fmt.Errorf("check whether %s.%s is empty: %w", targetDB, targetColl.Name(), err)
	}

	cursor, err := sourceColl.Find(ctx, bson.M{}, options.Find().SetBatchSize(int32(snapshotBatch)))
	if err != nil {
		return fmt.Errorf("source find fail => %v", err)
	}
	defer cursor.Close(ctx)

	batchSize := snapshotBatch
	var batch []bson.M
	inserted := 0

	for cursor.Next(ctx) {
		var doc bson.M
		if errD := cursor.Decode(&doc); errD != nil {
			return fmt.Errorf("decode doc fail => %v", errD)
		}
		batch = append(batch, s.maskDocument(sourceColl.Name(), doc))
		if len(batch) >= batchSize {
			written, err := s.copyBatch(ctx, targetColl, batch, targetDB, address, &fresh)
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
		written, err := s.copyBatch(ctx, targetColl, batch, targetDB, address, &fresh)
		if err != nil {
			return err
		}
		inserted += written
	}

	s.logger.Infof("[MongoDB] doInitialSync => %s.%s => %s.%s inserted=%d docs",
		sourceDB, sourceColl.Name(), targetDB, targetColl.Name(), inserted)
	return nil
}

// snapshotBatch is how many documents move per round trip. It was 100, which
// held the copy at about 1,200 documents a second on a sharded staging target
// -- roughly twelve round trips a second, so the batch was the limit rather
// than either database.
const snapshotBatch = 1000

// collectionIsEmpty reports whether anything is in the collection yet. The
// count stops at the first document: the question is "is there anything", and
// counting a large collection to answer it would cost more than it saves.
func collectionIsEmpty(ctx context.Context, coll *mongo.Collection) (bool, error) {
	found, err := coll.CountDocuments(ctx, bson.M{}, options.Count().SetLimit(1))
	if err != nil {
		return false, err
	}
	return found == 0, nil
}

// insertBatch fills an empty collection with plain inserts. An upsert has to
// find each document before it writes it, and on a sharded target that is a
// keyed lookup per document; inserting sends the batch once.
func (s *MongoDBSyncer) insertBatch(ctx context.Context, targetColl *mongo.Collection, batch []bson.M, targetDB string) error {
	docs := make([]interface{}, 0, len(batch))
	for _, doc := range batch {
		docs = append(docs, doc)
	}
	return resilience.RetryMongoOperation(ctx, s.logger,
		fmt.Sprintf("insert %d documents into %s.%s", len(docs), targetDB, targetColl.Name()),
		func() error {
			_, err := targetColl.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
			return err
		})
}

// copyBatch writes one batch of the snapshot.
//
// An empty target is filled with inserts; anything else is written by key,
// because a copy that is interrupted and resumed re-reads documents it already
// wrote, and because the change stream that resumes from the pinned cluster
// time replays the writes made while the copy was running.
//
// fresh is the caller's flag and is cleared here: once inserting has failed
// once -- a document already there, a shard key the target will not take -- the
// rest of the collection goes by key. Falling back is always safe, since the
// keyed write is idempotent and re-covers whatever the failed insert managed to
// put in, and it is what produces the useful error when the cause was not a
// duplicate.
func (s *MongoDBSyncer) copyBatch(ctx context.Context, targetColl *mongo.Collection, batch []bson.M, targetDB string, address documentAddress, fresh *bool) (int, error) {
	if *fresh {
		err := s.insertBatch(ctx, targetColl, batch, targetDB)
		if err == nil {
			return len(batch), nil
		}
		*fresh = false
		s.logger.Warnf("[MongoDB] Inserting into %s.%s did not work (%v), so the "+
			"rest of this collection is copied by key, which is slower but takes "+
			"a target that already holds some of it",
			targetDB, targetColl.Name(), err)
	}

	models := make([]mongo.WriteModel, 0, len(batch))
	for _, doc := range batch {
		filter, err := address.filter(doc)
		if err != nil {
			if _, hasID := doc["_id"]; !hasID {
				// Nothing to address it by; an insert is the only option and a
				// resumed copy will duplicate it.
				models = append(models, mongo.NewInsertOneModel().SetDocument(doc))
				continue
			}
			// It has an _id but not the shard key, so the target cannot be told
			// where to put it. Guessing would either broadcast the write or have
			// it refused, and both are worse than saying so.
			return 0, fmt.Errorf("copy to %s.%s: %w", targetDB, targetColl.Name(), err)
		}
		models = append(models, mongo.NewReplaceOneModel().
			SetFilter(filter).
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
