//go:build integration

package mongodb

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/test/harness"
)

// TestSameDocumentUpdatesConverge exercises F-029. The syncer applies a batch
// with options.BulkWrite().SetOrdered(false), so MongoDB may execute the
// operations in any order. When several updates to the same document land in
// one batch, an older value can be written last and stay there — the stream
// carries no further event to correct it.
//
// The scenario drives many rapid updates to a single document and then waits
// for the target to reach the final value. A timeout here means the target
// settled on a stale value, which is the defect.
func TestSameDocumentUpdatesConverge(t *testing.T) {
	collection := harness.UniqueName("ordering")
	src, tgt := connect(t, harness.MongoSource), connect(t, harness.MongoTarget)
	ctx := context.Background()
	srcColl := src.Database(sourceDB).Collection(collection)

	res, err := srcColl.InsertOne(ctx, bson.M{"key": "counter", "v": 0})
	if err != nil {
		t.Fatalf("seed source: %v", err)
	}
	id := res.InsertedID

	startSyncer(t, syncTask(t, collection))
	harness.Eventually(t, 30*time.Second, func() error {
		if n := countIn(t, tgt, targetDB, collection, bson.M{}); n != 1 {
			return fmt.Errorf("initial sync has not landed")
		}
		return nil
	})

	// Enough updates to fill several buffer batches; the writer flushes at 100
	// events or every two seconds.
	const updates = 300
	for i := 1; i <= updates; i++ {
		if _, err := srcColl.UpdateOne(ctx, bson.M{"_id": id}, bson.M{"$set": bson.M{"v": i}}); err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
	}

	harness.Eventually(t, 60*time.Second, func() error {
		doc, err := findOne(t, tgt, targetDB, collection, bson.M{"_id": id})
		if err != nil {
			return fmt.Errorf("document is missing: %w", err)
		}
		v, ok := doc["v"].(int32)
		if !ok {
			return fmt.Errorf("v has type %T, value %v", doc["v"], doc["v"])
		}
		if int(v) != updates {
			return fmt.Errorf("target settled on v=%d, source is v=%d", v, updates)
		}
		return nil
	})
}

// TestWritesDuringInitialSyncAreNotLost exercises F-020. doInitialSync reads
// the whole collection with Find and only afterwards opens a change stream,
// and on a first run there is no resume token, so Watch starts from the moment
// it is called. Anything written between the snapshot read and that call is in
// neither path.
//
// The source is seeded large enough that the snapshot takes seconds, then a
// marker is written while it runs. The marker must reach the target.
func TestWritesDuringInitialSyncAreNotLost(t *testing.T) {
	collection := harness.UniqueName("snapshotgap")
	src, tgt := connect(t, harness.MongoSource), connect(t, harness.MongoTarget)
	ctx := context.Background()
	srcColl := src.Database(sourceDB).Collection(collection)

	const seeded = 20000
	batch := make([]interface{}, 0, 1000)
	for i := 0; i < seeded; i++ {
		batch = append(batch, bson.M{"seq": i, "payload": "x"})
		if len(batch) == 1000 {
			if _, err := srcColl.InsertMany(ctx, batch); err != nil {
				t.Fatalf("seed source: %v", err)
			}
			batch = batch[:0]
		}
	}

	startSyncer(t, syncTask(t, collection))

	// Write markers while the snapshot is still copying the seeded documents.
	markers := 0
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		copied := countIn(t, tgt, targetDB, collection, bson.M{})
		if copied >= seeded {
			break // the snapshot finished before any marker could be written
		}
		if _, err := srcColl.InsertOne(ctx, bson.M{"marker": markers}); err != nil {
			t.Fatalf("write marker: %v", err)
		}
		markers++
		time.Sleep(150 * time.Millisecond)
	}
	if markers == 0 {
		t.Skip("the snapshot completed too quickly to write a marker; raise the seed size")
	}
	t.Logf("wrote %d markers while the snapshot was running", markers)

	harness.Eventually(t, 90*time.Second, func() error {
		if n := countIn(t, tgt, targetDB, collection, bson.M{"seq": bson.M{"$exists": true}}); n != seeded {
			return fmt.Errorf("snapshot still running: %d of %d seeded documents", n, seeded)
		}
		return nil
	})

	arrived := countIn(t, tgt, targetDB, collection, bson.M{"marker": bson.M{"$exists": true}})
	if arrived != int64(markers) {
		t.Errorf("%d of %d documents written during the snapshot reached the target; "+
			"writes in the window between the snapshot read and the change stream "+
			"opening are lost (F-020)", arrived, markers)
	}
}

// TestResumeAfterRestart checks that a stopped syncer picks up where it left
// off. The resume token is written under MongoDBResumeTokenPath, so the second
// run must reuse the same directory.
func TestResumeAfterRestart(t *testing.T) {
	collection := harness.UniqueName("resume")
	src, tgt := connect(t, harness.MongoSource), connect(t, harness.MongoTarget)
	ctx := context.Background()
	srcColl := src.Database(sourceDB).Collection(collection)

	if _, err := srcColl.InsertOne(ctx, bson.M{"seq": 0}); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	statePath := t.TempDir()
	newCfg := func() config.SyncConfig {
		cfg := syncTask(t, collection)
		cfg.MongoDBResumeTokenPath = statePath
		return cfg
	}

	stop := startSyncer(t, newCfg())
	harness.Eventually(t, 30*time.Second, func() error {
		if n := countIn(t, tgt, targetDB, collection, bson.M{}); n != 1 {
			return fmt.Errorf("initial sync has not landed")
		}
		return nil
	})

	// One change while running, so a resume token is definitely persisted.
	if _, err := srcColl.InsertOne(ctx, bson.M{"seq": 1}); err != nil {
		t.Fatalf("insert while running: %v", err)
	}
	harness.Eventually(t, 20*time.Second, func() error {
		if n := countIn(t, tgt, targetDB, collection, bson.M{"seq": 1}); n != 1 {
			return fmt.Errorf("first change has not arrived")
		}
		return nil
	})

	stop()
	time.Sleep(time.Second)

	// Written while nothing is watching.
	if _, err := srcColl.InsertOne(ctx, bson.M{"seq": 2}); err != nil {
		t.Fatalf("insert while stopped: %v", err)
	}

	startSyncer(t, newCfg())

	harness.Eventually(t, 30*time.Second, func() error {
		if n := countIn(t, tgt, targetDB, collection, bson.M{"seq": 2}); n != 1 {
			return fmt.Errorf("the change made while the syncer was stopped has not " +
				"been replayed; resuming from the stored token is not working")
		}
		return nil
	})
}

// TestIgnoreDeleteOpsLeavesDeletedDocuments pins F-033: with the option on, a
// document removed at the source stays on the target for good. That is the
// documented intent for an archive, and it is also why a task using it can
// never serve as a failover replica.
func TestIgnoreDeleteOpsLeavesDeletedDocuments(t *testing.T) {
	collection := harness.UniqueName("ignoredelete")
	src, tgt := connect(t, harness.MongoSource), connect(t, harness.MongoTarget)
	ctx := context.Background()
	srcColl := src.Database(sourceDB).Collection(collection)

	if _, err := srcColl.InsertOne(ctx, bson.M{"seq": 0}); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	cfg := syncTask(t, collection, config.TableMapping{
		SourceTable:      collection,
		TargetTable:      collection,
		AdvancedSettings: config.AdvancedSettings{IgnoreDeleteOps: true},
	})
	startSyncer(t, cfg)

	harness.Eventually(t, 30*time.Second, func() error {
		if n := countIn(t, tgt, targetDB, collection, bson.M{}); n != 1 {
			return fmt.Errorf("initial sync has not landed")
		}
		return nil
	})

	if _, err := srcColl.DeleteOne(ctx, bson.M{"seq": 0}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// The deletion must not propagate, and must keep not propagating.
	harness.Consistently(t, 8*time.Second, func() error {
		if n := countIn(t, tgt, targetDB, collection, bson.M{"seq": 0}); n != 1 {
			return fmt.Errorf("the document was removed from the target; deletions " +
				"now propagate despite ignoreDeleteOps, so assert that instead")
		}
		return nil
	})
}

// TestInsertThenDeleteInSameBatch targets F-029 from the angle that
// SetFullDocument(UpdateLookup) cannot mask. Repeated updates all resolve to
// the same looked-up document, so their order stops mattering; a create paired
// with a delete does not. The syncer turns inserts into ReplaceOne with upsert
// and deletes into DeleteOne, and applies the batch with SetOrdered(false), so
// the server may run the delete first and let the upsert recreate the document.
func TestInsertThenDeleteInSameBatch(t *testing.T) {
	collection := harness.UniqueName("insertdelete")
	src, tgt := connect(t, harness.MongoSource), connect(t, harness.MongoTarget)
	ctx := context.Background()
	srcColl := src.Database(sourceDB).Collection(collection)

	if _, err := srcColl.InsertOne(ctx, bson.M{"seq": -1}); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	startSyncer(t, syncTask(t, collection))
	harness.Eventually(t, 30*time.Second, func() error {
		if n := countIn(t, tgt, targetDB, collection, bson.M{}); n != 1 {
			return fmt.Errorf("initial sync has not landed")
		}
		return nil
	})

	// Create and immediately remove many documents. Each pair lands inside one
	// buffer flush, so both operations reach the same bulk write.
	const pairs = 150
	for i := 0; i < pairs; i++ {
		res, err := srcColl.InsertOne(ctx, bson.M{"pair": i})
		if err != nil {
			t.Fatalf("insert pair %d: %v", i, err)
		}
		if _, err := srcColl.DeleteOne(ctx, bson.M{"_id": res.InsertedID}); err != nil {
			t.Fatalf("delete pair %d: %v", i, err)
		}
	}

	// The source holds only the seed document, so the target must too.
	harness.Eventually(t, 60*time.Second, func() error {
		srcCount := countIn(t, src, sourceDB, collection, bson.M{"pair": bson.M{"$exists": true}})
		if srcCount != 0 {
			return fmt.Errorf("the source still holds %d paired documents", srcCount)
		}
		tgtCount := countIn(t, tgt, targetDB, collection, bson.M{"pair": bson.M{"$exists": true}})
		if tgtCount != 0 {
			return fmt.Errorf("%d of %d create/delete pairs left a document behind on "+
				"the target; the unordered bulk write applied the delete before the "+
				"upsert that recreated it (F-029)", tgtCount, pairs)
		}
		return nil
	})
}

// TestDeleteThenReinsertSameID is the mirror image: a document removed and
// immediately recreated with the same identifier must exist on the target. An
// unordered batch that runs the upsert before the delete leaves it missing.
func TestDeleteThenReinsertSameID(t *testing.T) {
	collection := harness.UniqueName("recreate")
	src, tgt := connect(t, harness.MongoSource), connect(t, harness.MongoTarget)
	ctx := context.Background()
	srcColl := src.Database(sourceDB).Collection(collection)

	res, err := srcColl.InsertOne(ctx, bson.M{"key": "recreated", "generation": 0})
	if err != nil {
		t.Fatalf("seed source: %v", err)
	}
	id := res.InsertedID

	startSyncer(t, syncTask(t, collection))
	harness.Eventually(t, 30*time.Second, func() error {
		if n := countIn(t, tgt, targetDB, collection, bson.M{}); n != 1 {
			return fmt.Errorf("initial sync has not landed")
		}
		return nil
	})

	const generations = 100
	for g := 1; g <= generations; g++ {
		if _, err := srcColl.DeleteOne(ctx, bson.M{"_id": id}); err != nil {
			t.Fatalf("delete generation %d: %v", g, err)
		}
		if _, err := srcColl.InsertOne(ctx, bson.M{"_id": id, "key": "recreated", "generation": g}); err != nil {
			t.Fatalf("reinsert generation %d: %v", g, err)
		}
	}

	// The source finishes its cycles in well under a second, so the target has
	// either caught up or diverged long before this window elapses. Ten seconds
	// is generous for convergence and cheap when the defect is present.
	harness.Eventually(t, 10*time.Second, func() error {
		doc, err := findOne(t, tgt, targetDB, collection, bson.M{"_id": id})
		if err != nil {
			return fmt.Errorf("the document is missing from the target after "+
				"%d delete/reinsert cycles; an unordered batch applied the delete "+
				"after the upsert that recreated it (F-029): %w", generations, err)
		}
		g, ok := doc["generation"].(int32)
		if !ok {
			return fmt.Errorf("generation has type %T", doc["generation"])
		}
		if int(g) != generations {
			return fmt.Errorf("target holds generation %d, source is at %d", g, generations)
		}
		return nil
	})
}

// TestFailedWritesReachTheDeadLetterQueue exercises the durability mechanism
// that is supposed to catch what bulk writes cannot apply. A unique index on
// the target rejects a document the source accepted, which is exactly the
// divergence a dead-letter queue exists for: the operation must be preserved
// on disk rather than dropped.
func TestFailedWritesReachTheDeadLetterQueue(t *testing.T) {
	collection := harness.UniqueName("deadletter")
	src, tgt := connect(t, harness.MongoSource), connect(t, harness.MongoTarget)
	ctx := context.Background()
	srcColl := src.Database(sourceDB).Collection(collection)
	tgtColl := tgt.Database(targetDB).Collection(collection)

	// The target rejects duplicate addresses; the source does not.
	if _, err := tgtColl.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "email", Value: 1}},
		Options: options.Index().SetUnique(true),
	}); err != nil {
		t.Fatalf("create unique index: %v", err)
	}

	if _, err := srcColl.InsertOne(ctx, bson.M{"seq": 1, "email": "a@b.com"}); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	statePath := t.TempDir()
	cfg := syncTask(t, collection)
	cfg.MongoDBResumeTokenPath = statePath
	startSyncer(t, cfg)

	harness.Eventually(t, 30*time.Second, func() error {
		if n := countIn(t, tgt, targetDB, collection, bson.M{}); n != 1 {
			return fmt.Errorf("initial sync has not landed")
		}
		return nil
	})

	// A second document the target cannot accept.
	if _, err := srcColl.InsertOne(ctx, bson.M{"seq": 2, "email": "a@b.com"}); err != nil {
		t.Fatalf("insert duplicate: %v", err)
	}

	// Batches are filed under <dead_letter>/<sourceDB>_<collection>/.
	collectionDir := filepath.Join(statePath, "dead_letter", sourceDB+"_"+collection)
	harness.Eventually(t, 60*time.Second, func() error {
		entries, err := os.ReadDir(collectionDir)
		if err != nil {
			return fmt.Errorf("dead-letter directory is not readable: %w", err)
		}
		if len(entries) == 0 {
			return fmt.Errorf("no batch was written to the dead-letter queue; the " +
				"rejected operation may have been dropped instead of preserved")
		}
		return nil
	})

	entries, err := os.ReadDir(collectionDir)
	if err != nil {
		t.Fatalf("read dead-letter directory: %v", err)
	}
	content, err := os.ReadFile(filepath.Join(collectionDir, entries[0].Name()))
	if err != nil {
		t.Fatalf("read dead-letter batch: %v", err)
	}

	var batch DeadLetterBatch
	if err := json.Unmarshal(content, &batch); err != nil {
		t.Fatalf("the batch is not the documented JSON shape: %v", err)
	}
	if len(batch.FailedOps) == 0 {
		t.Errorf("the batch records no failed operations: %+v", batch)
	}
	for _, op := range batch.FailedOps {
		if op.Error == "" {
			t.Error("a failed operation carries no error message, so the reason " +
				"for the rejection is lost")
		}
		if op.SourceColl != collection {
			t.Errorf("the operation names collection %q, want %q", op.SourceColl, collection)
		}
	}
	t.Logf("dead-letter batch %s holds %d failed operations out of %d, first error: %s",
		batch.BatchID, len(batch.FailedOps), batch.TotalOps, batch.FailedOps[0].Error)

	// The source moved on, the target could not, and nothing surfaces the gap
	// beyond this file: both collections are queried to show the divergence.
	srcCount := countIn(t, src, sourceDB, collection, bson.M{})
	tgtCount := countIn(t, tgt, targetDB, collection, bson.M{})
	if srcCount == tgtCount {
		t.Errorf("source and target both hold %d documents; the write was applied "+
			"after all, so this scenario no longer exercises the queue", srcCount)
	}
	t.Logf("source holds %d documents, target holds %d: the divergence persists "+
		"and is visible only in the dead-letter file", srcCount, tgtCount)
}
