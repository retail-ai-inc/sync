//go:build integration

package mongodb

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/test/harness"
	"github.com/sirupsen/logrus"
)

// TestSameDocumentUpdatesConverge exercises F-029. The syncer applies a batch
// with options.BulkWrite().SetOrdered(false), so MongoDB may execute the
// operations in any order. When several updates to the same document land in
// one batch, an older value could be written last and stay there — the stream
// carries no further event to correct it. The batch is now split into runs that
// hold at most one write per document, so a document's own changes cannot cross.
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

// TestWritesDuringInitialSyncAreNotLost exercises F-020. The source's cluster
// time is read before the copy begins and the change stream starts from it, so
// a write made while the copy is running is replayed by the stream rather than
// falling between the two.
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

	// The seeded documents arrive through the copy; the markers arrive
	// afterwards, through the stream replaying from the cluster time the copy
	// pinned. So this has to be polled rather than read once: the copy
	// converging says nothing about the replay having caught up.
	harness.Eventually(t, 60*time.Second, func() error {
		arrived := countIn(t, tgt, targetDB, collection, bson.M{"marker": bson.M{"$exists": true}})
		if arrived != int64(markers) {
			return fmt.Errorf("%d of %d documents written during the snapshot have "+
				"reached the target; writes in the window between the snapshot read "+
				"and the change stream opening are lost (F-020)", arrived, markers)
		}
		return nil
	})
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

	// One configuration, reused: the two runs have to be the same task, because
	// the resume token is keyed by task id.
	base := syncTask(t, collection)
	base.MongoDBResumeTokenPath = t.TempDir()
	newCfg := func() config.SyncConfig { return base }

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

// TestAnUnshardedSourceIsLeftAlone covers the ordinary case against a real
// server: a replica set has no config database to read a shard key from, and
// nothing should be attempted on the target because of it.
func TestAnUnshardedSourceIsLeftAlone(t *testing.T) {
	collection := harness.UniqueName("unsharded")
	src, tgt := connect(t, harness.MongoSource), connect(t, harness.MongoTarget)
	ctx := context.Background()

	if _, err := src.Database(sourceDB).Collection(collection).
		InsertOne(ctx, bson.M{"seq": 1}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	key, err := collectionShardKey(ctx, src, sourceDB+"."+collection)
	if err != nil {
		t.Fatalf("collectionShardKey: %v", err)
	}
	if key != nil {
		t.Fatalf("an unsharded collection reported a shard key: %v", key.Key)
	}

	// And the target is not a sharded cluster, which is what the fixture is.
	sharded, err := isMongos(ctx, tgt)
	if err != nil {
		t.Fatalf("isMongos: %v", err)
	}
	if sharded {
		t.Error("the fixture target reports itself as a sharded cluster")
	}

	startSyncer(t, syncTask(t, collection))
	harness.Eventually(t, 30*time.Second, func() error {
		if n := countIn(t, tgt, targetDB, collection, bson.M{}); n != 1 {
			return fmt.Errorf("the document has not arrived")
		}
		return nil
	})
}

// TestACollectionTheTaskDoesNotListIsReported covers the gap a named task
// leaves. A task that names its collections replicates those and no more, which
// is the point of naming them — but a collection added at the source afterwards
// is then missing from the replica, and a failover is a bad time to find out.
func TestACollectionTheTaskDoesNotListIsReported(t *testing.T) {
	listed := harness.UniqueName("listed")
	unlisted := harness.UniqueName("unlisted")
	src, tgt := connect(t, harness.MongoSource), connect(t, harness.MongoTarget)
	ctx := context.Background()

	for _, name := range []string{listed, unlisted} {
		if _, err := src.Database(sourceDB).Collection(name).
			InsertOne(ctx, bson.M{"seq": 1}); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
		t.Cleanup(func() {
			_ = src.Database(sourceDB).Collection(name).Drop(context.Background())
			_ = tgt.Database(targetDB).Collection(name).Drop(context.Background())
		})
	}

	var recorded bytes.Buffer
	logger := logrus.New()
	logger.SetOutput(&recorded)
	logger.SetLevel(logrus.WarnLevel)

	syncer := NewSyncer(syncTask(t, listed), &config.Config{}, logger)
	if syncer == nil {
		t.Fatal("NewSyncer returned nil")
	}
	t.Cleanup(harness.RunSyncer(t, syncer.Start))

	harness.Eventually(t, 30*time.Second, func() error {
		if n := countIn(t, tgt, targetDB, listed, bson.M{}); n != 1 {
			return fmt.Errorf("the listed collection has not been copied")
		}
		return nil
	})
	if n := countIn(t, tgt, targetDB, unlisted, bson.M{}); n > 0 {
		t.Errorf("the unlisted collection was replicated (%d documents); a named task "+
			"should replicate only what it names", n)
	}
	harness.Eventually(t, 30*time.Second, func() error {
		if !strings.Contains(recorded.String(), unlisted) {
			return fmt.Errorf("nothing warned about %s", unlisted)
		}
		return nil
	})
}
