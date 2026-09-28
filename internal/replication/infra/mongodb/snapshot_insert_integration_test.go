//go:build integration

package mongodb

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/retail-ai-inc/sync/test/harness"
)

// quietSnapshotSyncer is a syncer with only what copyBatch reaches for.
func quietSnapshotSyncer() *MongoDBSyncer {
	log := logrus.New()
	log.SetOutput(discardWriter{})
	return &MongoDBSyncer{logger: logrus.NewEntry(log)}
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// An empty target is filled with inserts, and the flag stays set so the rest of
// the collection keeps taking the fast path.
func TestAnEmptyTargetIsFilledWithInserts(t *testing.T) {
	ctx := context.Background()
	tgt := connect(t, harness.MongoTarget)
	coll := tgt.Database(harness.UniqueName("insertdb")).Collection("orders")
	t.Cleanup(func() { _ = coll.Database().Drop(ctx) })

	empty, err := collectionIsEmpty(ctx, coll)
	if err != nil {
		t.Fatalf("collectionIsEmpty: %v", err)
	}
	if !empty {
		t.Fatal("a collection that was just named is not empty")
	}

	s := quietSnapshotSyncer()
	fresh := true
	batch := []bson.M{{"_id": 1, "total": 10}, {"_id": 2, "total": 20}}
	written, err := s.copyBatch(ctx, coll, batch, coll.Database().Name(), documentAddress{}, &fresh)
	if err != nil {
		t.Fatalf("copyBatch: %v", err)
	}
	if written != 2 {
		t.Errorf("wrote %d documents, want 2", written)
	}
	if !fresh {
		t.Error("the fast path was given up after a batch that worked")
	}

	count, err := coll.CountDocuments(ctx, bson.M{})
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 2 {
		t.Errorf("the target holds %d documents, want 2", count)
	}
}

// A target that already holds a document cannot be inserted into, and the copy
// has to fall back rather than fail -- this is the interrupted-copy case. The
// fallback is per collection, not per batch, so the flag must stay cleared.
func TestATargetThatAlreadyHasTheDocumentFallsBackToKeyedWrites(t *testing.T) {
	ctx := context.Background()
	tgt := connect(t, harness.MongoTarget)
	coll := tgt.Database(harness.UniqueName("falldb")).Collection("orders")
	t.Cleanup(func() { _ = coll.Database().Drop(ctx) })

	if _, err := coll.InsertOne(ctx, bson.M{"_id": 1, "total": 999}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	empty, err := collectionIsEmpty(ctx, coll)
	if err != nil {
		t.Fatalf("collectionIsEmpty: %v", err)
	}
	if empty {
		t.Fatal("a collection holding a document is reported empty")
	}

	// Ask for the fast path anyway: this is what an interrupted copy looks like
	// when the emptiness check ran before the interruption.
	s := quietSnapshotSyncer()
	fresh := true
	batch := []bson.M{{"_id": 1, "total": 10}, {"_id": 2, "total": 20}}
	written, err := s.copyBatch(ctx, coll, batch, coll.Database().Name(), documentAddress{}, &fresh)
	if err != nil {
		t.Fatalf("copyBatch did not recover from a duplicate: %v", err)
	}
	if written != 2 {
		t.Errorf("wrote %d documents, want 2", written)
	}
	if fresh {
		t.Error("the fast path is still set after inserting failed")
	}

	// The keyed write replaces rather than duplicates: the seeded 999 is gone.
	var got bson.M
	if err := coll.FindOne(ctx, bson.M{"_id": 1}).Decode(&got); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if total, _ := got["total"].(int32); total != 10 {
		t.Errorf("_id 1 has total %v, want the copied 10", got["total"])
	}
	count, err := coll.CountDocuments(ctx, bson.M{})
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 2 {
		t.Errorf("the target holds %d documents, want 2 rather than duplicates", count)
	}
}
