//go:build integration

package mongodb

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/test/harness"
)

// Applying an update as the fields it touched, against the two databases.
//
// The delta itself is the cheap path and the interesting cases are the two it
// cannot cover: a document the target does not have, which a delta cannot
// create, and a change the event does not describe exactly.

// deltaApplier writes to the test target and can read back from the test
// source, which is what the two fallbacks need.
func deltaApplier(t *testing.T, collection string) (*Applier, *mongo.Client, *mongo.Client) {
	t.Helper()

	src, tgt := connect(t, harness.MongoSource), connect(t, harness.MongoTarget)
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	return &Applier{
		Client:         tgt,
		Source:         src,
		TargetDatabase: targetDB,
		Mappings: []config.DatabaseMapping{{
			Tables: []config.TableMapping{{SourceTable: collection, TargetTable: collection}},
		}},
		Logger: logger,
		// No transaction: the fixture's nodes are pinned single-node replica
		// sets, and what is under test is which write is made rather than how it
		// is committed.
		NoTransaction: true,
	}, src, tgt
}

func deltaEvent(collection string, id interface{}, update bson.M) *domain.Event {
	return &domain.Event{
		NS: domain.Namespace{DB: sourceDB, Object: collection},
		Op: domain.OpUpdate,
		Payload: mongo.NewUpdateOneModel().
			SetFilter(bson.M{"_id": id}).
			SetUpdate(update),
	}
}

// The whole point: the fields the change touched are written and the rest of
// the document is not sent at all.
func TestADeltaLeavesTheRestOfTheDocumentAlone(t *testing.T) {
	collection := harness.UniqueName("delta")
	applier, src, tgt := deltaApplier(t, collection)
	ctx := context.Background()

	// A document big enough that sending it whole for one field would be the
	// cost this avoids.
	document := bson.M{
		"_id":    "order-1",
		"status": 1,
		"blob":   strings.Repeat("x", 256<<10),
	}
	if _, err := src.Database(sourceDB).Collection(collection).InsertOne(ctx, document); err != nil {
		t.Fatalf("seed the source: %v", err)
	}
	if _, err := tgt.Database(targetDB).Collection(collection).InsertOne(ctx, document); err != nil {
		t.Fatalf("seed the target: %v", err)
	}

	run := []*domain.Event{deltaEvent(collection, "order-1", bson.M{"$set": bson.M{"status": 2}})}
	if _, err := applier.Apply(ctx, [][]*domain.Event{run}, domain.Position{}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	got, err := findOne(t, tgt, targetDB, collection, bson.M{"_id": "order-1"})
	if err != nil {
		t.Fatalf("read the target: %v", err)
	}
	if fmt.Sprint(got["status"]) != "2" {
		t.Errorf("status = %v, want 2", got["status"])
	}
	if blob, _ := got["blob"].(string); len(blob) != 256<<10 {
		t.Errorf("the rest of the document is %d bytes, want it untouched at %d",
			len(blob), 256<<10)
	}
}

// A delta carries the fields that changed, not the document, so it cannot
// create one. An update that matched nothing used to be a divergence with
// nothing to show for it.
func TestAnUpdateToADocumentTheTargetLacksIsWrittenWhole(t *testing.T) {
	collection := harness.UniqueName("missing")
	applier, src, tgt := deltaApplier(t, collection)
	ctx := context.Background()

	if _, err := src.Database(sourceDB).Collection(collection).InsertOne(ctx, bson.M{
		"_id": "order-2", "status": 2, "note": "kept on the source alone",
	}); err != nil {
		t.Fatalf("seed the source: %v", err)
	}

	// The target has nothing, which is the state a delta cannot repair on its
	// own.
	run := []*domain.Event{deltaEvent(collection, "order-2", bson.M{"$set": bson.M{"status": 2}})}
	if _, err := applier.Apply(ctx, [][]*domain.Event{run}, domain.Position{}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	got, err := findOne(t, tgt, targetDB, collection, bson.M{"_id": "order-2"})
	if err != nil {
		t.Fatalf("the target still does not hold the document the update addressed: %v", err)
	}
	if got["note"] != "kept on the source alone" {
		t.Errorf("the document was written as %v, want the whole of it from the source", got)
	}
}

// A document the source no longer holds is not written at all: the delete for
// it is further along the same stream, and an upsert here would leave the
// target holding a document the source does not have.
func TestAChangeToADocumentGoneFromBothEndsWritesNothing(t *testing.T) {
	collection := harness.UniqueName("vanished")
	applier, _, tgt := deltaApplier(t, collection)
	ctx := context.Background()

	run := []*domain.Event{{
		NS:      domain.Namespace{DB: sourceDB, Object: collection},
		Op:      domain.OpUpdate,
		Payload: &fullDocumentRead{filter: bson.M{"_id": "order-3"}, reason: reasonUndescribed},
	}}
	if _, err := applier.Apply(ctx, [][]*domain.Event{run}, domain.Position{}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if n := countIn(t, tgt, targetDB, collection, bson.M{"_id": "order-3"}); n != 0 {
		t.Errorf("the target holds %d documents the source does not have", n)
	}
}

// End to end, through the stream: an array the source shortened is shortened
// on the target. The event says how long the array is now and not which
// elements went, so without that being written the target would keep elements
// the source has dropped.
func TestAnArrayShortenedOnTheSourceIsShortenedOnTheTarget(t *testing.T) {
	collection := harness.UniqueName("truncate")
	src, tgt := connect(t, harness.MongoSource), connect(t, harness.MongoTarget)
	ctx := context.Background()
	srcColl := src.Database(sourceDB).Collection(collection)

	if _, err := srcColl.InsertOne(ctx, bson.M{
		"_id": "cart-1", "items": bson.A{1, 2, 3, 4, 5}, "status": 1,
	}); err != nil {
		t.Fatalf("seed the source: %v", err)
	}

	startSyncer(t, syncTask(t, collection))
	harness.Eventually(t, 30*time.Second, func() error {
		if n := countIn(t, tgt, targetDB, collection, bson.M{}); n != 1 {
			return fmt.Errorf("the first copy has not landed: %d documents", n)
		}
		return nil
	})

	// Two elements off the end, which is what the server reports as a truncated
	// array rather than as a new value for the field.
	if _, err := srcColl.UpdateOne(ctx, bson.M{"_id": "cart-1"},
		bson.M{"$push": bson.M{"items": bson.M{"$each": bson.A{}, "$slice": 3}}}); err != nil {
		t.Fatalf("shorten the array: %v", err)
	}

	harness.Eventually(t, 30*time.Second, func() error {
		got, err := findOne(t, tgt, targetDB, collection, bson.M{"_id": "cart-1"})
		if err != nil {
			return fmt.Errorf("read the target: %w", err)
		}
		items, ok := got["items"].(bson.A)
		if !ok {
			return fmt.Errorf("items is a %T", got["items"])
		}
		if len(items) != 3 {
			return fmt.Errorf("items has %d elements, want 3: %v", len(items), items)
		}
		return nil
	})
}

// The stream stops asking for whole documents once it has been read to its
// end, and an update after that carries only what it changed. Before then it
// may be replaying changes older than what the first copy wrote, where a delta
// would put old values back.
func TestUpdatesAreReplicatedAsFieldsOnceTheStreamIsCaughtUp(t *testing.T) {
	collection := harness.UniqueName("caughtup")
	src, tgt := connect(t, harness.MongoSource), connect(t, harness.MongoTarget)
	ctx := context.Background()
	srcColl := src.Database(sourceDB).Collection(collection)

	if _, err := srcColl.InsertOne(ctx, bson.M{
		"_id": "order-4", "status": 1, "blob": strings.Repeat("y", 128<<10),
	}); err != nil {
		t.Fatalf("seed the source: %v", err)
	}

	task := syncTask(t, collection)
	taskID := task.ID
	startSyncer(t, task)
	harness.Eventually(t, 30*time.Second, func() error {
		if n := countIn(t, tgt, targetDB, collection, bson.M{}); n != 1 {
			return fmt.Errorf("the first copy has not landed: %d documents", n)
		}
		return nil
	})

	// The stream reports that it stopped asking for whole documents, which is
	// the condition under which the delta is safe.
	labels := metrics.Labels{"task": fmt.Sprint(taskID), "engine": "mongodb"}
	harness.Eventually(t, 30*time.Second, func() error {
		for _, sample := range metrics.Default.Snapshot(metrics.WholeDocumentMode) {
			if sample.Labels.Key() != labels.Key() {
				continue
			}
			if sample.Value != 0 {
				return fmt.Errorf("still asking for whole documents")
			}
			return nil
		}
		return fmt.Errorf("%s{%s} was never recorded", metrics.WholeDocumentMode, labels.Key())
	})

	for _, status := range []int{2, 3} {
		if _, err := srcColl.UpdateOne(ctx, bson.M{"_id": "order-4"},
			bson.M{"$set": bson.M{"status": status}}); err != nil {
			t.Fatalf("update: %v", err)
		}
		harness.Eventually(t, 30*time.Second, func() error {
			got, err := findOne(t, tgt, targetDB, collection, bson.M{"_id": "order-4"})
			if err != nil {
				return fmt.Errorf("read the target: %w", err)
			}
			if fmt.Sprint(got["status"]) != fmt.Sprint(status) {
				return fmt.Errorf("status = %v, want %d", got["status"], status)
			}
			// The field moved and the document is still whole, whichever way the
			// change was written.
			if blob, _ := got["blob"].(string); len(blob) != 128<<10 {
				return fmt.Errorf("the rest of the document is %d bytes, want %d",
					len(blob), 128<<10)
			}
			return nil
		})
	}
}
