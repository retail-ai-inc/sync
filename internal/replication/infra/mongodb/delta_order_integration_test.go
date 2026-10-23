//go:build integration

package mongodb

import (
	"context"
	"fmt"
	"reflect"
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

// Two things about replicating an update as the fields it touched: that the
// document is not what goes over the link, and that a run of updates leaves
// the target holding what the source holds. The second is the one that would
// be quiet if it broke -- a lost field is not a count and not an error.

const blobBytes = 256 << 10

// TestAnUpdateCarriesTheFieldsAndNotTheDocument reads the events a real change
// stream produces and looks at what the update would write.
func TestAnUpdateCarriesTheFieldsAndNotTheDocument(t *testing.T) {
	client := connect(t, harness.MongoSource)
	database := harness.UniqueName("deltawire")
	ctx := context.Background()
	t.Cleanup(func() { _ = client.Database(database).Drop(context.Background()) })

	coll := client.Database(database).Collection("orders")
	if _, err := coll.InsertOne(ctx, bson.M{
		"_id": "order-1", "status": 1, "blob": strings.Repeat("x", blobBytes),
	}); err != nil {
		t.Fatalf("seed the source: %v", err)
	}

	quiet := logrus.New()
	quiet.SetLevel(logrus.ErrorLevel)
	reader := &Reader{
		Client: client,
		Config: config.SyncConfig{
			ID: 9601, Type: "mongodb",
			Mappings: []config.DatabaseMapping{{
				SourceDatabase: database,
				Tables:         []config.TableMapping{{SourceTable: "orders"}},
			}},
		},
		Logger: quiet,
		Labels: metrics.Labels{"task": "9601"},
	}
	if err := reader.Open(ctx, domain.Position{}); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer reader.Close()

	read, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	// A heartbeat is only emitted once the stream has been read to its end,
	// which is the moment the reader stops asking for whole documents.
	for !reader.deltas {
		event, err := reader.Next(read)
		if err != nil {
			t.Fatalf("Next while waiting to catch up: %v", err)
		}
		if event != nil && !event.Heartbeat {
			t.Fatalf("the stream delivered a change before it was caught up: %v", event.Op)
		}
	}

	if _, err := coll.UpdateOne(ctx, bson.M{"_id": "order-1"},
		bson.M{"$set": bson.M{"status": 2}}); err != nil {
		t.Fatalf("update: %v", err)
	}

	var event *domain.Event
	for {
		next, err := reader.Next(read)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if next != nil && !next.Heartbeat {
			event = next
			break
		}
	}

	update, ok := event.Payload.(*mongo.UpdateOneModel)
	if !ok {
		t.Fatalf("the change is a %T. An update is meant to be replicated as the "+
			"fields it touched", event.Payload)
	}

	rendered := fmt.Sprint(update.Update)
	if !strings.Contains(rendered, "status") {
		t.Errorf("the update does not carry the field that changed: %v", update.Update)
	}
	if strings.Contains(rendered, "blob") || strings.Contains(rendered, "xxxx") {
		t.Errorf("the update carries the rest of the document, which is the cost this "+
			"is meant to avoid: %d bytes", len(rendered))
	}
	// The event itself, which is what the queue holds and the byte budget
	// counts: a whole-document update of this document was a quarter of a
	// megabyte.
	if event.Bytes > 4<<10 {
		t.Errorf("the event is %d bytes for a %d byte document, want a few hundred",
			event.Bytes, blobBytes)
	}
	t.Logf("a one-field update of a %d byte document is a %d byte event",
		blobBytes, event.Bytes)
}

// TestARunOfUpdatesLeavesTheTwoSidesIdentical is the ordering test: every
// update is applied, in order, and no field is left holding an earlier value.
//
// The cases in it are the ones where a delta can lose a field and a whole
// document cannot: two changes to one field inside a single batch, a field
// removed and written again, an array shortened, and one statement changing
// many documents at once.
func TestARunOfUpdatesLeavesTheTwoSidesIdentical(t *testing.T) {
	collection := harness.UniqueName("deltaorder")
	src, tgt := connect(t, harness.MongoSource), connect(t, harness.MongoTarget)
	ctx := context.Background()
	srcColl := src.Database(sourceDB).Collection(collection)

	const documents = 50
	for i := 0; i < documents; i++ {
		if _, err := srcColl.InsertOne(ctx, bson.M{
			"_id": fmt.Sprintf("order-%02d", i),
			"a":   0, "b": 0, "c": 0, "batch": 0,
			"items": bson.A{1, 2, 3, 4, 5},
			"blob":  strings.Repeat("z", 4<<10),
		}); err != nil {
			t.Fatalf("seed the source: %v", err)
		}
	}

	task := syncTask(t, collection)
	startSyncer(t, task)
	harness.Eventually(t, 60*time.Second, func() error {
		if n := countIn(t, tgt, targetDB, collection, bson.M{}); n != documents {
			return fmt.Errorf("the first copy has not landed: %d of %d", n, documents)
		}
		return nil
	})
	waitForDeltas(t, task.ID)

	// Three fields of the same document, one statement each, as fast as they
	// can be issued: several land in one batch, and a batch is written as one
	// ordered request.
	for round := 1; round <= 20; round++ {
		for _, field := range []string{"a", "b", "c"} {
			if _, err := srcColl.UpdateOne(ctx, bson.M{"_id": "order-00"},
				bson.M{"$set": bson.M{field: round}}); err != nil {
				t.Fatalf("round %d, field %s: %v", round, field, err)
			}
		}
	}

	// The same field twice in a row, which is the pair that must not be
	// reordered.
	for _, status := range []int{7, 8, 9} {
		if _, err := srcColl.UpdateOne(ctx, bson.M{"_id": "order-01"},
			bson.M{"$set": bson.M{"a": status}}); err != nil {
			t.Fatalf("set a=%d: %v", status, err)
		}
	}

	// A field removed and written again: the removal must not arrive after the
	// value that replaced it.
	if _, err := srcColl.UpdateOne(ctx, bson.M{"_id": "order-02"},
		bson.M{"$unset": bson.M{"c": ""}}); err != nil {
		t.Fatalf("unset c: %v", err)
	}
	if _, err := srcColl.UpdateOne(ctx, bson.M{"_id": "order-02"},
		bson.M{"$set": bson.M{"c": 99}}); err != nil {
		t.Fatalf("set c: %v", err)
	}

	// An array shortened, which the event describes by its new length rather
	// than by the elements it dropped.
	if _, err := srcColl.UpdateOne(ctx, bson.M{"_id": "order-03"},
		bson.M{"$push": bson.M{"items": bson.M{"$each": bson.A{}, "$slice": 2}}}); err != nil {
		t.Fatalf("shorten the array: %v", err)
	}

	// A burst on one document, back to back, so that a batch carries many
	// changes to the same document. That is the only arrangement in which the
	// order inside a batch can be got wrong, and the check below refuses to
	// pass if the batches turned out to hold one change each.
	const burst = 2000
	for i := 1; i <= burst; i++ {
		if _, err := srcColl.UpdateOne(ctx, bson.M{"_id": "order-04"},
			bson.M{"$set": bson.M{"seq": i}}); err != nil {
			t.Fatalf("burst %d: %v", i, err)
		}
	}

	// One statement, every document: updateMany is one change event per
	// document matched.
	for _, batch := range []int{1, 2} {
		if _, err := srcColl.UpdateMany(ctx, bson.M{},
			bson.M{"$set": bson.M{"batch": batch}}); err != nil {
			t.Fatalf("updateMany batch=%d: %v", batch, err)
		}
	}

	// A document written, changed and removed, then written again under the
	// same _id.
	churn := bson.M{"_id": "order-99", "a": 1, "b": 1, "c": 1, "batch": 2}
	if _, err := srcColl.InsertOne(ctx, churn); err != nil {
		t.Fatalf("insert the churned document: %v", err)
	}
	if _, err := srcColl.UpdateOne(ctx, bson.M{"_id": "order-99"},
		bson.M{"$set": bson.M{"a": 2}}); err != nil {
		t.Fatalf("update the churned document: %v", err)
	}
	if _, err := srcColl.DeleteOne(ctx, bson.M{"_id": "order-99"}); err != nil {
		t.Fatalf("delete the churned document: %v", err)
	}
	if _, err := srcColl.InsertOne(ctx, churn); err != nil {
		t.Fatalf("insert the churned document again: %v", err)
	}

	// Every document, field for field. A comparison of counts would pass with a
	// field holding the value it had three updates ago.
	harness.Eventually(t, 90*time.Second, func() error {
		want := allDocuments(t, src, sourceDB, collection)
		got := allDocuments(t, tgt, targetDB, collection)
		return sameDocuments(want, got)
	})

	// The assumption every ordering assertion above rests on: that batches
	// carried more than one change. One change per batch cannot be applied out
	// of order, so a run that never saw a full batch has not tested the order.
	events, batches := batchShape(t, task.ID)
	if batches == 0 || events <= batches {
		t.Errorf("%.0f changes arrived in %.0f batches, so no batch held more than one "+
			"and the order inside a batch was never exercised", events, batches)
	}
	t.Logf("%.0f changes in %.0f batches, %.1f per batch", events, batches, events/batches)

	// And the values the run was built around, named rather than left to the
	// comparison, so a failure says which case broke.
	final := allDocuments(t, tgt, targetDB, collection)
	for field, want := range map[string]interface{}{"a": 20, "b": 20, "c": 20} {
		if fmt.Sprint(final["order-00"][field]) != fmt.Sprint(want) {
			t.Errorf("order-00.%s = %v, want %v — an update was lost or reordered",
				field, final["order-00"][field], want)
		}
	}
	if fmt.Sprint(final["order-01"]["a"]) != "9" {
		t.Errorf("order-01.a = %v, want 9", final["order-01"]["a"])
	}
	if fmt.Sprint(final["order-02"]["c"]) != "99" {
		t.Errorf("order-02.c = %v, want 99", final["order-02"]["c"])
	}
	if items, _ := final["order-03"]["items"].(bson.A); len(items) != 2 {
		t.Errorf("order-03.items = %v, want two elements", final["order-03"]["items"])
	}
	// The burst: the last value of the field, not one from part way through.
	if fmt.Sprint(final["order-04"]["seq"]) != fmt.Sprint(burst) {
		t.Errorf("order-04.seq = %v, want %d — changes to one document were applied "+
			"out of order", final["order-04"]["seq"], burst)
	}
	for id, document := range final {
		if fmt.Sprint(document["batch"]) != "2" {
			t.Errorf("%s.batch = %v, want 2 from the last updateMany", id, document["batch"])
		}
		if blob, _ := document["blob"].(string); id != "order-99" && len(blob) != 4<<10 {
			t.Errorf("%s.blob is %d bytes, want %d — a field nothing touched", id,
				len(blob), 4<<10)
		}
	}
}

// waitForDeltas waits until a task has stopped asking for whole documents, so
// that what follows is replicated as the fields it touches.
func waitForDeltas(t *testing.T, taskID int) {
	t.Helper()

	labels := metrics.Labels{"task": fmt.Sprint(taskID), "engine": "mongodb"}
	harness.Eventually(t, 60*time.Second, func() error {
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
}

// batchShape reports how many changes a task applied and in how many batches.
func batchShape(t *testing.T, taskID int) (events, batches float64) {
	t.Helper()

	labels := metrics.Labels{"task": fmt.Sprint(taskID), "engine": "mongodb"}
	for name, into := range map[string]*float64{
		metrics.BatchEventsSum:  &events,
		metrics.BatchApplyCount: &batches,
	} {
		for _, sample := range metrics.Default.Snapshot(name) {
			if sample.Labels.Key() == labels.Key() {
				*into = sample.Value
			}
		}
	}
	return events, batches
}

func allDocuments(t *testing.T, client *mongo.Client, database, collection string) map[string]bson.M {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cursor, err := client.Database(database).Collection(collection).Find(ctx, bson.M{})
	if err != nil {
		t.Fatalf("read %s.%s: %v", database, collection, err)
	}
	defer cursor.Close(ctx)

	out := map[string]bson.M{}
	for cursor.Next(ctx) {
		var document bson.M
		if err := cursor.Decode(&document); err != nil {
			t.Fatalf("decode a document of %s.%s: %v", database, collection, err)
		}
		out[fmt.Sprint(document["_id"])] = document
	}
	if err := cursor.Err(); err != nil {
		t.Fatalf("read %s.%s: %v", database, collection, err)
	}
	return out
}

// sameDocuments reports what differs between the two sides, field by field.
func sameDocuments(want, got map[string]bson.M) error {
	if len(want) != len(got) {
		return fmt.Errorf("the source holds %d documents and the target %d", len(want), len(got))
	}
	for id, source := range want {
		target, ok := got[id]
		if !ok {
			return fmt.Errorf("the target does not hold %s", id)
		}
		for field, value := range source {
			held, ok := target[field]
			if !ok {
				return fmt.Errorf("%s is missing the field %s, which holds %v on the "+
					"source", id, field, value)
			}
			if !reflect.DeepEqual(value, held) {
				return fmt.Errorf("%s.%s is %v on the target and %v on the source",
					id, field, held, value)
			}
		}
		for field := range target {
			if _, ok := source[field]; !ok {
				return fmt.Errorf("%s holds the field %s, which the source does not", id, field)
			}
		}
	}
	return nil
}
