//go:build integration

package mongodb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/test/harness"
)

// indexesOn reports the indexes a collection carries, by name.
func indexesOn(t *testing.T, client *mongo.Client, db, coll string) map[string]bool {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cursor, err := client.Database(db).Collection(coll).Indexes().List(ctx)
	if err != nil {
		t.Fatalf("list indexes of %s.%s: %v", db, coll, err)
	}
	var docs []bson.M
	if err := cursor.All(ctx, &docs); err != nil {
		t.Fatalf("read indexes of %s.%s: %v", db, coll, err)
	}
	names := map[string]bool{}
	for _, d := range docs {
		if name, ok := d["name"].(string); ok {
			names[name] = true
		}
	}
	return names
}

// A task that names no collections is copying a database whole, and a standby
// without the source's indexes answers every query with a collection scan.
// There is no per-collection setting to read in that case, so the discovery has
// to ask for them itself.
func TestAWholeDatabaseTaskAsksForIndexes(t *testing.T) {
	ctx := context.Background()
	src := connect(t, harness.MongoSource)

	name := harness.UniqueName("wholedb")
	if _, err := src.Database(sourceDB).Collection(name).InsertOne(ctx, bson.M{"v": 1}); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	t.Cleanup(func() { _ = src.Database(sourceDB).Collection(name).Drop(ctx) })

	log := logrus.New()
	log.SetOutput(discardWriter{})
	snap := &Snapshotter{
		Config:   config.SyncConfig{Mappings: []config.DatabaseMapping{{}}},
		Logger:   logrus.NewEntry(log),
		sourceDB: sourceDB,
	}

	tables, err := snap.collections(ctx, src.Database(sourceDB))
	if err != nil {
		t.Fatalf("collections: %v", err)
	}
	if len(tables) == 0 {
		t.Fatal("discovery found no collections to copy")
	}

	found := false
	for _, table := range tables {
		if !table.AdvancedSettings.SyncIndexes {
			t.Errorf("%s was discovered without asking for its indexes", table.SourceTable)
		}
		if table.SourceTable == name {
			found = true
		}
	}
	if !found {
		t.Errorf("discovery did not report %s, so the assertion above proved nothing", name)
	}
}

// The copy carries the source's secondary indexes, which is what keeps the
// target able to serve rather than merely able to answer.
func TestTheCopyCreatesTheSourceIndexes(t *testing.T) {
	ctx := context.Background()
	collection := harness.UniqueName("withindexes")
	src, tgt := connect(t, harness.MongoSource), connect(t, harness.MongoTarget)
	srcColl := src.Database(sourceDB).Collection(collection)

	if _, err := srcColl.InsertMany(ctx, []interface{}{
		bson.M{"sku": "a", "shop": 1}, bson.M{"sku": "b", "shop": 2},
	}); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	wanted, err := srcColl.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "sku", Value: 1}}},
		{Keys: bson.D{{Key: "shop", Value: 1}, {Key: "sku", Value: -1}}},
	})
	if err != nil {
		t.Fatalf("create source indexes: %v", err)
	}

	startSyncer(t, syncTask(t, collection, config.TableMapping{
		SourceTable:      collection,
		TargetTable:      collection,
		AdvancedSettings: config.AdvancedSettings{SyncIndexes: true},
	}))

	harness.Eventually(t, 60*time.Second, func() error {
		if n := countIn(t, tgt, targetDB, collection, bson.M{}); n != 2 {
			return fmt.Errorf("the documents have not landed yet")
		}
		have := indexesOn(t, tgt, targetDB, collection)
		for _, name := range wanted {
			if !have[name] {
				return fmt.Errorf("the target is missing index %q", name)
			}
		}
		return nil
	})
}

// indexSpecsOn reports the indexes a collection carries, by name, as the server
// describes them.
func indexSpecsOn(t *testing.T, client *mongo.Client, db, coll string) map[string]bson.M {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cursor, err := client.Database(db).Collection(coll).Indexes().List(ctx)
	if err != nil {
		t.Fatalf("list indexes of %s.%s: %v", db, coll, err)
	}
	var docs []bson.M
	if err := cursor.All(ctx, &docs); err != nil {
		t.Fatalf("read indexes of %s.%s: %v", db, coll, err)
	}
	specs := map[string]bson.M{}
	for _, d := range docs {
		if name, ok := d["name"].(string); ok {
			specs[name] = d
		}
	}
	return specs
}

func TestACopiedIndexKeepsItsOptionsAndAcceptsWhatTheSourceAccepts(t *testing.T) {
	ctx := context.Background()
	collection := harness.UniqueName("indexoptions")
	src, tgt := connect(t, harness.MongoSource), connect(t, harness.MongoTarget)
	srcColl := src.Database(sourceDB).Collection(collection)
	t.Cleanup(func() {
		_ = srcColl.Drop(context.Background())
		_ = tgt.Database(targetDB).Collection(collection).Drop(context.Background())
	})

	if _, err := srcColl.InsertMany(ctx, []interface{}{
		bson.M{"sku": "a", "email": "a@example.com", "code": "c-a", "title": "red shoe"},
		bson.M{"sku": "b", "title": "blue hat"},
	}); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	if _, err := srcColl.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "sku", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "email", Value: 1}}, Options: options.Index().SetUnique(true).SetSparse(true)},
		{Keys: bson.D{{Key: "code", Value: 1}}, Options: options.Index().SetUnique(true).
			SetPartialFilterExpression(bson.D{{Key: "code", Value: bson.D{{Key: "$exists", Value: true}}}})},
		{Keys: bson.D{{Key: "title", Value: "text"}}, Options: options.Index().
			SetWeights(bson.D{{Key: "title", Value: 3}}).SetDefaultLanguage("spanish")},
	}); err != nil {
		t.Fatalf("create source indexes: %v", err)
	}

	startSyncer(t, syncTask(t, collection, config.TableMapping{
		SourceTable:      collection,
		TargetTable:      collection,
		AdvancedSettings: config.AdvancedSettings{SyncIndexes: true},
	}))

	want := indexSpecsOn(t, src, sourceDB, collection)
	fields := []string{"key", "unique", "sparse", "partialFilterExpression",
		"weights", "default_language", "language_override"}
	harness.Eventually(t, 60*time.Second, func() error {
		have := indexSpecsOn(t, tgt, targetDB, collection)
		for _, name := range []string{"sku_1", "email_1", "code_1", "title_text"} {
			if _, ok := have[name]; !ok {
				return fmt.Errorf("the target is missing index %q", name)
			}
			for _, field := range fields {
				if got, wanted := fmt.Sprint(have[name][field]), fmt.Sprint(want[name][field]); got != wanted {
					return fmt.Errorf("index %s has %s = %s on the target, want %s", name, field, got, wanted)
				}
			}
		}
		return nil
	})

	// A plain unique index on the target refuses the second document without the field, and the stream stalls on it.
	if _, err := srcColl.InsertMany(ctx, []interface{}{
		bson.M{"sku": "c", "title": "green scarf"},
		bson.M{"sku": "d", "title": "grey sock"},
	}); err != nil {
		t.Fatalf("insert documents without the sparse and partial fields: %v", err)
	}
	harness.Eventually(t, 60*time.Second, func() error {
		if n := countIn(t, tgt, targetDB, collection, bson.M{}); n != 4 {
			return fmt.Errorf("the target holds %d of 4 documents", n)
		}
		return nil
	})
}

// A task that lists no collections replicates the database as a whole, and the
// captured count is then knowable only from the source. It used to be read off
// the task's own list, which is empty in exactly that case, so the one task
// shape a disaster-recovery copy uses reported nothing captured while it
// replicated everything.
func TestAWholeDatabaseTaskReportsWhatItCaptures(t *testing.T) {
	ctx := context.Background()
	src := connect(t, harness.MongoSource)

	name := harness.UniqueName("captured")
	if _, err := src.Database(sourceDB).Collection(name).InsertOne(ctx, bson.M{"v": 1}); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	t.Cleanup(func() { _ = src.Database(sourceDB).Collection(name).Drop(ctx) })

	cfg := syncTask(t, name)
	cfg.Mappings = []config.DatabaseMapping{{}} // names no collection: the database as a whole
	startSyncer(t, cfg)

	labels := metrics.Labels{"task": fmt.Sprint(cfg.ID), "engine": "mongodb"}
	harness.Eventually(t, 60*time.Second, func() error {
		for _, s := range metrics.Default.Snapshot(metrics.CapturedTables) {
			if s.Labels.Key() != labels.Key() {
				continue
			}
			if s.Value < 1 {
				return fmt.Errorf("captured = %v, want the collections the source holds", s.Value)
			}
			return nil
		}
		return fmt.Errorf("%s{%s} was never recorded", metrics.CapturedTables, labels.Key())
	})
}
