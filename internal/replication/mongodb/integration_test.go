//go:build integration

package mongodb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/test/harness"
)

const (
	sourceDB = "source_db"
	targetDB = "target_db"
)

func connect(t *testing.T, endpoint string) *mongo.Client {
	t.Helper()

	host, port := harness.SplitHostPort(t, endpoint)
	uri := config.BuildDSNByType("mongodb", map[string]string{
		"host": host, "port": port, "database": sourceDB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("connect to %s: %v", endpoint, err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		t.Fatalf("ping %s: %v", endpoint, err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	return client
}

// syncTask builds the configuration for one collection, mirroring what the
// loader produces from a task's config_json.
func syncTask(t *testing.T, collection string, tables ...config.TableMapping) config.SyncConfig {
	t.Helper()

	srcHost, srcPort := harness.SplitHostPort(t, harness.MongoSource)
	tgtHost, tgtPort := harness.SplitHostPort(t, harness.MongoTarget)

	if len(tables) == 0 {
		tables = []config.TableMapping{{SourceTable: collection, TargetTable: collection}}
	}

	return config.SyncConfig{
		ID:     1,
		Enable: true,
		Type:   "mongodb",
		SourceConnection: config.BuildDSNByType("mongodb", map[string]string{
			"host": srcHost, "port": srcPort, "database": sourceDB,
		}),
		TargetConnection: config.BuildDSNByType("mongodb", map[string]string{
			"host": tgtHost, "port": tgtPort, "database": targetDB,
		}),
		MongoDBResumeTokenPath: t.TempDir(),
		Mappings:               []config.DatabaseMapping{{Tables: tables}},
	}
}

func startSyncer(t *testing.T, cfg config.SyncConfig) (stop func()) {
	t.Helper()

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	syncer := NewMongoDBSyncer(cfg, &config.Config{}, logger)
	if syncer == nil {
		t.Fatal("NewMongoDBSyncer returned nil; the endpoints are probably unreachable")
	}

	stop = harness.RunSyncer(t, syncer.Start)
	t.Cleanup(stop)
	return stop
}

// countIn reports how many documents match filter in the given collection.
func countIn(t *testing.T, client *mongo.Client, db, coll string, filter interface{}) int64 {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	n, err := client.Database(db).Collection(coll).CountDocuments(ctx, filter)
	if err != nil {
		t.Fatalf("count %s.%s: %v", db, coll, err)
	}
	return n
}

func findOne(t *testing.T, client *mongo.Client, db, coll string, filter interface{}) (bson.M, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var doc bson.M
	err := client.Database(db).Collection(coll).FindOne(ctx, filter).Decode(&doc)
	return doc, err
}

func TestInitialSyncCopiesExistingDocuments(t *testing.T) {
	collection := harness.UniqueName("initial")
	src, tgt := connect(t, harness.MongoSource), connect(t, harness.MongoTarget)
	ctx := context.Background()

	const total = 50
	docs := make([]interface{}, 0, total)
	for i := 0; i < total; i++ {
		docs = append(docs, bson.M{"seq": i, "name": fmt.Sprintf("user_%d", i)})
	}
	if _, err := src.Database(sourceDB).Collection(collection).InsertMany(ctx, docs); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	startSyncer(t, syncTask(t, collection))

	harness.Eventually(t, 30*time.Second, func() error {
		if n := countIn(t, tgt, targetDB, collection, bson.M{}); n != total {
			return fmt.Errorf("target holds %d documents, want %d", n, total)
		}
		return nil
	})
}

func TestIncrementalSyncAppliesInsertUpdateDelete(t *testing.T) {
	collection := harness.UniqueName("incremental")
	src, tgt := connect(t, harness.MongoSource), connect(t, harness.MongoTarget)
	ctx := context.Background()
	srcColl := src.Database(sourceDB).Collection(collection)

	// Seed one document so the collection exists before the watcher starts.
	if _, err := srcColl.InsertOne(ctx, bson.M{"seq": 0, "name": "seed"}); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	startSyncer(t, syncTask(t, collection))
	harness.Eventually(t, 30*time.Second, func() error {
		if n := countIn(t, tgt, targetDB, collection, bson.M{}); n != 1 {
			return fmt.Errorf("initial sync has not landed: %d documents", n)
		}
		return nil
	})

	t.Run("insert", func(t *testing.T) {
		if _, err := srcColl.InsertOne(ctx, bson.M{"seq": 1, "name": "inserted"}); err != nil {
			t.Fatalf("insert: %v", err)
		}
		harness.Eventually(t, 20*time.Second, func() error {
			if n := countIn(t, tgt, targetDB, collection, bson.M{"seq": 1}); n != 1 {
				return fmt.Errorf("insert has not arrived")
			}
			return nil
		})
	})

	t.Run("update", func(t *testing.T) {
		if _, err := srcColl.UpdateOne(ctx, bson.M{"seq": 1}, bson.M{"$set": bson.M{"name": "updated"}}); err != nil {
			t.Fatalf("update: %v", err)
		}
		harness.Eventually(t, 20*time.Second, func() error {
			doc, err := findOne(t, tgt, targetDB, collection, bson.M{"seq": 1})
			if err != nil {
				return fmt.Errorf("document is gone: %w", err)
			}
			if doc["name"] != "updated" {
				return fmt.Errorf("name is %v, want updated", doc["name"])
			}
			return nil
		})
	})

	t.Run("delete", func(t *testing.T) {
		if _, err := srcColl.DeleteOne(ctx, bson.M{"seq": 1}); err != nil {
			t.Fatalf("delete: %v", err)
		}
		harness.Eventually(t, 20*time.Second, func() error {
			if n := countIn(t, tgt, targetDB, collection, bson.M{"seq": 1}); n != 0 {
				return fmt.Errorf("delete has not arrived: %d documents remain", n)
			}
			return nil
		})
	})
}

// TestSecurityPolicyIsIgnoredForMongoDB demonstrates F-104 end to end: the task
// declares a masking rule, the syncer accepts it, and the target receives the
// value in the clear. MySQL and PostgreSQL apply the same configuration; the
// MongoDB path never calls the security package at all.
func TestSecurityPolicyIsIgnoredForMongoDB(t *testing.T) {
	collection := harness.UniqueName("security")
	src, tgt := connect(t, harness.MongoSource), connect(t, harness.MongoTarget)
	ctx := context.Background()

	const email = "jack@example.com"
	if _, err := src.Database(sourceDB).Collection(collection).InsertOne(ctx,
		bson.M{"seq": 1, "email": email}); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	cfg := syncTask(t, collection, config.TableMapping{
		SourceTable:     collection,
		TargetTable:     collection,
		SecurityEnabled: true,
		FieldSecurity: []interface{}{
			map[string]interface{}{"field": "email", "securityType": "masked"},
		},
	})
	startSyncer(t, cfg)

	harness.Eventually(t, 30*time.Second, func() error {
		doc, err := findOne(t, tgt, targetDB, collection, bson.M{"seq": 1})
		if err != nil {
			return fmt.Errorf("document has not arrived: %w", err)
		}
		if doc["email"] == nil {
			return fmt.Errorf("email field is missing")
		}
		return nil
	})

	doc, err := findOne(t, tgt, targetDB, collection, bson.M{"seq": 1})
	if err != nil {
		t.Fatalf("read target: %v", err)
	}
	if doc["email"] != email {
		t.Fatalf("target holds %v; masking now appears to run for MongoDB, "+
			"so assert the masked value instead", doc["email"])
	}
}
