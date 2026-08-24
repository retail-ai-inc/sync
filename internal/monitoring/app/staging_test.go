//go:build staging

// The scheduled consistency check, run against the real MongoDB 8.0 sharded
// cluster rather than the fixture. What it establishes is narrow but not
// covered anywhere else: that turning SYNC_VERIFY_INTERVAL on actually compares
// a sharded collection against its replica, reports the difference it finds, and
// records the number a dashboard would read.
package app

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
)

func stgEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func stgURI(t *testing.T, database string) string {
	t.Helper()

	password := os.Getenv("SYNC_STG_PASS")
	if password == "" {
		t.Skip("SYNC_STG_PASS is not set; skipping the staging cluster tests")
	}
	endpoint := stgEnv("SYNC_STG_MONGO", "127.0.0.1:27500")
	host, port := endpoint, ""
	if i := strings.LastIndex(endpoint, ":"); i > 0 {
		host, port = endpoint[:i], endpoint[i+1:]
	}
	return dsn.BuildDSNByType("mongodb", map[string]string{
		"host": host, "port": port, "database": database,
		"user": stgEnv("SYNC_STG_USER", "root"), "password": password,
	})
}

// TestTheScheduledCheckComparesTheRealCluster turns the check on the way an
// operator would — through the interval setting — and gives it a collection that
// has genuinely diverged.
func TestTheScheduledCheckComparesTheRealCluster(t *testing.T) {
	sourceDB := stgEnv("SYNC_STG_SOURCE_DB", "sync_stg_source")
	targetDB := stgEnv("SYNC_STG_TARGET_DB", "sync_stg_target")
	collection := fmt.Sprintf("verify_check_%d", time.Now().UnixNano())

	ctx := context.Background()
	client, err := mongo.Connect(options.Client().ApplyURI(stgURI(t, sourceDB)))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })

	source := client.Database(sourceDB).Collection(collection)
	target := client.Database(targetDB).Collection(collection)
	t.Cleanup(func() {
		_ = source.Drop(context.Background())
		_ = target.Drop(context.Background())
	})

	// A sharded source, because that is the shape of the data being replicated.
	if err := client.Database("admin").RunCommand(ctx, bson.D{
		{Key: "shardCollection", Value: sourceDB + "." + collection},
		{Key: "key", Value: bson.D{{Key: "_id", Value: "hashed"}}},
	}).Err(); err != nil {
		t.Fatalf("shard %s.%s: %v", sourceDB, collection, err)
	}

	// Both sides hold the same 200 documents, except that the target is missing
	// one and disagrees about another — the two failures replication cannot
	// report about itself.
	const total = 200
	for i := 0; i < total; i++ {
		doc := bson.M{"_id": i, "amount": i * 13, "status": "captured"}
		if _, err := source.InsertOne(ctx, doc); err != nil {
			t.Fatalf("seed the source: %v", err)
		}
		if i == 7 {
			continue // never arrived
		}
		if i == 11 {
			doc = bson.M{"_id": i, "amount": 0, "status": "captured"} // drifted
		}
		if _, err := target.InsertOne(ctx, doc); err != nil {
			t.Fatalf("seed the target: %v", err)
		}
	}

	task := config.SyncConfig{
		ID:               991,
		Type:             "mongodb",
		Enable:           true,
		SourceConnection: stgURI(t, sourceDB),
		TargetConnection: stgURI(t, targetDB),
		Mappings: []config.DatabaseMapping{{
			SourceDatabase: sourceDB, TargetDatabase: targetDB,
			Tables: []config.TableMapping{{SourceTable: collection, TargetTable: collection}},
		}},
	}

	t.Setenv("SYNC_VERIFY_INTERVAL", "1s")
	if got := verifyInterval(); got != time.Second {
		t.Fatalf("verifyInterval() = %v; the setting is not being read", got)
	}

	notifier := &recordingNotifier{configured: true}
	runConsistencyChecks(ctx, &config.Config{SyncConfigs: []config.SyncConfig{task}},
		notifier, quiet())

	var found float64
	var reported bool
	for _, sample := range metrics.Default.Snapshot("sync_verify_differences") {
		if sample.Labels["task"] == "991" && sample.Labels["table"] == collection {
			found, reported = sample.Value, true
			t.Cleanup(func() { metrics.Default.Forget(sample.Labels) })
		}
	}
	if !reported {
		t.Fatal("the check recorded nothing for the task")
	}
	if found != 2 {
		t.Errorf("the check found %v differences, want the one missing and the one "+
			"that drifted", found)
	}
	if len(notifier.messages) != 1 {
		t.Fatalf("%d alerts were sent, want one", len(notifier.messages))
	}
	t.Logf("alert:\n%s", notifier.messages[0])
	if !strings.Contains(notifier.messages[0], "missing") ||
		!strings.Contains(notifier.messages[0], "differing") {
		t.Errorf("the alert does not name both kinds of difference")
	}

	// With repair on, the same check has to make the replica right.
	t.Setenv("SYNC_VERIFY_REPAIR", "true")
	runConsistencyChecks(ctx, &config.Config{SyncConfigs: []config.SyncConfig{task}},
		notifier, quiet())

	if err := target.FindOne(ctx, bson.M{"_id": 7}).Err(); err != nil {
		t.Errorf("the missing document was not repaired: %v", err)
	}
	var repaired bson.M
	if err := target.FindOne(ctx, bson.M{"_id": 11}).Decode(&repaired); err != nil {
		t.Fatalf("read the repaired document: %v", err)
	}
	if repaired["amount"] != int32(143) && repaired["amount"] != int64(143) {
		t.Errorf("the drifted document holds amount=%v after the repair", repaired["amount"])
	}

	// And a third pass finds nothing, which is what convergence means.
	runConsistencyChecks(ctx, &config.Config{SyncConfigs: []config.SyncConfig{task}},
		notifier, quiet())
	for _, sample := range metrics.Default.Snapshot("sync_verify_differences") {
		if sample.Labels["task"] == "991" && sample.Labels["table"] == collection {
			if sample.Value != 0 {
				t.Errorf("%v differences remain after the repair", sample.Value)
			}
		}
	}
}
