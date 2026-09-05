//go:build integration

package mongodb

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/test/harness"
)

// mongoURI is the address a task would be configured with, rather than the
// client the other tests build, because these entry points take a config and
// connect for themselves.
func mongoURI(t *testing.T, endpoint, database string) string {
	t.Helper()
	host, port := harness.SplitHostPort(t, endpoint)
	return dsn.BuildDSNByType("mongodb", map[string]string{
		"host": host, "port": port, "database": database,
		dsn.KeyDirect: "true",
	})
}

func seedCollection(t *testing.T, endpoint, database, collection string, documents int) {
	t.Helper()
	client := connect(t, endpoint)
	ctx := context.Background()
	coll := client.Database(database).Collection(collection)
	for i := 0; i < documents; i++ {
		if _, err := coll.InsertOne(ctx, bson.M{"_id": i, "n": i}); err != nil {
			t.Fatalf("seed %s.%s: %v", database, collection, err)
		}
	}
	t.Cleanup(func() { _ = client.Database(database).Drop(context.Background()) })
}

func TestRowCountsCountsBothSidesOfEveryMapping(t *testing.T) {
	database := harness.UniqueName("rowcounts")
	seedCollection(t, harness.MongoSource, database, "orders", 7)
	seedCollection(t, harness.MongoTarget, database, "orders", 5)
	seedCollection(t, harness.MongoSource, database, "customers", 3)
	seedCollection(t, harness.MongoTarget, database, "customers", 3)

	cfg := config.SyncConfig{
		ID: 9101, Type: "mongodb",
		SourceConnection: mongoURI(t, harness.MongoSource, database),
		TargetConnection: mongoURI(t, harness.MongoTarget, database),
		Mappings: []config.DatabaseMapping{{Tables: []config.TableMapping{
			{SourceTable: "orders", TargetTable: "orders"},
			{SourceTable: "customers"},
		}}},
	}

	counts, err := RowCounts(context.Background(), cfg)
	if err != nil {
		t.Fatalf("RowCounts: %v", err)
	}
	if counts.Discovered {
		t.Error("a config that names its collections was reported as discovered")
	}
	got := map[string][2]int64{}
	for _, object := range counts.Objects {
		got[object.Source] = [2]int64{object.SourceRows, object.TargetRows}
	}
	// The difference is the point: a pair that agrees and a pair that does not,
	// so a count that silently returned the same number for both sides fails.
	if got["orders"] != [2]int64{7, 5} {
		t.Errorf("orders counted %v, want [7 5]", got["orders"])
	}
	if got["customers"] != [2]int64{3, 3} {
		t.Errorf("customers counted %v, want [3 3]", got["customers"])
	}
}

func TestRowCountsDiscoversTheWholeDatabaseWhenNothingIsNamed(t *testing.T) {
	database := harness.UniqueName("rowcounts-all")
	seedCollection(t, harness.MongoSource, database, "alpha", 2)
	seedCollection(t, harness.MongoSource, database, "beta", 4)
	seedCollection(t, harness.MongoTarget, database, "alpha", 2)

	cfg := config.SyncConfig{
		ID: 9102, Type: "mongodb",
		SourceConnection: mongoURI(t, harness.MongoSource, database),
		TargetConnection: mongoURI(t, harness.MongoTarget, database),
	}

	counts, err := RowCounts(context.Background(), cfg)
	if err != nil {
		t.Fatalf("RowCounts: %v", err)
	}
	if !counts.Discovered {
		t.Error("a whole-database task was not reported as discovered")
	}
	found := map[string]int64{}
	for _, object := range counts.Objects {
		found[object.Source] = object.TargetRows
	}
	if _, ok := found["alpha"]; !ok {
		t.Error("alpha was not discovered on the source")
	}
	// beta exists on the source and not on the target. Counting it as zero is
	// the answer; a collection the target lacks is what this report is for.
	if beta, ok := found["beta"]; !ok {
		t.Error("beta was not discovered on the source")
	} else if beta != 0 {
		t.Errorf("beta counted %d on a target that does not have it", beta)
	}
}

func TestRowCountsWillNotConnectToNowhere(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := config.SyncConfig{
		ID: 9103, Type: "mongodb",
		SourceConnection: "mongodb://127.0.0.1:1/x?directConnection=true",
		TargetConnection: "mongodb://127.0.0.1:1/x?directConnection=true",
	}
	if _, err := RowCounts(ctx, cfg); err == nil {
		t.Error("RowCounts reported on a source that does not exist")
	}
}

func TestProgressComparesTheSourceClockAgainstWhatWasStored(t *testing.T) {
	database := harness.UniqueName("progress")
	seedCollection(t, harness.MongoTarget, database, "placeholder", 1)

	cfg := config.SyncConfig{
		ID: 9104, Type: "mongodb",
		SourceConnection: mongoURI(t, harness.MongoSource, database),
		TargetConnection: mongoURI(t, harness.MongoTarget, database),
	}

	report, err := Progress(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Progress: %v", err)
	}
	if report.Engine != "mongodb" {
		t.Errorf("engine = %q, want mongodb", report.Engine)
	}
	if len(report.Shards) != 1 {
		t.Fatalf("a replica set reported %d shards, want 1", len(report.Shards))
	}
	// Nothing has been applied for this task, so it has to say so rather than
	// report a position it does not hold.
	if shard := report.Shards[0]; shard.Comparable {
		t.Errorf("a task that never ran was reported as comparable: %+v", shard)
	} else if shard.Note == "" {
		t.Error("a shard that cannot be compared gave no reason")
	}
}

func TestTheSourceClusterTimeIsReadFromHello(t *testing.T) {
	client := connect(t, harness.MongoSource)
	at, err := sourceClusterTime(context.Background(), client)
	if err != nil {
		t.Fatalf("sourceClusterTime: %v", err)
	}
	if at.IsZero() {
		t.Error("the source reported a zero cluster time")
	}
	if drift := time.Since(at); drift > time.Hour || drift < -time.Hour {
		t.Errorf("the source's clock is %v away from this one, so the value read "+
			"is not a cluster time", drift)
	}
}
