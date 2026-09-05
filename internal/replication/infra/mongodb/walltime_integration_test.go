//go:build integration

package mongodb

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/test/harness"
)

// What a real server sends, rather than what a marshalled fixture says it
// sends. The whole fix rests on wallTime being present on every event.
func TestARealChangeStreamEventCarriesAWallTime(t *testing.T) {
	client := connect(t, harness.MongoSource)
	database := harness.UniqueName("walltime")
	collection := "changes"
	ctx := context.Background()
	t.Cleanup(func() { _ = client.Database(database).Drop(context.Background()) })

	// The collection has to exist before the stream opens on it.
	coll := client.Database(database).Collection(collection)
	if _, err := coll.InsertOne(ctx, bson.M{"_id": 0}); err != nil {
		t.Fatalf("create the collection: %v", err)
	}

	quiet := logrus.New()
	quiet.SetLevel(logrus.ErrorLevel)
	reader := &Reader{
		Client: client,
		Config: config.SyncConfig{
			ID: 9501, Type: "mongodb",
			SourceConnection: "mongodb://127.0.0.1:27117/" + database,
			Mappings: []config.DatabaseMapping{{
				SourceDatabase: database,
				Tables:         []config.TableMapping{{SourceTable: collection}},
			}},
		},
		Logger: quiet,
		Labels: metrics.Labels{"task": "9501"},
	}
	reader.mapped = reader.mappedCollections()
	reader.databases = reader.mappedDatabaseSet()
	if err := reader.Open(ctx, domain.Position{}); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer reader.Close()

	// Enough events to see the spread: measured from cluster time, roughly half
	// of these would report more than half a second of delay purely because of
	// where the second boundary fell.
	const writes = 12
	go func() {
		for i := 1; i <= writes; i++ {
			_, _ = coll.InsertOne(context.Background(), bson.M{"_id": i})
			time.Sleep(120 * time.Millisecond)
		}
	}()

	read, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	var seen, withWall int
	var worst time.Duration
	for seen < writes {
		event, err := reader.Next(read)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if event == nil || event.Heartbeat {
			continue
		}
		seen++
		if event.WallTime.IsZero() {
			continue
		}
		withWall++
		if age := time.Since(event.WallTime); age > worst {
			worst = age
		}
		// The ordering clock is still whole seconds, which is what makes it
		// unusable for this measurement and fine for ordering.
		if event.SourceTime.Nanosecond() != 0 {
			t.Errorf("the ordering clock carries a sub-second part: %v", event.SourceTime)
		}
	}

	if withWall != writes {
		t.Fatalf("%d of %d events carried a wallTime; the lag fix depends on all "+
			"of them doing so", withWall, writes)
	}
	// Generous, because a loaded machine running the whole suite is not a
	// latency measurement. The point is that it is bounded well below the one
	// second the ordering clock would have reported at random.
	if worst > 500*time.Millisecond {
		t.Errorf("the worst read lag over %d events was %v", writes, worst)
	}
	t.Logf("%d events, all with wallTime, worst measured age %v", writes, worst)
}
