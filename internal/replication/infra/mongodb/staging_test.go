//go:build staging

// These tests run against a real MongoDB 8.0 sharded cluster rather than the
// hermetic fixture, because three things cannot be exercised against a
// single-node replica set on loopback:
//
//   - topology discovery. The fixture advertises 127.0.0.1:27017, an address
//     reachable only inside its own container, so every fixture test pins one
//     node with directConnection. Production must not, and the path where the
//     driver finds the set for itself and follows an election was therefore
//     never run.
//   - a sharded collection. A change stream opened through mongos merges the
//     streams of every shard, and the resume token it hands back is a composite
//     of all of them. Ordering, resumption and the snapshot's cluster time all
//     behave differently from the single-shard case.
//   - failover. An election only happens where there is a set to hold one.
//
// They are behind their own build tag so they never run in CI, and they only
// ever read or write the two databases named by SYNC_STG_SOURCE_DB and
// SYNC_STG_TARGET_DB.
//
// Run with:
//
//	kubectl port-forward svc/mongodb-sharded 27500:27017
//	SYNC_STG_MONGO=127.0.0.1:27500 SYNC_STG_USER=root SYNC_STG_PASS=... \
//	  go test -tags staging -v -timeout 30m ./internal/replication/infra/mongodb/
package mongodb

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/infra/verify"
	"github.com/retail-ai-inc/sync/test/harness"
)

func stgEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

var (
	stgEndpoint = stgEnv("SYNC_STG_MONGO", "127.0.0.1:27500")
	stgUser     = stgEnv("SYNC_STG_USER", "root")
	stgPassword = os.Getenv("SYNC_STG_PASS")
	stgSourceDB = stgEnv("SYNC_STG_SOURCE_DB", "sync_stg_source")
	stgTargetDB = stgEnv("SYNC_STG_TARGET_DB", "sync_stg_target")
)

// stgDSN builds the connection string the syncer itself would use. Note what is
// absent: directConnection. The driver is being asked to discover the cluster,
// which is the point.
func stgDSN(t *testing.T, database string) string {
	t.Helper()

	if stgPassword == "" {
		t.Skip("SYNC_STG_PASS is not set; skipping the staging cluster tests")
	}
	host, port := harness.SplitHostPort(t, stgEndpoint)
	return dsn.BuildDSNByType("mongodb", map[string]string{
		"host": host, "port": port, "database": database,
		"user": stgUser, "password": stgPassword,
	})
}

func stgConnect(t *testing.T, database string) *mongo.Client {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(stgDSN(t, database)))
	if err != nil {
		t.Fatalf("connect to %s: %v", stgEndpoint, err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		t.Fatalf("ping %s: %v", stgEndpoint, err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	return client
}

// stgTask configures one collection, the way the loader would from a stored
// task. The buffer directory is a temporary one per test: on the cluster it
// belongs on a persistent volume, which is a deployment matter rather than
// something a test can assert.
func stgTask(t *testing.T, collection string) config.SyncConfig {
	t.Helper()

	return config.SyncConfig{
		ID:                     harness.UniqueTaskID(),
		Enable:                 true,
		Type:                   "mongodb",
		SourceConnection:       stgDSN(t, stgSourceDB),
		TargetConnection:       stgDSN(t, stgTargetDB),
		MongoDBResumeTokenPath: t.TempDir(),
		Mappings: []config.DatabaseMapping{{
			SourceDatabase: stgSourceDB, TargetDatabase: stgTargetDB,
			Tables: []config.TableMapping{{SourceTable: collection, TargetTable: collection}},
		}},
	}
}

func stgStart(t *testing.T, cfg config.SyncConfig) (stop func()) {
	t.Helper()

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)
	if level := os.Getenv("SYNC_STG_LOG"); level != "" {
		parsed, err := logrus.ParseLevel(level)
		if err != nil {
			t.Fatalf("SYNC_STG_LOG=%q: %v", level, err)
		}
		logger.SetLevel(parsed)
	}

	syncer := NewSyncer(cfg, &config.Config{}, logger)
	if syncer == nil {
		t.Fatal("NewSyncer returned nil; the cluster is not reachable")
	}
	stop = harness.RunSyncer(t, syncer.Start)
	t.Cleanup(stop)
	return stop
}

// stgCollection makes a sharded collection in the source database and removes
// both sides afterwards. Sharded because that is the shape of the data this has
// to replicate, and because an unsharded collection would only ever exercise
// one shard's change stream.
func stgCollection(t *testing.T, prefix string) (name string, source, target *mongo.Collection) {
	t.Helper()

	name = harness.UniqueName(prefix)
	client := stgConnect(t, stgSourceDB)
	ctx := context.Background()

	// A hashed shard key spreads the documents over every shard.
	if err := client.Database("admin").RunCommand(ctx, bson.D{
		{Key: "shardCollection", Value: stgSourceDB + "." + name},
		{Key: "key", Value: bson.D{{Key: "_id", Value: "hashed"}}},
	}).Err(); err != nil {
		t.Fatalf("shard %s.%s: %v", stgSourceDB, name, err)
	}

	source = client.Database(stgSourceDB).Collection(name)
	target = client.Database(stgTargetDB).Collection(name)
	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = source.Drop(dropCtx)
		_ = target.Drop(dropCtx)
	})
	return name, source, target
}

func stgCount(t *testing.T, coll *mongo.Collection) int64 {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	n, err := coll.CountDocuments(ctx, bson.M{})
	if err != nil {
		t.Fatalf("count %s: %v", coll.Name(), err)
	}
	return n
}

// payment is the sort of document this is meant to carry: a few scalars, a
// nested object and an array, so the digest and the write model see something
// other than flat fields.
func payment(seq int) bson.M {
	return bson.M{
		"seq":       seq,
		"orderID":   fmt.Sprintf("ORD-%08d", seq),
		"amount":    seq * 13,
		"currency":  "JPY",
		"status":    "captured",
		"customer":  bson.M{"id": fmt.Sprintf("CUS-%06d", seq%1000), "tier": "gold"},
		"items":     bson.A{bson.M{"sku": "A", "qty": 1}, bson.M{"sku": "B", "qty": 2}},
		"writtenAt": time.Now().UTC(),
	}
}

// ------------------------------------------------------------- the cluster

// TestTheClusterIsFoundWithoutPinningANode is the path the fixture cannot run.
// A driver given directConnection stops writing after an election instead of
// following the new primary, so production must discover the topology — and
// until now nothing had checked that the syncer's own connection string does.
func TestTheClusterIsFoundWithoutPinningANode(t *testing.T) {
	uri := stgDSN(t, stgSourceDB)
	if got := uri; got == "" {
		t.Fatal("no DSN was built")
	}
	t.Logf("connection string: %s", maskPassword(uri))

	for _, unwanted := range []string{"directConnection"} {
		if strings.Contains(uri, unwanted) {
			t.Errorf("the connection string pins the driver: %s", maskPassword(uri))
		}
	}
	for _, wanted := range []string{"w=majority", "journal=true", "authSource=admin"} {
		if !strings.Contains(uri, wanted) {
			t.Errorf("the connection string is missing %s: %s", wanted, maskPassword(uri))
		}
	}

	client := stgConnect(t, stgSourceDB)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var hello bson.M
	if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).
		Decode(&hello); err != nil {
		t.Fatalf("hello: %v", err)
	}
	if hello["msg"] != "isdbgrid" {
		t.Errorf("msg = %v, want isdbgrid: this is not a mongos", hello["msg"])
	}

	var shards bson.M
	if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "listShards", Value: 1}}).
		Decode(&shards); err != nil {
		t.Fatalf("listShards: %v", err)
	}
	list, _ := shards["shards"].(bson.A)
	if len(list) < 2 {
		t.Fatalf("the cluster reports %d shards; these tests need a sharded one", len(list))
	}
	t.Logf("cluster: mongos, %d shards", len(list))
}

// ---------------------------------------------------------------- the sync

// TestAShardedCollectionIsCopiedAndFollowed is the whole path in one: a
// snapshot of a collection spread over three shards, then the changes made
// after it, read from a merged change stream through mongos.
func TestAShardedCollectionIsCopiedAndFollowed(t *testing.T) {
	name, source, target := stgCollection(t, "payments_copy")
	ctx := context.Background()

	const seeded = 2000
	docs := make([]interface{}, 0, seeded)
	for i := 0; i < seeded; i++ {
		docs = append(docs, payment(i))
	}
	if _, err := source.InsertMany(ctx, docs); err != nil {
		t.Fatalf("seed the source: %v", err)
	}
	t.Logf("seeded %d documents across the shards", seeded)

	stgStart(t, stgTask(t, name))

	harness.Eventually(t, 3*time.Minute, func() error {
		if n := stgCount(t, target); n != seeded {
			return fmt.Errorf("the target holds %d of %d documents", n, seeded)
		}
		return nil
	})

	// Changes made after the copy, which arrive through the change stream.
	if _, err := source.InsertOne(ctx, payment(seeded)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := source.UpdateOne(ctx, bson.M{"seq": 7},
		bson.M{"$set": bson.M{"status": "refunded", "amount": 0}}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, err := source.DeleteOne(ctx, bson.M{"seq": 11}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	harness.Eventually(t, 2*time.Minute, func() error {
		if n := stgCount(t, target); n != seeded {
			return fmt.Errorf("the target holds %d documents, want %d after one insert "+
				"and one delete", n, seeded)
		}
		var refunded bson.M
		if err := target.FindOne(ctx, bson.M{"seq": 7}).Decode(&refunded); err != nil {
			return fmt.Errorf("read the updated document: %w", err)
		}
		if refunded["status"] != "refunded" {
			return fmt.Errorf("the update has not landed: status=%v", refunded["status"])
		}
		if err := target.FindOne(ctx, bson.M{"seq": 11}).Err(); err != mongo.ErrNoDocuments {
			return fmt.Errorf("the deleted document is still on the target")
		}
		if err := target.FindOne(ctx, bson.M{"seq": seeded}).Err(); err != nil {
			return fmt.Errorf("read the inserted document: %w", err)
		}
		return nil
	})
}

// TestTheTwoSidesAreIdentical runs the consistency comparison over real
// sharded data. It is the check an operator would run before declaring the
// replica usable, and it exercises the document digest against _ids the fixture
// never produces.
func TestTheTwoSidesAreIdentical(t *testing.T) {
	name, source, target := stgCollection(t, "payments_verify")
	ctx := context.Background()

	const seeded = 1000
	docs := make([]interface{}, 0, seeded)
	for i := 0; i < seeded; i++ {
		docs = append(docs, payment(i))
	}
	if _, err := source.InsertMany(ctx, docs); err != nil {
		t.Fatalf("seed the source: %v", err)
	}

	stgStart(t, stgTask(t, name))
	harness.Eventually(t, 3*time.Minute, func() error {
		if n := stgCount(t, target); n != seeded {
			return fmt.Errorf("the target holds %d of %d documents", n, seeded)
		}
		return nil
	})

	result, err := verify.Compare(ctx,
		&verify.MongoEnd{Coll: source}, &verify.MongoEnd{Coll: target}, 200)
	if err != nil {
		t.Fatalf("Compare: %v", err)
	}
	t.Logf("comparison: %s", result.Summary())
	if !result.Identical() {
		t.Errorf("the two sides disagree: %s\nfirst differences: %+v",
			result.Summary(), result.Sample)
	}

	// And it has to notice when they do differ, or the check is worthless.
	if _, err := target.DeleteOne(ctx, bson.M{"seq": 3}); err != nil {
		t.Fatalf("remove a document from the target: %v", err)
	}
	again, err := verify.Compare(ctx,
		&verify.MongoEnd{Coll: source}, &verify.MongoEnd{Coll: target}, 200)
	if err != nil {
		t.Fatalf("Compare: %v", err)
	}
	if again.Missing != 1 {
		t.Errorf("the comparison reported %s after one document was removed "+
			"from the target", again.Summary())
	}
}

// TestTheCheckpointIsOnTheTargetAndResumesFromIt is what makes a syncer
// replaceable: the position lives with the data, so a pod rescheduled — or a
// replacement started in the other region — carries on rather than copying
// everything again.
func TestTheCheckpointIsOnTheTargetAndResumesFromIt(t *testing.T) {
	name, source, target := stgCollection(t, "payments_resume")
	ctx := context.Background()

	if _, err := source.InsertOne(ctx, payment(0)); err != nil {
		t.Fatalf("seed the source: %v", err)
	}

	cfg := stgTask(t, name)
	stop := stgStart(t, cfg)
	harness.Eventually(t, 2*time.Minute, func() error {
		if n := stgCount(t, target); n != 1 {
			return fmt.Errorf("the first document has not arrived")
		}
		return nil
	})

	// The checkpoint has to be on the target, not on this machine's disk.
	client := stgConnect(t, stgTargetDB)
	checkpoints := client.Database(stgTargetDB).Collection("_sync_checkpoint")
	n, err := checkpoints.CountDocuments(ctx, bson.M{"task_id": cfg.ID})
	if err != nil {
		t.Fatalf("read the checkpoints: %v", err)
	}
	if n == 0 {
		t.Fatal("no checkpoint was recorded on the target")
	}
	t.Logf("the target holds %d checkpoint documents for task %d", n, cfg.ID)

	// Stop, write while nothing is watching, and start again with the same task
	// id — which is how a rescheduled pod comes back.
	stop()
	const whileStopped = 50
	docs := make([]interface{}, 0, whileStopped)
	for i := 1; i <= whileStopped; i++ {
		docs = append(docs, payment(i))
	}
	if _, err := source.InsertMany(ctx, docs); err != nil {
		t.Fatalf("write while stopped: %v", err)
	}

	stgStart(t, cfg)
	harness.Eventually(t, 3*time.Minute, func() error {
		if got := stgCount(t, target); got != whileStopped+1 {
			return fmt.Errorf("the target holds %d of %d documents", got, whileStopped+1)
		}
		return nil
	})
}

// ------------------------------------------------------------------ the lag

// TestTheLagIsMeasuredUnderLoad is the RPO measurement: how far behind the
// target is while the source is being written to. Whatever that interval is when
// the source region disappears is what is lost.
//
// Two things are measured, because one number alone is misleading:
//
//   - the latency of individual documents, by polling the target for one
//     document in every sampleEvery. Polling rather than watching the target,
//     because a change stream on this cluster has a delivery floor of its own —
//     watching the target would charge the syncer for the cluster's latency
//     twice.
//   - the backlog once a second, which says whether the pipeline keeps up at
//     all. A backlog that grows through the run means the lag figures are a
//     property of the run's length rather than of the pipeline.
//
// Rate and duration stay deliberately low: this cluster is shared and not fast,
// and a load test that disturbs its other databases is not a measurement anybody
// wants. SYNC_STG_RATE and SYNC_STG_SECONDS scale it.
func TestTheLagIsMeasuredUnderLoad(t *testing.T) {
	name, source, target := stgCollection(t, "payments_lag")
	ctx := context.Background()

	if _, err := source.InsertOne(ctx, payment(0)); err != nil {
		t.Fatalf("seed the source: %v", err)
	}
	cfg := stgTask(t, name)
	stgStart(t, cfg)
	harness.Eventually(t, 2*time.Minute, func() error {
		if stgCount(t, target) != 1 {
			return fmt.Errorf("the syncer has not caught up with the seed document")
		}
		return nil
	})

	rate := stgInt("SYNC_STG_RATE", 50)
	seconds := stgInt("SYNC_STG_SECONDS", 30)
	sampleEvery := stgInt("SYNC_STG_SAMPLE_EVERY", 25)
	t.Logf("writing at %d documents/s for %ds, tracing one document in %d",
		rate, seconds, sampleEvery)

	var (
		mu        sync.Mutex
		latencies []time.Duration
		traced    sync.WaitGroup
		written   int64
	)

	// trace follows one document to the target by polling for it.
	trace := func(seq int, sentAt time.Time) {
		defer traced.Done()
		for time.Since(sentAt) < 90*time.Second {
			err := target.FindOne(ctx, bson.M{"seq": seq}).Err()
			if err == nil {
				mu.Lock()
				latencies = append(latencies, time.Since(sentAt))
				mu.Unlock()
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Errorf("document %d had not arrived after 90s", seq)
	}

	// The backlog sampler, one count a second.
	sampleCtx, stopSampling := context.WithCancel(ctx)
	sampling := make(chan struct{})
	type sample struct {
		at             time.Time
		source, target int64
	}
	var samples []sample
	go func() {
		defer close(sampling)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-sampleCtx.Done():
				return
			case <-ticker.C:
				n, err := target.CountDocuments(sampleCtx, bson.M{})
				if err != nil {
					continue
				}
				samples = append(samples, sample{
					at: time.Now(), source: atomic.LoadInt64(&written), target: n - 1,
				})
			}
		}
	}()

	interval := time.Second / time.Duration(rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	writeStart := time.Now()
	deadline := writeStart.Add(time.Duration(seconds) * time.Second)
	seq := 1
	var slow int
	for now := range ticker.C {
		if now.After(deadline) {
			break
		}
		before := time.Now()
		if _, err := source.InsertOne(ctx, payment(seq)); err != nil {
			t.Fatalf("insert %d: %v", seq, err)
		}
		atomic.AddInt64(&written, 1)
		if time.Since(before) > interval {
			slow++
		}
		if seq%sampleEvery == 0 {
			traced.Add(1)
			go trace(seq, before)
		}
		seq++
	}
	total := seq - 1
	writeElapsed := time.Since(writeStart)
	t.Logf("wrote %d documents in %v (%.0f/s achieved, %d writes slower than the "+
		"interval)", total, writeElapsed.Round(time.Millisecond),
		float64(total)/writeElapsed.Seconds(), slow)

	traced.Wait()

	// Drain, which is the recovery point after the writes stop.
	drainStart := time.Now()
	harness.Eventually(t, 5*time.Minute, func() error {
		if got := stgCount(t, target); got < int64(total+1) {
			return fmt.Errorf("the target holds %d of %d documents", got, total+1)
		}
		return nil
	})
	drained := time.Since(drainStart)
	stopSampling()
	<-sampling

	mu.Lock()
	defer mu.Unlock()
	if len(latencies) == 0 {
		t.Fatal("no documents were traced")
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	at := func(q float64) time.Duration {
		return latencies[int(float64(len(latencies)-1)*q)].Round(10 * time.Millisecond)
	}
	t.Logf("traced %d documents: p50=%v p90=%v max=%v",
		len(latencies), at(0.50), at(0.90),
		latencies[len(latencies)-1].Round(10*time.Millisecond))
	t.Logf("the target caught up %v after the last write", drained.Round(time.Millisecond))

	// The backlog over time says whether the pipeline held the rate.
	var worst int64
	for _, s := range samples {
		if behind := s.source - s.target; behind > worst {
			worst = behind
		}
	}
	t.Logf("largest backlog observed: %d documents (%.1fs of writing at %d/s)",
		worst, float64(worst)/float64(rate), rate)
	if len(samples) >= 4 {
		early := samples[1].source - samples[1].target
		late := samples[len(samples)-1].source - samples[len(samples)-1].target
		t.Logf("backlog after 2s: %d documents; at the end of the run: %d", early, late)
	}

	for _, metric := range []string{metrics.LagSeconds, metrics.ReadLagSeconds} {
		for _, s := range metrics.Default.Snapshot(metric) {
			if s.Labels["task"] == fmt.Sprint(cfg.ID) {
				t.Logf("%s = %.3fs", metric, s.Value)
			}
		}
	}
}

func stgInt(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	var n int
	if _, err := fmt.Sscanf(raw, "%d", &n); err != nil || n <= 0 {
		return fallback
	}
	return n
}

// maskPassword keeps the credentials out of the test output, which ends up in
// logs and pull requests.
func maskPassword(uri string) string {
	if stgPassword == "" {
		return uri
	}
	return strings.ReplaceAll(uri, stgPassword, "***")
}

// TestTheTargetIsShardedLikeTheSource is why this suite needs a sharded cluster.
// A sharded source replicated into an unsharded target is not the same
// collection: every document lands on whichever shard is primary for the target
// database, so the copy has one shard's capacity where the source had three. The
// region it exists to stand in for could not be stood in for.
func TestTheTargetIsShardedLikeTheSource(t *testing.T) {
	name, source, target := stgCollection(t, "payments_sharded")
	ctx := context.Background()

	const seeded = 500
	docs := make([]interface{}, 0, seeded)
	for i := 0; i < seeded; i++ {
		docs = append(docs, payment(i))
	}
	if _, err := source.InsertMany(ctx, docs); err != nil {
		t.Fatalf("seed the source: %v", err)
	}

	stgStart(t, stgTask(t, name))
	harness.Eventually(t, 3*time.Minute, func() error {
		if n := stgCount(t, target); n != seeded {
			return fmt.Errorf("the target holds %d of %d documents", n, seeded)
		}
		return nil
	})

	client := stgConnect(t, stgTargetDB)
	key, err := collectionShardKey(ctx, client, stgTargetDB+"."+name)
	if err != nil {
		t.Fatalf("read the target's shard key: %v", err)
	}
	if key == nil {
		t.Fatal("the target collection is not sharded, so it holds the whole " +
			"collection on one shard")
	}
	t.Logf("the target is sharded on %v", key.Key)

	// And the documents are actually spread, not merely allowed to be.
	var stats []bson.M
	cursor, err := client.Database(stgTargetDB).Collection(name).Aggregate(ctx,
		mongo.Pipeline{bson.D{{Key: "$collStats", Value: bson.D{
			{Key: "storageStats", Value: bson.D{}},
		}}}})
	if err != nil {
		t.Fatalf("collStats: %v", err)
	}
	if err := cursor.All(ctx, &stats); err != nil {
		t.Fatalf("read collStats: %v", err)
	}
	if len(stats) < 2 {
		t.Errorf("the target reports storage on %d shard(s); the documents are not "+
			"spread", len(stats))
	}
	t.Logf("the target's documents are spread over %d shards", len(stats))
}

// TestAChunkMigrationIsNotReplicated is the one behaviour in this design that
// was asserted from documentation and never tested.
//
// The balancer moves a chunk by inserting the documents on the destination shard
// and deleting them on the source. Both go to the oplog, so a change stream that
// carried them would deliver a delete and an insert per document — work that
// achieves nothing, and a window in which the document is absent from the
// target. MongoDB marks such oplog entries fromMigrate and a change stream drops
// them, but that is a claim about somebody else's code, and the balancer is on in
// every cluster this will run against.
//
// The end state cannot catch it: a delete followed by an insert of the same
// document leaves the collection exactly as it was. What has to be checked is
// that the applier was given nothing at all, which is what the event counter is
// for.
func TestAChunkMigrationIsNotReplicated(t *testing.T) {
	name, source, target := stgCollection(t, "orders_migrating")
	ctx := context.Background()

	const seeded = 400
	docs := make([]interface{}, 0, seeded)
	for i := 0; i < seeded; i++ {
		docs = append(docs, payment(i))
	}
	if _, err := source.InsertMany(ctx, docs); err != nil {
		t.Fatalf("seed the source: %v", err)
	}

	cfg := stgTask(t, name)
	stgStart(t, cfg)
	labels := metrics.Labels{"task": fmt.Sprint(cfg.ID), "engine": "mongodb"}
	t.Cleanup(func() { metrics.Default.Forget(labels) })

	harness.Eventually(t, 3*time.Minute, func() error {
		if n := stgCount(t, target); n != seeded {
			return fmt.Errorf("the target holds %d of %d documents", n, seeded)
		}
		return nil
	})

	// The copy is done and the stream is following. Everything from here on is
	// the migration's doing.
	appliedBefore := counter(t, metrics.BatchEvents, labels)

	client := stgConnect(t, stgSourceDB)
	moved, err := moveOneChunk(ctx, client, stgSourceDB+"."+name)
	if err != nil {
		t.Fatalf("move a chunk of %s: %v", name, err)
	}
	t.Logf("moved a chunk from %s to %s", moved.from, moved.to)

	// Long enough for the events to have arrived if they were going to.
	time.Sleep(20 * time.Second)

	if applied := counter(t, metrics.BatchEvents, labels); applied != appliedBefore {
		t.Errorf("the applier was given %v changes by a chunk migration, want none. "+
			"Every document in the chunk is being deleted and re-inserted on the "+
			"target for no reason, and is absent from it in between",
			applied-appliedBefore)
	}
	if n := stgCount(t, target); n != seeded {
		t.Errorf("the target holds %d of %d documents after the migration", n, seeded)
	}

	// And the stream is still working, rather than having been quietly broken by
	// the migration — which would look identical to the migration being filtered.
	probe := bson.M{"_id": "after-the-migration", "amount": 1}
	if _, err := source.InsertOne(ctx, probe); err != nil {
		t.Fatalf("write the probe: %v", err)
	}
	harness.Eventually(t, time.Minute, func() error {
		if n := stgCount(t, target); n != seeded+1 {
			return fmt.Errorf("the probe written after the migration has not arrived")
		}
		return nil
	})
	if applied := counter(t, metrics.BatchEvents, labels); applied != appliedBefore+1 {
		t.Errorf("the applier was given %v changes for one document written after the "+
			"migration", applied-appliedBefore)
	}
}

// movedChunk records where a chunk went.
type movedChunk struct{ from, to string }

// moveOneChunk moves one chunk of a collection to another shard, splitting the
// collection first when it is still in one piece.
func moveOneChunk(ctx context.Context, client *mongo.Client, ns string) (movedChunk, error) {
	admin := client.Database("admin")
	config := client.Database("config")

	var collection struct {
		UUID interface{} `bson:"uuid"`
	}
	if err := config.Collection("collections").FindOne(ctx, bson.M{"_id": ns}).
		Decode(&collection); err != nil {
		return movedChunk{}, fmt.Errorf("read the collection's routing entry: %w", err)
	}

	chunks, err := config.Collection("chunks").Find(ctx, bson.M{"uuid": collection.UUID})
	if err != nil {
		return movedChunk{}, fmt.Errorf("list the chunks: %w", err)
	}
	var found []struct {
		Shard string   `bson:"shard"`
		Min   bson.Raw `bson:"min"`
	}
	if err := chunks.All(ctx, &found); err != nil {
		return movedChunk{}, fmt.Errorf("read the chunks: %w", err)
	}
	if len(found) == 0 {
		return movedChunk{}, fmt.Errorf("%s has no chunks", ns)
	}

	var shards struct {
		Shards []struct {
			ID string `bson:"_id"`
		} `bson:"shards"`
	}
	if err := admin.RunCommand(ctx, bson.D{{Key: "listShards", Value: 1}}).
		Decode(&shards); err != nil {
		return movedChunk{}, fmt.Errorf("list the shards: %w", err)
	}

	from := found[0].Shard
	to := ""
	for _, s := range shards.Shards {
		if s.ID != from {
			to = s.ID
			break
		}
	}
	if to == "" {
		return movedChunk{}, fmt.Errorf("the cluster has only one shard, so nothing can move")
	}

	if err := admin.RunCommand(ctx, bson.D{
		{Key: "moveChunk", Value: ns},
		{Key: "bounds", Value: bson.A{found[0].Min, nextChunkBound(found, 0)}},
		{Key: "to", Value: to},
	}).Err(); err != nil {
		return movedChunk{}, fmt.Errorf("moveChunk: %w", err)
	}
	return movedChunk{from: from, to: to}, nil
}

// nextChunkBound reports the upper bound of the nth chunk, which is the lower
// bound of the one after it.
func nextChunkBound(chunks []struct {
	Shard string   `bson:"shard"`
	Min   bson.Raw `bson:"min"`
}, n int) bson.Raw {
	if n+1 < len(chunks) {
		return chunks[n+1].Min
	}
	// The last chunk runs to MaxKey, which is what the routing table records as
	// the next bound of the highest chunk.
	raw, _ := bson.Marshal(bson.D{{Key: "_id", Value: bson.MaxKey{}}})
	return bson.Raw(raw)
}

// counter reads one counter's current value for a label set, zero when it has
// not been recorded yet.
func counter(t *testing.T, name string, labels metrics.Labels) float64 {
	t.Helper()
	for _, sample := range metrics.Default.Snapshot(name) {
		if sample.Labels.Key() == labels.Key() {
			return sample.Value
		}
	}
	return 0
}
