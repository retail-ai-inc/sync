//go:build integration

package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/test/harness"
)

func client(t *testing.T, endpoint string, db int) *goredis.Client {
	t.Helper()

	c := goredis.NewClient(&goredis.Options{Addr: endpoint, DB: db})
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("ping %s db%d: %v", endpoint, db, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func syncTask(t *testing.T, db string) config.SyncConfig {
	t.Helper()

	srcHost, srcPort := harness.SplitHostPort(t, harness.RedisSource)
	tgtHost, tgtPort := harness.SplitHostPort(t, harness.RedisTarget)

	return config.SyncConfig{
		ID:     1,
		Enable: true,
		Type:   "redis",
		SourceConnection: config.BuildDSNByType("redis", map[string]string{
			"host": srcHost, "port": srcPort, "database": db,
		}),
		TargetConnection: config.BuildDSNByType("redis", map[string]string{
			"host": tgtHost, "port": tgtPort, "database": db,
		}),
		RedisPositionPath: t.TempDir() + "/redis.pos",
		// A stream mapping is mandatory: Start indexes Mappings[0].Tables[0]
		// without a bounds check, see TestStartPanicsWithoutTableMapping.
		Mappings: []config.DatabaseMapping{{Tables: []config.TableMapping{
			{SourceTable: "sync_stream", TargetTable: "sync_stream"},
		}}},
	}
}

func startSyncer(t *testing.T, cfg config.SyncConfig) (stop func()) {
	t.Helper()

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	syncer := NewRedisSyncer(cfg, logger)
	if syncer == nil {
		t.Fatal("NewRedisSyncer returned nil")
	}
	stop = harness.RunSyncer(t, syncer.Start)
	t.Cleanup(stop)
	return stop
}

// flush clears both sides so each test starts from a known state; the syncer
// copies every key it finds, so leftovers from one test would confuse the next.
func flush(t *testing.T, dbs ...int) {
	t.Helper()

	ctx := context.Background()
	for _, db := range dbs {
		for _, endpoint := range []string{harness.RedisSource, harness.RedisTarget} {
			c := goredis.NewClient(&goredis.Options{Addr: endpoint, DB: db})
			if err := c.FlushDB(ctx).Err(); err != nil {
				t.Fatalf("flush %s db%d: %v", endpoint, db, err)
			}
			_ = c.Close()
		}
	}
}

func TestInitialSyncCopiesExistingKeys(t *testing.T) {
	flush(t, 0)
	src, tgt := client(t, harness.RedisSource, 0), client(t, harness.RedisTarget, 0)
	ctx := context.Background()

	// One key of each shape the DUMP/RESTORE path has to carry.
	if err := src.Set(ctx, "str", "hello", 0).Err(); err != nil {
		t.Fatalf("seed string: %v", err)
	}
	if err := src.HSet(ctx, "hash", "field", "value").Err(); err != nil {
		t.Fatalf("seed hash: %v", err)
	}
	if err := src.RPush(ctx, "list", "a", "b", "c").Err(); err != nil {
		t.Fatalf("seed list: %v", err)
	}
	if err := src.SAdd(ctx, "set", "x", "y").Err(); err != nil {
		t.Fatalf("seed set: %v", err)
	}
	if err := src.ZAdd(ctx, "zset", goredis.Z{Score: 1, Member: "m"}).Err(); err != nil {
		t.Fatalf("seed zset: %v", err)
	}
	if err := src.Set(ctx, "expiring", "v", time.Hour).Err(); err != nil {
		t.Fatalf("seed expiring: %v", err)
	}

	startSyncer(t, syncTask(t, "0"))

	harness.Eventually(t, 30*time.Second, func() error {
		for _, key := range []string{"str", "hash", "list", "set", "zset", "expiring"} {
			n, err := tgt.Exists(ctx, key).Result()
			if err != nil {
				return err
			}
			if n != 1 {
				return fmt.Errorf("key %q has not arrived", key)
			}
		}
		return nil
	})

	if got := tgt.Get(ctx, "str").Val(); got != "hello" {
		t.Errorf("str = %q, want hello", got)
	}
	if got := tgt.LRange(ctx, "list", 0, -1).Val(); len(got) != 3 {
		t.Errorf("list = %v, want three elements", got)
	}
	// TTLs must survive the copy, otherwise the target keeps data the source drops.
	if ttl := tgt.TTL(ctx, "expiring").Val(); ttl <= 0 {
		t.Errorf("expiring has TTL %v, want it preserved", ttl)
	}
}

func TestIncrementalSyncAppliesSetAndDelete(t *testing.T) {
	flush(t, 0)
	src, tgt := client(t, harness.RedisSource, 0), client(t, harness.RedisTarget, 0)
	ctx := context.Background()

	if err := src.Set(ctx, "seed", "v", 0).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}
	startSyncer(t, syncTask(t, "0"))
	harness.Eventually(t, 30*time.Second, func() error {
		if tgt.Exists(ctx, "seed").Val() != 1 {
			return fmt.Errorf("initial sync has not landed")
		}
		return nil
	})

	t.Run("set", func(t *testing.T) {
		if err := src.Set(ctx, "added", "value", 0).Err(); err != nil {
			t.Fatalf("set: %v", err)
		}
		harness.Eventually(t, 20*time.Second, func() error {
			if got := tgt.Get(ctx, "added").Val(); got != "value" {
				return fmt.Errorf("added = %q, want value", got)
			}
			return nil
		})
	})

	t.Run("delete", func(t *testing.T) {
		if err := src.Del(ctx, "added").Err(); err != nil {
			t.Fatalf("del: %v", err)
		}
		harness.Eventually(t, 20*time.Second, func() error {
			if tgt.Exists(ctx, "added").Val() != 0 {
				return fmt.Errorf("the deletion has not arrived")
			}
			return nil
		})
	})
}

// TestKeyspaceNotificationsOnlyCoverDB0 exercises F-082. watchKeyspaceChanges
// subscribes to the literal pattern __keyspace@0__:*, so a task configured for
// any other database receives no incremental events at all: the initial copy
// works and everything after it is silently dropped.
func TestKeyspaceNotificationsOnlyCoverDB0(t *testing.T) {
	flush(t, 1)
	src, tgt := client(t, harness.RedisSource, 1), client(t, harness.RedisTarget, 1)
	ctx := context.Background()

	if err := src.Set(ctx, "before", "v", 0).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}

	startSyncer(t, syncTask(t, "1"))

	// The initial copy uses SCAN against the configured database, so it works.
	harness.Eventually(t, 30*time.Second, func() error {
		if tgt.Exists(ctx, "before").Val() != 1 {
			return fmt.Errorf("initial sync has not landed")
		}
		return nil
	})

	if err := src.Set(ctx, "after", "v", 0).Err(); err != nil {
		t.Fatalf("write after start: %v", err)
	}

	// Incremental changes on db1 must reach the target.
	// db1 writes never arrive, so this window is spent in full while the
	// defect stands and exits immediately once it is fixed.
	harness.Eventually(t, 5*time.Second, func() error {
		if tgt.Exists(ctx, "after").Val() != 1 {
			return fmt.Errorf("the key written after startup never arrived; keyspace " +
				"notifications are subscribed on db0 only (F-082)")
		}
		return nil
	})
}

// TestChangesWhileStoppedAreReplayed exercises F-081 and F-086 together. Redis
// keyspace notifications are fire-and-forget: nothing is buffered while no
// subscriber is listening, and the syncer stores no position it could resume
// from. A restart therefore has to re-copy everything, and anything deleted
// while it was down stays on the target for good.
func TestChangesWhileStoppedAreReplayed(t *testing.T) {
	flush(t, 0)
	src, tgt := client(t, harness.RedisSource, 0), client(t, harness.RedisTarget, 0)
	ctx := context.Background()

	if err := src.Set(ctx, "kept", "v", 0).Err(); err != nil {
		t.Fatalf("seed kept: %v", err)
	}
	if err := src.Set(ctx, "removed-while-down", "v", 0).Err(); err != nil {
		t.Fatalf("seed removed: %v", err)
	}

	stop := startSyncer(t, syncTask(t, "0"))
	harness.Eventually(t, 30*time.Second, func() error {
		if tgt.Exists(ctx, "kept", "removed-while-down").Val() != 2 {
			return fmt.Errorf("initial sync has not landed")
		}
		return nil
	})

	stop()
	time.Sleep(time.Second)

	// Both a creation and a deletion while nothing is subscribed.
	if err := src.Set(ctx, "created-while-down", "v", 0).Err(); err != nil {
		t.Fatalf("create while down: %v", err)
	}
	if err := src.Del(ctx, "removed-while-down").Err(); err != nil {
		t.Fatalf("delete while down: %v", err)
	}

	startSyncer(t, syncTask(t, "0"))

	// The re-run of the initial copy picks up the new key.
	harness.Eventually(t, 30*time.Second, func() error {
		if tgt.Exists(ctx, "created-while-down").Val() != 1 {
			return fmt.Errorf("the key created while the syncer was down has not arrived")
		}
		return nil
	})

	// The deletion has no such safety net: SCAN only reports keys that exist,
	// so a key removed at the source is never removed from the target.
	if tgt.Exists(ctx, "removed-while-down").Val() == 0 {
		t.Fatal("the deletion made while the syncer was down was applied; a " +
			"reconciliation pass may have been added, so assert that instead")
	}
	t.Log("the key deleted while the syncer was down still exists on the target: " +
		"deletions missed during downtime are never reconciled (F-081, F-085)")
}

// TestStartPanicsWithoutTableMapping records a crash. Start reads the stream
// name as Mappings[0].Tables[0].SourceTable with no bounds check, while
// config.loadSyncTasks synthesises exactly that shape — one mapping with an
// empty Tables slice — whenever a task's config_json carries no mappings.
//
// A Redis task saved without table mappings therefore panics on startup, and
// because cmd/sync launches each syncer in a bare goroutine the panic is not
// recovered: the whole process dies, stopping replication for every other task
// as well. "Tables" is also a meaningless notion for Redis, so this is an easy
// configuration to arrive at.
func TestStartPanicsWithoutTableMapping(t *testing.T) {
	cfg := syncTask(t, "0")
	cfg.Mappings = []config.DatabaseMapping{{Tables: []config.TableMapping{}}}

	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	syncer := NewRedisSyncer(cfg, logger)

	done := make(chan interface{}, 1)
	go func() {
		defer func() { done <- recover() }()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		syncer.Start(ctx)
	}()

	select {
	case recovered := <-done:
		if recovered == nil {
			t.Fatal("Start returned without panicking; a bounds check may have been " +
				"added, so assert the graceful behaviour instead")
		}
		t.Logf("Start panicked as expected: %v", recovered)
	case <-time.After(30 * time.Second):
		t.Fatal("Start neither panicked nor returned")
	}
}

// TestStreamEntriesReachTheTarget exercises F-083, which the feature inventory
// lists as working. Two things in the stream path look wrong on reading, and
// this checks whether either bites:
//
//   - watchStreamChanges calls XReadGroup with lastID, which starts at "0-0"
//     and is only ever set to an id that was already processed. For a consumer
//     group that means "give me my pending entries", never ">" for new ones.
//   - processStreamMessage builds the target key name msg:<id> and then asks
//     the *source* for that key's type. The name is invented for the target, so
//     the source returns "none" and the message is rejected as an unsupported
//     type before anything is written.
func TestStreamEntriesReachTheTarget(t *testing.T) {
	flush(t, 0)
	src, tgt := client(t, harness.RedisSource, 0), client(t, harness.RedisTarget, 0)
	ctx := context.Background()

	const stream = "sync_stream"
	if err := src.XAdd(ctx, &goredis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"user_id": "1", "name": "before"},
	}).Err(); err != nil {
		t.Fatalf("seed stream: %v", err)
	}

	startSyncer(t, syncTask(t, "0"))

	// The initial full copy carries the stream key itself across, so the target
	// has the data as a stream; that is not what the stream replication path is
	// supposed to produce.
	harness.Eventually(t, 30*time.Second, func() error {
		if tgt.Exists(ctx, stream).Val() != 1 {
			return fmt.Errorf("the initial copy has not landed")
		}
		return nil
	})

	if err := src.XAdd(ctx, &goredis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"user_id": "2", "name": "after"},
	}).Err(); err != nil {
		t.Fatalf("add entry: %v", err)
	}

	// processStreamMessage stores each entry under msg:<id> on the target.
	// Nothing is ever written to the target, so this window is spent in full
	// while the defect stands and exits immediately once it is fixed.
	harness.Eventually(t, 5*time.Second, func() error {
		keys, err := tgt.Keys(ctx, "msg:*").Result()
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			return fmt.Errorf("no msg:* key was written; the stream entry added " +
				"after startup was never replicated (F-083)")
		}
		return nil
	})
}

// TestStreamConsumerGroupNeverReceivesNewEntries isolates the first of the two
// problems: whether the consumer group is delivered anything at all. If the
// group's pending list stays empty while entries pile up in the stream, the
// reader never asked for new messages.
func TestStreamConsumerGroupNeverReceivesNewEntries(t *testing.T) {
	flush(t, 0)
	src := client(t, harness.RedisSource, 0)
	ctx := context.Background()

	const stream = "sync_stream"
	if err := src.XAdd(ctx, &goredis.XAddArgs{
		Stream: stream, Values: map[string]interface{}{"seed": "1"},
	}).Err(); err != nil {
		t.Fatalf("seed stream: %v", err)
	}

	startSyncer(t, syncTask(t, "0"))

	// Wait for the group to exist rather than sleeping a fixed interval: the
	// syncer creates it during startup, normally within a few hundred ms.
	harness.Eventually(t, 5*time.Second, func() error {
		groups, err := src.XInfoGroups(ctx, stream).Result()
		if err != nil {
			return fmt.Errorf("XInfoGroups: %w", err)
		}
		if len(groups) == 0 {
			return fmt.Errorf("the consumer group has not been created yet")
		}
		return nil
	})

	for i := 0; i < 5; i++ {
		if err := src.XAdd(ctx, &goredis.XAddArgs{
			Stream: stream, Values: map[string]interface{}{"n": i},
		}).Err(); err != nil {
			t.Fatalf("add entry %d: %v", i, err)
		}
	}

	// The group never advances while the defect stands, so this window is spent
	// in full; it returns at once if delivery starts working.
	harness.WaitFor(2*time.Second, func() error {
		groups, err := src.XInfoGroups(ctx, stream).Result()
		if err != nil || len(groups) == 0 {
			return fmt.Errorf("group not readable")
		}
		if groups[0].LastDeliveredID == "0-0" {
			return fmt.Errorf("still at 0-0")
		}
		return nil
	})

	groups, err := src.XInfoGroups(ctx, stream).Result()
	if err != nil {
		t.Fatalf("XInfoGroups: %v", err)
	}
	if len(groups) == 0 {
		t.Fatal("the consumer group was never created")
	}
	g := groups[0]
	t.Logf("group %q: pending=%d last-delivered=%s entries-read=%d",
		g.Name, g.Pending, g.LastDeliveredID, g.EntriesRead)

	if g.LastDeliveredID != "0-0" {
		t.Skipf("the group has advanced to %s, so delivery is working after all",
			g.LastDeliveredID)
	}
	t.Errorf("the consumer group is still at last-delivered %s after six entries "+
		"were added: XReadGroup is never called with \">\", so new entries are "+
		"never delivered (F-083)", g.LastDeliveredID)
}
