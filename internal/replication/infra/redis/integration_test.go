//go:build integration

package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/dsn"

	goredis "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	intRedis "github.com/retail-ai-inc/sync/internal/platform/dbconn/redis"
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
		ID:     harness.UniqueTaskID(),
		Enable: true,
		Type:   "redis",
		SourceConnection: dsn.BuildDSNByType("redis", map[string]string{
			"host": srcHost, "port": srcPort, "database": db,
		}),
		TargetConnection: dsn.BuildDSNByType("redis", map[string]string{
			"host": tgtHost, "port": tgtPort, "database": db,
		}),
		RedisPositionPath: t.TempDir() + "/redis.pos",
		// The stream mapping is optional now; it is set here because most of
		// these tests exercise the stream path.
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

// TestKeyspaceNotificationsFollowTheConfiguredDatabase exercises F-082. The
// subscription used to name database 0 whatever the task was configured with,
// so a task on any other database did its initial copy and then silently
// replicated nothing.
func TestKeyspaceNotificationsFollowTheConfiguredDatabase(t *testing.T) {
	flush(t, 1)
	src, tgt := client(t, harness.RedisSource, 1), client(t, harness.RedisTarget, 1)
	ctx := context.Background()

	if err := src.Set(ctx, "before", "v", 0).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}

	startSyncer(t, syncTask(t, "1"))

	harness.Eventually(t, 30*time.Second, func() error {
		if tgt.Exists(ctx, "before").Val() != 1 {
			return fmt.Errorf("initial sync has not landed")
		}
		return nil
	})

	if err := src.Set(ctx, "after", "v", 0).Err(); err != nil {
		t.Fatalf("write after start: %v", err)
	}

	harness.Eventually(t, 20*time.Second, func() error {
		if tgt.Exists(ctx, "after").Val() != 1 {
			return fmt.Errorf("the key written after startup has not arrived")
		}
		return nil
	})
}

// TestAnExpiryIsPropagated covers the notification kinds that used to fall
// through to a copy of a key that no longer exists, leaving the target holding
// a value the source had dropped.
func TestAnExpiryIsPropagated(t *testing.T) {
	flush(t, 0)
	src, tgt := client(t, harness.RedisSource, 0), client(t, harness.RedisTarget, 0)
	ctx := context.Background()

	if err := src.Set(ctx, "short-lived", "v", 0).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}

	startSyncer(t, syncTask(t, "0"))
	harness.Eventually(t, 30*time.Second, func() error {
		if tgt.Exists(ctx, "short-lived").Val() != 1 {
			return fmt.Errorf("initial sync has not landed")
		}
		return nil
	})

	if err := src.Del(ctx, "short-lived").Err(); err != nil {
		t.Fatalf("delete: %v", err)
	}

	harness.Eventually(t, 20*time.Second, func() error {
		if tgt.Exists(ctx, "short-lived").Val() != 0 {
			return fmt.Errorf("the deletion has not reached the target")
		}
		return nil
	})
}

// TestATTLSurvivesAnIncrementalChange covers what rebuilding the value from its
// type used to lose: the SET the incremental path issued carried no expiry, so
// a key that was meant to age out became permanent on the target.
func TestATTLSurvivesAnIncrementalChange(t *testing.T) {
	flush(t, 0)
	src, tgt := client(t, harness.RedisSource, 0), client(t, harness.RedisTarget, 0)
	ctx := context.Background()

	startSyncer(t, syncTask(t, "0"))
	harness.Eventually(t, 30*time.Second, func() error {
		if err := src.Ping(ctx).Err(); err != nil {
			return err
		}
		return nil
	})

	if err := src.Set(ctx, "session:1", "v", time.Hour).Err(); err != nil {
		t.Fatalf("set with expiry: %v", err)
	}

	harness.Eventually(t, 20*time.Second, func() error {
		ttl := tgt.TTL(ctx, "session:1").Val()
		if ttl <= 0 {
			return fmt.Errorf("the target holds the key with ttl %v", ttl)
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

	// One configuration, reused: the two runs have to be the same task, because
	// the stream offset is keyed by task id.
	task := syncTask(t, "0")

	stop := startSyncer(t, task)
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

	startSyncer(t, task)

	// The re-run of the initial copy picks up the new key.
	harness.Eventually(t, 30*time.Second, func() error {
		if tgt.Exists(ctx, "created-while-down").Val() != 1 {
			return fmt.Errorf("the key created while the syncer was down has not arrived")
		}
		return nil
	})

	// A deletion missed while nothing was subscribed is only corrected by the
	// periodic comparison, which runs hourly by default. Rather than wait, run
	// one directly against the same endpoints.
	reconciler := NewRedisSyncer(syncTask(t, "0"), logrus.New())
	reconcileOnce(t, reconciler)

	harness.Eventually(t, 10*time.Second, func() error {
		if tgt.Exists(ctx, "removed-while-down").Val() != 0 {
			return fmt.Errorf("the key deleted while the syncer was down is still " +
				"on the target after a reconciliation pass")
		}
		return nil
	})
}

// reconcileOnce connects a syncer to both endpoints and runs a single
// comparison, which is what the hourly loop does on each tick.
func reconcileOnce(t *testing.T, s *RedisSyncer) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var err error
	if s.source, err = intRedis.GetRedisClient(s.cfg.SourceConnection); err != nil {
		t.Fatalf("connect source: %v", err)
	}
	defer s.source.Close()
	if s.target, err = intRedis.GetRedisClient(s.cfg.TargetConnection); err != nil {
		t.Fatalf("connect target: %v", err)
	}
	defer s.target.Close()

	s.reconcile(ctx)
}

// TestATaskWithoutTableMappingStarts is the configuration that used to take the
// whole process down. Start read the stream name as Mappings[0].Tables[0]
// without a bounds check, while the configuration loader synthesises exactly
// that shape — one mapping with an empty table list — for a task whose
// config_json carries no mappings. "Tables" is a meaningless notion for Redis,
// so it is an easy configuration to arrive at, and cmd/sync launches each
// syncer in a bare goroutine, so the panic was not recovered.
func TestATaskWithoutTableMappingStarts(t *testing.T) {
	flush(t, 0)
	src, tgt := client(t, harness.RedisSource, 0), client(t, harness.RedisTarget, 0)
	ctx := context.Background()

	if err := src.Set(ctx, "keyspace-only", "v", 0).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}

	cfg := syncTask(t, "0")
	cfg.Mappings = []config.DatabaseMapping{{Tables: []config.TableMapping{}}}

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)
	syncer := NewRedisSyncer(cfg, logger)

	done := make(chan interface{}, 1)
	runCtx, cancel := context.WithCancel(context.Background())
	go func() {
		defer func() { done <- recover() }()
		syncer.Start(runCtx)
	}()
	t.Cleanup(cancel)

	// The keyspace path still runs, which is the point: a task with no stream
	// mapping replicates the keyspace rather than crashing.
	harness.Eventually(t, 30*time.Second, func() error {
		if tgt.Exists(ctx, "keyspace-only").Val() != 1 {
			return fmt.Errorf("the initial copy has not landed")
		}
		return nil
	})

	cancel()
	select {
	case recovered := <-done:
		if recovered != nil {
			t.Fatalf("Start panicked on a task with no table mapping: %v", recovered)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Start did not return after the context was cancelled")
	}
}

// TestStreamEntriesReachTheTarget exercises F-083. Two things used to break it:
// the reader asked its consumer group for already-delivered entries rather than
// new ones, and each entry was written into a hash called msg:<id> on the
// target, chosen by asking the *source* for the type of a key it has never had.
// Entries are now appended to the target stream under their own identifiers.
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

	harness.Eventually(t, 20*time.Second, func() error {
		entries, err := tgt.XRange(ctx, stream, "-", "+").Result()
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.Values["name"] == "after" {
				return nil
			}
		}
		return fmt.Errorf("the entry added after startup is not on the target stream: %v", entries)
	})
}

// TestStreamEntriesKeepTheirIdentifiers is what makes the replication replayable:
// an entry that arrives twice after a restart is refused by the target as an
// identifier that is not greater than the last one, and that refusal is read as
// "already applied" rather than as a failure.
func TestStreamEntriesKeepTheirIdentifiers(t *testing.T) {
	flush(t, 0)
	src, tgt := client(t, harness.RedisSource, 0), client(t, harness.RedisTarget, 0)
	ctx := context.Background()

	const stream = "sync_stream"
	startSyncer(t, syncTask(t, "0"))

	id, err := src.XAdd(ctx, &goredis.XAddArgs{
		Stream: stream, Values: map[string]interface{}{"n": "1"},
	}).Result()
	if err != nil {
		t.Fatalf("add entry: %v", err)
	}

	harness.Eventually(t, 30*time.Second, func() error {
		entries, err := tgt.XRange(ctx, stream, id, id).Result()
		if err != nil {
			return err
		}
		if len(entries) != 1 {
			return fmt.Errorf("the entry is not on the target under its own id")
		}
		return nil
	})
}

// TestTheConsumerGroupAdvances isolates the first of the two problems: whether
// the group is delivered anything at all. It used to read with the stored id,
// which for a consumer group means "my pending entries", so the group never
// advanced past 0-0 however many entries were added.
func TestTheConsumerGroupAdvances(t *testing.T) {
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

	harness.Eventually(t, 10*time.Second, func() error {
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

	harness.Eventually(t, 20*time.Second, func() error {
		groups, err := src.XInfoGroups(ctx, stream).Result()
		if err != nil || len(groups) == 0 {
			return fmt.Errorf("group not readable")
		}
		if groups[0].LastDeliveredID == "0-0" {
			return fmt.Errorf("the group is still at 0-0; nothing has been delivered")
		}
		return nil
	})
}
