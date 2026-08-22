//go:build perf

// Performance of the Redis path against a real pair of servers.
//
// Two numbers matter here and they are unrelated to each other:
//
//   - how quickly a change reaches the target through keyspace notifications,
//     which is the recovery point while everything is working.
//   - how long a full reconciliation takes, which is the recovery point when
//     anything has gone wrong. Notifications are published with no
//     acknowledgement and no replay, so the periodic comparison is not an
//     optimisation — it is the only thing that makes the target correct after a
//     dropped subscription. If it cannot finish, the guarantee it provides does
//     not exist.
//
// Run with:
//
//	go test -tags perf -v -timeout 30m ./internal/replication/infra/redis/
package redis

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	intRedis "github.com/retail-ai-inc/sync/internal/platform/dbconn/redis"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/test/harness"
)

// perfDB is its own database index so a perf run does not collide with the
// integration suite's, which flushes what it uses.
const perfDB = 7

func perfInt(key string, fallback int) int {
	if raw := os.Getenv(key); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

func perfClient(t *testing.T, endpoint string) *goredis.Client {
	t.Helper()

	c := goredis.NewClient(&goredis.Options{Addr: endpoint, DB: perfDB})
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("ping %s: %v", endpoint, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func perfFlush(t *testing.T) {
	t.Helper()

	for _, endpoint := range []string{harness.RedisSource, harness.RedisTarget} {
		c := goredis.NewClient(&goredis.Options{Addr: endpoint, DB: perfDB})
		if err := c.FlushDB(context.Background()).Err(); err != nil {
			t.Fatalf("flush %s: %v", endpoint, err)
		}
		_ = c.Close()
	}
}

func perfTask(t *testing.T) config.SyncConfig {
	t.Helper()

	srcHost, srcPort := harness.SplitHostPort(t, harness.RedisSource)
	tgtHost, tgtPort := harness.SplitHostPort(t, harness.RedisTarget)
	db := strconv.Itoa(perfDB)

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
	}
}

func perfSyncer(t *testing.T, cfg config.SyncConfig) *RedisSyncer {
	t.Helper()

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)
	s := NewRedisSyncer(cfg, logger)
	if s == nil {
		t.Fatal("NewRedisSyncer returned nil")
	}
	return s
}

func perfPercentiles(t *testing.T, what string, latencies []time.Duration) {
	t.Helper()

	if len(latencies) == 0 {
		t.Fatalf("no %s were observed", what)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	at := func(q float64) time.Duration {
		return latencies[int(float64(len(latencies)-1)*q)].Round(time.Millisecond)
	}
	t.Logf("traced %d %s: p50=%v p90=%v p99=%v max=%v", len(latencies), what,
		at(0.50), at(0.90), at(0.99),
		latencies[len(latencies)-1].Round(time.Millisecond))
}

// TestTheKeyspaceLatencyUnderSteadyWrites measures the recovery point while the
// subscription is healthy: a key written at the source, and how long until it
// can be read at the target.
func TestTheKeyspaceLatencyUnderSteadyWrites(t *testing.T) {
	perfFlush(t)
	src := perfClient(t, harness.RedisSource)
	tgt := perfClient(t, harness.RedisTarget)
	ctx := context.Background()

	if err := src.Set(ctx, "perf:seed", "1", 0).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}
	cfg := perfTask(t)
	t.Cleanup(harness.RunSyncer(t, perfSyncer(t, cfg).Start))
	harness.Eventually(t, 60*time.Second, func() error {
		if err := tgt.Get(ctx, "perf:seed").Err(); err != nil {
			return fmt.Errorf("the syncer has not started: %w", err)
		}
		return nil
	})

	rate := perfInt("SYNC_PERF_RATE", 200)
	seconds := perfInt("SYNC_PERF_SECONDS", 30)
	sampleEvery := perfInt("SYNC_PERF_SAMPLE_EVERY", 20)
	t.Logf("writing at %d keys/s for %ds, tracing one key in %d", rate, seconds, sampleEvery)

	var (
		mu        sync.Mutex
		latencies []time.Duration
		overran   int
		traced    sync.WaitGroup
		written   int64
	)

	trace := func(key string, sentAt time.Time) {
		defer traced.Done()
		for time.Since(sentAt) < 60*time.Second {
			if err := tgt.Get(context.Background(), key).Err(); err == nil {
				mu.Lock()
				latencies = append(latencies, time.Since(sentAt))
				mu.Unlock()
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		mu.Lock()
		overran++
		mu.Unlock()
	}

	interval := time.Second / time.Duration(rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	start := time.Now()
	deadline := start.Add(time.Duration(seconds) * time.Second)
	n := 0
	for now := range ticker.C {
		if now.After(deadline) {
			break
		}
		n++
		key := fmt.Sprintf("perf:key:%d", n)
		before := time.Now()
		if err := src.Set(ctx, key, fmt.Sprintf("value-%d", n), 0).Err(); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
		atomic.AddInt64(&written, 1)
		if n%sampleEvery == 0 {
			traced.Add(1)
			go trace(key, before)
		}
	}
	elapsed := time.Since(start)
	t.Logf("wrote %d keys in %v (%.0f/s achieved)", n, elapsed.Round(time.Millisecond),
		float64(n)/elapsed.Seconds())

	traced.Wait()
	perfPercentiles(t, "keys", latencies)
	if overran > 0 {
		t.Logf("%d traced keys had not arrived after 60s", overran)
	}

	drainStart := time.Now()
	harness.Eventually(t, 3*time.Minute, func() error {
		got, err := tgt.DBSize(ctx).Result()
		if err != nil {
			return err
		}
		if got < int64(n+1) {
			return fmt.Errorf("the target holds %d of %d keys", got, n+1)
		}
		return nil
	})
	t.Logf("the target caught up %v after the last write",
		time.Since(drainStart).Round(time.Millisecond))
}

// TestTheReconciliationCost is the number that decides whether Redis can be
// replicated this way at all.
//
// Keyspace notifications are lossy by design, so the periodic full comparison is
// the only thing that makes the target correct. This times one pass against a
// keyspace of a given size, and reports the rate — from which the largest
// keyspace that can be reconciled within a given interval follows directly.
func TestTheReconciliationCost(t *testing.T) {
	keys := perfInt("SYNC_PERF_KEYS", 20000)

	perfFlush(t)
	src := perfClient(t, harness.RedisSource)
	tgt := perfClient(t, harness.RedisTarget)
	ctx := context.Background()

	// Fill the source, pipelined, so the fixture setup is not itself the
	// measurement.
	fill := time.Now()
	pipe := src.Pipeline()
	for i := 0; i < keys; i++ {
		pipe.Set(ctx, fmt.Sprintf("perf:rec:%d", i), fmt.Sprintf("value-%d", i), 0)
		if i%1000 == 999 {
			if _, err := pipe.Exec(ctx); err != nil {
				t.Fatalf("fill: %v", err)
			}
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("fill: %v", err)
	}
	t.Logf("filled the source with %d keys in %v (pipelined)", keys,
		time.Since(fill).Round(time.Millisecond))

	// The syncer's own clients, built the way Start builds them, but without
	// starting it: this measures one reconciliation pass on its own.
	s := perfSyncer(t, perfTask(t))
	source, err := intRedis.GetRedisClient(s.cfg.SourceConnection)
	if err != nil {
		t.Fatalf("connect to the source: %v", err)
	}
	defer source.Close()
	target, err := intRedis.GetRedisClient(s.cfg.TargetConnection)
	if err != nil {
		t.Fatalf("connect to the target: %v", err)
	}
	defer target.Close()
	s.source, s.target = source, target

	// First pass: the target is empty, so this is the cost of copying
	// everything — the same work the initial sync does.
	empty := time.Now()
	s.reconcile(ctx)
	copyAll := time.Since(empty)
	t.Logf("reconciling onto an empty target: %v for %d keys (%.0f keys/s)",
		copyAll.Round(time.Millisecond), keys, float64(keys)/copyAll.Seconds())

	got, sizeErr := tgt.DBSize(ctx).Result()
	if sizeErr != nil {
		t.Fatalf("dbsize: %v", sizeErr)
	}
	if got != int64(keys) {
		t.Errorf("the target holds %d of %d keys after reconciliation", got, keys)
	}

	// Second pass: the two sides already agree. This is what the periodic pass
	// costs in the steady state, which is the number that decides whether the
	// interval is affordable.
	settled := time.Now()
	s.reconcile(ctx)
	steady := time.Since(settled)
	t.Logf("reconciling two sides that already agree: %v for %d keys (%.0f keys/s)",
		steady.Round(time.Millisecond), keys, float64(keys)/steady.Seconds())

	perKey := steady / time.Duration(keys)
	t.Logf("steady-state cost: %v per key", perKey)
	for _, size := range []int{100_000, 1_000_000, 10_000_000} {
		t.Logf("  extrapolated to %9d keys: %v per pass",
			size, (perKey * time.Duration(size)).Round(time.Second))
	}
}
