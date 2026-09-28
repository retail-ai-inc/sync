//go:build integration

package redis

import (
	"context"
	"maps"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/infra/directionlock"
)

// standaloneReconciler compares one source server with one target server.
// applied is what it is told the target has reached.
func standaloneReconciler(t *testing.T, source, target goredis.UniversalClient,
	applied func() int64) *Reconciler {
	t.Helper()
	quiet := logrus.New()
	quiet.SetLevel(logrus.ErrorLevel)
	return &Reconciler{
		Node: source, Source: source, Target: target, Shard: "0",
		Repair: true, Settle: 10 * time.Millisecond, Applied: applied,
		Logger: quiet, Labels: metrics.Labels{"task": "recon", "shard": "0"},
	}
}

// ensureABacklog makes the source count its replication offset, which it only
// does once a replica has connected; a repair is measured against it.
func ensureABacklog(t *testing.T, addr string, source goredis.UniversalClient) {
	t.Helper()
	ctx := context.Background()
	if head, err := masterOffset(ctx, source); err == nil && head > 0 {
		return
	}
	stream, err := Dial(ctx, StreamOptions{Addr: addr, IdleTimeout: 15 * time.Second})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer stream.Close()
	if _, err := stream.Sync(Point{}); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if _, err := stream.SkipRDB(ctx); err != nil {
		t.Fatalf("SkipRDB: %v", err)
	}
}

func headOf(t *testing.T, source goredis.UniversalClient) int64 {
	t.Helper()
	head, err := masterOffset(context.Background(), source)
	if err != nil {
		t.Fatalf("read the source's offset: %v", err)
	}
	if head == 0 {
		t.Fatal("the source reports no replication offset, so no repair could ever be allowed")
	}
	return head
}

// A failure means the reconciler writes over a change that is still on its way to the target.
func TestARepairIsHeldBackWhileTheTargetIsBehind(t *testing.T) {
	sourceAddr := addrsFrom(t, "SYNC_REDIS_SOURCE")[0]
	source := redisAt(t, []string{sourceAddr})
	target := redisAt(t, addrsFrom(t, "SYNC_REDIS_TARGET"))
	emptyBoth(t, source, target)
	ensureABacklog(t, sourceAddr, source)
	ctx := context.Background()

	if err := source.Set(ctx, "recon:held", "source-value", 0).Err(); err != nil {
		t.Fatalf("seed the source: %v", err)
	}
	if err := target.Set(ctx, "recon:held", "stale-value", 0).Err(); err != nil {
		t.Fatalf("seed the target: %v", err)
	}
	behind := headOf(t, source) - 1

	found, err := standaloneReconciler(t, source, target, func() int64 { return behind }).pass(ctx)
	if err != nil {
		t.Fatalf("pass: %v", err)
	}
	if found != 1 {
		t.Errorf("the comparison found %d differences, want the one it held back", found)
	}
	if got, err := target.Get(ctx, "recon:held").Result(); err != nil || got != "stale-value" {
		t.Errorf("recon:held reads %q (%v) on the target, want it left to the stream", got, err)
	}
}

// A failure means a change that arrived between the two looks is counted, and repaired, as divergence.
func TestASecondLookThatAgreesIsNotADifference(t *testing.T) {
	sourceAddr := addrsFrom(t, "SYNC_REDIS_SOURCE")[0]
	source := redisAt(t, []string{sourceAddr})
	target := redisAt(t, addrsFrom(t, "SYNC_REDIS_TARGET"))
	emptyBoth(t, source, target)
	ensureABacklog(t, sourceAddr, source)
	ctx := context.Background()

	for _, seed := range []*goredis.StatusCmd{
		source.Set(ctx, "recon:late", "new", 0),
		target.Set(ctx, "recon:late", "old", 0),
		target.Set(ctx, "recon:back", "kept", 0),
	} {
		if err := seed.Err(); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	reconciler := standaloneReconciler(t, source, target, func() int64 {
		head, _ := masterOffset(ctx, source)
		return head
	})
	inFlight := []func() error{
		func() error { return target.Set(ctx, "recon:late", "new", 0).Err() },
		func() error { return source.Set(ctx, "recon:back", "kept", 0).Err() },
	}
	reconciler.settled = func(context.Context) error {
		if len(inFlight) == 0 {
			t.Error("the comparison looked a second time at more than the two keys")
			return nil
		}
		next := inFlight[0]
		inFlight = inFlight[1:]
		return next()
	}

	found, err := reconciler.pass(ctx)
	if err != nil {
		t.Fatalf("pass: %v", err)
	}
	if len(inFlight) != 0 {
		t.Errorf("%d second looks never happened", len(inFlight))
	}
	if found != 0 {
		t.Errorf("the comparison found %d differences, want none after the second looks", found)
	}
	if n, err := target.Exists(ctx, "recon:back").Result(); err != nil || n != 1 {
		t.Errorf("recon:back was removed from the target (%v), though the source has it again", err)
	}
}

// claimOn writes a claim the way the guard's heartbeat does.
func claimOn(t *testing.T, client goredis.UniversalClient, taskID int, role directionlock.Role) {
	t.Helper()
	store := &directionlock.RedisStore{Client: client}
	claim := directionlock.Claim{TaskID: taskID, Role: role, Owner: "tokyo", UpdatedAt: time.Now().UTC()}
	if err := store.Put(context.Background(), claim); err != nil {
		t.Fatalf("claim the %s: %v", role, err)
	}
}

// A failure means the comparison copies the source's claims over the target's and erases a promotion.
func TestTheComparisonLeavesThisToolsOwnKeysAlone(t *testing.T) {
	sourceAddr := addrsFrom(t, "SYNC_REDIS_SOURCE")[0]
	source := redisAt(t, []string{sourceAddr})
	target := redisAt(t, addrsFrom(t, "SYNC_REDIS_TARGET"))
	emptyBoth(t, source, target)
	ensureABacklog(t, sourceAddr, source)
	ctx := context.Background()

	claimOn(t, source, 7, directionlock.RoleSource)
	claimOn(t, target, 7, directionlock.RoleTarget)
	if err := directionlock.Promote(ctx, &directionlock.RedisStore{Client: target}, "operator"); err != nil {
		t.Fatalf("promote the target: %v", err)
	}
	for _, key := range []string{OffsetKey(12, 7), metaKey(7, "0")} {
		if err := source.Set(ctx, key, `{"replid":"another-task","offset":1}`, 0).Err(); err != nil {
			t.Fatalf("seed %s on the source: %v", key, err)
		}
	}
	before, err := target.HGetAll(ctx, directionlock.RedisKey).Result()
	if err != nil {
		t.Fatalf("read the target's claims: %v", err)
	}

	reconciler := standaloneReconciler(t, source, target, func() int64 {
		head, _ := masterOffset(ctx, source)
		return head
	})
	headOf(t, source)
	for _, pass := range []string{"with the source's claims", "without them"} {
		found, err := reconciler.pass(ctx)
		if err != nil {
			t.Fatalf("pass %s: %v", pass, err)
		}
		if found != 0 {
			t.Errorf("the pass %s found %d differences among this tool's own keys", pass, found)
		}
		after, err := target.HGetAll(ctx, directionlock.RedisKey).Result()
		if err != nil {
			t.Fatalf("read the target's claims: %v", err)
		}
		if !maps.Equal(before, after) {
			t.Errorf("the pass %s rewrote the target's claims:\n  before %v\n  after  %v",
				pass, before, after)
		}
		if err := source.Del(ctx, directionlock.RedisKey).Err(); err != nil {
			t.Fatalf("drop the source's claims: %v", err)
		}
	}
	if n, err := target.Exists(ctx, OffsetKey(12, 7), metaKey(7, "0")).Result(); err != nil || n != 0 {
		t.Errorf("the source's own marker or position was copied to the target (%d, %v)", n, err)
	}
}
