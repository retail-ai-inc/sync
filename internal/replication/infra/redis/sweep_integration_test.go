//go:build integration

package redis

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"testing"

	goredis "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/directionlock"
)

// Removing what the target holds and the source does not.
//
// This is the destructive half of recovering a position by copying, and it is
// the half a copy cannot do: a copy writes what the source has, so a key the
// source deleted while the task was away survives every copy that follows.
// What it must not remove is anything else -- the keys the source still has,
// this task's own bookkeeping, and on a cluster the slots belonging to another
// shard.

const sweepTaskID = 8831

func sweeper(t *testing.T, source, target goredis.UniversalClient, shard string) *Snapshotter {
	t.Helper()

	quiet := logrus.New()
	quiet.SetLevel(logrus.ErrorLevel)
	return &Snapshotter{
		Link:   &link{shard: shard, logger: quiet},
		Node:   source,
		Source: source,
		Target: target,
		Logger: quiet,
	}
}

func TestTheSweepRemovesOnlyWhatTheSourceNoLongerHas(t *testing.T) {
	source := redisAt(t, addrsFrom(t, "SYNC_REDIS_SOURCE"))
	target := redisAt(t, addrsFrom(t, "SYNC_REDIS_TARGET"))
	defer source.Close()
	defer target.Close()
	emptyBoth(t, source, target)

	ctx := context.Background()

	// What both sides have, which is what a copy has just written.
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("kept:%d", i)
		if err := source.Set(ctx, key, i, 0).Err(); err != nil {
			t.Fatalf("seed the source: %v", err)
		}
		if err := target.Set(ctx, key, i, 0).Err(); err != nil {
			t.Fatalf("seed the target: %v", err)
		}
	}

	// What the source deleted while the task was away. No copy mentions these.
	for i := 0; i < 3; i++ {
		if err := target.Set(ctx, fmt.Sprintf("stale:%d", i), i, 0).Err(); err != nil {
			t.Fatalf("seed the target: %v", err)
		}
	}

	// This task's own bookkeeping, which lives on the target by design and
	// which the source has never heard of.
	marker := OffsetKey(SlotOf([]byte("kept:0")), sweepTaskID)
	position := metaKey(sweepTaskID, "0-16383")
	for _, key := range []string{marker, position} {
		if err := target.Set(ctx, key, "x", 0).Err(); err != nil {
			t.Fatalf("seed the bookkeeping: %v", err)
		}
	}

	if err := sweeper(t, source, target, "0-16383").SweepStale(ctx); err != nil {
		t.Fatalf("SweepStale: %v", err)
	}

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("kept:%d", i)
		if n, err := target.Exists(ctx, key).Result(); err != nil || n != 1 {
			t.Errorf("%s was removed from the target, and the source still has it", key)
		}
	}
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("stale:%d", i)
		if n, err := target.Exists(ctx, key).Result(); err != nil || n != 0 {
			t.Errorf("%s is still on the target, and the source does not have it", key)
		}
	}
	for _, key := range []string{marker, position} {
		if n, err := target.Exists(ctx, key).Result(); err != nil || n != 1 {
			t.Errorf("the sweep removed this task's own %s", key)
		}
	}
}

// A shard sweeps its own slots and no more. Sweeping the whole key space would
// have each shard delete the others' keys, which the source still has.
func TestTheSweepOnAClusterLeavesTheOtherShardsAlone(t *testing.T) {
	source := redisAt(t, addrsFrom(t, "SYNC_REDIS_SOURCE_CLUSTER"))
	target := redisAt(t, addrsFrom(t, "SYNC_REDIS_TARGET_CLUSTER"))
	defer source.Close()
	defer target.Close()
	emptyBoth(t, source, target)

	ctx := context.Background()

	// Two keys the source does not have, in different halves of the slot
	// space, and a shard name that covers only the first. Plus one the source
	// does have, because a source holding nothing is refused outright.
	mine, theirs := keyInSlots(t, 0, 8191), keyInSlots(t, 8192, 16383)
	kept := keyInSlots(t, 0, 8191, mine)
	for _, key := range []string{mine, theirs, kept} {
		if err := target.Set(ctx, key, "x", 0).Err(); err != nil {
			t.Fatalf("seed the target: %v", err)
		}
	}
	if err := source.Set(ctx, kept, "x", 0).Err(); err != nil {
		t.Fatalf("seed the source: %v", err)
	}

	if err := sweeper(t, source, target, "0-8191").SweepStale(ctx); err != nil {
		t.Fatalf("SweepStale: %v", err)
	}

	if n, err := target.Exists(ctx, mine).Result(); err != nil || n != 0 {
		t.Errorf("%s is in this shard's slots and the source does not have it, yet it "+
			"is still on the target", mine)
	}
	if n, err := target.Exists(ctx, theirs).Result(); err != nil || n != 1 {
		t.Errorf("%s belongs to another shard and was removed anyway; that shard's "+
			"source still has it", theirs)
	}
	if n, err := target.Exists(ctx, kept).Result(); err != nil || n != 1 {
		t.Errorf("%s was removed from the target and the source still has it", kept)
	}
}

// A source that reports nothing is as likely to be a connection pointing
// somewhere else as a source that was emptied, and the standby is what is at
// stake: staging has a task whose source reads as empty while its target holds
// five thousand keys, and sweeping that would have destroyed the copy.
func TestTheSweepRefusesASourceThatHoldsNothing(t *testing.T) {
	source := redisAt(t, addrsFrom(t, "SYNC_REDIS_SOURCE"))
	target := redisAt(t, addrsFrom(t, "SYNC_REDIS_TARGET"))
	defer source.Close()
	defer target.Close()
	emptyBoth(t, source, target)

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if err := target.Set(ctx, fmt.Sprintf("standby:%d", i), i, 0).Err(); err != nil {
			t.Fatalf("seed the target: %v", err)
		}
	}

	err := sweeper(t, source, target, "0-16383").SweepStale(ctx)
	if err == nil {
		t.Fatal("the sweep emptied the standby against a source holding nothing")
	}
	if !domain.IsUnrecoverable(err) {
		t.Errorf("the sweep failed with %v, want it reported as needing intervention", err)
	}

	if n, err := target.DBSize(ctx).Result(); err != nil || n != 10 {
		t.Errorf("the target holds %d keys, want the 10 it started with", n)
	}
}

// A standalone server holds up to sixteen databases, and the copy walks every
// one that holds keys. So does the sweep, and it walks the target's databases
// rather than the source's: a database the source has emptied since is one the
// copy would never open, and it is exactly where these keys would be.
func TestTheSweepReachesEveryDatabaseOfAStandaloneTarget(t *testing.T) {
	sourceAddr := addrsFrom(t, "SYNC_REDIS_SOURCE")[0]
	targetAddr := addrsFrom(t, "SYNC_REDIS_TARGET")[0]
	source := redisAt(t, []string{sourceAddr})
	target := redisAt(t, []string{targetAddr})
	defer source.Close()
	defer target.Close()
	emptyBoth(t, source, target)

	ctx := context.Background()
	sourceConn := "redis://" + sourceAddr + "/0"
	targetConn := "redis://" + targetAddr + "/0"

	// Database 3 on the target holds a key the source has, and one it does not.
	// Nothing this task does has ever opened database 3 on the source.
	third, err := clientOnDB(targetConn, 3)
	if err != nil {
		t.Fatalf("open the target on database 3: %v", err)
	}
	defer third.Close()
	sourceThird, err := clientOnDB(sourceConn, 3)
	if err != nil {
		t.Fatalf("open the source on database 3: %v", err)
	}
	defer sourceThird.Close()
	t.Cleanup(func() {
		_ = third.FlushDB(context.Background()).Err()
		_ = sourceThird.FlushDB(context.Background()).Err()
	})

	if err := sourceThird.Set(ctx, "kept:third", 1, 0).Err(); err != nil {
		t.Fatalf("seed the source's third database: %v", err)
	}
	for _, key := range []string{"kept:third", "stale:third"} {
		if err := third.Set(ctx, key, 1, 0).Err(); err != nil {
			t.Fatalf("seed the target's third database: %v", err)
		}
	}

	sweep := sweeper(t, source, target, "standalone")
	sweep.SourceConn = sourceConn
	sweep.TargetConn = targetConn
	if err := sweep.SweepStale(ctx); err != nil {
		t.Fatalf("SweepStale: %v", err)
	}

	if n, err := third.Exists(ctx, "kept:third").Result(); err != nil || n != 1 {
		t.Error("a key the source has in database 3 was removed from the target")
	}
	if n, err := third.Exists(ctx, "stale:third").Result(); err != nil || n != 0 {
		t.Error("a key the source does not have in database 3 is still on the target, " +
			"which is a database the copy never opens")
	}
}

// A failure means the sweep deletes the target's claims whenever they live in a database the source's claims do not.
func TestTheSweepLeavesTheDirectionLockAlone(t *testing.T) {
	sourceAddr := addrsFrom(t, "SYNC_REDIS_SOURCE")[0]
	targetAddr := addrsFrom(t, "SYNC_REDIS_TARGET")[0]
	source := redisAt(t, []string{sourceAddr})
	target := redisAt(t, []string{targetAddr})
	emptyBoth(t, source, target)
	ctx := context.Background()

	targetConn := "redis://" + targetAddr + "/2"
	second, err := clientOnDB(targetConn, 2)
	if err != nil {
		t.Fatalf("open the target on database 2: %v", err)
	}
	defer second.Close()

	claimOn(t, source, 7, directionlock.RoleSource)
	claimOn(t, second, 7, directionlock.RoleTarget)
	if err := directionlock.Promote(ctx, &directionlock.RedisStore{Client: second}, "operator"); err != nil {
		t.Fatalf("promote the target: %v", err)
	}
	if err := second.Set(ctx, "stale:second", 1, 0).Err(); err != nil {
		t.Fatalf("seed the target's second database: %v", err)
	}
	before, err := second.HGetAll(ctx, directionlock.RedisKey).Result()
	if err != nil {
		t.Fatalf("read the target's claims: %v", err)
	}

	sweep := sweeper(t, source, second, "standalone")
	sweep.SourceConn = "redis://" + sourceAddr + "/0"
	sweep.TargetConn = targetConn
	if err := sweep.SweepStale(ctx); err != nil {
		t.Fatalf("SweepStale: %v", err)
	}

	if n, err := second.Exists(ctx, "stale:second").Result(); err != nil || n != 0 {
		t.Fatal("the sweep never reached database 2, so this proves nothing about the claims there")
	}
	after, err := second.HGetAll(ctx, directionlock.RedisKey).Result()
	if err != nil {
		t.Fatalf("read the target's claims: %v", err)
	}
	if !maps.Equal(before, after) {
		t.Errorf("the sweep changed the target's claims:\n  before %v\n  after  %v", before, after)
	}
}

// keyInSlots finds a key whose slot falls in a range and is none of taken, so
// a test can put one on each side of a shard boundary.
func keyInSlots(t *testing.T, start, end int, taken ...string) string {
	t.Helper()

	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("sweep:%d", i)
		if slot := SlotOf([]byte(key)); slot < start || slot > end {
			continue
		}
		if slices.Contains(taken, key) {
			continue
		}
		return key
	}
	t.Fatalf("no key found in slots %d-%d", start, end)
	return ""
}
