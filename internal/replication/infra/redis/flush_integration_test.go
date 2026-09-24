//go:build integration

package redis

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/directionlock"
)

// A flush is the one command that destroys data on the target rather than
// adding to it, and the batch it arrives in has to be split around it: writes
// before it are erased, writes after it are kept. Nothing exercised that path.

const flushTaskID = 8821

func seedAndReplicate(t *testing.T, source, target goredis.UniversalClient,
	taskID int, seed string) (*rig, context.CancelFunc, chan error) {

	t.Helper()
	ctx := context.Background()
	commands, err := loadCommandTable(ctx, target)
	if err != nil {
		t.Fatalf("loadCommandTable: %v", err)
	}
	for i := 0; i < 20; i++ {
		if err := source.Set(ctx, fmt.Sprintf("pre:%d", i), i, 0).Err(); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	runCtx, stop := context.WithCancel(ctx)
	built := newRig(t, source, target, t.TempDir(), commands, taskID, seed)
	done := built.run(runCtx)
	built.reachCommandPhase(t, source, target, taskID, 40*time.Second)
	return built, stop, done
}

func TestAFlushOnTheSourceEmptiesTheTargetAndKeepsWhatFollows(t *testing.T) {
	source := redisAt(t, addrsFrom(t, "SYNC_REDIS_SOURCE"))
	target := redisAt(t, addrsFrom(t, "SYNC_REDIS_TARGET"))
	defer source.Close()
	defer target.Close()
	emptyBoth(t, source, target)
	widenBacklogs(t, source)

	ctx := context.Background()
	sourceAddrs := addrsFrom(t, "SYNC_REDIS_SOURCE")
	rig, stop, done := seedAndReplicate(t, source, target, flushTaskID, sourceAddrs[0])
	defer rig.stop()

	if same, difference := converge(t, source, target, 40*time.Second, nil); !same {
		stop()
		t.Fatalf("the seed had not replicated before the flush:\n%s", difference)
	}

	// The flush and the writes that follow it go in together, so the batch the
	// applier sees has events on both sides of the flush.
	if err := source.FlushAll(ctx).Err(); err != nil {
		t.Fatalf("flush the source: %v", err)
	}
	for i := 0; i < 10; i++ {
		if err := source.Set(ctx, fmt.Sprintf("post:%d", i), i, 0).Err(); err != nil {
			t.Fatalf("write after the flush: %v", err)
		}
	}

	same, difference := converge(t, source, target, 40*time.Second, nil)
	stop()
	rig.wait(t, done, 20*time.Second)
	if !same {
		t.Fatalf("the target did not follow the source through a flush:\n%s", difference)
	}

	// Said directly, because a comparison that walks both sides would also pass
	// if the flush had been carried too far and taken the new writes with it.
	for i := 0; i < 20; i++ {
		if err := target.Get(ctx, fmt.Sprintf("pre:%d", i)).Err(); err != goredis.Nil {
			t.Fatalf("pre:%d survived the flush on the target (%v), so the target "+
				"still holds data the source has thrown away", i, err)
		}
	}
	for i := 0; i < 10; i++ {
		if err := target.Get(ctx, fmt.Sprintf("post:%d", i)).Err(); err != nil {
			t.Fatalf("post:%d never reached the target (%v), so the flush took the "+
				"writes that came after it", i, err)
		}
	}
}

func TestAFlushDropsTheSlotMarkersItCovers(t *testing.T) {
	target := redisAt(t, addrsFrom(t, "SYNC_REDIS_TARGET"))
	defer target.Close()
	ctx := context.Background()

	applier := &Applier{
		Target:    target,
		Positions: &Checkpoints{Target: target, TaskID: 8822, Shard: "0-16383"},
	}

	// Markers inside the range and one outside it. A flush covers a slot range,
	// and dropping a marker outside it would make another shard replay.
	for _, slot := range []int{0, 5, 100, 900} {
		if err := target.Set(ctx, OffsetKey(slot, 8822), "1", 0).Err(); err != nil {
			t.Fatalf("write a marker: %v", err)
		}
	}
	defer func() {
		for _, slot := range []int{0, 5, 100, 900} {
			target.Del(ctx, OffsetKey(slot, 8822))
		}
	}()

	if err := applier.dropMarkersIn(ctx, 0, 100); err != nil {
		t.Fatalf("dropMarkersIn: %v", err)
	}
	for _, slot := range []int{0, 5, 100} {
		if err := target.Get(ctx, OffsetKey(slot, 8822)).Err(); err != goredis.Nil {
			t.Errorf("the marker of slot %d inside the flushed range survived (%v)", slot, err)
		}
	}
	if err := target.Get(ctx, OffsetKey(900, 8822)).Err(); err != nil {
		t.Errorf("the marker of slot 900, outside the flushed range, was dropped (%v)", err)
	}
}

func TestASlotRangeIsReadOutOfTheShardName(t *testing.T) {
	if start, end, ok := slotRange("0-100"); !ok || start != 0 || end != 100 {
		t.Errorf("slotRange(\"0-100\") = %d, %d, %v", start, end, ok)
	}
	// A single server is named "0" and owns no range, which is what says its
	// flush covers the whole database.
	for _, name := range []string{"0", "", "a-b", "100-0", "-1-5", "0-99999"} {
		if _, _, ok := slotRange(name); ok {
			t.Errorf("slotRange(%q) reported a range", name)
		}
	}
}

func TestAFlushOnAClusterOnlyEmptiesTheShardsSlots(t *testing.T) {
	source, target := sourceCluster(t), targetCluster(t)
	defer source.Close()
	defer target.Close()
	emptyBoth(t, source, target)
	ctx := context.Background()

	cluster, ok := target.(*goredis.ClusterClient)
	if !ok {
		t.Fatal("the target cluster client is not a cluster client")
	}

	// Keys spread over the whole slot space, then a delete of one half of it.
	// There is no command for "flush these slots", so this is the walk that
	// stands in for one, and its bug would be deleting the other shard's data.
	kept, doomed := 0, 0
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("slotrange:%d", i)
		if err := target.Set(ctx, key, i, 0).Err(); err != nil {
			t.Fatalf("seed: %v", err)
		}
		if slot := SlotOf([]byte(key)); slot <= 8191 {
			doomed++
		} else {
			kept++
		}
	}
	if kept == 0 || doomed == 0 {
		t.Fatalf("the seed landed entirely on one side of the split (%d/%d)", doomed, kept)
	}

	if err := deleteSlotRanges(ctx, cluster, slotSpans{{0, 8191}}); err != nil {
		t.Fatalf("deleteSlotRanges: %v", err)
	}

	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("slotrange:%d", i)
		err := target.Get(ctx, key).Err()
		if slot := SlotOf([]byte(key)); slot <= 8191 {
			if err != goredis.Nil {
				t.Fatalf("%s in slot %d survived the range delete (%v)", key, slot, err)
			}
		} else if err != nil {
			t.Fatalf("%s in slot %d was deleted with the other half (%v)", key, slot, err)
		}
	}
}

const flushRestoreTaskID = 8823

// restoringApplier puts back the position and a claim after a flush, as the
// syncer's RestoreState does.
func restoringApplier(t *testing.T, target *goredis.Client, taskID int, floor int64) *Applier {
	t.Helper()
	applier := standaloneApplier(t, nil, target, taskID, floor)
	applier.RestoreState = func(ctx context.Context, pipe goredis.Pipeliner, position string) {
		pipe.Set(ctx, metaKey(taskID, "0"), position, 0)
		pipe.HSet(ctx, directionlock.RedisKey, strconv.Itoa(taskID), "target-claim")
	}
	return applier
}

func restoredOffset(t *testing.T, client goredis.UniversalClient, taskID int) int64 {
	t.Helper()
	offset, err := storedOffset(context.Background(), client, taskID, "0")
	if err != nil {
		t.Fatalf("read the stored position: %v", err)
	}
	return offset
}

// A failure means the flush erases the position and the claim it restores.
func TestAFlushRestoresThePositionInsideItsTransaction(t *testing.T) {
	target := oneConnection(t, addrsFrom(t, "SYNC_REDIS_TARGET")[0])
	emptyOne(t, target)
	ctx := context.Background()

	applier := restoringApplier(t, target, flushRestoreTaskID, 100)
	if err := target.Set(ctx, "flushed:data", 1, 0).Err(); err != nil {
		t.Fatalf("seed the target: %v", err)
	}
	all := streamedFlush(0, 110, "FLUSHALL").Payload.(*flush)
	err := applier.flushTarget(ctx, all, applier.Positions.markersFor(100), storedPosition(200).Payload)
	if err != nil {
		t.Fatalf("flushTarget: %v", err)
	}

	if n, err := target.Exists(ctx, "flushed:data").Result(); err != nil || n != 0 {
		t.Fatalf("the flush never reached the target (%v)", err)
	}
	if got := restoredOffset(t, target, flushRestoreTaskID); got != 110 {
		t.Errorf("the position after the flush names offset %d, want the flush's 110", got)
	}
	claim, err := target.HGet(ctx, directionlock.RedisKey, strconv.Itoa(flushRestoreTaskID)).Result()
	if err != nil || claim != "target-claim" {
		t.Errorf("the claim after the flush reads %q (%v), want it restored", claim, err)
	}
}

// A failure means a flush of one database empties another or moves this task's bookkeeping into it.
func TestAFlushOfAnotherDatabaseRestoresThePositionWhereItLives(t *testing.T) {
	targetAddr := addrsFrom(t, "SYNC_REDIS_TARGET")[0]
	target := oneConnection(t, targetAddr)
	emptyOne(t, target)
	ctx := context.Background()
	home, third := onDB(t, targetAddr, 0), onDB(t, targetAddr, 3)

	applier := restoringApplier(t, target, flushRestoreTaskID, 100)
	marker := OffsetKey(5, flushRestoreTaskID)
	for _, seed := range []*goredis.StatusCmd{
		third.Set(ctx, "flushed:third", 1, 0),
		home.Set(ctx, "kept:home", 1, 0),
		home.Set(ctx, marker, markerValue("h1", 100), 0),
	} {
		if err := seed.Err(); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	flushThird := streamedFlush(3, 110, "FLUSHDB").Payload.(*flush)
	err := applier.flushTarget(ctx, flushThird, applier.Positions.markersFor(100), storedPosition(200).Payload)
	if err != nil {
		t.Fatalf("flushTarget: %v", err)
	}

	if n, err := third.DBSize(ctx).Result(); err != nil || n != 0 {
		t.Errorf("database 3 of the target holds %d keys (%v), want it emptied", n, err)
	}
	if n, err := home.Exists(ctx, "kept:home").Result(); err != nil || n != 1 {
		t.Errorf("the flush of database 3 emptied database 0 (%v)", err)
	}
	if n, err := home.Exists(ctx, marker).Result(); err != nil || n != 0 {
		t.Errorf("the slot marker in the bookkeeping database survived the flush (%v)", err)
	}
	if got := restoredOffset(t, home, flushRestoreTaskID); got != 110 {
		t.Errorf("the position in the bookkeeping database names offset %d, want the flush's 110", got)
	}
	if n, err := third.Exists(ctx, metaKey(flushRestoreTaskID, "0"), directionlock.RedisKey).Result(); err != nil || n != 0 {
		t.Errorf("%d of this task's own keys were written into database 3 (%v)", n, err)
	}
}

// A failure means a write on either side of a flush in the same batch is kept, lost or applied twice.
func TestAFlushInTheMiddleOfABatchKeepsTheStreamOrder(t *testing.T) {
	target := oneConnection(t, addrsFrom(t, "SYNC_REDIS_TARGET")[0])
	emptyOne(t, target)
	ctx := context.Background()

	applier := standaloneApplier(t, nil, target, flushRestoreTaskID, 90)
	runs := [][]*domain.Event{{
		streamed(0, 100, "RPUSH", "mixed:queue", "x"),
		streamed(0, 105, "SET", "mixed:before", "1"),
		streamedFlush(0, 110, "FLUSHALL"),
		streamed(0, 120, "RPUSH", "mixed:queue", "y"),
		streamed(0, 130, "SET", "mixed:after", "1"),
	}}
	marker := OffsetKey(SlotOf([]byte("mixed:queue")), flushRestoreTaskID)

	for _, attempt := range []string{"the batch", "the batch again"} {
		if _, err := applier.Apply(ctx, runs, storedPosition(130)); err != nil {
			t.Fatalf("applying %s: %v", attempt, err)
		}
		if attempt == "the batch" && applier.Skipped() != 0 {
			t.Errorf("%d commands were skipped on the way to being applied at all", applier.Skipped())
		}
		if queue, err := target.LRange(ctx, "mixed:queue", 0, -1).Result(); err != nil ||
			!slices.Equal(queue, []string{"y"}) {
			t.Errorf("after %s the queue holds %v (%v), want only what followed the flush",
				attempt, queue, err)
		}
		if n, err := target.Exists(ctx, "mixed:before").Result(); err != nil || n != 0 {
			t.Errorf("after %s the write before the flush survived it (%v)", attempt, err)
		}
		if n, err := target.Exists(ctx, "mixed:after").Result(); err != nil || n != 1 {
			t.Errorf("after %s the write after the flush is missing (%v)", attempt, err)
		}
		if got, err := target.Get(ctx, marker).Result(); err != nil || got != markerValue("h1", 130) {
			t.Errorf("after %s the queue's slot marker reads %q (%v), want the segment's end",
				attempt, got, err)
		}
		if err := applier.Positions.Refresh(ctx); err != nil {
			t.Fatalf("Refresh: %v", err)
		}
	}
}
