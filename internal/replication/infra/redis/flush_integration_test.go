//go:build integration

package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
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

	if err := deleteSlotRange(ctx, cluster, 0, 8191); err != nil {
		t.Fatalf("deleteSlotRange: %v", err)
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
