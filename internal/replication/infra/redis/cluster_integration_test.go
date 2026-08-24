//go:build integration

package redis

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// The same claim, against a cluster that really has more than one node.
//
// The single-node tests exercise the position arithmetic but not the reason it
// exists: on one node a transaction spanning slots simply works, so the per-slot
// grouping was never really under test. Here the slots are spread over three
// masters, a transaction reaching across them is refused by the server, and the
// source's stream arrives on three separate connections with a buffer and a
// position each.

const clusterTaskID = 2

func sourceCluster(t *testing.T) goredis.UniversalClient {
	return redisAt(t, addrsFrom(t, "SYNC_REDIS_SOURCE_CLUSTER"))
}

func targetCluster(t *testing.T) goredis.UniversalClient {
	return redisAt(t, addrsFrom(t, "SYNC_REDIS_TARGET_CLUSTER"))
}

// TestACrossSlotTransactionIsRefusedByTheServer is the premise, checked rather
// than assumed.
//
// If a cluster allowed one transaction to span slots, none of the per-slot
// machinery would be needed: a batch could be committed with its position in a
// single unit, exactly as it is for MySQL and MongoDB. It does not, and this says
// so — so that if a future version relaxes it, the reason for the complexity is
// on record and can be removed deliberately.
//
// The commands go straight to one node, unrouted. Asking the cluster client to do
// it proves nothing, for the reason the next test explains.
func TestACrossSlotTransactionIsRefusedByTheServer(t *testing.T) {
	addrs := addrsFrom(t, "SYNC_REDIS_TARGET_CLUSTER")
	node := goredis.NewClient(&goredis.Options{Addr: addrs[0]})
	defer node.Close()
	ctx := context.Background()

	owned, err := node.ClusterSlots(ctx).Result()
	if err != nil {
		t.Fatalf("ClusterSlots: %v", err)
	}
	var mine []int
	for _, slot := range owned {
		if len(slot.Nodes) > 0 && slot.Nodes[0].Addr == addrs[0] {
			mine = append(mine, int(slot.Start), int(slot.End))
		}
	}
	if len(mine) < 2 || mine[0] == mine[1] {
		t.Skipf("node %s does not own two distinct slots", addrs[0])
	}
	a := "{" + SlotTags()[mine[0]] + "}:a"
	b := "{" + SlotTags()[mine[1]] + "}:b"

	if err := node.Do(ctx, "MULTI").Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	node.Do(ctx, "SET", a, 1)
	queued := node.Do(ctx, "SET", b, 1).Err()
	execed := node.Do(ctx, "EXEC").Err()

	if queued == nil && execed == nil {
		t.Fatalf("a transaction spanning slots %d and %d succeeded on one node. If "+
			"that is really allowed now, the per-slot position could be replaced by "+
			"one position for the whole batch — but check very carefully first.",
			mine[0], mine[1])
	}
	t.Logf("the server refuses it, as the design assumes: queue=%v exec=%v", queued, execed)
	node.Do(ctx, "DISCARD")
}

// TestAClusterClientSplitsACrossSlotTransactionSilently records the trap that
// makes the explicit per-slot grouping necessary.
//
// Handing every slot's commands to one TxPipeline looks tidier and appears to
// work: the client sorts them by slot and sends a separate MULTI to each node.
// So there is no error, and no atomicity across slots either — the convenience
// hides exactly the property being relied on. The applier therefore builds one
// transaction per slot itself, and does not depend on a client library's
// grouping staying what it is today.
func TestAClusterClientSplitsACrossSlotTransactionSilently(t *testing.T) {
	client := targetCluster(t)
	ctx := context.Background()

	a, b := "cross:a", "cross:b"
	if SlotOf([]byte(a)) == SlotOf([]byte(b)) {
		t.Fatalf("%s and %s share a slot, so this proves nothing", a, b)
	}
	defer client.Del(ctx, a, b)

	tx := client.TxPipeline()
	tx.Set(ctx, a, 1, 0)
	tx.Set(ctx, b, 1, 0)
	if _, err := tx.Exec(ctx); err != nil {
		t.Skipf("the client refused a cross-slot transaction (%v); it no longer "+
			"splits them, and the applier's own grouping is simply belt and braces", err)
	}
	t.Log("the cluster client accepted a cross-slot transaction by splitting it " +
		"into one per slot — no error, and no atomicity across them. This is why " +
		"the applier groups by slot itself.")
}

// TestTheTargetClusterTakesPerSlotTransactions covers the mechanism directly:
// a slot's data and its marker, committed together, on whichever of three
// masters owns that slot.
func TestTheTargetClusterTakesPerSlotTransactions(t *testing.T) {
	client := targetCluster(t)
	ctx := context.Background()

	for _, slot := range []int{0, 5461, 10922, 16383} {
		key := fmt.Sprintf("{%s}:payload", SlotTags()[slot])
		tx := client.TxPipeline()
		tx.Set(ctx, key, "value", 0)
		marker := tx.Set(ctx, OffsetKey(slot, 99), "12345", 0)
		if _, err := tx.Exec(ctx); err != nil {
			t.Fatalf("slot %d refused a transaction holding its data and its marker: %v",
				slot, err)
		}
		if err := marker.Err(); err != nil {
			t.Fatalf("slot %d marker: %v", slot, err)
		}
		got, err := client.Get(ctx, OffsetKey(slot, 99)).Result()
		if err != nil || got != "12345" {
			t.Fatalf("slot %d marker reads %q (%v), want 12345", slot, got, err)
		}
		client.Del(ctx, key, OffsetKey(slot, 99))
	}
}

// TestAClusterIsReplicatedExactlyOnceAcrossCrashes is the single-node crash test
// again, with the slots spread over three masters on each side.
func TestAClusterIsReplicatedExactlyOnceAcrossCrashes(t *testing.T) {
	source, target := sourceCluster(t), targetCluster(t)
	emptyBoth(t, source, target)
	widenBacklogs(t, source)

	ctx := context.Background()
	commands, err := loadCommandTable(ctx, target)
	if err != nil {
		t.Fatalf("loadCommandTable: %v", err)
	}

	root := t.TempDir()
	for i := 0; i < 60; i++ {
		if err := source.Set(ctx, fmt.Sprintf("seed:%d", i), i, 0).Err(); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	warmCtx, stopWarm := context.WithCancel(ctx)
	warm := newRig(t, source, target, root, commands, clusterTaskID, "")
	warmDone := warm.run(warmCtx)
	if len(warm.runners) < 2 {
		stopWarm()
		warm.stop()
		t.Fatalf("the source reported %d shard(s); this test needs a real cluster",
			len(warm.runners))
	}
	warm.reachCommandPhase(t, source, target, clusterTaskID, 40*time.Second)
	stopWarm()
	warm.wait(t, warmDone, 20*time.Second)
	warm.stop()

	stopWorkload := workload(ctx, source, 60)

	const crashes = 12
	random := rand.New(rand.NewSource(11))
	skipped := 0
	for attempt := 0; attempt < crashes; attempt++ {
		runCtx, kill := context.WithCancel(ctx)
		current := newRig(t, source, target, root, commands, clusterTaskID, "")
		done := current.run(runCtx)

		time.Sleep(time.Duration(120+random.Intn(200)) * time.Millisecond)
		kill()

		for _, err := range current.wait(t, done, 20*time.Second) {
			if domain.IsUnrecoverable(err) {
				t.Fatalf("attempt %d stopped for good: %v", attempt, err)
			}
		}
		skipped += current.skipped()
		current.stop()
	}
	written := stopWorkload()
	t.Logf("%d commands written across %d crashes on %d shards",
		written, crashes, len(warm.runners))

	runCtx, stopRun := context.WithCancel(ctx)
	final := newRig(t, source, target, root, commands, clusterTaskID, "")
	done := final.run(runCtx)

	same, difference := converge(t, source, target, 60*time.Second, func() bool {
		select {
		case err := <-done:
			t.Logf("a shard stopped while catching up: %v", err)
			return true
		default:
			return false
		}
	})
	stopRun()
	skipped += final.skipped()
	final.stop()

	if !same {
		t.Fatalf("the two clusters differ after %d crashes:\n%s", crashes, difference)
	}
	// Whether a crash produces a replay depends on where it landed: a kill
	// between batches leaves the floor exactly where the markers are, and there
	// is nothing to re-read. So this is reported rather than required, and the
	// replay itself is tested deliberately below.
	t.Logf("%d commands skipped as already applied", skipped)
}

// TestReplayingAnAppliedRangeChangesNothing tests the property directly instead
// of hoping a crash produces it.
//
// The resume floor is written after each batch on a best-effort basis: losing
// that write costs a longer replay next time and nothing else, which is the whole
// reason it is allowed to fail. So rewinding it by hand is not an artificial
// scenario — it is the scenario, arranged on purpose rather than waited for.
//
// The commands in the replayed range are INCR, RPUSH and ZINCRBY. If the skip
// does not work, every one of them lands a second time and the two sides differ.
func TestReplayingAnAppliedRangeChangesNothing(t *testing.T) {
	source, target := sourceCluster(t), targetCluster(t)
	emptyBoth(t, source, target)
	widenBacklogs(t, source)

	ctx := context.Background()
	commands, err := loadCommandTable(ctx, target)
	if err != nil {
		t.Fatalf("loadCommandTable: %v", err)
	}

	root := t.TempDir()
	for i := 0; i < 30; i++ {
		if err := source.Set(ctx, fmt.Sprintf("seed:%d", i), i, 0).Err(); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	runCtx, stop := context.WithCancel(ctx)
	rig := newRig(t, source, target, root, commands, clusterTaskID, "")
	done := rig.run(runCtx)
	rig.reachCommandPhase(t, source, target, clusterTaskID, 40*time.Second)

	const rounds = 40
	for i := 0; i < rounds; i++ {
		for k := 0; k < 5; k++ {
			source.Incr(ctx, fmt.Sprintf("replay:count:%d", k))
			source.RPush(ctx, fmt.Sprintf("replay:queue:%d", k), fmt.Sprintf("job-%d", i))
			source.ZIncrBy(ctx, fmt.Sprintf("replay:score:%d", k), 1, "member")
		}
	}

	if same, difference := converge(t, source, target, 40*time.Second, nil); !same {
		stop()
		rig.stop()
		t.Fatalf("the two sides had not converged before the replay was arranged:\n%s",
			difference)
	}

	// Stop, rewind every shard's floor, and let it read the range again.
	stop()
	rig.wait(t, done, 20*time.Second)
	rig.stop()

	for i, shard := range rig.shards {
		key := metaKey(clusterTaskID, shard)
		payload, err := target.Get(ctx, key).Result()
		if err != nil {
			t.Fatalf("read the position of shard %s: %v", shard, err)
		}
		position, err := decodePosition(payload)
		if err != nil {
			t.Fatalf("decode the position of shard %s: %v", shard, err)
		}
		// Back to the start of what is still on disk, which is as far as a lost
		// floor could ever put it. Any further and the buffer would refuse, which
		// is a different behaviour with its own test.
		oldest := rig.links[i].buffer.Oldest()
		if oldest >= position.Offset {
			t.Fatalf("shard %s holds nothing before offset %d, so there is nothing "+
				"to re-read", shard, position.Offset)
		}
		position.Offset = oldest
		encoded, err := position.encode()
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		if err := target.Set(ctx, key, encoded, 0).Err(); err != nil {
			t.Fatalf("rewind the position of shard %s: %v", shard, err)
		}
	}
	t.Logf("rewound the resume floor of %d shard(s)", len(rig.shards))

	replayCtx, stopReplay := context.WithCancel(ctx)
	replay := newRig(t, source, target, root, commands, clusterTaskID, "")
	replayDone := replay.run(replayCtx)

	// Give it time to read the whole range again.
	time.Sleep(6 * time.Second)
	stopReplay()
	for _, err := range replay.wait(t, replayDone, 20*time.Second) {
		if domain.IsUnrecoverable(err) {
			t.Fatalf("a shard refused to re-read the range: %v", err)
		}
	}
	skipped := replay.skipped()
	replay.stop()

	// The comparison first: it is the property, and a failure here is the one
	// that says what actually went wrong.
	if same, difference := compare(t, source, target); !same {
		t.Fatalf("re-reading an already applied range changed the target:\n%s", difference)
	}
	if skipped == 0 {
		t.Fatal("nothing was skipped after the floor was rewound, so the range was " +
			"not actually re-read and this proves nothing")
	}
	t.Logf("re-read a range and skipped %d commands; the two sides are still identical",
		skipped)
}

// TestTheComparisonFindsAndFixesADifference covers the backstop.
//
// It is the one thing in this package that does not share the assumptions of the
// replication path, so it is what would catch a case nobody thought of. That
// makes it worth testing directly rather than trusting it to be exercised by the
// crash tests, where by construction there is nothing for it to find.
func TestTheComparisonFindsAndFixesADifference(t *testing.T) {
	source, target := sourceCluster(t), targetCluster(t)
	emptyBoth(t, source, target)
	ctx := context.Background()

	// Four shapes: one that agrees, one that differs, one the target lacks, and
	// one only the target has.
	for _, seed := range []struct {
		client     goredis.UniversalClient
		key, value string
	}{
		{source, "recon:right", "same"},
		{target, "recon:right", "same"},
		{source, "recon:wrong", "source-value"},
		{target, "recon:wrong", "stale-value"},
		{source, "recon:missing", "only-on-source"},
		{target, "recon:ghost", "only-on-target"},
	} {
		if err := seed.client.Set(ctx, seed.key, seed.value, 0).Err(); err != nil {
			t.Fatalf("seed %s: %v", seed.key, err)
		}
	}

	quiet := logrus.New()
	quiet.SetLevel(logrus.ErrorLevel)
	shards, err := shardsOf(ctx, source, "")
	if err != nil {
		t.Fatalf("find the source's shards: %v", err)
	}

	total := 0
	for _, sh := range shards {
		node := goredis.NewClient(&goredis.Options{Addr: sh.addr})
		reconciler := &Reconciler{
			Node: node, Source: source, Target: target, Shard: sh.id,
			Repair: true, Settle: 200 * time.Millisecond, Logger: quiet,
			Labels: metrics.Labels{"task": "recon", "shard": sh.id},
		}
		found, err := reconciler.pass(ctx)
		node.Close()
		if err != nil {
			t.Fatalf("shard %s: %v", sh.id, err)
		}
		total += found
	}

	if total < 3 {
		t.Errorf("the comparison found %d differences, want at least 3 (a wrong "+
			"value, a missing key and a ghost)", total)
	}
	for key, want := range map[string]string{
		"recon:right":   "same",
		"recon:wrong":   "source-value",
		"recon:missing": "only-on-source",
	} {
		got, err := target.Get(ctx, key).Result()
		if err != nil {
			t.Errorf("%s was not repaired onto the target: %v", key, err)
			continue
		}
		if got != want {
			t.Errorf("%s reads %q on the target, want %q", key, got, want)
		}
	}
	if _, err := target.Get(ctx, "recon:ghost").Result(); err != goredis.Nil {
		t.Errorf("recon:ghost is still on the target (%v); a key the source does "+
			"not have is the difference nothing else looks for", err)
	}
}
