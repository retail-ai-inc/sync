//go:build integration

package redis

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/app/pipeline"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// The same claim, against a cluster that really has more than one node.
//
// Everything up to here ran against a single node owning all sixteen thousand
// slots, which is enough to exercise the position arithmetic but not the reason
// it exists: on one node a transaction spanning slots would simply work, so the
// per-slot grouping was never really under test. Here the slots are spread over
// three masters, and a transaction that reaches across them is refused by the
// server — which is the constraint the whole design is shaped around.
//
// It also puts the source's stream on three separate connections, one per shard,
// each with its own buffer and its own position.

func clusterAddrs(t *testing.T, variable string) []string {
	t.Helper()
	value := os.Getenv(variable)
	if value == "" {
		t.Skipf("set %s to a comma-separated list of cluster node addresses", variable)
	}
	return strings.Split(value, ",")
}

func clusterClient(t *testing.T, addrs []string) *goredis.ClusterClient {
	t.Helper()
	client := goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: addrs})
	t.Cleanup(func() { client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("ping %v: %v", addrs, err)
	}
	return client
}

// emptyCluster clears every master of a cluster.
func emptyCluster(t *testing.T, client *goredis.ClusterClient) {
	t.Helper()
	if os.Getenv("SYNC_REDIS_ALLOW_FLUSH") != "1" {
		t.Skip("set SYNC_REDIS_ALLOW_FLUSH=1 to let this test empty the clusters")
	}
	err := client.ForEachMaster(context.Background(),
		func(ctx context.Context, node *goredis.Client) error {
			return node.FlushAll(ctx).Err()
		})
	if err != nil {
		t.Fatalf("empty the cluster: %v", err)
	}
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
	addrs := clusterAddrs(t, "SYNC_REDIS_TARGET_CLUSTER")
	node := goredis.NewClient(&goredis.Options{Addr: addrs[0]})
	defer node.Close()
	ctx := context.Background()

	// Two slots this node owns, far enough apart to be different slots.
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
	client := clusterClient(t, clusterAddrs(t, "SYNC_REDIS_TARGET_CLUSTER"))
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
	client := clusterClient(t, clusterAddrs(t, "SYNC_REDIS_TARGET_CLUSTER"))
	ctx := context.Background()

	// Slots spread far enough apart to land on different masters.
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

// clusterRig assembles one pipeline per source shard, the way the syncer does.
type clusterRig struct {
	runners  []*pipeline.Runner
	links    []*link
	appliers []*Applier
}

func buildClusterRig(t *testing.T, source, target *goredis.ClusterClient, root string,
	commands *commandTable) *clusterRig {
	t.Helper()

	ctx := context.Background()
	slots, err := source.ClusterSlots(ctx).Result()
	if err != nil {
		t.Fatalf("ClusterSlots: %v", err)
	}
	quiet := logrus.New()
	quiet.SetLevel(logrus.ErrorLevel)

	rig := &clusterRig{}
	seen := map[string]bool{}
	for _, slot := range slots {
		if len(slot.Nodes) == 0 {
			continue
		}
		id := fmt.Sprintf("%d-%d", slot.Start, slot.End)
		if seen[id] {
			continue
		}
		seen[id] = true

		buffer, err := OpenBuffer(BufferOptions{
			Dir:          fmt.Sprintf("%s/%s", root, id),
			SegmentBytes: 1 << 20,
		})
		if err != nil {
			t.Fatalf("OpenBuffer: %v", err)
		}
		labels := metrics.Labels{"task": "cluster", "shard": id}
		connection := &link{
			opts:   StreamOptions{Addr: slot.Nodes[0].Addr, IdleTimeout: 20 * time.Second},
			buffer: buffer,
			shard:  id,
			logger: quiet,
			labels: labels,
		}
		positions := &Checkpoints{Target: target, TaskID: 2, Shard: id}
		applier := &Applier{
			Target: target, Source: source, Positions: positions,
			Commands: commands, Logger: quiet, Labels: labels,
		}

		rig.links = append(rig.links, connection)
		rig.appliers = append(rig.appliers, applier)
		rig.runners = append(rig.runners, &pipeline.Runner{
			Reader: &Reader{
				Shard: id, Link: connection, Target: target,
				Commands: commands, Logger: quiet, Labels: labels,
			},
			Applier: applier,
			Snapshotter: &Snapshotter{
				Link: connection, Node: goredis.NewClient(&goredis.Options{
					Addr: slot.Nodes[0].Addr,
				}),
				Source: source, Target: target,
				Logger: quiet, Labels: labels,
			},
			Checkpoints:   positions,
			CheckpointKey: id,
			Opts: pipeline.Options{
				Limits:        pipeline.Limits{MaxEvents: 200},
				FlushInterval: 20 * time.Millisecond,
				StreamOrder:   true,
				Engine:        "Redis",
				Logger:        quiet,
				Labels:        labels,
			},
		})
	}
	if len(rig.runners) < 2 {
		t.Fatalf("the source reported %d shard(s); this test needs a real cluster",
			len(rig.runners))
	}
	return rig
}

func (r *clusterRig) run(ctx context.Context) chan error {
	done := make(chan error, len(r.runners))
	for _, runner := range r.runners {
		runner := runner
		go func() { done <- runner.Run(ctx) }()
	}
	return done
}

func (r *clusterRig) stop() {
	for i, connection := range r.links {
		connection.close()
		_ = r.links[i].buffer.Close()
	}
}

func (r *clusterRig) skipped() int {
	total := 0
	for _, applier := range r.appliers {
		total += applier.Skipped()
	}
	return total
}

// TestAClusterIsReplicatedExactlyOnceAcrossCrashes is the single-node crash test
// again, with the slots spread over three masters on each side.
func TestAClusterIsReplicatedExactlyOnceAcrossCrashes(t *testing.T) {
	source := clusterClient(t, clusterAddrs(t, "SYNC_REDIS_SOURCE_CLUSTER"))
	target := clusterClient(t, clusterAddrs(t, "SYNC_REDIS_TARGET_CLUSTER"))
	emptyCluster(t, source)
	emptyCluster(t, target)

	ctx := context.Background()
	err := source.ForEachMaster(ctx, func(ctx context.Context, node *goredis.Client) error {
		if err := node.ConfigSet(ctx, "repl-backlog-size", "67108864").Err(); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("widen the backlogs: %v", err)
	}

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

	// Reach the command phase on every shard before disturbing anything.
	warmCtx, stopWarm := context.WithCancel(ctx)
	warm := buildClusterRig(t, source, target, root, commands)
	warmDone := warm.run(warmCtx)

	deadline := time.Now().Add(40 * time.Second)
	for round := 0; ; round++ {
		if time.Now().After(deadline) {
			stopWarm()
			warm.stop()
			t.Fatal("not every shard reached the command phase")
		}
		if err := source.Set(ctx, fmt.Sprintf("warm:%d", round), round, 0).Err(); err != nil {
			t.Fatalf("warm write: %v", err)
		}
		time.Sleep(250 * time.Millisecond)
		if allInCommandPhase(t, target, warm) {
			break
		}
		select {
		case err := <-warmDone:
			stopWarm()
			warm.stop()
			t.Fatalf("a shard stopped during warm-up: %v", err)
		default:
		}
	}
	stopWarm()
	for range warm.runners {
		<-warmDone
	}
	warm.stop()

	stopWorkload := clusterWorkload(ctx, t, source)

	const crashes = 12
	random := rand.New(rand.NewSource(11))
	skipped := 0
	for attempt := 0; attempt < crashes; attempt++ {
		runCtx, kill := context.WithCancel(ctx)
		current := buildClusterRig(t, source, target, root, commands)
		done := current.run(runCtx)

		time.Sleep(time.Duration(120+random.Intn(200)) * time.Millisecond)
		kill()
		for range current.runners {
			select {
			case err := <-done:
				if domain.IsUnrecoverable(err) {
					t.Fatalf("attempt %d stopped for good: %v", attempt, err)
				}
			case <-time.After(20 * time.Second):
				t.Fatalf("attempt %d did not stop when killed", attempt)
			}
		}
		skipped += current.skipped()
		current.stop()
	}
	written := stopWorkload()
	t.Logf("%d commands written across %d crashes on %d shards",
		written, crashes, len(warm.runners))

	runCtx, stopRun := context.WithCancel(ctx)
	final := buildClusterRig(t, source, target, root, commands)
	done := final.run(runCtx)

	var same bool
	var difference string
	catchUp := time.Now().Add(60 * time.Second)
	for time.Now().Before(catchUp) {
		time.Sleep(500 * time.Millisecond)
		same, difference = compareClusters(t, source, target)
		if same {
			break
		}
		select {
		case err := <-done:
			t.Logf("a shard stopped while catching up: %v", err)
			catchUp = time.Now()
		default:
		}
	}
	stopRun()
	skipped += final.skipped()
	final.stop()

	if !same {
		t.Fatalf("the two clusters differ after %d crashes:\n%s", crashes, difference)
	}
	// Whether a crash produces a replay depends on where it landed: a kill
	// between batches leaves the floor exactly where the markers are, and there
	// is nothing to re-read. So this is reported rather than required, and the
	// replay itself is tested deliberately in
	// TestReplayingAnAppliedRangeChangesNothing.
	t.Logf("%d commands skipped as already applied", skipped)
}

func allInCommandPhase(t *testing.T, target *goredis.ClusterClient, rig *clusterRig) bool {
	t.Helper()
	ctx := context.Background()
	for _, runner := range rig.runners {
		payload, err := target.Get(ctx, metaKey(2, runner.CheckpointKey)).Result()
		if err != nil {
			return false
		}
		position, err := decodePosition(payload)
		if err != nil || position.Phase != phaseCommand {
			return false
		}
	}
	return true
}

func clusterWorkload(ctx context.Context, t *testing.T, client *goredis.ClusterClient) func() int {
	inner, cancel := context.WithCancel(ctx)
	written := make(chan int, 1)

	go func() {
		count := 0
		for i := 0; inner.Err() == nil; i++ {
			// One key at a time: a cluster client cannot pipeline across slots as
			// one request, and the point here is spread rather than throughput.
			for k := 0; k < 6; k++ {
				n := (i*6 + k) % 60
				client.Incr(inner, fmt.Sprintf("count:%d", n))
				client.RPush(inner, fmt.Sprintf("queue:%d", n%12), fmt.Sprintf("job-%d-%d", i, k))
				client.ZIncrBy(inner, fmt.Sprintf("score:%d", n%8), 1, fmt.Sprintf("m%d", n))
				client.Append(inner, fmt.Sprintf("log:%d", n%9), "x")
				count += 4
			}
			time.Sleep(8 * time.Millisecond)
		}
		written <- count
	}()

	return func() int {
		cancel()
		return <-written
	}
}

// compareClusters checks every key of the source cluster against the target.
func compareClusters(t *testing.T, source, target *goredis.ClusterClient) (bool, string) {
	t.Helper()
	ctx := context.Background()

	var keys []string
	err := scanAll(ctx, source, 500, func(page []string) error {
		keys = append(keys, page...)
		return nil
	})
	if err != nil {
		return false, fmt.Sprintf("read the source's keys: %v", err)
	}
	sort.Strings(keys)

	var problems []string
	for _, key := range keys {
		want, err := clusterDigest(ctx, source, key)
		if err != nil {
			return false, fmt.Sprintf("read %s from the source: %v", key, err)
		}
		got, err := clusterDigest(ctx, target, key)
		if err != nil {
			return false, fmt.Sprintf("read %s from the target: %v", key, err)
		}
		if want != got {
			problems = append(problems, fmt.Sprintf("  %s\n    source: %s\n    target: %s",
				key, truncate(want), truncate(got)))
		}
	}

	// Ghosts on the target: records nobody can explain after a failover.
	present := make(map[string]bool, len(keys))
	for _, key := range keys {
		present[key] = true
	}
	err = scanAll(ctx, target, 500, func(page []string) error {
		for _, key := range page {
			if IsOffsetKey(key) || isMetaKey(key) || present[key] {
				continue
			}
			problems = append(problems, "  "+key+" exists only on the target")
		}
		return nil
	})
	if err != nil {
		return false, fmt.Sprintf("read the target's keys: %v", err)
	}

	if len(problems) == 0 {
		return true, ""
	}
	sort.Strings(problems)
	if len(problems) > 12 {
		problems = append(problems[:12], fmt.Sprintf("  ... and %d more", len(problems)-12))
	}
	return false, strings.Join(problems, "\n")
}

func clusterDigest(ctx context.Context, client *goredis.ClusterClient, key string) (string, error) {
	kind, err := client.Type(ctx, key).Result()
	if err == goredis.Nil {
		return "<missing>", nil
	}
	if err != nil {
		return "", err
	}
	switch kind {
	case "none":
		return "<missing>", nil
	case "string":
		value, err := client.Get(ctx, key).Result()
		if err == goredis.Nil {
			return "<missing>", nil
		}
		return "string:" + value, err
	case "list":
		items, err := client.LRange(ctx, key, 0, -1).Result()
		return "list:" + strings.Join(items, ","), err
	case "set":
		items, err := client.SMembers(ctx, key).Result()
		sort.Strings(items)
		return "set:" + strings.Join(items, ","), err
	case "zset":
		items, err := client.ZRangeWithScores(ctx, key, 0, -1).Result()
		if err != nil {
			return "", err
		}
		var parts []string
		for _, item := range items {
			parts = append(parts, fmt.Sprintf("%v=%g", item.Member, item.Score))
		}
		return "zset:" + strings.Join(parts, ","), nil
	case "hash":
		fields, err := client.HGetAll(ctx, key).Result()
		if err != nil {
			return "", err
		}
		names := make([]string, 0, len(fields))
		for name := range fields {
			names = append(names, name)
		}
		sort.Strings(names)
		var parts []string
		for _, name := range names {
			parts = append(parts, name+"="+fields[name])
		}
		return "hash:" + strings.Join(parts, ","), nil
	}
	return kind + ":<not compared>", nil
}

// TestTheComparisonFindsAndFixesADifference covers the backstop.
//
// It is the one thing in this package that does not share the assumptions of the
// replication path, so it is what would catch a case nobody thought of. That
// makes it worth testing directly rather than trusting it to be exercised by the
// crash tests, where by construction there is nothing for it to find.
func TestTheComparisonFindsAndFixesADifference(t *testing.T) {
	source := clusterClient(t, clusterAddrs(t, "SYNC_REDIS_SOURCE_CLUSTER"))
	target := clusterClient(t, clusterAddrs(t, "SYNC_REDIS_TARGET_CLUSTER"))
	emptyCluster(t, source)
	emptyCluster(t, target)
	ctx := context.Background()

	// Three shapes of difference, one of each kind that can happen.
	if err := source.Set(ctx, "recon:right", "same", 0).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := target.Set(ctx, "recon:right", "same", 0).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := source.Set(ctx, "recon:wrong", "source-value", 0).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := target.Set(ctx, "recon:wrong", "stale-value", 0).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := source.Set(ctx, "recon:missing", "only-on-source", 0).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := target.Set(ctx, "recon:ghost", "only-on-target", 0).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// One reconciler per source master, as the syncer runs them.
	slots, err := source.ClusterSlots(ctx).Result()
	if err != nil {
		t.Fatalf("ClusterSlots: %v", err)
	}
	quiet := logrus.New()
	quiet.SetLevel(logrus.ErrorLevel)

	total := 0
	seen := map[string]bool{}
	for _, slot := range slots {
		if len(slot.Nodes) == 0 {
			continue
		}
		id := fmt.Sprintf("%d-%d", slot.Start, slot.End)
		if seen[id] {
			continue
		}
		seen[id] = true

		node := goredis.NewClient(&goredis.Options{Addr: slot.Nodes[0].Addr})
		reconciler := &Reconciler{
			Node: node, Source: source, Target: target, Shard: id,
			Repair: true, Logger: quiet,
			Labels: metrics.Labels{"task": "recon", "shard": id},
		}
		found, err := reconciler.pass(ctx)
		node.Close()
		if err != nil {
			t.Fatalf("shard %s: %v", id, err)
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
	source := clusterClient(t, clusterAddrs(t, "SYNC_REDIS_SOURCE_CLUSTER"))
	target := clusterClient(t, clusterAddrs(t, "SYNC_REDIS_TARGET_CLUSTER"))
	emptyCluster(t, source)
	emptyCluster(t, target)
	ctx := context.Background()

	err := source.ForEachMaster(ctx, func(ctx context.Context, node *goredis.Client) error {
		return node.ConfigSet(ctx, "repl-backlog-size", "67108864").Err()
	})
	if err != nil {
		t.Fatalf("widen the backlogs: %v", err)
	}
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
	rig := buildClusterRig(t, source, target, root, commands)
	done := rig.run(runCtx)

	// Reach the command phase, then write the commands that must not be applied
	// twice.
	deadline := time.Now().Add(40 * time.Second)
	for {
		if time.Now().After(deadline) {
			stop()
			rig.stop()
			t.Fatal("not every shard reached the command phase")
		}
		source.Set(ctx, "kick", time.Now().UnixNano(), 0)
		time.Sleep(250 * time.Millisecond)
		if allInCommandPhase(t, target, rig) {
			break
		}
	}

	const rounds = 40
	for i := 0; i < rounds; i++ {
		for k := 0; k < 5; k++ {
			source.Incr(ctx, fmt.Sprintf("replay:count:%d", k))
			source.RPush(ctx, fmt.Sprintf("replay:queue:%d", k), fmt.Sprintf("job-%d", i))
			source.ZIncrBy(ctx, fmt.Sprintf("replay:score:%d", k), 1, "member")
		}
	}

	// Wait for it all to land.
	settled := time.Now().Add(40 * time.Second)
	for time.Now().Before(settled) {
		time.Sleep(300 * time.Millisecond)
		if same, _ := compareClusters(t, source, target); same {
			break
		}
	}
	before, difference := compareClusters(t, source, target)
	if !before {
		stop()
		rig.stop()
		t.Fatalf("the two sides had not converged before the replay was arranged:\n%s",
			difference)
	}

	// Stop, rewind every shard's floor, and let it read the range again.
	stop()
	for range rig.runners {
		<-done
	}
	rig.stop()

	rewound := 0
	for i, runner := range rig.runners {
		key := metaKey(2, runner.CheckpointKey)
		payload, err := target.Get(ctx, key).Result()
		if err != nil {
			t.Fatalf("read the position of shard %s: %v", runner.CheckpointKey, err)
		}
		position, err := decodePosition(payload)
		if err != nil {
			t.Fatalf("decode the position of shard %s: %v", runner.CheckpointKey, err)
		}
		// Back to the start of what is still on disk, which is as far as a lost
		// floor could ever put it. Any further and the buffer would refuse, which
		// is a different behaviour with its own test.
		oldest := rig.links[i].buffer.Oldest()
		if oldest >= position.Offset {
			t.Fatalf("shard %s holds nothing before offset %d, so there is nothing "+
				"to re-read", runner.CheckpointKey, position.Offset)
		}
		position.Offset = oldest
		encoded, err := position.encode()
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		if err := target.Set(ctx, key, encoded, 0).Err(); err != nil {
			t.Fatalf("rewind the position of shard %s: %v", runner.CheckpointKey, err)
		}
		rewound++
	}
	t.Logf("rewound the resume floor of %d shard(s)", rewound)

	replayCtx, stopReplay := context.WithCancel(ctx)
	replay := buildClusterRig(t, source, target, root, commands)
	replayDone := replay.run(replayCtx)

	// Give it time to read the whole range again.
	time.Sleep(6 * time.Second)
	stopReplay()
	for range replay.runners {
		if err := <-replayDone; err != nil && !domain.IsUnrecoverable(err) {
			continue
		} else if err != nil {
			t.Fatalf("a shard refused to re-read the range: %v", err)
		}
	}
	skipped := replay.skipped()
	replay.stop()

	// The comparison first: it is the property, and a failure here is the one
	// that says what actually went wrong.
	after, difference := compareClusters(t, source, target)
	if !after {
		t.Fatalf("re-reading an already applied range changed the target:\n%s", difference)
	}
	if skipped == 0 {
		t.Fatal("nothing was skipped after the floor was rewound, so the range was " +
			"not actually re-read and this proves nothing")
	}
	t.Logf("re-read a range and skipped %d commands; the two sides are still identical",
		skipped)
}
