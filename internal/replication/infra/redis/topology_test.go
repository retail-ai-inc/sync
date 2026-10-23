package redis

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/sirupsen/logrus"
)

// TestAFailoverIsReportedAsAMovedMaster covers the harmless change: the slots
// stayed where they were, so the keys did too, and the position still applies.
func TestAFailoverIsReportedAsAMovedMaster(t *testing.T) {
	before := map[string]string{"0-5460": "a:6379", "5461-10922": "b:6379"}
	after := map[string]string{"0-5460": "c:6379", "5461-10922": "b:6379"}

	got := describe(before, after)
	if !strings.Contains(got, "0-5460 moved from a:6379 to c:6379") {
		t.Errorf("describe = %q, want the moved master named", got)
	}
	if strings.Contains(got, "5461-10922") {
		t.Errorf("describe = %q, want the untouched shard left out", got)
	}
}

// Slots moving between shards is what deletes keys from one master and restores
// them on another, down two connections with no ordering between them.
func TestAReshardIsReportedAsNewAndDepartedShards(t *testing.T) {
	before := map[string]string{"0-8191": "a:6379", "8192-16383": "b:6379"}
	after := map[string]string{
		"0-5460": "a:6379", "5461-10922": "b:6379", "10923-16383": "c:6379",
	}

	got := describe(before, after)
	for _, want := range []string{"0-8191", "8192-16383", "0-5460", "10923-16383"} {
		if !strings.Contains(got, want) {
			t.Errorf("describe = %q, want it to mention %s", got, want)
		}
	}
}

// TestNoChangeIsReportedAsNothing keeps the watcher from crying wolf every time
// it looks.
func TestNoChangeIsReportedAsNothing(t *testing.T) {
	shape := map[string]string{"0-5460": "a:6379", "5461-16383": "b:6379"}
	if got := describe(shape, shape); got != "" {
		t.Errorf("describe of an unchanged shape = %q, want nothing", got)
	}
}

// The rig builds its shards from a seed the test supplies, so every existing
// case passes a clean host:port and none of them exercised what production
// passes.
func TestASingleServerSourceIsDialledWithoutItsDatabase(t *testing.T) {
	s := &Syncer{cfg: config.SyncConfig{
		SourceConnection: "redis://10.105.174.243:6379/0",
	}}
	if got := s.sourceAddr(); got != "10.105.174.243:6379" {
		t.Fatalf("sourceAddr = %q, want 10.105.174.243:6379", got)
	}

	shards, err := shardsOf(context.Background(), goredis.NewClient(
		&goredis.Options{Addr: s.sourceAddr()}), s.sourceAddr())
	if err != nil {
		t.Fatalf("shardsOf: %v", err)
	}
	if len(shards) != 1 {
		t.Fatalf("shards = %d, want 1", len(shards))
	}
	if _, _, err := net.SplitHostPort(shards[0].addr); err != nil {
		t.Fatalf("shard address %q is not dialable: %v", shards[0].addr, err)
	}
}

// A failure means a master owning two ranges gets two replica links, so every write on it is applied twice.
func TestAMasterOwningTwoRangesIsStreamedOnce(t *testing.T) {
	ctx := context.Background()
	cluster := clusterAnswering(t, []goredis.ClusterSlot{
		{Start: 0, End: 100, Nodes: []goredis.ClusterNode{{Addr: "10.0.0.1:6379"}, {Addr: "10.0.0.11:6379"}}},
		{Start: 101, End: 5460, Nodes: []goredis.ClusterNode{{Addr: "10.0.0.2:6379"}, {Addr: "10.0.0.12:6379"}}},
		{Start: 5461, End: 10922, Nodes: []goredis.ClusterNode{{Addr: "10.0.0.1:6379"}, {Addr: "10.0.0.11:6379"}}},
		{Start: 10923, End: 16383, Nodes: []goredis.ClusterNode{{Addr: "10.0.0.3:6379"}, {Addr: "10.0.0.13:6379"}}},
	})

	shards, err := shardsOf(ctx, cluster, "")
	if err != nil {
		t.Fatalf("shardsOf: %v", err)
	}
	streamed := make(map[string]string, len(shards))
	for _, sh := range shards {
		if other, twice := streamed[sh.addr]; twice {
			t.Fatalf("%s is streamed by shards %s and %s, so every write on it is "+
				"applied twice", sh.addr, other, sh.id)
		}
		streamed[sh.addr] = sh.id
	}
	for addr, want := range map[string]string{
		"10.0.0.1:6379": "0-100,5461-10922",
		"10.0.0.2:6379": "101-5460",
		"10.0.0.3:6379": "10923-16383",
	} {
		if got := streamed[addr]; got != want {
			t.Errorf("the shard of %s is named %q, want %q: its stored position is "+
				"found by that name", addr, got, want)
		}
	}

	shape, err := ownership(ctx, cluster)
	if err != nil {
		t.Fatalf("ownership: %v", err)
	}
	if added, removed := rangesMoved(shapeOf(shards), shape); added != "" || removed != "" {
		t.Errorf("the cluster the shards were built from reads as a reshard: %s%s", added, removed)
	}

	owned, err := (&Reconciler{Source: cluster, Shard: streamed["10.0.0.1:6379"]}).ownedSlots(ctx)
	if err != nil {
		t.Fatalf("ownedSlots: %v", err)
	}
	if !owned[0] || !owned[100] || !owned[5461] || !owned[10922] || owned[101] || owned[10923] {
		t.Errorf("the reconciler of 0-100,5461-10922 owns %d slots, not both ranges and nothing else",
			len(owned))
	}
}

// clusterAnswering is a cluster client that answers CLUSTER SLOTS with slots and dials nothing for it.
func clusterAnswering(t *testing.T, slots []goredis.ClusterSlot) *goredis.ClusterClient {
	t.Helper()
	cluster := goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: []string{"127.0.0.1:1"}})
	cluster.AddHook(slotsReply(slots))
	t.Cleanup(func() { cluster.Close() })
	return cluster
}

type slotsReply []goredis.ClusterSlot

func (r slotsReply) DialHook(next goredis.DialHook) goredis.DialHook { return next }

func (r slotsReply) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return next
}

func (r slotsReply) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		if reply, ok := cmd.(*goredis.ClusterSlotsCmd); ok {
			reply.SetVal(r)
			return nil
		}
		return next(ctx, cmd)
	}
}

// TestTheSourceOffsetIsReadFromInfoReplication covers the number the lag alarm
// should be built on.
func TestTheSourceOffsetIsReadFromInfoReplication(t *testing.T) {
	// Not a live server: masterOffset only has to read the field out of what
	// INFO returns, and the shape of that reply is what this pins down.
	const reply = "# Replication\r\nrole:master\r\nconnected_slaves:1\r\n" +
		"master_repl_offset:102009797831\r\n"
	got, err := parseMasterOffset(reply)
	if err != nil {
		t.Fatalf("parseMasterOffset: %v", err)
	}
	if got != 102009797831 {
		t.Errorf("offset = %d, want 102009797831", got)
	}
}

func TestASourceThatReportsNoOffsetIsAnError(t *testing.T) {
	if _, err := parseMasterOffset("# Replication\r\nrole:master\r\n"); err == nil {
		t.Fatal("want an error: publishing an unknown lag as zero would hide a stall")
	}
}

// A shard is named by the slots it owns, so a master that failed over to
// another address is the same shard and its stream reconnects on its own.
// Slots moving between shards is a different thing: the readers were built at
// start, so a range that appeared has none, and carrying on would replicate
// part of the cluster and say nothing about the rest.
func TestAFailoverIsNotAReshard(t *testing.T) {
	before := map[string]string{"0-5460": "a:6379", "5461-10922": "b:6379"}
	sameRangesNewAddress := map[string]string{"0-5460": "a2:6379", "5461-10922": "b:6379"}

	if added, removed := rangesMoved(before, sameRangesNewAddress); added != "" || removed != "" {
		t.Errorf("a failover read as a reshard: %q %q", added, removed)
	}
}

func TestASplitRangeIsAReshard(t *testing.T) {
	before := map[string]string{"0-5460": "a:6379"}
	after := map[string]string{"0-2730": "a:6379", "2731-5460": "c:6379"}

	added, removed := rangesMoved(before, after)
	if added == "" {
		t.Error("the ranges that appeared were not reported, so nothing would read them")
	}
	if removed == "" {
		t.Error("the range that is gone was not reported")
	}
	for _, want := range []string{"0-2730", "2731-5460", "0-5460"} {
		if !strings.Contains(added+removed, want) {
			t.Errorf("%q missing from %q %q", want, added, removed)
		}
	}
}

// A confirmed reshard has to stop the task in a way the supervisor restarts.
// Unrecoverable would leave it blocked, and the ranges that appeared would stay
// unreplicated for as long as it took somebody to notice.
func TestAReshardStopsTheTaskWithoutBlockingIt(t *testing.T) {
	before := map[string]string{"0-8191": "10.0.0.1:6379", "8192-16383": "10.0.0.2:6379"}
	after := map[string]string{"0-5461": "10.0.0.1:6379", "5462-10922": "10.0.0.3:6379",
		"10923-16383": "10.0.0.2:6379"}

	added, removed := rangesMoved(before, after)
	if added == "" && removed == "" {
		t.Fatal("a reshard was not detected, so the rest of this proves nothing")
	}

	err := reshardError(added, removed)
	if domain.IsUnrecoverable(err) {
		t.Error("the reshard error is unrecoverable, so the supervisor would block " +
			"the task rather than restart it")
	}
	// The ranges belong in the message: a restart that takes a first copy needs
	// to be traceable to what caused it.
	for _, want := range []string{"5462-10922", "resharded"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the reshard error does not mention %q: %v", want, err)
		}
	}
}

var (
	threeShards = map[string]string{"0-5460": "10.0.0.1:6379",
		"5461-10922": "10.0.0.2:6379", "10923-16383": "10.0.0.3:6379"}
	firstShardSplit = map[string]string{"0-2730": "10.0.0.1:6379",
		"2731-5460": "10.0.0.4:6379", "5461-10922": "10.0.0.2:6379",
		"10923-16383": "10.0.0.3:6379"}
	middleMasterMissing = map[string]string{"0-5460": "10.0.0.1:6379",
		"10923-16383": "10.0.0.3:6379"}
)

// watchShapes runs w with the source answering shapes in order, and reports
// whether Run stopped before asking for one past the last.
func watchShapes(t *testing.T, w *topologyWatcher, shapes ...map[string]string) (stopped bool, err error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	exhausted := make(chan struct{})
	next := 0
	quiet := logrus.New()
	quiet.SetLevel(logrus.ErrorLevel)
	w.Every, w.Logger = time.Millisecond, quiet
	w.shape = func(ctx context.Context) (map[string]string, error) {
		if next < len(shapes) {
			next++
			return shapes[next-1], nil
		}
		close(exhausted)
		<-ctx.Done()
		return nil, ctx.Err()
	}

	result := make(chan error, 1)
	go func() { result <- w.Run(ctx) }()
	select {
	case err := <-result:
		return true, err
	case <-exhausted:
		cancel()
		return false, <-result
	case <-time.After(5 * time.Second):
		t.Fatal("the watcher neither stopped nor read every shape")
		return false, nil
	}
}

func TestAReshardThatHoldsForThreePollsStopsTheTask(t *testing.T) {
	changes := 0
	w := &topologyWatcher{OnChange: func(string) { changes++ }}

	stopped, err := watchShapes(t, w, threeShards,
		firstShardSplit, firstShardSplit, firstShardSplit)
	if !stopped || err == nil {
		t.Fatal("a confirmed reshard left the task running, so slots 2731-5460 have no reader")
	}
	if domain.IsUnrecoverable(err) {
		t.Errorf("the reshard stopped the task as unrecoverable, so nothing restarts it: %v", err)
	}
	if !strings.Contains(err.Error(), "2731-5460") {
		t.Errorf("the reshard error does not name the range that appeared: %v", err)
	}
	if changes != 1 {
		t.Errorf("OnChange ran %d times for one change, want 1", changes)
	}
}

func TestAReshardBeforeTheWatcherStartsStillStopsTheTask(t *testing.T) {
	w := &topologyWatcher{Baseline: threeShards}

	stopped, err := watchShapes(t, w, firstShardSplit, firstShardSplit, firstShardSplit)
	if !stopped || err == nil {
		t.Fatal("a reshard between building the shards and the first poll went unnoticed")
	}
}

func TestAMasterBrieflyMissingDoesNotStopTheTask(t *testing.T) {
	stopped, err := watchShapes(t, &topologyWatcher{}, threeShards,
		middleMasterMissing, threeShards, threeShards)
	if stopped {
		t.Fatalf("a master missing for one poll stopped the task: %v", err)
	}
}

func TestAShapeThatFlapsDoesNotStopTheTask(t *testing.T) {
	stopped, err := watchShapes(t, &topologyWatcher{}, threeShards,
		firstShardSplit, threeShards, firstShardSplit)
	if stopped {
		t.Fatalf("a shape that never held for %d polls stopped the task: %v",
			reshardPollsToConfirm, err)
	}
}
