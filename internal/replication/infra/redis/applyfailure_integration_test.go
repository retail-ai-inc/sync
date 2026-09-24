//go:build integration

package redis

import (
	"context"
	"slices"
	"strings"
	"testing"

	goredis "github.com/redis/go-redis/v9"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// A command that fails at run time is the one case a retry cannot fix. Redis
// does not roll a transaction back, so the slot marker in the same transaction
// has already moved past the command that failed -- restarting skips it, and
// the difference is permanent and invisible.
//
// go-redis reports the first failing command as the error from Exec, so the
// applier used to return that as an ordinary error and never look at the
// per-command results.
func TestACommandThatFailsAtRunTimeStopsTheTask(t *testing.T) {
	target := redisAt(t, addrsFrom(t, "SYNC_REDIS_TARGET"))
	defer target.Close()
	ctx := context.Background()

	const taskID = 8841
	key := "wrongtype:probe"
	if err := target.Set(ctx, key, "a string", 0).Err(); err != nil {
		t.Fatalf("seed the target: %v", err)
	}
	defer target.Del(ctx, key)

	positions := &Checkpoints{Target: target, TaskID: taskID, Shard: "0"}
	// A position first, so the applier stamps its marker with a history the way
	// it does in production -- the runner loads one before anything is applied.
	if err := positions.Save(ctx, "", `{"replid":"h1","offset":0}`); err != nil {
		t.Fatalf("seed a position: %v", err)
	}
	applier := &Applier{Target: target, Positions: positions}
	defer applier.Positions.Purge(context.Background())

	// RPUSH against a string is WRONGTYPE: the command fails, the transaction
	// carries on, and the marker beside it is set.
	slot := SlotOf([]byte(key))
	job := work{slot: slot, commands: []*command{{
		args:   [][]byte{[]byte("RPUSH"), []byte(key), []byte("x")},
		slot:   slot,
		offset: 100,
	}}}

	err := applier.applySlot(ctx, job, 100)
	if err == nil {
		t.Fatal("a command that failed at run time was reported as success")
	}
	if !domain.IsUnrecoverable(err) {
		t.Fatalf("the failure came back as something a retry would repeat: %v", err)
	}
	if !strings.Contains(err.Error(), "diverged") {
		t.Errorf("the error does not say the target has diverged: %v", err)
	}

	// The point of refusing to retry: the marker moved, so a restart would skip
	// the command rather than apply it.
	marker, markerErr := target.Get(ctx, OffsetKey(slot, taskID)).Result()
	if markerErr != nil {
		t.Fatalf("read the marker: %v", markerErr)
	}
	offset, ours := markerOffset(marker, applier.Positions.ReplID())
	if !ours || offset != 100 {
		t.Errorf("the marker reads %q; the test's premise is that it moved", marker)
	}
}

// A transport failure is the opposite case: the transaction never ran, so
// nothing was applied and nothing moved. That has to stay retryable.
func TestATransactionThatNeverRanIsRetryable(t *testing.T) {
	dead := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"})
	defer dead.Close()

	applier := &Applier{
		Target:    dead,
		Positions: &Checkpoints{Target: dead, TaskID: 8842, Shard: "0"},
	}
	job := work{slot: 0, commands: []*command{{
		args: [][]byte{[]byte("SET"), []byte("k"), []byte("v")}, slot: 0, offset: 1,
	}}}

	err := applier.applySlot(context.Background(), job, 1)
	if err == nil {
		t.Fatal("writing to a server that is not there reported success")
	}
	if domain.IsUnrecoverable(err) {
		t.Errorf("a target that could not be reached was reported as diverged, so "+
			"the task would stop for something a retry fixes: %v", err)
	}
}

// A cancelled context is the task stopping, not the data diverging. Treating it
// as divergence blocked the task every time it was shut down mid-batch, which
// is what the crash test does on purpose.
func TestAStoppedTaskIsNotADivergedTarget(t *testing.T) {
	target := redisAt(t, addrsFrom(t, "SYNC_REDIS_TARGET"))
	defer target.Close()

	applier := &Applier{
		Target:    target,
		Positions: &Checkpoints{Target: target, TaskID: 8843, Shard: "0"},
	}
	defer applier.Positions.Purge(context.Background())

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	job := work{slot: 0, commands: []*command{{
		args: [][]byte{[]byte("SET"), []byte("stopped:probe"), []byte("v")}, slot: 0, offset: 1,
	}}}
	err := applier.applySlot(cancelled, job, 1)
	if err == nil {
		t.Fatal("applying with a cancelled context reported success")
	}
	if domain.IsUnrecoverable(err) {
		t.Errorf("stopping the task was reported as a diverged target, so it would "+
			"never restart: %v", err)
	}
}

func TestServerRefusedTellsTheTwoApart(t *testing.T) {
	if serverRefused(nil) || serverRefused(goredis.Nil) {
		t.Error("a missing key is not a refusal")
	}
	if serverRefused(context.Canceled) || serverRefused(context.DeadlineExceeded) {
		t.Error("a context that ended is not a refusal")
	}

	// A real one from a real server, because what is being told apart is the
	// type the driver gives a server's answer -- not a string.
	target := redisAt(t, addrsFrom(t, "SYNC_REDIS_TARGET"))
	defer target.Close()
	ctx := context.Background()
	key := "refused:probe"
	if err := target.Set(ctx, key, "a string", 0).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}
	defer target.Del(ctx, key)

	refusal := target.LPush(ctx, key, "x").Err()
	if refusal == nil {
		t.Fatal("pushing to a string was accepted, so there is no refusal to test")
	}
	if !serverRefused(refusal) {
		t.Errorf("a server error is a refusal, and it is the case the marker has "+
			"already moved past: %v", refusal)
	}

	// A server that is not there gives the other kind.
	dead := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"})
	defer dead.Close()
	if unreachable := dead.Get(ctx, "k").Err(); serverRefused(unreachable) {
		t.Errorf("a server that never answered was read as a refusal: %v", unreachable)
	}
}

// A failure means a slot's marker or the position lands in a data database, so a restart replays what was applied.
func TestASlotTransactionSpanningDatabasesKeepsItsMarkerInTheBookkeepingDB(t *testing.T) {
	targetAddr := addrsFrom(t, "SYNC_REDIS_TARGET")[0]
	target := oneConnection(t, targetAddr)
	emptyOne(t, target)
	ctx := context.Background()
	home, second := onDB(t, targetAddr, 0), onDB(t, targetAddr, 2)

	const taskID = 8844
	applier := standaloneApplier(t, nil, target, taskID, 90)
	runs := [][]*domain.Event{{
		streamed(2, 100, "RPUSH", "{spans}a", "x"),
		streamed(0, 110, "RPUSH", "{spans}b", "y"),
		streamed(2, 120, "RPUSH", "{spans}c", "z"),
	}}
	placed := func(when string) {
		for _, want := range []struct {
			client *goredis.Client
			db     int
			key    string
			items  []string
		}{
			{second, 2, "{spans}a", []string{"x"}}, {home, 0, "{spans}a", nil},
			{home, 0, "{spans}b", []string{"y"}}, {second, 2, "{spans}b", nil},
			{second, 2, "{spans}c", []string{"z"}}, {home, 0, "{spans}c", nil},
		} {
			got, err := want.client.LRange(ctx, want.key, 0, -1).Result()
			if err != nil || !slices.Equal(got, want.items) {
				t.Errorf("%s: %s in database %d holds %v (%v), want %v",
					when, want.key, want.db, got, err, want.items)
			}
		}
	}

	if _, err := applier.Apply(ctx, runs, storedPosition(120)); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	placed("after the batch")
	marker := OffsetKey(SlotOf([]byte("{spans}a")), taskID)
	if got, err := home.Get(ctx, marker).Result(); err != nil || got != markerValue("h1", 120) {
		t.Errorf("the slot marker reads %q (%v) in the bookkeeping database, want %q",
			got, err, markerValue("h1", 120))
	}
	if n, err := home.Exists(ctx, metaKey(taskID, "0")).Result(); err != nil || n != 1 {
		t.Errorf("the position is not in the bookkeeping database (%v)", err)
	}
	if n, err := second.Exists(ctx, marker, metaKey(taskID, "0")).Result(); err != nil || n != 0 {
		t.Errorf("%d of this task's own keys were written into database 2 (%v)", n, err)
	}

	if err := home.Set(ctx, metaKey(taskID, "0"), storedPosition(90).Payload, 0).Err(); err != nil {
		t.Fatalf("rewind the position: %v", err)
	}
	restarted := &Applier{Target: target, Positions: &Checkpoints{Target: target, TaskID: taskID, Shard: "0"}}
	if _, err := restarted.Positions.Load(ctx, ""); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if _, err := restarted.Apply(ctx, runs, storedPosition(120)); err != nil {
		t.Fatalf("Apply after the restart: %v", err)
	}
	if restarted.Skipped() != 3 {
		t.Errorf("the restart skipped %d of the 3 commands its slot had already applied",
			restarted.Skipped())
	}
	placed("after the restart")
}

// A failure means a transaction the server discarded is taken to have written data or moved the marker.
func TestATransactionTheServerDiscardedMovesNothing(t *testing.T) {
	target := oneConnection(t, addrsFrom(t, "SYNC_REDIS_TARGET")[0])
	emptyOne(t, target)
	ctx := context.Background()

	const taskID = 8845
	applier := standaloneApplier(t, nil, target, taskID, 90)
	slot := SlotOf([]byte("{discarded}k"))
	job := work{slot: slot, commands: []*command{
		{args: asArgs([]string{"SET", "{discarded}k", "v"}), slot: slot, offset: 100},
		{args: asArgs([]string{"SET", "{discarded}k2"}), slot: slot, offset: 100},
	}}

	err := applier.applySlot(ctx, job, 100)
	if err == nil {
		t.Fatal("a transaction the server refused to run was reported as applied")
	}
	if !strings.Contains(err.Error(), "EXECABORT") {
		t.Errorf("the error does not pass on why the server discarded the transaction: %v", err)
	}
	for _, key := range []string{"{discarded}k", OffsetKey(slot, taskID)} {
		if err := target.Get(ctx, key).Err(); err != goredis.Nil {
			t.Errorf("%s is on the target (%v), though the transaction never ran", key, err)
		}
	}
}
