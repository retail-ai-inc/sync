//go:build integration

package redis

import (
	"context"
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

	applier := &Applier{
		Target:    target,
		Positions: &Checkpoints{Target: target, TaskID: taskID, Shard: "0"},
	}
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
	if marker != "100" {
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
