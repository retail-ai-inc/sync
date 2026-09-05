//go:build integration

package redis

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

func TestTheWindowIsMeasuredFromTheSourcesOwnBacklog(t *testing.T) {
	addrs := addrsFrom(t, "SYNC_REDIS_SOURCE")
	node := goredis.NewClient(&goredis.Options{Addr: addrs[0]})
	defer node.Close()
	ctx := context.Background()

	size, err := backlogBytes(ctx, node)
	if err != nil {
		t.Fatalf("backlogBytes: %v", err)
	}
	if size <= 0 {
		t.Fatalf("the source reports a backlog of %d bytes", size)
	}

	// INFO is asked first and answers on a server that allows CONFIG too, so
	// the two have to agree about the same server.
	fromInfo, err := backlogFromInfo(ctx, node)
	if err != nil {
		t.Fatalf("backlogFromInfo: %v", err)
	}
	if fromInfo != size {
		t.Errorf("INFO reports %d bytes and the reader took %d", fromInfo, size)
	}
}

func TestTheBacklogOfAnUnreachableShardIsNotAWindow(t *testing.T) {
	dead := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"})
	defer dead.Close()
	ctx := context.Background()

	if _, err := backlogFromInfo(ctx, dead); err == nil {
		t.Error("a server that never answered reported a backlog")
	}
	if _, err := backlogBytes(ctx, dead); err == nil {
		t.Error("a server that answers neither INFO nor CONFIG reported a backlog")
	}

	// No connection at all, which is a different failure from a connection that
	// will not answer, and it names the setting that stands in for the answer.
	reader := &Reader{Shard: "0"}
	if _, err := reader.Window(ctx); err == nil {
		t.Error("a reader with no connection reported a retention window")
	}
}

func TestAConfiguredWindowIsUsedWithoutAskingTheSource(t *testing.T) {
	// Nothing to ask: no Node is set, so an answer can only come from the
	// setting.
	reader := &Reader{Shard: "0", Configured: 90 * time.Minute}
	got, err := reader.Window(context.Background())
	if err != nil {
		t.Fatalf("Window: %v", err)
	}
	if got != 90*time.Minute {
		t.Errorf("Window = %v, want the configured 90m", got)
	}
}

func TestTheBacklogIsKeptForAWhileRatherThanAskedEveryTick(t *testing.T) {
	addrs := addrsFrom(t, "SYNC_REDIS_SOURCE")
	node := goredis.NewClient(&goredis.Options{Addr: addrs[0]})
	defer node.Close()

	reader := &Reader{Shard: "0", Node: node}
	first, err := reader.backlog(context.Background())
	if err != nil {
		t.Fatalf("backlog: %v", err)
	}

	// Close the connection: a second call that still answers proves it did not
	// ask again.
	node.Close()
	second, err := reader.backlog(context.Background())
	if err != nil {
		t.Fatalf("the backlog was asked for again rather than kept: %v", err)
	}
	if second != first {
		t.Errorf("the kept backlog reads %d, want %d", second, first)
	}
}

func TestPurgeRemovesEveryPositionATaskWrote(t *testing.T) {
	target := redisAt(t, addrsFrom(t, "SYNC_REDIS_TARGET"))
	defer target.Close()
	ctx := context.Background()

	const taskID = 8831
	checkpoints := &Checkpoints{Target: target, TaskID: taskID, Shard: "0"}
	if err := target.Set(ctx, metaKey(taskID, "0"), `{"replid":"a","offset":5}`, 0).Err(); err != nil {
		t.Fatalf("seed a position: %v", err)
	}
	for _, slot := range []int{0, 1, 16383} {
		if err := target.Set(ctx, OffsetKey(slot, taskID), "5", 0).Err(); err != nil {
			t.Fatalf("seed a marker: %v", err)
		}
	}
	// Another task's marker, which a purge must not take with it.
	if err := target.Set(ctx, OffsetKey(0, taskID+1), "9", 0).Err(); err != nil {
		t.Fatalf("seed another task's marker: %v", err)
	}
	defer target.Del(ctx, OffsetKey(0, taskID+1))

	if err := checkpoints.Purge(ctx); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if err := target.Get(ctx, metaKey(taskID, "0")).Err(); err != goredis.Nil {
		t.Errorf("the stored position survived the purge (%v)", err)
	}
	for _, slot := range []int{0, 1, 16383} {
		if err := target.Get(ctx, OffsetKey(slot, taskID)).Err(); err != goredis.Nil {
			t.Errorf("the marker of slot %d survived the purge (%v)", slot, err)
		}
	}
	if err := target.Get(ctx, OffsetKey(0, taskID+1)).Err(); err != nil {
		t.Errorf("another task's marker was purged with this one (%v)", err)
	}
}

func TestRefreshReReadsTheMarkersFromTheTarget(t *testing.T) {
	target := redisAt(t, addrsFrom(t, "SYNC_REDIS_TARGET"))
	defer target.Close()
	ctx := context.Background()

	const taskID = 8832
	checkpoints := &Checkpoints{Target: target, TaskID: taskID, Shard: "0"}
	defer checkpoints.Purge(ctx)

	// The markers are only read alongside a stored position, so there has to be
	// one for there to be anything to refresh.
	if err := target.Set(ctx, metaKey(taskID, "0"), `{"replid":"a","offset":100}`, 0).Err(); err != nil {
		t.Fatalf("seed a position: %v", err)
	}
	if _, err := checkpoints.Load(ctx, ""); err != nil {
		t.Fatalf("Load: %v", err)
	}
	// Written behind the in-memory copy's back, which is what a second process
	// applying the same task looks like.
	if err := target.Set(ctx, OffsetKey(7, taskID), "4242", 0).Err(); err != nil {
		t.Fatalf("write a marker: %v", err)
	}

	if before := checkpoints.markersFor(100)[7]; before == 4242 {
		t.Fatal("the marker was already in memory, so the refresh proves nothing")
	}
	if err := checkpoints.Refresh(ctx); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if after := checkpoints.markersFor(100)[7]; after != 4242 {
		t.Errorf("after a refresh slot 7 reads %d, want 4242", after)
	}
}
