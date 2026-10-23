//go:build integration

package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/retail-ai-inc/sync/internal/replication/infra/directionlock"
)

// A failure means a standalone source's other databases, or this tool's own keys, reach the standby wrong.
func TestTheFirstCopyWalksEveryDatabase(t *testing.T) {
	sourceAddr := addrsFrom(t, "SYNC_REDIS_SOURCE")[0]
	targetAddr := addrsFrom(t, "SYNC_REDIS_TARGET")[0]
	source := redisAt(t, []string{sourceAddr})
	target := redisAt(t, []string{targetAddr})
	emptyBoth(t, source, target)
	ctx := context.Background()

	sourceConn := "redis://" + sourceAddr + "/0"
	targetConn := "redis://" + targetAddr + "/0"
	sourceThird := onDB(t, sourceAddr, 3)
	targetThird := onDB(t, targetAddr, 3)
	sourceFifth := onDB(t, sourceAddr, 5)
	targetFifth := onDB(t, targetAddr, 5)
	t.Cleanup(func() {
		for _, db := range []*goredis.Client{sourceThird, targetThird, sourceFifth, targetFifth} {
			_ = db.FlushDB(context.Background()).Err()
		}
	})

	if err := source.Set(ctx, "a", "0", 0).Err(); err != nil {
		t.Fatalf("seed database 0: %v", err)
	}
	if err := sourceThird.Set(ctx, "b", "3", 0).Err(); err != nil {
		t.Fatalf("seed database 3: %v", err)
	}
	claimOn(t, source, 7, directionlock.RoleSource)
	for _, key := range []string{metaKey(7, "0"), OffsetKey(12, 7)} {
		if err := sourceFifth.Set(ctx, key, `{"replid":"another-task","offset":1}`, 0).Err(); err != nil {
			t.Fatalf("seed %s on the source: %v", key, err)
		}
	}

	copier := &Snapshotter{
		Link: &link{shard: "0"}, Node: source, Source: source, Target: target,
		SourceConn: sourceConn, TargetConn: targetConn,
	}
	if err := copier.Copy(ctx); err != nil {
		t.Fatalf("Copy: %v", err)
	}

	if n, err := onDB(t, targetAddr, 0).Exists(ctx, directionlock.RedisKey).Result(); err != nil || n != 0 {
		t.Errorf("target database 0 holds the source's direction lock (%d, %v)", n, err)
	}
	for _, c := range []struct {
		db   *goredis.Client
		name int
		key  string
		want string
	}{
		{onDB(t, targetAddr, 0), 0, "a", "0"},
		{targetThird, 3, "b", "3"},
	} {
		if got, err := c.db.Get(ctx, c.key).Result(); err != nil || got != c.want {
			t.Errorf("target database %d: %s = %q (%v), want %q", c.name, c.key, got, err, c.want)
		}
		keys, err := c.db.Keys(ctx, "*").Result()
		if err != nil {
			t.Fatalf("list target database %d: %v", c.name, err)
		}
		if len(keys) != 1 || keys[0] != c.key {
			t.Errorf("target database %d holds %q, want only %q", c.name, keys, c.key)
		}
	}
	if n, err := targetFifth.DBSize(ctx).Result(); err != nil || n != 0 {
		t.Errorf("target database 5 holds %d keys (%v), want none of the source's own", n, err)
	}
}

// refuseRestore leaves Exec returning nil and puts the error on one RESTORE
// only, so only the per-command check can catch it.
type refuseRestore struct{ key string }

func (refuseRestore) DialHook(next goredis.DialHook) goredis.DialHook          { return next }
func (refuseRestore) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook { return next }

func (h refuseRestore) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		err := next(ctx, cmds)
		for _, cmd := range cmds {
			args := cmd.Args()
			if cmd.Name() == "restore" && len(args) > 1 && fmt.Sprint(args[1]) == h.key {
				cmd.SetErr(errors.New("OOM command not allowed when used memory > 'maxmemory'"))
			}
		}
		return err
	}
}

// A failure means a key the target refused is counted as copied and the position is recorded past it.
func TestARefusedCopiedKeyFailsTheCopy(t *testing.T) {
	sourceAddrs := addrsFrom(t, "SYNC_REDIS_SOURCE")
	source := redisAt(t, sourceAddrs)
	target := redisAt(t, addrsFrom(t, "SYNC_REDIS_TARGET")[:1])
	emptyBoth(t, source, target)
	ctx := context.Background()

	for _, key := range []string{"copy:a", "copy:b", "copy:poison", "copy:c", "copy:d"} {
		if err := source.Set(ctx, key, key, 0).Err(); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
	}
	commands, err := loadCommandTable(ctx, target)
	if err != nil {
		t.Fatalf("loadCommandTable: %v", err)
	}
	target.AddHook(refuseRestore{key: "copy:poison"})

	const taskID = 8851
	built := newRig(t, source, target, t.TempDir(), commands, taskID, sourceAddrs[0])
	defer built.stop()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := built.run(runCtx)

	position := metaKey(taskID, built.shards[0])
	recorded := func() bool {
		n, err := target.Exists(ctx, position).Result()
		return err != nil || n != 0
	}
	var runErr error
	deadline := time.Now().Add(30 * time.Second)
	for stopped := false; !stopped; {
		select {
		case runErr = <-done:
			stopped = true
		case <-time.After(50 * time.Millisecond):
			if recorded() {
				t.Fatal("a position was recorded for a copy that lost a key")
			}
			if time.Now().After(deadline) {
				t.Fatal("the runner did not stop after the target refused a copied key")
			}
		}
	}
	if runErr == nil || !strings.Contains(runErr.Error(), "write a copied key") {
		t.Fatalf("the runner returned %v, want the refused write", runErr)
	}
	if recorded() {
		t.Error("a position was recorded for a copy that lost a key")
	}
}

// A failure means a batch the target never took is counted as copied.
func TestATargetThatCannotBeWrittenFailsTheCopy(t *testing.T) {
	source := redisAt(t, addrsFrom(t, "SYNC_REDIS_SOURCE"))
	target := redisAt(t, addrsFrom(t, "SYNC_REDIS_TARGET"))
	emptyBoth(t, source, target)
	ctx := context.Background()
	if err := source.Set(ctx, "copy:a", "a", 0).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}

	dead := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	defer dead.Close()
	copier := &Snapshotter{Link: &link{shard: "0"}, Source: source, Target: dead}
	err := copier.Copy(ctx)
	if err == nil || !strings.Contains(err.Error(), "write 1 copied keys") {
		t.Fatalf("Copy returned %v, want the failed write", err)
	}
}
