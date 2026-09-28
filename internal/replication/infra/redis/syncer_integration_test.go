//go:build integration

package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/directionlock"
)

// claimOf reads the claim a task holds on one database of an endpoint.
func claimOf(t *testing.T, client goredis.UniversalClient, taskID int) (directionlock.Claim, bool) {
	t.Helper()
	claims, err := (&directionlock.RedisStore{Client: client}).Claims(context.Background())
	if err != nil {
		t.Fatalf("read the claims: %v", err)
	}
	for _, claim := range claims {
		if claim.TaskID == taskID {
			return claim, true
		}
	}
	return directionlock.Claim{}, false
}

// untilTrue polls a condition, failing the test if it does not hold in time.
func untilTrue(t *testing.T, within time.Duration, what string, done <-chan error,
	condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !condition() {
		select {
		case err := <-done:
			t.Fatalf("Start returned %v before %s", err, what)
		case <-time.After(50 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen within %s", what, within)
		}
	}
}

// A failure means the task's own wiring -- claims, database numbers, bookkeeping, shutdown -- is wrong where no rig can see it.
func TestTheSyncerReplicatesThroughStart(t *testing.T) {
	sourceAddr := addrsFrom(t, "SYNC_REDIS_SOURCE")[0]
	targetAddr := addrsFrom(t, "SYNC_REDIS_TARGET")[0]
	source := redisAt(t, []string{sourceAddr})
	target := redisAt(t, []string{targetAddr})
	emptyBoth(t, source, target)
	ctx := context.Background()

	const taskID = 8861
	sourceDB := map[int]*goredis.Client{0: onDB(t, sourceAddr, 0), 2: onDB(t, sourceAddr, 2)}
	targetDB := map[int]*goredis.Client{
		0: onDB(t, targetAddr, 0), 2: onDB(t, targetAddr, 2), 3: onDB(t, targetAddr, 3),
	}
	t.Cleanup(func() {
		for _, db := range []*goredis.Client{sourceDB[2], targetDB[2], targetDB[3]} {
			_ = db.FlushDB(context.Background()).Err()
		}
	})
	for db, client := range sourceDB {
		if err := client.Set(ctx, "copied", db, 0).Err(); err != nil {
			t.Fatalf("seed source database %d: %v", db, err)
		}
	}
	for i := 0; i < 4; i++ {
		if err := sourceDB[2].Set(ctx, fmt.Sprintf("slows-the-copy:%d", i), i, 0).Err(); err != nil {
			t.Fatalf("seed source database 2 to slow its copy: %v", err)
		}
	}

	quiet := logrus.New()
	quiet.SetLevel(logrus.ErrorLevel)
	syncer := NewSyncer(config.SyncConfig{
		ID:                  taskID,
		Type:                "redis",
		SourceConnection:    "redis://" + sourceAddr + "/2",
		TargetConnection:    "redis://" + targetAddr + "/3",
		RedisBufferDir:      t.TempDir(),
		RedisBatchWindow:    20 * time.Millisecond,
		RedisSourceReadRate: 1,
	}, quiet)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- syncer.Start(runCtx) }()

	arrived := func(key string) func() bool {
		return func() bool {
			for db, client := range targetDB {
				if db == 3 {
					continue
				}
				got, err := client.Get(ctx, key).Int()
				if err != nil || got != db {
					return false
				}
			}
			return true
		}
	}
	untilTrue(t, 30*time.Second, "the first copy of database 0", done, func() bool {
		got, err := targetDB[0].Get(ctx, "copied").Int()
		return err == nil && got == 0
	})
	if err := sourceDB[0].Set(ctx, "during", 0, 0).Err(); err != nil {
		t.Fatalf("write to source database 0 during the copy: %v", err)
	}
	untilTrue(t, 30*time.Second, "the first copy of databases 0 and 2", done, arrived("copied"))
	untilTrue(t, 30*time.Second, "the position being recorded", done, func() bool {
		n, _ := targetDB[3].Exists(ctx, metaKey(taskID, "0")).Result()
		return n == 1
	})
	for db, client := range sourceDB {
		if err := client.Set(ctx, "streamed", db, 0).Err(); err != nil {
			t.Fatalf("write to source database %d: %v", db, err)
		}
	}
	untilTrue(t, 30*time.Second, "the streamed writes to databases 0 and 2", done, arrived("streamed"))
	if got, err := targetDB[0].Get(ctx, "during").Int(); err != nil || got != 0 {
		t.Errorf("a write made during the copy reads %d (%v) on target database 0", got, err)
	}

	for _, db := range []int{0, 2} {
		if n, _ := targetDB[db].Exists(ctx, metaKey(taskID, "0")).Result(); n != 0 {
			t.Errorf("the position is in target database %d, not the one the connection names", db)
		}
		if _, held := claimOf(t, targetDB[db], taskID); held {
			t.Errorf("target database %d holds a claim, which belongs in database 3 only", db)
		}
	}
	for _, key := range []string{"copied", "streamed"} {
		if n, _ := targetDB[3].Exists(ctx, key).Result(); n != 0 {
			t.Errorf("target database 3 holds %s, which no source database 3 has", key)
		}
	}
	if claim, held := claimOf(t, targetDB[3], taskID); !held || claim.Role != directionlock.RoleTarget {
		t.Errorf("target database 3 holds %+v (held %v), want this task's target claim", claim, held)
	}
	if claim, held := claimOf(t, sourceDB[2], taskID); !held || claim.Role != directionlock.RoleSource {
		t.Errorf("source database 2 holds %+v (held %v), want this task's source claim", claim, held)
	}

	cancel()
	select {
	case err := <-done:
		if domain.IsUnrecoverable(err) {
			t.Errorf("a cancelled task reported it cannot carry on: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return after its context was cancelled")
	}
	if _, held := claimOf(t, targetDB[3], taskID); held {
		t.Error("the target claim outlived the task")
	}
	if _, held := claimOf(t, sourceDB[2], taskID); held {
		t.Error("the source claim outlived the task")
	}
}
