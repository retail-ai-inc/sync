//go:build integration

package redis

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/retail-ai-inc/sync/internal/platform/config"
)

// urlFor turns the address the harness was given into the URL the connection
// helper parses.
func urlFor(t *testing.T, variable string) string {
	t.Helper()
	addrs := addrsFrom(t, variable)
	return "redis://" + addrs[0] + "/0"
}

func writeStoredPosition(t *testing.T, target goredis.UniversalClient,
	taskID int, shardID string, offset int64) {

	t.Helper()
	payload, err := json.Marshal(streamPosition{ReplID: "abc", Offset: offset})
	if err != nil {
		t.Fatalf("encode a position: %v", err)
	}
	if err := target.Set(context.Background(), metaKey(taskID, shardID),
		string(payload), 0).Err(); err != nil {
		t.Fatalf("store a position: %v", err)
	}
}

func TestTheStoredOffsetIsWhatWasWrittenDown(t *testing.T) {
	target := redisAt(t, addrsFrom(t, "SYNC_REDIS_TARGET"))
	defer target.Close()
	ctx := context.Background()

	if _, err := storedOffset(ctx, target, 8801, "never-written"); err == nil {
		t.Error("a shard with no stored position reported an offset")
	}

	writeStoredPosition(t, target, 8801, "shard-a", 4096)
	defer target.Del(ctx, metaKey(8801, "shard-a"))

	got, err := storedOffset(ctx, target, 8801, "shard-a")
	if err != nil {
		t.Fatalf("storedOffset: %v", err)
	}
	if got != 4096 {
		t.Errorf("storedOffset = %d, want 4096", got)
	}

	// A position that is not a position has to be an error rather than zero.
	// Zero would read as "nothing applied yet", which is a full re-copy.
	if err := target.Set(ctx, metaKey(8801, "shard-b"), "not json", 0).Err(); err != nil {
		t.Fatalf("store a broken position: %v", err)
	}
	defer target.Del(ctx, metaKey(8801, "shard-b"))
	if _, err := storedOffset(ctx, target, 8801, "shard-b"); err == nil {
		t.Error("an unreadable stored position was reported as an offset")
	}
}

func TestTheSourceOffsetComesFromInfoReplication(t *testing.T) {
	source := redisAt(t, addrsFrom(t, "SYNC_REDIS_SOURCE"))
	defer source.Close()
	ctx := context.Background()

	// A server that has never carried a replica reports master_repl_offset 0,
	// which this refuses rather than passes on: zero would compare as "the
	// source has nothing", and every shard would read as caught up.
	offset, err := sourceOffset(ctx, source)
	if err != nil {
		if !strings.Contains(err.Error(), "master_repl_offset") {
			t.Fatalf("sourceOffset: %v", err)
		}
	} else if offset <= 0 {
		t.Errorf("sourceOffset returned %d with no error", offset)
	}

	dead := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"})
	defer dead.Close()
	if _, err := sourceOffset(ctx, dead); err == nil {
		t.Error("a server that never answered reported an offset")
	}
}

func TestShardProgressNotesWhatItCouldNotRead(t *testing.T) {
	target := redisAt(t, addrsFrom(t, "SYNC_REDIS_TARGET"))
	defer target.Close()
	dead := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"})
	defer dead.Close()
	ctx := context.Background()

	unreachable := shardProgress(ctx, shard{id: "s1"}, dead, target, 8802, nil)
	if unreachable.Note == "" || unreachable.Comparable {
		t.Errorf("a shard whose source would not answer came back comparable: %+v", unreachable)
	}
}

func TestProgressReportsEveryShardOfALiveTask(t *testing.T) {
	sourceURL := urlFor(t, "SYNC_REDIS_SOURCE")
	targetURL := urlFor(t, "SYNC_REDIS_TARGET")

	target := redisAt(t, addrsFrom(t, "SYNC_REDIS_TARGET"))
	defer target.Close()

	cfg := config.SyncConfig{
		ID: 8803, Type: "redis",
		SourceConnection: sourceURL, TargetConnection: targetURL,
	}

	report, err := Progress(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Progress: %v", err)
	}
	if report.Engine != "redis" {
		t.Errorf("engine = %q, want redis", report.Engine)
	}
	if len(report.Shards) == 0 {
		t.Fatal("a standalone source reported no shards, so nothing was measured")
	}
	for _, sh := range report.Shards {
		// Nothing has been applied for this task id, so every shard has to say so
		// rather than report a position it does not have.
		if sh.Applied != "" && sh.Applied != "0" {
			t.Errorf("shard %s claims %q applied for a task that never ran", sh.Shard, sh.Applied)
		}
	}
}

func TestProgressWillNotConnectToNowhere(t *testing.T) {
	cfg := config.SyncConfig{
		ID: 8804, Type: "redis",
		SourceConnection: "redis://127.0.0.1:1/0",
		TargetConnection: "redis://127.0.0.1:1/0",
	}
	// A deadline, because connecting retries with a backoff and this test is
	// about the refusal rather than about waiting out the retries.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := Progress(ctx, cfg); err == nil {
		t.Error("Progress reported on a source that does not exist")
	}
}

func TestClosingBothEndsToleratesAMissingOne(t *testing.T) {
	target := redisAt(t, addrsFrom(t, "SYNC_REDIS_TARGET"))
	closeBoth(nil, target)
	closeBoth(nil, nil)
}

func TestMetaKeyIsPerTaskAndShard(t *testing.T) {
	if metaKey(1, "a") == metaKey(1, "b") || metaKey(1, "a") == metaKey(2, "a") {
		t.Error("two different shards or tasks share a position key")
	}
	if want := "__sync:pos:7:s"; metaKey(7, "s") != want {
		t.Errorf("metaKey = %q, want %q", metaKey(7, "s"), want)
	}
}

// TestProgressStillAnswersWhenTokyoIsGone is the case the endpoint exists for.
//
// "Has Osaka applied everything?" is asked when the source is unreachable --
// that is what a regional outage is. The answer is written on the target for
// exactly that reason, and the report used to throw it away and fail because
// the source could not be dialled.
func TestProgressStillAnswersWhenTokyoIsGone(t *testing.T) {
	target := redisAt(t, addrsFrom(t, "SYNC_REDIS_TARGET"))
	defer target.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const taskID = 8807
	writeStoredPosition(t, target, taskID, "0", 987654)
	defer target.Del(context.Background(), metaKey(taskID, "0"))

	report, err := Progress(ctx, config.SyncConfig{
		ID: taskID, Type: "redis",
		SourceConnection: "redis://127.0.0.1:1/0", // Tokyo is gone
		TargetConnection: urlFor(t, "SYNC_REDIS_TARGET"),
	})
	if err != nil {
		t.Fatalf("Progress refused to answer with the source unreachable: %v", err)
	}
	if len(report.Shards) != 1 {
		t.Fatalf("report covers %d shards, want the one the target holds a position for", len(report.Shards))
	}

	shard := report.Shards[0]
	if shard.Applied != "987654" {
		t.Errorf("Applied = %q, want what the target holds", shard.Applied)
	}
	if shard.CaughtUp {
		t.Error("a shard whose source could not be read is reported as caught up")
	}
	if shard.Comparable {
		t.Error("a shard with no source position is reported as comparable")
	}
	if !strings.Contains(shard.Note, "source") {
		t.Errorf("Note = %q, want it to say the source could not be reached", shard.Note)
	}
}

func TestProgressReadsEveryShardAClusterTargetHoldsWhenTokyoIsGone(t *testing.T) {
	target := targetCluster(t)
	defer target.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const taskID = 8809
	shards := []string{"0-5460", "5461-10922", "10923-16383", "0-100,200-300"}
	for i, id := range shards {
		writeStoredPosition(t, target, taskID, id, int64(1000+i))
		defer target.Del(context.Background(), metaKey(taskID, id))
	}

	report, err := Progress(ctx, config.SyncConfig{
		ID: taskID, Type: "redis",
		SourceConnection: "redis://127.0.0.1:1/0",
		TargetConnection: "redis://" + strings.Join(addrsFrom(t, "SYNC_REDIS_TARGET_CLUSTER"), ",") + "/0",
	})
	if err != nil {
		t.Fatalf("Progress refused to answer with the source unreachable: %v", err)
	}
	if len(report.Shards) != len(shards) {
		t.Fatalf("report covers %d shards, want the %d the target holds positions for",
			len(report.Shards), len(shards))
	}
}

// And the other half: a target that cannot be read is still a failed request,
// because then there is no answer at all.
func TestProgressFailsWhenTheTargetCannotBeRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := Progress(ctx, config.SyncConfig{
		ID: 8808, Type: "redis",
		SourceConnection: urlFor(t, "SYNC_REDIS_SOURCE"),
		TargetConnection: "redis://127.0.0.1:1/0",
	})
	if err == nil {
		t.Error("Progress reported on a target it could not read")
	}
}
