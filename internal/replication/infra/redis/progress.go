package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	goredis "github.com/redis/go-redis/v9"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	intRedis "github.com/retail-ai-inc/sync/internal/platform/dbconn/redis"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// How far the target has been written, read from the target rather than from a
// running task. The whole point is to be answerable when nothing is running.

// Progress reports, for each shard, where the source is and what the target
// holds.
//
// Redis is the one engine where the answer is exact: a position is a byte count
// in the master's replication stream, both sides report it as a number, and the
// difference is how many bytes Osaka has not seen.
func Progress(ctx context.Context, cfg config.SyncConfig) (domain.Progress, error) {
	// The target first, and it is the only one that may fail the request: this
	// endpoint is asked when Tokyo is gone, and the answer it exists to give --
	// what has Osaka durably applied -- is written on the target for exactly
	// that reason. A source that cannot be reached leaves the comparison
	// unavailable, not the whole report.
	target, err := intRedis.GetRedisClient(cfg.TargetConnection)
	if err != nil {
		return domain.Progress{}, fmt.Errorf("connect to the target: %w", err)
	}
	defer target.Close()

	source, sourceErr := intRedis.GetRedisClient(cfg.SourceConnection)
	if source != nil {
		defer source.Close()
	}

	shards, shardsErr := shardsFor(ctx, cfg, source, sourceErr)
	if len(shards) == 0 {
		return domain.Progress{}, fmt.Errorf("work out which shards this task covers: %w",
			errors.Join(sourceErr, shardsErr))
	}

	username, password := credentials(cfg.SourceConnection)
	// The same TLS settings the replication link uses: a rediss:// source needs
	// them here too, or the switch-over report cannot reach the shard it is
	// reporting on.
	sourceTLS, tlsErr := intRedis.TLSFor(cfg.SourceConnection)
	if tlsErr != nil {
		return domain.Progress{}, fmt.Errorf("read the source's TLS settings: %w", tlsErr)
	}

	unreachable := errors.Join(sourceErr, shardsErr)
	report := domain.Progress{Engine: "redis"}
	for _, sh := range shards {
		var node goredis.UniversalClient
		if unreachable == nil {
			// A plain connection to the shard's master. The task's own connection
			// to it is a replica link, which takes no ordinary commands.
			node = goredis.NewClient(&goredis.Options{
				Addr:      sh.addr,
				Username:  username,
				Password:  password,
				TLSConfig: sourceTLS,
			})
		}
		report.Shards = append(report.Shards,
			shardProgress(ctx, sh, node, target, cfg.ID, unreachable))
		if node != nil {
			_ = node.Close()
		}
	}
	return report, nil
}

// shardsFor works out which streams this task covers, falling back to what the
// target remembers when the source cannot be asked.
//
// The addresses are only needed to read the source's head, which is exactly
// what is unavailable in that case, so a shard with no address still carries
// the applied position -- which is the half worth having.
func shardsFor(ctx context.Context, cfg config.SyncConfig,
	source goredis.UniversalClient, sourceErr error) ([]shard, error) {

	if sourceErr == nil {
		// dsn.HostPort, not the DSN: Options.Addr is dialled as a tcp address,
		// so a redis:// URL here made every standalone task report nothing.
		shards, err := shardsOf(ctx, source, dsn.HostPort("redis", cfg.SourceConnection))
		if err == nil {
			return shards, nil
		}
		sourceErr = err
	}

	stored, err := storedShards(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return stored, sourceErr
}

func shardProgress(ctx context.Context, sh shard, node, target goredis.UniversalClient,
	taskID int, unreachable error) domain.ShardProgress {

	progress := domain.ShardProgress{Shard: sh.id}

	// The target first. It is the side that survives the outage this question
	// is asked during, and reading it does not depend on the source being
	// there: answering "the source would not say where its stream ends" and
	// nothing else made the endpoint useless in the one case it exists for.
	stored, storedErr := storedOffset(ctx, target, taskID, sh.id)
	if storedErr != nil {
		progress.Note = fmt.Sprintf("the target holds no readable position: %v", storedErr)
		return progress
	}
	progress.Applied = strconv.FormatInt(stored, 10)

	if unreachable != nil || node == nil {
		progress.Note = fmt.Sprintf("the source could not be reached, so this is what "+
			"the target has applied and not how far behind it is: %v", unreachable)
		return progress
	}

	head, headErr := sourceOffset(ctx, node)
	if headErr != nil {
		progress.Note = fmt.Sprintf("the source would not say where its stream ends, "+
			"so this is what the target has applied and not how far behind it is: %v",
			headErr)
		return progress
	}
	progress.Source = strconv.FormatInt(head, 10)

	return compareOffsets(sh.id, head, stored)
}

// compareOffsets orders two offsets in the same replication stream.
//
// A stored offset ahead of the source's own means the source's history was
// replaced -- a restart or a failover gives a new replication id and starts its
// offset from zero -- so what is stored belongs to a stream that no longer
// exists. Reporting "caught up" for that is the one wrong answer: it says Osaka
// holds everything Tokyo had, when what it holds is part of a history Tokyo has
// forgotten.
func compareOffsets(shard string, head, stored int64) domain.ShardProgress {
	progress := domain.ShardProgress{
		Shard:      shard,
		Source:     strconv.FormatInt(head, 10),
		Applied:    strconv.FormatInt(stored, 10),
		Comparable: true,
	}

	switch {
	case stored > head:
		progress.Comparable = false
		progress.BehindBytes = -1
		progress.Note = "the stored position is past the end of the source's stream, so " +
			"it belongs to a history the source no longer has: the source was restarted " +
			"or failed over, and this shard needs copying again"
	case stored == head:
		progress.CaughtUp = true
	default:
		progress.BehindBytes = head - stored
	}
	return progress
}

// sourceOffset reads where the source's replication stream ends.
func sourceOffset(ctx context.Context, node goredis.UniversalClient) (int64, error) {
	info, err := node.Info(ctx, "replication").Result()
	if err != nil {
		return 0, err
	}
	offset := infoNumber(info, "master_repl_offset")
	if offset == 0 {
		return 0, fmt.Errorf("INFO replication reported no master_repl_offset")
	}
	return offset, nil
}

// storedOffset reads the position this task last committed for a shard.
//
// It is the metadata offset and not the per-slot markers: the metadata only
// advances once a batch has landed in every slot it touched, so it is the point
// the whole shard is known to be applied up to. A slot marker further ahead
// belongs to a batch that landed partly.
// storedShards names the streams the target holds a position for.
//
// It is how the report still knows what to report on when the source cannot be
// asked which shards exist: the target was written by this task, one position
// key per shard, so the keys are the task's own record of its shape.
func storedShards(ctx context.Context, cfg config.SyncConfig) ([]shard, error) {
	target, err := intRedis.GetRedisClient(cfg.TargetConnection)
	if err != nil {
		return nil, fmt.Errorf("connect to the target: %w", err)
	}
	defer target.Close()

	prefix := positionKeyPrefix + strconv.Itoa(cfg.ID) + ":"
	var (
		mu    sync.Mutex
		found []shard
	)
	seen := map[string]bool{}

	// ForEachMaster runs this on every master at once.
	scan := func(ctx context.Context, client goredis.UniversalClient) error {
		var cursor uint64
		for {
			keys, next, err := client.Scan(ctx, cursor, prefix+"*", 200).Result()
			if err != nil {
				return err
			}
			mu.Lock()
			for _, key := range keys {
				id := strings.TrimPrefix(key, prefix)
				if id == "" || seen[id] {
					continue
				}
				seen[id] = true
				found = append(found, shard{id: id})
			}
			mu.Unlock()
			if next == 0 {
				return nil
			}
			cursor = next
		}
	}

	if cluster, ok := target.(*goredis.ClusterClient); ok {
		if err := cluster.ForEachMaster(ctx, func(ctx context.Context, node *goredis.Client) error {
			return scan(ctx, node)
		}); err != nil {
			return nil, err
		}
	} else if err := scan(ctx, target); err != nil {
		return nil, err
	}

	if len(found) == 0 {
		return nil, fmt.Errorf("the target holds no position for task %d, so there is "+
			"nothing to report and no source to ask", cfg.ID)
	}
	return found, nil
}

func storedOffset(ctx context.Context, target goredis.UniversalClient,
	taskID int, shardID string) (int64, error) {

	payload, err := target.Get(ctx, metaKey(taskID, shardID)).Result()
	if err == goredis.Nil {
		return 0, fmt.Errorf("nothing has been applied to this target for shard %q", shardID)
	}
	if err != nil {
		return 0, err
	}
	position, err := decodePosition(payload)
	if err != nil {
		return 0, err
	}
	return position.Offset, nil
}

func closeBoth(source, target goredis.UniversalClient) {
	if source != nil {
		_ = source.Close()
	}
	if target != nil {
		_ = target.Close()
	}
}
