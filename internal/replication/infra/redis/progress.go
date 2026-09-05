package redis

import (
	"context"
	"fmt"
	"strconv"

	goredis "github.com/redis/go-redis/v9"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	intRedis "github.com/retail-ai-inc/sync/internal/platform/dbconn/redis"
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
	syncer := &Syncer{cfg: cfg}
	source, target, err := syncer.connect(ctx)
	if err != nil {
		return domain.Progress{}, err
	}
	defer closeBoth(source, target)

	shards, err := shardsOf(ctx, source, cfg.SourceConnection)
	if err != nil {
		return domain.Progress{}, err
	}

	username, password := credentials(cfg.SourceConnection)
	// The same TLS settings the replication link uses: a rediss:// source needs
	// them here too, or the switch-over report cannot reach the shard it is
	// reporting on.
	sourceTLS, err := intRedis.TLSFor(cfg.SourceConnection)
	if err != nil {
		return domain.Progress{}, fmt.Errorf("read the source's TLS settings: %w", err)
	}

	report := domain.Progress{Engine: "redis"}
	for _, sh := range shards {
		// A plain connection to the shard's master. The task's own connection to
		// it is a replica link, which takes no ordinary commands.
		node := goredis.NewClient(&goredis.Options{
			Addr:      sh.addr,
			Username:  username,
			Password:  password,
			TLSConfig: sourceTLS,
		})
		report.Shards = append(report.Shards,
			shardProgress(ctx, sh, node, target, cfg.ID))
		_ = node.Close()
	}
	return report, nil
}

func shardProgress(ctx context.Context, sh shard, node, target goredis.UniversalClient,
	taskID int) domain.ShardProgress {

	progress := domain.ShardProgress{Shard: sh.id}

	// The source's own offset, which is the end of its stream.
	head, headErr := sourceOffset(ctx, node)
	if headErr != nil {
		progress.Note = fmt.Sprintf("the source would not say where its stream ends: %v", headErr)
		return progress
	}
	progress.Source = strconv.FormatInt(head, 10)

	stored, storedErr := storedOffset(ctx, target, taskID, sh.id)
	if storedErr != nil {
		progress.Note = fmt.Sprintf("the target holds no readable position: %v", storedErr)
		return progress
	}
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
