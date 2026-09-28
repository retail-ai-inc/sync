package mongodb

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
)

// How far the target has been written, read from the target rather than from a
// running task.

// Progress reports the source's cluster time and the cluster time of the last
// event the target committed.
//
// The comparison is by cluster time and not by resume token. A token is opaque:
// it is exact for resuming and says nothing that can be ordered, so a stored
// token on its own answers "what did we apply" with a hex string. The cluster
// time of the event the token came from is stored beside it for this, and a
// position stored before that was added has no time to compare -- which is
// reported rather than guessed.
func Progress(ctx context.Context, cfg config.SyncConfig) (domain.Progress, error) {
	source, err := mongo.Connect(options.Client().ApplyURI(cfg.SourceConnection))
	if err != nil {
		return domain.Progress{}, fmt.Errorf("connect to the source: %w", err)
	}
	defer func() { _ = source.Disconnect(ctx) }()

	target, err := mongo.Connect(options.Client().ApplyURI(cfg.TargetConnection))
	if err != nil {
		return domain.Progress{}, fmt.Errorf("connect to the target: %w", err)
	}
	defer func() { _ = target.Disconnect(ctx) }()

	store := &checkpoint.MongoStore{
		Database: target.Database(dsn.GetDatabaseName(cfg.Type, cfg.TargetConnection)),
		TaskID:   cfg.ID,
	}
	payload, err := store.Load(ctx, "")
	if err != nil {
		return domain.Progress{}, fmt.Errorf("read the stored position: %w", err)
	}

	head, err := sourceClusterTime(ctx, source)
	if err != nil {
		// The question this answers is asked when Tokyo is gone, and what the
		// target has applied is written on the target for that reason. Failing
		// here discarded the half that survives the outage.
		applied := compareClusterTime(time.Time{}, payload)
		applied.Source = ""
		applied.Comparable = false
		applied.CaughtUp = false
		applied.Note = fmt.Sprintf("the source could not be reached, so this is what "+
			"the target has applied and not how far behind it is: %v", err)
		return domain.Progress{
			Engine: "mongodb",
			Shards: []domain.ShardProgress{applied},
		}, nil
	}

	return domain.Progress{
		Engine: "mongodb",
		Shards: []domain.ShardProgress{compareClusterTime(head, payload)},
	}, nil
}

func compareClusterTime(head time.Time, payload string) domain.ShardProgress {
	progress := domain.ShardProgress{
		Source:      head.UTC().Format(time.RFC3339),
		BehindBytes: -1,
	}

	if payload == "" {
		progress.Note = "nothing has been applied to this target for this task"
		return progress
	}

	var stored streamPosition
	if _, err := checkpoint.Decode(payload, &stored); err != nil {
		progress.Note = fmt.Sprintf("the stored position could not be read: %v", err)
		return progress
	}

	applied, ok := appliedTime(stored)
	if !ok {
		progress.Applied = "a resume token with no cluster time beside it"
		progress.Note = "this position was stored before the cluster time was recorded " +
			"alongside the token. A resume token is opaque, so how far it has got " +
			"cannot be read from it; the next event this task applies records one, " +
			"and until then the lag metric is the only measure"
		return progress
	}

	progress.Applied = applied.UTC().Format(time.RFC3339)
	progress.Comparable = true
	if behind := head.Sub(applied); behind > 0 {
		progress.Note = fmt.Sprintf("the target is %s behind the source's last write",
			behind.Truncate(time.Second))
	}
	// The source's cluster time advances on its own, with no write involved, so
	// an idle source is always a second or two ahead of the last event anybody
	// applied. Caught up is "the target has the source's last event", which is
	// the most the target can be asked for.
	progress.CaughtUp = !applied.Before(head)
	return progress
}

// appliedTime is the cluster time of the last event the target committed: the
// one recorded beside the resume token, or the pinned time the snapshot left
// when the stream has not delivered anything yet.
func appliedTime(stored streamPosition) (time.Time, bool) {
	if stored.At > 0 {
		return time.Unix(stored.At, 0), true
	}
	if stored.Cluster > 0 {
		return time.Unix(int64(stored.Cluster), 0), true
	}
	return time.Time{}, false
}

// sourceClusterTime reads how far the source has been written to.
//
// The source's LAST WRITE, not its clock. $clusterTime is a gossiped logical
// clock that advances on every operation anywhere in the cluster and on the
// periodic no-op, so comparing the last event the target applied against it
// left an idle source permanently "not caught up" -- and "caught up" is what
// somebody waits for before promoting Osaka. lastWrite is what the source has
// actually committed, which is the thing the target can be level with.
func sourceClusterTime(ctx context.Context, source *mongo.Client) (time.Time, error) {
	raw, err := source.Database("admin").
		RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Raw()
	if err != nil {
		return time.Time{}, fmt.Errorf("read the source's cluster time: %w", err)
	}
	if at, ok := lastWriteFrom(raw); ok {
		return time.Unix(int64(at.T), 0), nil
	}
	// A source that reports no lastWrite -- a mongos, which does not carry one
	// -- falls back to the gossiped clock. It is an upper bound: the report then
	// errs towards "behind", which is the safe direction for a promotion.
	at, err := clusterTimeFrom(raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("read the source's cluster time: %w", err)
	}
	return time.Unix(int64(at.T), 0), nil
}

// lastWriteFrom reads the source's last committed write out of a hello reply.
func lastWriteFrom(raw bson.Raw) (bson.Timestamp, bool) {
	for _, path := range [][]string{
		{"lastWrite", "majorityOpTime", "ts"},
		{"lastWrite", "opTime", "ts"},
	} {
		v, err := raw.LookupErr(path...)
		if err != nil {
			continue
		}
		if t, i, ok := v.TimestampOK(); ok {
			return bson.Timestamp{T: t, I: i}, true
		}
	}
	return bson.Timestamp{}, false
}
