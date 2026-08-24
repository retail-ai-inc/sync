package mongodb

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Window is how far back the oplog reaches.
//
// It is measured rather than configured: the first and last entries of
// local.oplog.rs, whose difference is the real history a resume token has to
// land inside. The oplog is capped by size, so the window moves with the write
// rate — a busy hour can shorten a month of headroom to a day, which is exactly
// the change worth alerting on and exactly what a configured number would hide.
//
// It cannot be measured through mongos: the local database is not addressable
// there, and the window would in any case differ per shard. A sharded task
// therefore has to be told, and if it is not, no headroom is published.
func (r *Reader) Window(ctx context.Context) (time.Duration, error) {
	if r.Config.RetentionWindow > 0 {
		return r.Config.RetentionWindow, nil
	}
	if r.Client == nil {
		return 0, fmt.Errorf("no client, so the oplog cannot be measured")
	}

	first, err := r.oplogEdge(ctx, 1)
	if err != nil {
		return 0, err
	}
	last, err := r.oplogEdge(ctx, -1)
	if err != nil {
		return 0, err
	}
	if last.T <= first.T {
		return 0, fmt.Errorf("the oplog's first and last entries are not apart in time, " +
			"so its window cannot be measured")
	}
	return time.Duration(last.T-first.T) * time.Second, nil
}

// oplogEdge reads the timestamp of the oldest or newest oplog entry.
//
// Sorting by $natural rather than by ts is what keeps this cheap: the oplog has
// no index, and $natural walks the capped collection from whichever end is
// asked for, so each call reads one document.
func (r *Reader) oplogEdge(ctx context.Context, direction int) (bson.Timestamp, error) {
	var entry struct {
		TS bson.Timestamp `bson:"ts"`
	}
	err := r.Client.Database("local").Collection("oplog.rs").
		FindOne(ctx, bson.M{}, options.FindOne().SetSort(bson.D{{Key: "$natural", Value: direction}})).
		Decode(&entry)
	if err != nil {
		edge := "oldest"
		if direction < 0 {
			edge = "newest"
		}
		return bson.Timestamp{}, fmt.Errorf("read the %s oplog entry: %w. A sharded "+
			"deployment cannot be measured through mongos — set the task's retention "+
			"window to the shortest of its shards' oplog windows", edge, err)
	}
	return entry.TS, nil
}
