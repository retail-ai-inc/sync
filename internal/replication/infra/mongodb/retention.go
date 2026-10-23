package mongodb

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Window is how far back the oplog reaches. It is measured rather than
// configured: the first and last entries of local.oplog.rs, whose difference
// is the real history a resume token has to land inside.
func (r *Reader) Window(ctx context.Context) (time.Duration, error) {
	if r.Config.RetentionWindow > 0 {
		return r.Config.RetentionWindow, nil
	}
	if r.Client == nil {
		return 0, fmt.Errorf("no client, so the oplog cannot be measured")
	}

	// A router has no oplog of its own: the history lives on the shards behind
	// it, and each keeps its own. Measuring through mongos used to fail here and
	// publish nothing, which left the engine most likely to run its log short
	// -- a sharded source under a copy's own write load -- with no window at
	// all.
	sharded, err := isMongos(ctx, r.Client)
	if err != nil {
		return 0, fmt.Errorf("ask the source whether it is a router: %w", err)
	}
	if sharded {
		return r.shardedWindow(ctx)
	}

	return oplogWindow(ctx, r.Client)
}

// shardedWindow is the shortest window any shard holds.
//
// The shortest one is the answer: a change stream over a sharded cluster reads
// every shard, so the position is lost as soon as the first of them rolls past
// it. An average would describe a cluster that does not exist.
func (r *Reader) shardedWindow(ctx context.Context) (time.Duration, error) {
	hosts, err := shardHosts(ctx, r.Client, r.Config.SourceConnection)
	if err != nil {
		return 0, err
	}
	if len(hosts) == 0 {
		return 0, fmt.Errorf("the router lists no shards, so there is no oplog to " +
			"measure; set the task's retention window instead")
	}

	shortest := time.Duration(0)
	measured := 0
	var failures []string

	for _, shard := range hosts {
		client, connErr := mongo.Connect(options.Client().ApplyURI(shard.uri))
		if connErr != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", shard.name, connErr))
			continue
		}
		window, windowErr := oplogWindow(ctx, client)
		_ = client.Disconnect(ctx)
		if windowErr != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", shard.name, windowErr))
			continue
		}
		measured++
		if shortest == 0 || window < shortest {
			shortest = window
		}
	}

	if measured == 0 {
		return 0, fmt.Errorf("no shard's oplog could be measured (%s); set the "+
			"task's retention window instead", strings.Join(failures, "; "))
	}
	if len(failures) > 0 {
		// Reported rather than fatal: the shortest of the shards that answered is
		// still an upper bound on how long this task may stay stopped, and a
		// number covering most of the cluster beats none at all.
		r.warnf("[MongoDB] Could not measure every shard's oplog, so the "+
			"window is the shortest of %d: %s", measured, strings.Join(failures, "; "))
	}
	return shortest, nil
}

// shardHosts reads the shards a router knows about.
type shardHost struct {
	name string
	uri  string
}

func shardHosts(ctx context.Context, client *mongo.Client, source string) ([]shardHost, error) {
	cursor, err := client.Database("config").Collection("shards").Find(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("list the cluster's shards: %w", err)
	}
	defer cursor.Close(ctx)

	var docs []struct {
		ID   string `bson:"_id"`
		Host string `bson:"host"`
	}
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("read the cluster's shards: %w", err)
	}

	hosts := make([]shardHost, 0, len(docs))
	for _, doc := range docs {
		uri := shardURI(doc.Host, source)
		if uri == "" {
			continue
		}
		hosts = append(hosts, shardHost{name: doc.ID, uri: uri})
	}
	return hosts, nil
}

// warnf logs without requiring the caller to have set a logger.
func (r *Reader) warnf(format string, args ...interface{}) {
	if r.Logger != nil {
		r.Logger.Warnf(format, args...)
	}
}

// shardURI addresses one shard directly, borrowing the credentials the task
// already uses for the router.
//
// config.shards spells a shard as "setName/host:port,host:port". The set name
// has to travel as replicaSet: without it the driver treats the hosts as seeds
// of an unnamed set and will happily read from a secondary, whose oplog is not
// the one the change stream is reading.
func shardURI(host, source string) string {
	name, members, found := strings.Cut(host, "/")
	if !found {
		name, members = "", host
	}
	if members == "" {
		return ""
	}

	credentials := ""
	if parsed, err := url.Parse(source); err == nil && parsed.User != nil {
		credentials = parsed.User.String() + "@"
	}

	uri := "mongodb://" + credentials + members + "/?"
	if name != "" {
		uri += "replicaSet=" + url.QueryEscape(name) + "&"
	}
	// The oplog is read from the member the stream reads from, and reading it
	// from a secondary would measure a different history.
	return uri + "readPreference=primary&directConnection=false"
}

// oplogWindow measures one replica set's oplog: the difference between its
// oldest and newest entry.
func oplogWindow(ctx context.Context, client *mongo.Client) (time.Duration, error) {
	first, err := oplogEdgeOf(ctx, client, 1)
	if err != nil {
		return 0, err
	}
	last, err := oplogEdgeOf(ctx, client, -1)
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
	return oplogEdgeOf(ctx, r.Client, direction)
}

func oplogEdgeOf(ctx context.Context, client *mongo.Client, direction int) (bson.Timestamp, error) {
	var entry struct {
		TS bson.Timestamp `bson:"ts"`
	}
	err := client.Database("local").Collection("oplog.rs").
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
