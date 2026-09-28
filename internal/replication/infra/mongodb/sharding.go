package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// A sharded source collection replicated into an unsharded target is not the
// same collection. Every document lands on whichever shard is primary for the
// target database, so the copy has one shard's capacity and one shard's
// throughput where the source had three — and the region it exists to stand in
// for cannot be stood in for.

type shardKey struct {
	Key    bson.Raw
	Unique bool
}

// collectionShardKey reports how a collection is sharded, or nil when it is not
// — which includes the ordinary case of a replica set with no config database.
func collectionShardKey(ctx context.Context, client *mongo.Client, namespace string) (*shardKey, error) {
	var doc struct {
		Key    bson.Raw `bson:"key"`
		Unique bool     `bson:"unique"`
	}
	err := client.Database("config").Collection("collections").
		FindOne(ctx, bson.M{"_id": namespace, "dropped": bson.M{"$ne": true}}).Decode(&doc)
	switch {
	case err == mongo.ErrNoDocuments:
		return nil, nil
	case err != nil:
		return nil, err
	}
	if len(doc.Key) == 0 {
		return nil, nil
	}
	return &shardKey{Key: doc.Key, Unique: doc.Unique}, nil
}

func isMongos(ctx context.Context, client *mongo.Client) (bool, error) {
	var hello struct {
		Msg string `bson:"msg"`
	}
	if err := client.Database("admin").
		RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		return false, err
	}
	return hello.Msg == "isdbgrid", nil
}

type shardingAction int

const (
	// shardingNothingToDo: the source is not sharded, or the target already
	// matches it.
	shardingNothingToDo shardingAction = iota
	// shardingUnavailable: the source is sharded and the target cannot be,
	// which is worth saying out loud because the copy will not have the
	// source's capacity.
	shardingUnavailable
	// shardingApply: the target should be given the source's key.
	shardingApply
	// shardingMismatch: both sides are partitioned, and not the same way.
	shardingMismatch
)

// planSharding decides what to do from the state of both sides.
//
// Separated from the commands so the decision can be checked without a cluster
// to run them against: the interesting part is which of the four states leads
// where, not how shardCollection is spelled.
func planSharding(source *shardKey, targetIsCluster bool, target *shardKey) shardingAction {
	switch {
	case source == nil:
		return shardingNothingToDo
	case !targetIsCluster:
		return shardingUnavailable
	case target != nil:
		// Already partitioned, by a previous run or by hand. Repartitioning is
		// not this code's business — but checking is. Two sides partitioned
		// differently hold the same documents on different shards, and nothing
		// says so: the counts agree, every write succeeds, a comparison passes.
		// What differs is where the data sits, which is the one property the
		// standby exists to reproduce.
		if !sameShardKey(source, target) {
			return shardingMismatch
		}
		return shardingNothingToDo
	default:
		return shardingApply
	}
}

// sameShardKey reports whether two collections are partitioned the same way.
//
// The comparison is over the rendered key, fields and order and all: a key of
// {a: 1, b: 1} is not the key {b: 1, a: 1}, and {a: "hashed"} is not {a: 1}.
func sameShardKey(source, target *shardKey) bool {
	if source == nil || target == nil {
		return source == target
	}
	if source.Unique != target.Unique {
		return false
	}
	return source.Key.String() == target.Key.String()
}

// matchSharding gives the target collection the source's shard key.
//
// A failure is reported and not fatal: replicating into an unsharded collection
// works, it just does not scale the way the source does, and refusing to
// replicate at all would be worse.
func (s *MongoDBSyncer) matchSharding(ctx context.Context, sourceDB, sourceColl, targetDB, targetColl string) error {
	sourceNS := sourceDB + "." + sourceColl
	targetNS := targetDB + "." + targetColl

	key, err := collectionShardKey(ctx, s.sourceClient, sourceNS)
	if err != nil {
		s.logger.Debugf("[MongoDB] Could not read the shard key of %s: %v", sourceNS, err)
		return nil
	}
	if key == nil {
		return nil // the source collection is not sharded, which is most of them
	}

	clustered, err := isMongos(ctx, s.targetClient)
	if err != nil {
		s.logger.Warnf("[MongoDB] Could not tell whether the target is a sharded "+
			"cluster: %v", err)
		return nil
	}
	existing, err := collectionShardKey(ctx, s.targetClient, targetNS)
	if err != nil {
		s.logger.Debugf("[MongoDB] Could not read the shard key of %s: %v", targetNS, err)
	}

	switch planSharding(key, clustered, existing) {
	case shardingNothingToDo:
		return nil
	case shardingMismatch:
		return domain.Unrecoverable(
			"%s is sharded on %v and %s on %v. The two hold the same documents on "+
				"different shards, and nothing downstream shows it: the counts agree, "+
				"the writes succeed and a comparison passes, but the standby is not the "+
				"shape of the thing it stands in for. Repartitioning a collection that "+
				"already holds data is a migration, not something this task will do, so "+
				"either give %s the source's key or point the task at a collection that "+
				"has it",
			sourceNS, key.Key, targetNS, existing.Key, targetNS)
	case shardingUnavailable:
		s.logger.Warnf("[MongoDB] %s is sharded on %v but the target is not a sharded "+
			"cluster, so %s will hold the whole collection on one server and will not "+
			"have the source's capacity.", sourceNS, key.Key, targetNS)
		return nil
	}

	admin := s.targetClient.Database("admin")
	// Enabling sharding on a database that already has it is an error worth
	// ignoring rather than reporting: it says the state is already right.
	if err := admin.RunCommand(ctx, bson.D{
		{Key: "enableSharding", Value: targetDB},
	}).Err(); err != nil && !alreadySharded(err) {
		s.logger.Warnf("[MongoDB] Could not enable sharding on %s, so %s will hold "+
			"the whole collection on one server: %v", targetDB, targetNS, err)
		return nil
	}

	command := bson.D{
		{Key: "shardCollection", Value: targetNS},
		{Key: "key", Value: key.Key},
	}
	if key.Unique {
		command = append(command, bson.E{Key: "unique", Value: true})
	}
	if err := admin.RunCommand(ctx, command).Err(); err != nil {
		s.logger.Warnf("[MongoDB] Could not shard %s on %v, so it will hold the whole "+
			"collection on one server: %v", targetNS, key.Key, err)
		return nil
	}
	s.logger.Infof("[MongoDB] Sharded %s on %v, matching the source", targetNS, key.Key)
	return nil
}

// alreadySharded reports whether an error says the database is already
// partitioned, which is success as far as this is concerned.
func alreadySharded(err error) bool {
	var command mongo.CommandError
	if !errors.As(err, &command) {
		return false
	}
	// AlreadyInitialized, plus the message a server returns when the database
	// has been enabled already.
	return command.Code == 23 || command.HasErrorMessage("already enabled")
}

// A write to a sharded collection has to say which shard it is for.  mongos
// routes by the shard key, so a filter that carries only the _id cannot be
// routed: an updateOne or a deleteOne is broadcast to every shard, and an
// upsert — which is what every write here is, because replication is replayed
// — is refused outright with "could not extract exact shard key". A collection
// sharded on anything other than its _id therefore stops replication on the
// first document.

// documentAddress is how documents of one collection are addressed on the
// target.
type documentAddress struct {
	// Paths are the shard key's fields, in the order the key declares them,
	// excluding the _id. Empty means the _id addresses the document by itself,
	// which covers an unsharded collection and one sharded on {_id: ...}.
	Paths []string
}

// addressOf reads how a collection is partitioned and reports what a filter has
// to carry.
func addressOf(ctx context.Context, client *mongo.Client, namespace string) (documentAddress, error) {
	key, err := collectionShardKey(ctx, client, namespace)
	if err != nil || key == nil {
		return documentAddress{}, err
	}

	elements, err := key.Key.Elements()
	if err != nil {
		return documentAddress{}, fmt.Errorf("read the shard key of %s: %w", namespace, err)
	}
	var paths []string
	for _, element := range elements {
		if element.Key() == "_id" {
			continue
		}
		paths = append(paths, element.Key())
	}
	return documentAddress{Paths: paths}, nil
}

func (a documentAddress) filter(doc bson.M) (bson.M, error) {
	id, ok := doc["_id"]
	if !ok {
		return nil, fmt.Errorf("a document carries no _id")
	}
	out := bson.M{"_id": id}
	for _, path := range a.Paths {
		value, found := lookupPath(doc, path)
		if !found {
			// A missing shard key field is a document mongos would not have
			// accepted, so it is worth reporting rather than writing something
			// that will be refused or, worse, broadcast.
			return nil, fmt.Errorf("a document carries no %q, which is part of the "+
				"shard key and so part of how the target addresses it", path)
		}
		out[path] = value
	}
	return out, nil
}

// lookupPath reads a dotted path out of a document, since a shard key may name
// a field inside a subdocument.
//
// Every level is read through documentOf, which knows all four shapes the
// driver hands back. A nested document inside a bson.M decodes as bson.D, not
// bson.M -- so a shard key like "customer.region" was never found, and a
// resumed copy or a re-copy of a collection sharded that way failed for good.
func lookupPath(doc bson.M, path string) (interface{}, bool) {
	parts := strings.Split(path, ".")
	var current interface{} = doc
	for _, part := range parts {
		held := documentOf(current)
		if held == nil {
			return nil, false
		}
		value, ok := held[part]
		if !ok {
			return nil, false
		}
		current = value
	}
	return current, true
}
