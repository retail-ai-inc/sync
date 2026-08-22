package mongodb

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// A sharded source collection replicated into an unsharded target is not the
// same collection. Every document lands on whichever shard is primary for the
// target database, so the copy has one shard's capacity and one shard's
// throughput where the source had three — and the region it exists to stand in
// for cannot be stood in for.
//
// Nothing here reshards anything. The target is made to match the source before
// the copy runs, while the collection is still empty and sharding it costs
// nothing. A target that is not a sharded cluster, or a source collection that
// is not sharded, is left exactly as it is.

// shardKey describes how a collection is partitioned.
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

// isMongos reports whether a client is talking to a sharded cluster.
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

// matchSharding gives the target collection the source's shard key.
//
// A failure is reported and not fatal: replicating into an unsharded collection
// works, it just does not scale the way the source does, and refusing to
// replicate at all would be worse.
func (s *MongoDBSyncer) matchSharding(ctx context.Context, sourceDB, sourceColl, targetDB, targetColl string) {
	sourceNS := sourceDB + "." + sourceColl
	targetNS := targetDB + "." + targetColl

	key, err := collectionShardKey(ctx, s.sourceClient, sourceNS)
	if err != nil {
		s.logger.Debugf("[MongoDB] Could not read the shard key of %s: %v", sourceNS, err)
		return
	}
	if key == nil {
		return // the source collection is not sharded
	}

	sharded, err := isMongos(ctx, s.targetClient)
	if err != nil {
		s.logger.Warnf("[MongoDB] Could not tell whether the target is a sharded "+
			"cluster: %v", err)
		return
	}
	if !sharded {
		s.logger.Warnf("[MongoDB] %s is sharded on %v but the target is not a sharded "+
			"cluster, so %s will hold the whole collection on one server and will not "+
			"have the source's capacity.", sourceNS, key.Key, targetNS)
		return
	}

	if existing, err := collectionShardKey(ctx, s.targetClient, targetNS); err == nil && existing != nil {
		return // already partitioned, by a previous run or by hand
	}

	admin := s.targetClient.Database("admin")
	// Enabling sharding on a database that already has it is an error worth
	// ignoring rather than reporting: it says the state is already right.
	if err := admin.RunCommand(ctx, bson.D{
		{Key: "enableSharding", Value: targetDB},
	}).Err(); err != nil && !alreadySharded(err) {
		s.logger.Warnf("[MongoDB] Could not enable sharding on %s, so %s will hold "+
			"the whole collection on one server: %v", targetDB, targetNS, err)
		return
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
		return
	}
	s.logger.Infof("[MongoDB] Sharded %s on %v, matching the source", targetNS, key.Key)
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
