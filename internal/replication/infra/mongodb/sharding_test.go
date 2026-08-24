package mongodb

import (
	"errors"
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// TestADatabaseAlreadyShardedIsNotAFailure covers the state this is trying to
// reach. Enabling sharding on a database that already has it is an error the
// server reports and this has to ignore, or every restart logs a warning about
// something being right.
func TestADatabaseAlreadyShardedIsNotAFailure(t *testing.T) {
	for name, err := range map[string]error{
		"by code": mongo.CommandError{Code: 23, Message: "already initialized"},
		"by message": mongo.CommandError{
			Code: 9999, Message: "sharding already enabled for database shop"},
		"wrapped": fmt.Errorf("enable sharding: %w",
			mongo.CommandError{Code: 23, Message: "already initialized"}),
	} {
		t.Run(name, func(t *testing.T) {
			if !alreadySharded(err) {
				t.Errorf("alreadySharded(%v) = false", err)
			}
		})
	}
}

// TestARealFailureIsNotMistakenForSuccess is the other half: a permission error
// must not be read as "already done", or the collection silently ends up on one
// shard.
func TestARealFailureIsNotMistakenForSuccess(t *testing.T) {
	for name, err := range map[string]error{
		"unauthorised": mongo.CommandError{Code: 13, Message: "not authorized on admin"},
		"no such host": errors.New("server selection error: context deadline exceeded"),
		"nothing":      nil,
	} {
		t.Run(name, func(t *testing.T) {
			if alreadySharded(err) {
				t.Errorf("alreadySharded(%v) = true", err)
			}
		})
	}
}

// TestThePlanFollowsFromTheTwoSides covers every state the two sides can be in.
// The one that matters is a sharded source with an unsharded target: the copy
// then holds the whole collection on one shard, which is not the same collection
// and not a replacement for the region it stands in for.
func TestThePlanFollowsFromTheTwoSides(t *testing.T) {
	key := &shardKey{}

	for name, tc := range map[string]struct {
		source          *shardKey
		targetIsCluster bool
		target          *shardKey
		want            shardingAction
	}{
		"an unsharded source is left alone": {
			source: nil, targetIsCluster: true, want: shardingNothingToDo},
		"an unsharded source on a plain target": {
			source: nil, targetIsCluster: false, want: shardingNothingToDo},
		"a sharded source on a plain target cannot be matched": {
			source: key, targetIsCluster: false, want: shardingUnavailable},
		"a sharded source on a cluster is matched": {
			source: key, targetIsCluster: true, target: nil, want: shardingApply},
		"a target already partitioned is left alone": {
			source: key, targetIsCluster: true, target: key, want: shardingNothingToDo},
	} {
		t.Run(name, func(t *testing.T) {
			if got := planSharding(tc.source, tc.targetIsCluster, tc.target); got != tc.want {
				t.Errorf("planSharding = %v, want %v", got, tc.want)
			}
		})
	}
}
