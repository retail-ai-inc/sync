package mongodb

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Enabling sharding on a database that already has it is an error the server
// reports and this has to ignore, or every restart logs a warning about
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

// The one that matters is a sharded source with an unsharded target: the copy
// then holds the whole collection on one shard, which is not the same
// collection and not a replacement for the region it stands in for.
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
		"a target already partitioned the same way is left alone": {
			source: key, targetIsCluster: true, target: key, want: shardingNothingToDo},
		"a target partitioned differently is refused": {
			source: keyOn(t, "merchant_id"), targetIsCluster: true,
			target: keyOn(t, "_id"), want: shardingMismatch},
		"the order of a compound key is part of it": {
			source: keyOn(t, "merchant_id", "order_id"), targetIsCluster: true,
			target: keyOn(t, "order_id", "merchant_id"), want: shardingMismatch},
	} {
		t.Run(name, func(t *testing.T) {
			if got := planSharding(tc.source, tc.targetIsCluster, tc.target); got != tc.want {
				t.Errorf("planSharding = %v, want %v", got, tc.want)
			}
		})
	}
}

func keyOn(t *testing.T, fields ...string) *shardKey {
	t.Helper()

	var doc bson.D
	for _, field := range fields {
		doc = append(doc, bson.E{Key: field, Value: int32(1)})
	}
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal a shard key: %v", err)
	}
	return &shardKey{Key: raw}
}

// An upsert on a sharded collection has to name the whole shard key.
func TestTheFilterCarriesTheShardKey(t *testing.T) {
	address := documentAddress{Paths: []string{"merchant_id"}}

	filter, err := address.filter(bson.M{
		"_id": "abc", "merchant_id": 42, "amount": 100,
	})
	if err != nil {
		t.Fatalf("filter: %v", err)
	}
	if filter["_id"] != "abc" {
		t.Errorf("filter = %v, want it to carry the _id", filter)
	}
	if filter["merchant_id"] != 42 {
		t.Errorf("filter = %v, want it to carry the shard key", filter)
	}
	if len(filter) != 2 {
		t.Errorf("filter = %v, want nothing beyond the _id and the shard key", filter)
	}
}

// TestAnUnshardedCollectionIsAddressedByItsIDAlone keeps the common case as it
// was: most collections are not sharded, and a filter that named more than the
// _id would be a change in behaviour for them.
func TestAnUnshardedCollectionIsAddressedByItsIDAlone(t *testing.T) {
	filter, err := documentAddress{}.filter(bson.M{"_id": 7, "name": "a"})
	if err != nil {
		t.Fatalf("filter: %v", err)
	}
	if len(filter) != 1 || filter["_id"] != 7 {
		t.Errorf("filter = %v, want just the _id", filter)
	}
}

// TestAShardKeyInsideASubdocumentIsFound covers the dotted paths a shard key is
// allowed to name.
//
// The document is round-tripped through the driver rather than written by hand,
// because that is where the bug was: a nested document inside a bson.M comes
// back as bson.D, and a fixture written as a literal bson.M never sees it. A
// hand-written fixture passed while every resumed copy of a collection sharded
// on a dotted key failed for good.
func TestAShardKeyInsideASubdocumentIsFound(t *testing.T) {
	address := documentAddress{Paths: []string{"customer.region"}}

	raw, err := bson.Marshal(bson.D{
		{Key: "_id", Value: 1},
		{Key: "customer", Value: bson.D{
			{Key: "region", Value: "tokyo"},
			{Key: "tier", Value: "gold"},
		}},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded bson.M
	if err := bson.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, isD := decoded["customer"].(bson.D); !isD {
		t.Fatalf("the driver now decodes a subdocument as %T; the shape this "+
			"guards against has changed", decoded["customer"])
	}

	filter, err := address.filter(decoded)
	if err != nil {
		t.Fatalf("filter: %v", err)
	}
	if filter["customer.region"] != "tokyo" {
		t.Errorf("filter = %v, want the dotted path resolved", filter)
	}

	// The hand-written shape still works, since both reach this code.
	filter, err = address.filter(bson.M{
		"_id":      1,
		"customer": bson.M{"region": "osaka"},
	})
	if err != nil {
		t.Fatalf("filter: %v", err)
	}
	if filter["customer.region"] != "osaka" {
		t.Errorf("filter = %v, want the dotted path resolved", filter)
	}
}

// TestADocumentMissingItsShardKeyIsReported rather than written somewhere the
// server chooses. Guessing would either broadcast the write or have it refused.
func TestADocumentMissingItsShardKeyIsReported(t *testing.T) {
	address := documentAddress{Paths: []string{"merchant_id"}}

	if _, err := address.filter(bson.M{"_id": 1}); err == nil {
		t.Fatal("a document with no shard key was addressed anyway")
	} else if !strings.Contains(err.Error(), "merchant_id") {
		t.Errorf("error = %q, want it to name the missing field", err)
	}
}
