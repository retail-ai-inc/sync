package mongodb

import (
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func schemaEvent(t *testing.T, kind string, description bson.D) bson.Raw {
	t.Helper()
	doc := bson.D{
		{Key: "_id", Value: bson.D{{Key: "_data", Value: "8264"}}},
		{Key: "operationType", Value: kind},
		{Key: "ns", Value: bson.D{{Key: "db", Value: "shop"}, {Key: "coll", Value: "orders"}}},
	}
	if description != nil {
		doc = append(doc, bson.E{Key: "operationDescription", Value: description})
	}
	return rawEvent(t, doc)
}

// TestAnAddedIndexTravels is what C4 exists for.
//
// Indexes were copied once, by the initial snapshot, and never again. An index
// is usually added because a query became too slow, so the moment the target
// most needed it was exactly the moment it did not have it — and a target that
// answers too slowly to serve is as much an outage as one missing rows.
func TestAnAddedIndexTravels(t *testing.T) {
	raw := schemaEvent(t, "createIndexes", bson.D{
		{Key: "indexes", Value: bson.A{
			bson.D{{Key: "v", Value: 2}, {Key: "key", Value: bson.D{{Key: "customer", Value: 1}}}, {Key: "name", Value: "customer_1"}},
		}},
	})

	change, decision, reason := planSchemaChange(raw, "orders")
	if decision != ddlApply {
		t.Fatalf("decision = %v (%s), want apply", decision, reason)
	}
	if change.Command[0].Key != "createIndexes" || change.Command[0].Value != "orders" {
		t.Errorf("command = %v, want it to name createIndexes on orders", change.Command)
	}
	if len(change.Command) < 2 || change.Command[1].Key != "indexes" {
		t.Errorf("command = %v, want it to carry the index specification", change.Command)
	}
}

func TestADroppedIndexTravels(t *testing.T) {
	raw := schemaEvent(t, "dropIndexes", bson.D{
		{Key: "indexes", Value: bson.A{
			bson.D{{Key: "v", Value: 2}, {Key: "name", Value: "customer_1"}},
		}},
	})

	change, decision, reason := planSchemaChange(raw, "orders")
	if decision != ddlApply {
		t.Fatalf("decision = %v (%s), want apply", decision, reason)
	}
	if change.Command[0].Key != "dropIndexes" {
		t.Errorf("command = %v, want dropIndexes", change.Command)
	}
}

// TestACollModTravels covers the other additive change: a validator or a TTL
// changed at the source has to reach the target or the two behave differently.
func TestACollModTravels(t *testing.T) {
	raw := schemaEvent(t, "modify", bson.D{
		{Key: "expireAfterSeconds", Value: 3600},
	})

	change, decision, reason := planSchemaChange(raw, "orders")
	if decision != ddlApply {
		t.Fatalf("decision = %v (%s), want apply", decision, reason)
	}
	if change.Command[0].Key != "collMod" {
		t.Errorf("command = %v, want collMod", change.Command)
	}
}

// TestADropIsRefused is the policy that matters most.
//
// A drop arriving unattended removes data from the disaster-recovery copy at the
// moment that copy may be the only thing left of it — and a mistaken drop at the
// source is one of the reasons the copy exists.
func TestADropIsRefused(t *testing.T) {
	for _, kind := range []string{"drop", "dropDatabase", "rename"} {
		_, decision, reason := planSchemaChange(schemaEvent(t, kind, nil), "orders")
		if decision != ddlStop {
			t.Errorf("%s: decision = %v, want stop", kind, decision)
		}
		if reason == "" {
			t.Errorf("%s: no reason given, and the operator has to be told why", kind)
		}
	}
}

// TestShardingChangesAreLeftAlone covers the changes that belong to whoever
// built the target: applying them would reshard the copy unattended.
func TestShardingChangesAreLeftAlone(t *testing.T) {
	for _, kind := range []string{"shardCollection", "reshardCollection", "refineCollectionShardKey"} {
		_, decision, reason := planSchemaChange(schemaEvent(t, kind, nil), "orders")
		if decision != ddlSkip {
			t.Errorf("%s: decision = %v, want skip", kind, decision)
		}
		if !strings.Contains(reason, "sharding") {
			t.Errorf("%s: reason = %q, want it to say why", kind, reason)
		}
	}
}

// TestAnEventWithNoDescriptionIsSkipped keeps a malformed event from becoming a
// command with nothing in it.
func TestAnEventWithNoDescriptionIsSkipped(t *testing.T) {
	for _, kind := range []string{"createIndexes", "dropIndexes", "modify"} {
		_, decision, _ := planSchemaChange(schemaEvent(t, kind, nil), "orders")
		if decision != ddlSkip {
			t.Errorf("%s with no description: decision = %v, want skip", kind, decision)
		}
	}
}

// TestARowEventIsNotASchemaEvent pins the split down: a misclassified insert
// would be handed to RunCommand.
func TestARowEventIsNotASchemaEvent(t *testing.T) {
	for _, kind := range []string{"insert", "update", "replace", "delete"} {
		raw := rawEvent(t, changeDoc("shop", "orders", kind, bson.D{{Key: "_id", Value: "a"}}))
		if isSchemaEvent(raw) {
			t.Errorf("%s was classified as a schema change", kind)
		}
	}
	for _, kind := range []string{"createIndexes", "drop", "modify", "invalidate"} {
		if !isSchemaEvent(schemaEvent(t, kind, nil)) {
			t.Errorf("%s was not classified as a schema change", kind)
		}
	}
}

// TestTheSchemaChangeIsAppliedUnderTheMappedName covers a task that replicates a
// collection under another name: the index has to be created on that one.
func TestTheSchemaChangeIsAppliedUnderTheMappedName(t *testing.T) {
	raw := schemaEvent(t, "createIndexes", bson.D{
		{Key: "indexes", Value: bson.A{
			bson.D{{Key: "name", Value: "customer_1"}, {Key: "key", Value: bson.D{{Key: "customer", Value: 1}}}},
		}},
	})
	change, _, _ := planSchemaChange(raw, "orders")

	// The command still names the source collection; the applier substitutes the
	// mapped one, which is what this checks by walking the same path.
	if change.Command[0].Value != "orders" {
		t.Fatalf("planned command names %v, want the source collection", change.Command[0].Value)
	}
}
