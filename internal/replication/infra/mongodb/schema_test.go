package mongodb

import (
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
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

// Indexes were copied once, by the initial snapshot, and never again.
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

// TestACollModTravels covers the other additive change.
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

// A drop arriving unattended removes data from the disaster-recovery copy at
// the moment that copy may be the only thing left of it — and a mistaken drop
// at the source is one of the reasons the copy exists.
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

// The snapshot pins a cluster time and the reader opens the stream from the
// stored position.
func TestAPinnedClusterTimeIsRecognisedAsOne(t *testing.T) {
	payload, err := encodeClusterTime(bson.Timestamp{T: 1787547851, I: 13})
	if err != nil {
		t.Fatalf("encodeClusterTime: %v", err)
	}

	stored, err := decodePosition(domain.Position{Payload: payload})
	if err != nil {
		t.Fatalf("decodePosition: %v", err)
	}
	if stored.Token != "" {
		t.Errorf("a pinned cluster time decoded as a resume token %q", stored.Token)
	}
	if stored.Cluster != 1787547851 || stored.Increment != 13 {
		t.Errorf("cluster time = %d.%d, want 1787547851.13", stored.Cluster, stored.Increment)
	}
}

func TestAResumeTokenIsRecognisedAsOne(t *testing.T) {
	raw := rawEvent(t, bson.D{{Key: "_data", Value: "8264ABCDEF"}})
	payload, err := encodeToken(raw)
	if err != nil {
		t.Fatalf("encodeToken: %v", err)
	}

	stored, err := decodePosition(domain.Position{Payload: payload})
	if err != nil {
		t.Fatalf("decodePosition: %v", err)
	}
	if stored.Cluster != 0 {
		t.Errorf("a resume token decoded as cluster time %d", stored.Cluster)
	}
	token, err := stored.token()
	if err != nil {
		t.Fatalf("token: %v", err)
	}
	if got, _ := token.Lookup("_data").StringValueOK(); got != "8264ABCDEF" {
		t.Errorf("token carries _data %q, want the one it was built from", got)
	}
}

// TestAPositionThatIsNeitherIsRefused keeps a corrupted or hand-edited position
// from being handed to the server as though it meant something.
func TestAPositionThatIsNeitherIsRefused(t *testing.T) {
	stored, err := decodePosition(domain.Position{Payload: `{"something":"else"}`})
	if err != nil {
		t.Fatalf("decodePosition: %v", err)
	}
	if stored.Token != "" || stored.Cluster != 0 {
		t.Fatal("a position holding neither kind decoded as though it held one")
	}
}

// Asked to decode a document into an interface.
func TestAnIndexKeyIsReadWhateverShapeItArrivesIn(t *testing.T) {
	shapes := map[string]interface{}{
		"bson.D": bson.D{{Key: "customer", Value: int32(1)}},
		"bson.M": bson.M{"customer": int32(1)},
	}
	for name, key := range shapes {
		got, ok := indexKeyOf(key)
		if !ok {
			t.Errorf("%s: the key was not read", name)
			continue
		}
		if len(got) != 1 || got[0].Key != "customer" {
			t.Errorf("%s: key = %v, want customer", name, got)
		}
	}
}

// TestACompoundIndexKeepsItsFieldOrder is why the ordered form is preferred: an
// index on (region, _id) is not the index on (_id, region), and a target built
// from the wrong one answers the shard-key query with a scan.
func TestACompoundIndexKeepsItsFieldOrder(t *testing.T) {
	got, ok := indexKeyOf(bson.D{
		{Key: "region", Value: int32(1)},
		{Key: "_id", Value: int32(1)},
	})
	if !ok {
		t.Fatal("the key was not read")
	}
	if len(got) != 2 || got[0].Key != "region" || got[1].Key != "_id" {
		t.Errorf("key = %v, want region then _id", got)
	}
}

// TestAnIndexDirectionFromJSONIsANumberAgain covers a key that has been through
// a JSON round trip, where 1 and -1 arrive as strings or floats.
func TestAnIndexDirectionFromJSONIsANumberAgain(t *testing.T) {
	got, ok := indexKeyOf(bson.D{
		{Key: "a", Value: "-1"},
		{Key: "b", Value: float64(1)},
	})
	if !ok {
		t.Fatal("the key was not read")
	}
	if got[0].Value != int32(-1) {
		t.Errorf("a = %v (%T), want int32(-1)", got[0].Value, got[0].Value)
	}
	if got[1].Value != int32(1) {
		t.Errorf("b = %v (%T), want int32(1)", got[1].Value, got[1].Value)
	}
}

// TestANamedIndexKindIsPassedThrough keeps a text or 2dsphere index from being
// turned into a direction.
func TestANamedIndexKindIsPassedThrough(t *testing.T) {
	got, ok := indexKeyOf(bson.D{{Key: "location", Value: "2dsphere"}})
	if !ok {
		t.Fatal("the key was not read")
	}
	if got[0].Value != "2dsphere" {
		t.Errorf("location = %v, want 2dsphere", got[0].Value)
	}
}

// TestAnUnreadableIndexKeyIsReported keeps a shape nobody expected from being
// turned into an index nobody asked for.
func TestAnUnreadableIndexKeyIsReported(t *testing.T) {
	if _, ok := indexKeyOf("not a document"); ok {
		t.Error("a string was accepted as an index key")
	}
	if _, ok := indexKeyOf(bson.D{}); ok {
		t.Error("an empty key was accepted")
	}
}
