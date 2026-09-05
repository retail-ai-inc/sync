package mongodb

import (
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// Batch shape, reported for every applied batch. The figures are sums a
// dashboard divides, so one that counts wrongly makes the ratio wrong rather
// than absent -- which is harder to notice than a missing panel.

func TestShapeOfCountsEventsAndDistinctNamespaces(t *testing.T) {
	orders := domain.Namespace{DB: "shop", Object: "orders"}
	payments := domain.Namespace{DB: "shop", Object: "payments"}

	events, namespaces := shapeOf([][]*domain.Event{
		{{NS: orders}, {NS: orders}},
		{{NS: payments}, {NS: orders}},
	})

	if events != 4 {
		t.Errorf("events = %d, want 4", events)
	}
	if namespaces != 2 {
		t.Errorf("namespaces = %d, want 2 -- the same collection twice is one", namespaces)
	}
}

func TestShapeOfNothing(t *testing.T) {
	events, namespaces := shapeOf(nil)
	if events != 0 || namespaces != 0 {
		t.Errorf("shapeOf(nil) = %d, %d", events, namespaces)
	}
}

// onlySchemaChange reports the batch's single schema change when that is all it
// holds. The pipeline gives one its own batch, because MongoDB's catalogue is
// not transactional and a batch carrying both cannot be applied atomically --
// so anything alongside is a bug worth failing on rather than working around.

func TestASchemaChangeAloneIsRecognised(t *testing.T) {
	change := &domain.Event{Op: domain.OpSchema}

	found, alone := onlySchemaChange([][]*domain.Event{{change}})
	if !alone {
		t.Error("a batch holding one schema change and nothing else was not recognised")
	}
	if found != change {
		t.Error("the wrong event was returned")
	}
}

// TestASchemaChangeWithRowsIsNotAlone: the pipeline should never build one, so
// this is the check that turns a pipeline bug into a failure instead of a
// partly applied batch.
func TestASchemaChangeWithRowsIsNotAlone(t *testing.T) {
	for name, runs := range map[string][][]*domain.Event{
		"row after":  {{{Op: domain.OpSchema}, {Op: domain.OpInsert}}},
		"row before": {{{Op: domain.OpInsert}, {Op: domain.OpSchema}}},
		"row in another run": {
			{{Op: domain.OpSchema}},
			{{Op: domain.OpInsert}},
		},
		"two schema changes": {{{Op: domain.OpSchema}, {Op: domain.OpSchema}}},
	} {
		t.Run(name, func(t *testing.T) {
			found, alone := onlySchemaChange(runs)
			if alone {
				t.Error("a batch holding more than a schema change was treated as one")
			}
			if found == nil {
				t.Error("the schema change was not found at all")
			}
		})
	}
}

func TestABatchOfRowsHoldsNoSchemaChange(t *testing.T) {
	found, alone := onlySchemaChange([][]*domain.Event{
		{{Op: domain.OpInsert}, {Op: domain.OpUpdate}},
	})
	if alone || found != nil {
		t.Errorf("onlySchemaChange found %v, %v on a batch of rows", found, alone)
	}
}

// The re-copy's resume key. It walks a collection in _id order and stores the
// last key it read, so the encoding has to survive every _id type MongoDB
// allows -- an _id that cannot be stored and read back means the re-copy either
// restarts from the beginning or skips what it could not encode.

func TestTheResumeKeySurvivesEveryIDType(t *testing.T) {
	for name, id := range map[string]interface{}{
		"object id": bson.NewObjectID(),
		"string":    "an-order-id",
		"int32":     int32(42),
		"int64":     int64(1 << 40),
		"double":    3.5,
		"binary":    bson.Binary{Subtype: 0, Data: []byte{1, 2, 3}},
		"document":  bson.D{{Key: "tenant", Value: "a"}, {Key: "n", Value: int32(1)}},
	} {
		t.Run(name, func(t *testing.T) {
			encoded, err := encodeID(rawOf(id))
			if err != nil {
				t.Fatalf("encodeID: %v", err)
			}
			if encoded == "" {
				t.Fatal("encodeID produced nothing, so the re-copy would restart from " +
					"the beginning of the collection")
			}

			decoded, err := decodeID(encoded)
			if err != nil {
				t.Fatalf("decodeID(%q): %v", encoded, err)
			}
			if decoded == nil {
				t.Error("the key decoded to nothing")
			}

			// And it round-trips to the same stored text, which is what makes
			// the walk continue from where it stopped rather than near it.
			again, err := encodeID(rawOf(decoded))
			if err != nil {
				t.Fatalf("re-encode: %v", err)
			}
			if again != encoded {
				t.Errorf("the key does not round-trip: %q then %q", encoded, again)
			}
		})
	}
}

// TestAnUnsetKeyEncodesToNothing: a re-copy that has read no rows yet has no
// last key, and "" is how it says so -- not an error.
func TestAnUnsetKeyEncodesToNothing(t *testing.T) {
	encoded, err := encodeID(bson.RawValue{})
	if err != nil {
		t.Fatalf("encodeID of an unset key: %v", err)
	}
	if encoded != "" {
		t.Errorf("encodeID of an unset key = %q, want nothing", encoded)
	}
}

func TestAStoredKeyThatCannotBeReadIsReported(t *testing.T) {
	if _, err := decodeID("not extended json"); err == nil {
		t.Error("a stored key that cannot be read was accepted, so the walk would " +
			"continue from a zero value")
	}
}

// TestRawOfSomethingUnmarshallableIsUnset rather than a panic: it is called on
// whatever a collection's _id happens to be.
func TestRawOfSomethingUnmarshallableIsUnset(t *testing.T) {
	if raw := rawOf(func() {}); raw.Type != 0 {
		t.Errorf("a value BSON cannot marshal produced %v", raw.Type)
	}
}

// Field masking. A task may be configured to mask or encrypt named fields
// before they reach the other region; a policy that fails to apply sends the
// value in the clear, and nothing downstream would show it.

func maskingSyncer(collection string, fields ...string) *MongoDBSyncer {
	// The stored configuration carries these as decoded JSON, which is what
	// the policy reader expects to find.
	policy := make([]interface{}, 0, len(fields))
	for _, field := range fields {
		policy = append(policy, map[string]interface{}{
			"field": field, "securityType": "masked",
		})
	}
	return &MongoDBSyncer{cfg: config.SyncConfig{
		Mappings: []config.DatabaseMapping{{
			Tables: []config.TableMapping{{
				SourceTable:     collection,
				SecurityEnabled: true,
				FieldSecurity:   policy,
			}},
		}},
	}}
}

func TestAMaskedFieldIsNotSentInTheClear(t *testing.T) {
	syncer := maskingSyncer("orders", "card_number")

	masked := syncer.maskDocument("orders", bson.M{
		"_id":         "1",
		"card_number": "4111111111111111",
		"amount":      100,
	})

	if got, _ := masked["card_number"].(string); got == "4111111111111111" {
		t.Error("the masked field was sent in the clear")
	}
	if masked["amount"] != 100 {
		t.Errorf("an unmasked field was changed to %v", masked["amount"])
	}
}

// TestACollectionWithNoPolicyIsUntouched: masking is opt-in per collection, and
// a document rewritten without being asked for is a change the source never
// made.
func TestACollectionWithNoPolicyIsUntouched(t *testing.T) {
	syncer := maskingSyncer("orders", "card_number")

	document := bson.M{"card_number": "4111111111111111"}
	untouched := syncer.maskDocument("payments", document)

	if untouched["card_number"] != "4111111111111111" {
		t.Error("a collection with no policy was masked anyway")
	}
}

func TestMaskValueLeavesANonDocumentAlone(t *testing.T) {
	syncer := maskingSyncer("orders", "card_number")

	// A change stream's fullDocument may be absent, which arrives as something
	// that is not a document at all.
	for name, value := range map[string]interface{}{
		"nil":    nil,
		"string": "not a document",
		"number": 42,
	} {
		t.Run(name, func(t *testing.T) {
			if got := syncer.maskValue("orders", value); got != value {
				t.Errorf("maskValue changed %v to %v", value, got)
			}
		})
	}
}

func TestMaskValueMasksADocument(t *testing.T) {
	syncer := maskingSyncer("orders", "card_number")

	got := syncer.maskValue("orders", bson.M{"card_number": "4111111111111111"})

	rendered := bsonToString(t, got)
	if strings.Contains(rendered, "4111111111111111") {
		t.Errorf("the masked field survived: %s", rendered)
	}
}

func bsonToString(t *testing.T, value interface{}) string {
	t.Helper()
	encoded, err := bson.MarshalExtJSON(value, true, false)
	if err != nil {
		return ""
	}
	return string(encoded)
}

// TestTheIdleHeartbeatKeepsTheLagGaugesHonest pins the cadence rather than the
// number for its own sake.
//
// Every lag figure a task reports is the age of the newest thing the reader has
// seen. While the source is quiet the newest thing is the last heartbeat, so
// the gauges walk from zero up to this interval and drop back -- whatever the
// real delay is. At ten seconds that read as three to five seconds of steady
// lag on a link measured end to end at about a tenth of a second, and it left
// ten-second windows in which a stream that had stopped looked the same as one
// with nothing to carry.
func TestTheIdleHeartbeatKeepsTheLagGaugesHonest(t *testing.T) {
	if idleHeartbeat > time.Second {
		t.Errorf("idleHeartbeat = %v; the reported lag of an idle task rises to "+
			"that before resetting, so anything above a second cannot be read as "+
			"a lag figure", idleHeartbeat)
	}
	if idleHeartbeat <= 0 {
		t.Fatalf("idleHeartbeat = %v; the stream would spin", idleHeartbeat)
	}
}
