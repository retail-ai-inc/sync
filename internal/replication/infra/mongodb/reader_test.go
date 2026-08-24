package mongodb

import (
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// rawEvent renders a change stream document the way the server sends one.
func rawEvent(t *testing.T, doc bson.D) bson.Raw {
	t.Helper()
	encoded, err := bson.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return bson.Raw(encoded)
}

func changeDoc(db, coll, op string, documentKey bson.D) bson.D {
	return bson.D{
		{Key: "_id", Value: bson.D{{Key: "_data", Value: "8264"}}},
		{Key: "operationType", Value: op},
		{Key: "ns", Value: bson.D{{Key: "db", Value: db}, {Key: "coll", Value: coll}}},
		{Key: "documentKey", Value: documentKey},
	}
}

// ------------------------------------------------------------------ keys

// TestTheDocumentKeyCarriesTheShardKey is the difference between a targeted
// write and a broadcast one.
//
// On a sharded collection mongos needs the shard key to route an updateOne to a
// single shard. Keying on the _id alone — which is what the old write path did —
// leaves it no choice but to send the write to every shard: one wasted round
// trip per shard, per document, for the life of the task.
func TestTheDocumentKeyCarriesTheShardKey(t *testing.T) {
	raw := rawEvent(t, changeDoc("shop", "orders", "update", bson.D{
		{Key: "_id", Value: "abc"},
		{Key: "region", Value: "tokyo"},
	}))

	key := keyOf(raw)
	if !strings.Contains(key, "_id=") {
		t.Errorf("key = %q, want it to carry the _id", key)
	}
	if !strings.Contains(key, "region=") {
		t.Errorf("key = %q, want it to carry the shard key too", key)
	}
}

// TestTwoDocumentsInOneCollectionGetDifferentKeys is what lets them share an
// ordering group and be written together.
func TestTwoDocumentsInOneCollectionGetDifferentKeys(t *testing.T) {
	first := keyOf(rawEvent(t, changeDoc("shop", "orders", "insert", bson.D{{Key: "_id", Value: "a"}})))
	second := keyOf(rawEvent(t, changeDoc("shop", "orders", "insert", bson.D{{Key: "_id", Value: "b"}})))

	if first == second {
		t.Error("two documents produced the same key")
	}
}

// TestAnEventWithNoDocumentKeyHasNoKey means it acts as a barrier rather than
// joining a group it does not belong to.
func TestAnEventWithNoDocumentKeyHasNoKey(t *testing.T) {
	raw := rawEvent(t, bson.D{
		{Key: "operationType", Value: "insert"},
		{Key: "ns", Value: bson.D{{Key: "db", Value: "shop"}, {Key: "coll", Value: "orders"}}},
	})
	if got := keyOf(raw); got != "" {
		t.Errorf("key = %q, want empty", got)
	}
}

// ---------------------------------------------------------- transactions

// TestEventsOfOneTransactionShareAnIdentity is what makes a transaction's
// boundary visible from outside, and so what lets a batch avoid being cut inside
// one.
func TestEventsOfOneTransactionShareAnIdentity(t *testing.T) {
	session := bson.D{{Key: "id", Value: "s1"}}
	first := changeDoc("shop", "orders", "insert", bson.D{{Key: "_id", Value: "a"}})
	first = append(first, bson.E{Key: "lsid", Value: session}, bson.E{Key: "txnNumber", Value: int64(7)})
	second := changeDoc("shop", "payments", "insert", bson.D{{Key: "_id", Value: "b"}})
	second = append(second, bson.E{Key: "lsid", Value: session}, bson.E{Key: "txnNumber", Value: int64(7)})

	a := transactionOf(rawEvent(t, first))
	b := transactionOf(rawEvent(t, second))

	if a == "" {
		t.Fatal("an event inside a transaction reported no transaction")
	}
	if a != b {
		t.Errorf("two events of one transaction reported %q and %q", a, b)
	}
}

// TestADifferentTransactionNumberIsADifferentTransaction is the other half: the
// boundary has to actually move.
func TestADifferentTransactionNumberIsADifferentTransaction(t *testing.T) {
	session := bson.D{{Key: "id", Value: "s1"}}
	first := changeDoc("shop", "orders", "insert", bson.D{{Key: "_id", Value: "a"}})
	first = append(first, bson.E{Key: "lsid", Value: session}, bson.E{Key: "txnNumber", Value: int64(7)})
	second := changeDoc("shop", "orders", "insert", bson.D{{Key: "_id", Value: "b"}})
	second = append(second, bson.E{Key: "lsid", Value: session}, bson.E{Key: "txnNumber", Value: int64(8)})

	if transactionOf(rawEvent(t, first)) == transactionOf(rawEvent(t, second)) {
		t.Error("two transactions reported the same identity")
	}
}

// TestAnEventOutsideATransactionIsItsOwnBoundary covers the ordinary write,
// which is most of them.
func TestAnEventOutsideATransactionIsItsOwnBoundary(t *testing.T) {
	raw := rawEvent(t, changeDoc("shop", "orders", "insert", bson.D{{Key: "_id", Value: "a"}}))
	if got := transactionOf(raw); got != "" {
		t.Errorf("transaction = %q, want none", got)
	}
}

// ----------------------------------------------------------- namespaces

func TestTheNamespaceIsReadOffTheEvent(t *testing.T) {
	raw := rawEvent(t, changeDoc("shop", "orders", "insert", bson.D{{Key: "_id", Value: "a"}}))
	ns, ok := namespaceOf(raw)
	if !ok {
		t.Fatal("the namespace was not read")
	}
	if ns.DB != "shop" || ns.Object != "orders" {
		t.Errorf("namespace = %v, want shop.orders", ns)
	}
}

func TestTheOperationIsReadOffTheEvent(t *testing.T) {
	cases := map[string]domain.Op{
		"insert":  domain.OpInsert,
		"update":  domain.OpUpdate,
		"replace": domain.OpUpdate,
		"delete":  domain.OpDelete,
		"drop":    domain.OpSchema,
	}
	for op, want := range cases {
		raw := rawEvent(t, changeDoc("shop", "orders", op, bson.D{{Key: "_id", Value: "a"}}))
		if got := opOf(raw); got != want {
			t.Errorf("opOf(%q) = %v, want %v", op, got, want)
		}
	}
}

// ------------------------------------------------------------- filtering

// TestOneStreamStillOnlyReplicatesWhatTheTaskNames is what makes a single
// deployment-wide stream safe: it sees everything and carries only the mapped
// collections.
func TestOneStreamStillOnlyReplicatesWhatTheTaskNames(t *testing.T) {
	r := &Reader{Config: config.SyncConfig{Mappings: []config.DatabaseMapping{
		{Tables: []config.TableMapping{{SourceTable: "orders"}, {SourceTable: "payments"}}},
	}}}
	r.mapped = r.mappedCollections()

	if !r.replicates("orders") || !r.replicates("payments") {
		t.Error("a mapped collection was filtered out")
	}
	if r.replicates("audit_log") {
		t.Error("an unmapped collection was let through")
	}
}

// TestATaskThatNamesNothingReplicatesEverything covers discovery, and that it
// stops short of the syncer's own bookkeeping — replicating the checkpoint
// collection would write the target's position back over itself.
func TestATaskThatNamesNothingReplicatesEverything(t *testing.T) {
	r := &Reader{Config: config.SyncConfig{}}
	r.mapped = r.mappedCollections()

	if !r.replicates("orders") {
		t.Error("discovery filtered out an ordinary collection")
	}
	for _, internal := range []string{"_sync_checkpoint", "_sync_direction", "system.views"} {
		if r.replicates(internal) {
			t.Errorf("%s was let through; it is the syncer's own bookkeeping", internal)
		}
	}
}
