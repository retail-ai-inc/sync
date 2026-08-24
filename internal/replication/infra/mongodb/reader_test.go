package mongodb

import (
	"strings"
	"testing"

	"github.com/sirupsen/logrus"

	"go.mongodb.org/mongo-driver/v2/bson"

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

func changeDoc(db, coll, op string, key bson.D) bson.D {
	return bson.D{
		{Key: "_id", Value: bson.D{{Key: "_data", Value: "8264"}}},
		{Key: "operationType", Value: op},
		{Key: "ns", Value: bson.D{{Key: "db", Value: db}, {Key: "coll", Value: coll}}},
		{Key: "documentKey", Value: key},
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

// ------------------------------------------------------- transaction buffering

// txEvent renders one event of a multi-document transaction.
func txEvent(t *testing.T, coll, id, session string, number int64) bson.Raw {
	t.Helper()
	doc := changeDoc("shop", coll, "insert", bson.D{{Key: "_id", Value: id}})
	doc = append(doc,
		bson.E{Key: "fullDocument", Value: bson.D{{Key: "_id", Value: id}}},
		bson.E{Key: "lsid", Value: bson.D{{Key: "id", Value: session}}},
		bson.E{Key: "txnNumber", Value: number})
	return rawEvent(t, doc)
}

func bufferingReader(t *testing.T) *Reader {
	t.Helper()
	r := &Reader{Config: config.SyncConfig{}, Logger: quietLog()}
	r.conv = &MongoDBSyncer{cfg: r.Config, logger: r.Logger}
	r.mapped = r.mappedCollections()
	return r
}

// TestBackToBackTransactionsAreHandedOver is the defect a real workload found.
//
// A transaction's end used to be judged by an event arriving that belonged to no
// transaction. Fifty transactions back-to-back therefore handed over nothing at
// all: every event extended one buffer, the condition was never met, and the
// reader span having read a hundred documents it never passed on. Nothing in the
// unit tests fed it two consecutive transactions, so nothing caught it.
func TestBackToBackTransactionsAreHandedOver(t *testing.T) {
	r := bufferingReader(t)

	// Transaction one: an order and its payment.
	if err := r.take(txEvent(t, "orders", "o1", "s1", 1)); err != nil {
		t.Fatalf("take: %v", err)
	}
	if err := r.take(txEvent(t, "payments", "p1", "s1", 1)); err != nil {
		t.Fatalf("take: %v", err)
	}
	if len(r.ready) != 0 {
		t.Error("an open transaction was handed over before it ended")
	}

	// Transaction two starts, which is what proves the first has ended.
	if err := r.take(txEvent(t, "orders", "o2", "s1", 2)); err != nil {
		t.Fatalf("take: %v", err)
	}

	if len(r.ready) != 2 {
		t.Fatalf("handed over %d events, want the two of the finished transaction", len(r.ready))
	}
	if !r.ready[1].EndsTransaction {
		t.Error("the finished transaction's last event is not marked as the boundary")
	}
	if r.ready[0].EndsTransaction {
		t.Error("the first event of a transaction was marked as a boundary, so a batch could be cut inside it")
	}
	if len(r.open) != 1 {
		t.Errorf("the new transaction holds %d events, want 1", len(r.open))
	}
}

// TestAnIdleStreamSealsTheOpenTransaction is the other half: the last
// transaction of a run has nothing behind it to prove it ended, and a change
// stream delivers a committed transaction's events contiguously — so an
// exhausted cursor means it is complete.
func TestAnIdleStreamSealsTheOpenTransaction(t *testing.T) {
	r := bufferingReader(t)
	if err := r.take(txEvent(t, "orders", "o1", "s1", 1)); err != nil {
		t.Fatalf("take: %v", err)
	}
	if len(r.ready) != 0 {
		t.Fatal("an open transaction was handed over early")
	}

	r.seal()

	if len(r.ready) != 1 || !r.ready[0].EndsTransaction {
		t.Errorf("sealing left %d events ready, want one marked as the boundary", len(r.ready))
	}
	if len(r.open) != 0 || r.openID != "" {
		t.Error("sealing left the transaction open")
	}
}

// TestAStandaloneChangeClosesAnOpenTransaction covers the mixed stream: an
// ordinary write after a transaction proves the transaction ended.
func TestAStandaloneChangeClosesAnOpenTransaction(t *testing.T) {
	r := bufferingReader(t)
	if err := r.take(txEvent(t, "orders", "o1", "s1", 1)); err != nil {
		t.Fatalf("take: %v", err)
	}

	standalone := rawEvent(t, append(
		changeDoc("shop", "orders", "insert", bson.D{{Key: "_id", Value: "x1"}}),
		bson.E{Key: "fullDocument", Value: bson.D{{Key: "_id", Value: "x1"}}}))
	if err := r.take(standalone); err != nil {
		t.Fatalf("take: %v", err)
	}

	if len(r.ready) != 2 {
		t.Fatalf("handed over %d events, want the transaction plus the standalone change", len(r.ready))
	}
	if !r.ready[0].EndsTransaction {
		t.Error("the transaction's last event is not marked as the boundary")
	}
	if !r.ready[1].EndsTransaction {
		t.Error("a standalone change is its own boundary and was not marked as one")
	}
	if len(r.open) != 0 {
		t.Error("the transaction was left open")
	}
}

// TestEveryEventOfOneTransactionSharesItsBatch is the guarantee all of this
// exists for: the order and the payment written as one act reach the target as
// one act.
func TestEveryEventOfOneTransactionSharesItsBatch(t *testing.T) {
	r := bufferingReader(t)
	for _, e := range []bson.Raw{
		txEvent(t, "orders", "o1", "s1", 7),
		txEvent(t, "payments", "p1", "s1", 7),
		txEvent(t, "accounts", "a1", "s1", 7),
	} {
		if err := r.take(e); err != nil {
			t.Fatalf("take: %v", err)
		}
	}
	r.seal()

	boundaries := 0
	for _, e := range r.ready {
		if e.EndsTransaction {
			boundaries++
		}
	}
	if len(r.ready) != 3 {
		t.Fatalf("handed over %d events, want 3", len(r.ready))
	}
	if boundaries != 1 {
		t.Errorf("%d of the three events are batch boundaries, want exactly the last one", boundaries)
	}
	if !r.ready[2].EndsTransaction {
		t.Error("the boundary is not the last event")
	}
}

func quietLog() *logrus.Logger {
	l := logrus.New()
	l.SetLevel(logrus.PanicLevel)
	return l
}
