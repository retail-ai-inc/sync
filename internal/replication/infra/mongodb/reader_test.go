package mongodb

import (
	"fmt"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

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

// Two changes to one document may never be reordered against each other, and
// on a sharded collection two documents can share an _id while differing in
// the shard key.
func TestTheOrderingKeyCarriesTheShardKey(t *testing.T) {
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

// TestOneStreamStillOnlyReplicatesWhatTheTaskNames is what makes a single
// deployment-wide stream safe: it sees everything and carries only the mapped
// collections.
func TestOneStreamStillOnlyReplicatesWhatTheTaskNames(t *testing.T) {
	r := readerFor([]config.DatabaseMapping{{
		SourceDatabase: "shop",
		Tables:         []config.TableMapping{{SourceTable: "orders"}, {SourceTable: "payments"}},
	}})

	if !r.replicates(ns("shop", "orders")) || !r.replicates(ns("shop", "payments")) {
		t.Error("a mapped collection was filtered out")
	}
	if r.replicates(ns("shop", "audit_log")) {
		t.Error("an unmapped collection was let through")
	}
}

func ns(db, object string) domain.Namespace {
	return domain.Namespace{DB: db, Object: object}
}

func readerFor(mappings []config.DatabaseMapping) *Reader {
	r := &Reader{Config: config.SyncConfig{
		Type:             "mongodb",
		SourceConnection: "mongodb://u:p@h:27017/shop",
		Mappings:         mappings,
	}}
	r.mapped = r.mappedCollections()
	r.databases = r.mappedDatabaseSet()
	return r
}

// TestAnotherDatabaseIsNotReplicated is the defect a real cluster found within
// seconds of the chunk migration test being pointed at it.
func TestAnotherDatabaseIsNotReplicated(t *testing.T) {
	r := readerFor([]config.DatabaseMapping{{
		SourceDatabase: "shop",
		Tables:         []config.TableMapping{{SourceTable: "orders"}},
	}})

	if !r.replicates(ns("shop", "orders")) {
		t.Fatal("the mapped namespace was filtered out")
	}
	if r.replicates(ns("warehouse", "orders")) {
		t.Error("another tenant's collection of the same name was let through")
	}
	if r.replicates(ns("shop_dst", "orders")) {
		t.Error("the target's own writes were let through, so the syncer reads back " +
			"what it just wrote")
	}
}

// TestTheServerIsAskedToFilterTheDatabasesToo keeps the whole cluster's traffic
// from being pulled across the wire only to be dropped here.
func TestTheServerIsAskedToFilterTheDatabasesToo(t *testing.T) {
	r := readerFor([]config.DatabaseMapping{
		{SourceDatabase: "shop", Tables: []config.TableMapping{{SourceTable: "orders"}}},
		{SourceDatabase: "ledger", Tables: []config.TableMapping{{SourceTable: "entries"}}},
	})

	dbs := r.mappedDatabases()
	if len(dbs) != 2 {
		t.Fatalf("the server filter names %v, want both databases", dbs)
	}
}

// TestAMappingWithoutADatabaseUsesTheConnections covers the shape a
// single-database task has always had, so upgrading does not silently start
// filtering everything out.
func TestAMappingWithoutADatabaseUsesTheConnections(t *testing.T) {
	r := readerFor([]config.DatabaseMapping{
		{Tables: []config.TableMapping{{SourceTable: "orders"}}},
	})

	if !r.replicates(ns("shop", "orders")) {
		t.Error("the database from the connection string was not used")
	}
	if r.replicates(ns("warehouse", "orders")) {
		t.Error("another database was let through")
	}
}

// TestATaskThatNamesNothingReplicatesEverything covers discovery, and that it
// stops short of the syncer's own bookkeeping — replicating the checkpoint
// collection would write the target's position back over itself.
func TestATaskThatNamesNothingReplicatesEverythingInItsDatabase(t *testing.T) {
	r := readerFor([]config.DatabaseMapping{{SourceDatabase: "shop"}})

	if !r.replicates(ns("shop", "orders")) {
		t.Error("discovery filtered out an ordinary collection")
	}
	if r.replicates(ns("warehouse", "orders")) {
		t.Error("discovery reached into another database")
	}
	for _, internal := range []string{"_sync_checkpoint", "_sync_direction", "system.views"} {
		if r.replicates(ns("shop", internal)) {
			t.Errorf("%s was let through; it is the syncer's own bookkeeping", internal)
		}
	}
}

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
	r := readerFor([]config.DatabaseMapping{{SourceDatabase: "shop"}})
	r.Logger = quietLog()
	r.conv = &MongoDBSyncer{cfg: r.Config, logger: r.Logger}
	return r
}

// A transaction's end used to be judged by an event arriving that belonged to
// no transaction.
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

// Every write here is an upsert, because replication is replayed.
func TestTheWriteFilterCarriesTheShardKey(t *testing.T) {
	syncer := &MongoDBSyncer{logger: logrus.New()}

	key := bson.D{
		{Key: "_id", Value: "abc"},
		{Key: "merchant_id", Value: 42},
	}
	events := map[string]bson.D{
		"insert": append(changeDoc("shop", "orders", "insert", key),
			bson.E{Key: "fullDocument", Value: bson.D{
				{Key: "_id", Value: "abc"}, {Key: "merchant_id", Value: 42},
			}}),
		"replace": append(changeDoc("shop", "orders", "replace", key),
			bson.E{Key: "fullDocument", Value: bson.D{
				{Key: "_id", Value: "abc"}, {Key: "merchant_id", Value: 42},
			}}),
		"update": append(changeDoc("shop", "orders", "update", key),
			bson.E{Key: "updateDescription", Value: bson.D{
				{Key: "updatedFields", Value: bson.D{{Key: "amount", Value: 100}}},
			}}),
		"delete": changeDoc("shop", "orders", "delete", key),
	}

	for op, doc := range events {
		t.Run(op, func(t *testing.T) {
			raw := rawEvent(t, doc)

			model, err := syncer.convertRawBSONToWriteModel(raw, "shop", "orders")
			if err != nil {
				t.Fatalf("convert: %v", err)
			}
			filter := filterIn(t, model)

			if filter["_id"] != "abc" {
				t.Errorf("filter = %v, want it to carry the _id", filter)
			}
			if got, ok := filter["merchant_id"]; !ok {
				t.Errorf("filter = %v, want it to carry the shard key too — without it "+
					"an upsert is refused and a delete is broadcast", filter)
			} else if fmt.Sprint(got) != "42" {
				t.Errorf("filter[merchant_id] = %v, want 42", got)
			}
		})
	}
}

// TestAnUnshardedChangeIsStillFilteredByItsIDAlone keeps the common case as it
// was: documentKey is just the _id there, so the filter is what it always was.
func TestAnUnshardedChangeIsStillFilteredByItsIDAlone(t *testing.T) {
	syncer := &MongoDBSyncer{logger: logrus.New()}

	raw := rawEvent(t, changeDoc("shop", "orders", "delete", bson.D{
		{Key: "_id", Value: "abc"},
	}))
	model, err := syncer.convertRawBSONToWriteModel(raw, "shop", "orders")
	if err != nil {
		t.Fatalf("convert: %v", err)
	}

	filter := filterIn(t, model)
	if len(filter) != 1 || filter["_id"] != "abc" {
		t.Errorf("filter = %v, want just the _id", filter)
	}
}

func filterIn(t *testing.T, model mongo.WriteModel) bson.M {
	t.Helper()

	var filter interface{}
	switch m := model.(type) {
	case *mongo.ReplaceOneModel:
		filter = m.Filter
	case *mongo.UpdateOneModel:
		filter = m.Filter
	case *mongo.DeleteOneModel:
		filter = m.Filter
	default:
		t.Fatalf("model is a %T, which carries no filter", model)
	}

	out, ok := filter.(bson.M)
	if !ok {
		t.Fatalf("filter is a %T, want a bson.M", filter)
	}
	return out
}

// TestAnUpdateWithoutTheDocumentIsStillApplied covers the path taken when the
// document was deleted between the update and the lookup that would have
// fetched it: the change itself is in the event, so it can be applied without.
func TestAnUpdateWithoutTheDocumentIsStillApplied(t *testing.T) {
	syncer := &MongoDBSyncer{logger: logrus.New()}

	raw := rawEvent(t, append(
		changeDoc("shop", "orders", "update", bson.D{{Key: "_id", Value: "abc"}}),
		bson.E{Key: "updateDescription", Value: bson.D{
			{Key: "updatedFields", Value: bson.D{{Key: "amount", Value: 100}}},
			{Key: "removedFields", Value: bson.A{"note"}},
		}},
	))

	model, err := syncer.convertRawBSONToWriteModel(raw, "shop", "orders")
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	update, ok := model.(*mongo.UpdateOneModel)
	if !ok {
		t.Fatalf("model is a %T, want an UpdateOneModel", model)
	}

	rendered := fmt.Sprint(update.Update)
	if !strings.Contains(rendered, "amount") {
		t.Errorf("update = %v, want the changed field in a $set", update.Update)
	}
	if !strings.Contains(rendered, "note") {
		t.Errorf("update = %v, want the removed field in an $unset", update.Update)
	}
}

// The stream is opened with fullDocument=updateLookup.
func TestANullFullDocumentFallsBackToTheDescription(t *testing.T) {
	syncer := &MongoDBSyncer{logger: logrus.New()}

	raw := rawEvent(t, append(
		changeDoc("shop", "orders", "update", bson.D{{Key: "_id", Value: 1}}),
		bson.E{Key: "fullDocument", Value: nil},
		bson.E{Key: "updateDescription", Value: bson.D{
			{Key: "updatedFields", Value: bson.D{{Key: "n", Value: 1}}},
		}},
	))

	model, err := syncer.convertRawBSONToWriteModel(raw, "shop", "orders")
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	if replace, ok := model.(*mongo.ReplaceOneModel); ok {
		t.Fatalf("built a replacement of %v from a null document", replace.Replacement)
	}
	update, ok := model.(*mongo.UpdateOneModel)
	if !ok {
		t.Fatalf("model is a %T, want an UpdateOneModel from the description", model)
	}
	if !strings.Contains(fmt.Sprint(update.Update), "n") {
		t.Errorf("update = %v, want the changed field", update.Update)
	}
}

// TestTheCapturedCollectionCountIsPublished is Debezium's CapturedTables, and
// it earns its place by catching the change nothing else reports: a mapping
// edit that quietly drops a collection.
func TestTheCapturedCollectionCountIsPublished(t *testing.T) {
	r := &Reader{Config: config.SyncConfig{
		Mappings: []config.DatabaseMapping{
			{Tables: []config.TableMapping{
				{SourceTable: "orders", TargetTable: "orders"},
				{SourceTable: "users", TargetTable: "users"},
			}},
			{Tables: []config.TableMapping{
				{SourceTable: "payments", TargetTable: "payments"},
			}},
		},
	}}

	if got := r.capturedCollections(); got != 3 {
		t.Errorf("capturedCollections() = %d, want 3 across both mappings", got)
	}
}

// TestATaskWithNoMappingsCapturesNothing keeps the count honest rather than
// convenient: zero is the right answer and it is worth alerting on.
func TestATaskWithNoMappingsCapturesNothing(t *testing.T) {
	r := &Reader{Config: config.SyncConfig{}}

	if got := r.capturedCollections(); got != 0 {
		t.Errorf("capturedCollections() = %d, want 0", got)
	}
}
