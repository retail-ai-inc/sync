package mongodb

import (
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

func writeEvent(collection, id string) *domain.Event {
	return &domain.Event{
		NS:  domain.Namespace{DB: "shop", Object: collection},
		Op:  domain.OpInsert,
		Key: "_id=" + id,
		Payload: mongo.NewReplaceOneModel().
			SetFilter(bson.M{"_id": id}).
			SetReplacement(bson.M{"_id": id}).
			SetUpsert(true),
	}
}

// TestARunIsSplitIntoConsecutiveStretchesOfOneCollection is the grouping that
// keeps the run in order.
//
// BulkWrite addresses one collection, so a run spanning several needs one call
// each. Gathering every change to a collection into one group — which is what
// this used to do — puts all of one collection's writes before all of another's,
// and that is a reordering of the run. It was harmless while a run held at most
// one change per document and the batch was split into runs that separated
// them. It is not harmless now that a run is the whole batch, in the order it
// was read: a payment and the order it belongs to live in different
// collections, and which lands first decides whether the target is ever in a
// state the source was not.
func TestARunIsSplitIntoConsecutiveStretchesOfOneCollection(t *testing.T) {
	groups := groupByCollection([]*domain.Event{
		writeEvent("orders", "a"),
		writeEvent("payments", "b"),
		writeEvent("orders", "c"),
	})

	if len(groups) != 3 {
		t.Fatalf("groups = %d, want 3 — the second change to orders comes after "+
			"the one to payments and has to stay there", len(groups))
	}
	for i, want := range []string{"orders", "payments", "orders"} {
		if groups[i].collection != want {
			t.Errorf("group %d = %s, want %s", i, groups[i].collection, want)
		}
		if len(groups[i].models) != 1 {
			t.Errorf("group %d holds %d models, want 1", i, len(groups[i].models))
		}
	}
}

// TestConsecutiveChangesToOneCollectionShareARequest is the other half: the
// split is by stretch, not by event, so a batch that does not interleave still
// costs one request per collection.
func TestConsecutiveChangesToOneCollectionShareARequest(t *testing.T) {
	groups := groupByCollection([]*domain.Event{
		writeEvent("orders", "a"),
		writeEvent("orders", "c"),
		writeEvent("payments", "b"),
	})

	if len(groups) != 2 {
		t.Fatalf("groups = %d, want 2", len(groups))
	}
	if groups[0].collection != "orders" || len(groups[0].models) != 2 {
		t.Errorf("first group = %s with %d models, want orders with 2",
			groups[0].collection, len(groups[0].models))
	}
}

// TestTheCollectionsKeepTheOrderTheyFirstAppearedIn matters because a run may
// hold a change in one collection that a change in another depends on having
// landed — an order and the payment that refers to it.
func TestTheCollectionsKeepTheOrderTheyFirstAppearedIn(t *testing.T) {
	groups := groupByCollection([]*domain.Event{
		writeEvent("payments", "a"),
		writeEvent("orders", "b"),
	})

	if len(groups) != 2 || groups[0].collection != "payments" {
		t.Errorf("groups = %v, want payments first", groups)
	}
}

// TestAnEventWithoutAWriteModelIsSkipped keeps a heartbeat, which carries no
// model, from becoming an empty bulk write.
func TestAnEventWithoutAWriteModelIsSkipped(t *testing.T) {
	groups := groupByCollection([]*domain.Event{
		{NS: domain.Namespace{DB: "shop", Object: "orders"}, Heartbeat: true},
	})
	if len(groups) != 0 {
		t.Errorf("groups = %v, want none", groups)
	}
}

// ------------------------------------------------------------- target names

func TestTheTargetNameComesFromTheMapping(t *testing.T) {
	a := &Applier{Mappings: []config.DatabaseMapping{
		{Tables: []config.TableMapping{{SourceTable: "orders", TargetTable: "orders_archive"}}},
	}}
	if got := a.targetFor("orders"); got != "orders_archive" {
		t.Errorf("targetFor(orders) = %q, want orders_archive", got)
	}
}

func TestAMappingWithNoTargetKeepsTheSourceName(t *testing.T) {
	a := &Applier{Mappings: []config.DatabaseMapping{
		{Tables: []config.TableMapping{{SourceTable: "orders"}}},
	}}
	if got := a.targetFor("orders"); got != "orders" {
		t.Errorf("targetFor(orders) = %q, want orders", got)
	}
}

// TestAnUnmappedCollectionKeepsItsOwnName covers discovery, where every
// collection is replicated under the name it already has.
func TestAnUnmappedCollectionKeepsItsOwnName(t *testing.T) {
	a := &Applier{}
	if got := a.targetFor("invoices"); got != "invoices" {
		t.Errorf("targetFor(invoices) = %q, want invoices", got)
	}
}

// TestTheTransactionIsOnByDefault pins the default down.
//
// The safe setting has to be the one an operator gets without knowing to ask
// for it. Without a transaction a batch commits an operation at a time, so a
// partly failed bulk write — an ordinary occurrence, not a crash — leaves the
// target holding part of a batch, which can be a state the source was never in.
func TestTheTransactionIsOnByDefault(t *testing.T) {
	var a Applier
	if a.NoTransaction {
		t.Error("the zero value skips the transaction; the default has to be the safe one")
	}
}

// ------------------------------------------------- cross-collection writes

// TestAReplaceIsCarriedAsACrossCollectionWrite covers the mapping the 8.0
// bulkWrite command needs: the same write, addressed by a namespace given
// alongside it rather than by the collection the call was made on.
func TestAReplaceIsCarriedAsACrossCollectionWrite(t *testing.T) {
	upsert := true
	model := &mongo.ReplaceOneModel{
		Filter:      bson.M{"_id": "a"},
		Replacement: bson.M{"_id": "a", "amount": 1},
		Upsert:      &upsert,
	}

	got, err := clientModelOf(model)
	if err != nil {
		t.Fatalf("clientModelOf: %v", err)
	}
	replace, ok := got.(*mongo.ClientReplaceOneModel)
	if !ok {
		t.Fatalf("clientModelOf returned %T, want a ClientReplaceOneModel", got)
	}
	if replace.Upsert == nil || !*replace.Upsert {
		t.Error("the upsert was lost, so a change to a document the target lacks would be dropped")
	}
	if replace.Filter == nil || replace.Replacement == nil {
		t.Error("the filter or the replacement was lost")
	}
}

func TestEveryWriteShapeThisProducesCanBeCarried(t *testing.T) {
	upsert := true
	cases := []mongo.WriteModel{
		&mongo.ReplaceOneModel{Filter: bson.M{"_id": "a"}, Replacement: bson.M{}, Upsert: &upsert},
		&mongo.UpdateOneModel{Filter: bson.M{"_id": "a"}, Update: bson.M{"$set": bson.M{}}, Upsert: &upsert},
		&mongo.DeleteOneModel{Filter: bson.M{"_id": "a"}},
		&mongo.InsertOneModel{Document: bson.M{"_id": "a"}},
	}
	for _, model := range cases {
		if _, err := clientModelOf(model); err != nil {
			t.Errorf("clientModelOf(%T) = %v, want it carried", model, err)
		}
	}
}

// TestAnUnknownWriteShapeIsRefused keeps a write nobody asked for from being
// guessed at.
func TestAnUnknownWriteShapeIsRefused(t *testing.T) {
	_, err := clientModelOf(&mongo.UpdateManyModel{Filter: bson.M{}, Update: bson.M{}})
	if !domain.IsUnrecoverable(err) {
		t.Fatalf("clientModelOf returned %v for an unexpected shape, want an unrecoverable error", err)
	}
}

// TestAnOldTargetIsRecognisedOnce covers the fallback. A server before 8.0 has
// no bulkWrite command, and that is a reason to write per collection rather than
// a reason to stop.
func TestAnOldTargetIsRecognisedOnce(t *testing.T) {
	if lacksClientBulkWrite(nil) {
		t.Error("no error was read as a missing command")
	}
	if !lacksClientBulkWrite(errNoSuchCommand{}) {
		t.Error("a server reporting no such command was not recognised")
	}
	if lacksClientBulkWrite(errPlainFailure{}) {
		t.Error("an ordinary write failure was read as a missing command")
	}
}

type errNoSuchCommand struct{}

func (errNoSuchCommand) Error() string { return "(CommandNotFound) no such command: 'bulkWrite'" }

type errPlainFailure struct{}

func (errPlainFailure) Error() string { return "E11000 duplicate key error" }

// TestTheEscapeHatchSaysWhatItCosts covers the one environment variable that can
// turn off the guarantee the rest of this pipeline is built on.
//
// It was read once, deep in the applier, and nothing anywhere said it was on: a
// task applying batches without a transaction looked exactly like a task
// applying them with one, right up until a batch was interrupted part way and
// the position moved past changes the target did not hold.
func TestTheEscapeHatchSaysWhatItCosts(t *testing.T) {
	if got := describeNoTransaction(false, 7); got != "" {
		t.Errorf("a task with the default settings warned about them: %q", got)
	}

	warning := describeNoTransaction(true, 7)
	if warning == "" {
		t.Fatal("nothing was said about a task running without transactions")
	}
	for _, want := range []string{
		"SYNC_MONGO_NO_TRANSACTION", // what to unset
		"Task 7",                    // which task
		"SYNC_VERIFY_INTERVAL",      // what to turn on while it is set
	} {
		if !strings.Contains(warning, want) {
			t.Errorf("the warning does not mention %q, so it does not say what to do "+
				"about it: %q", want, warning)
		}
	}
}

// TestTheEscapeHatchIsOffUnlessItIsAskedFor guards the default. It is read from
// the environment, so a typo must not be taken as consent.
func TestTheEscapeHatchIsOffUnlessItIsAskedFor(t *testing.T) {
	for _, value := range []string{"", "0", "no", "false", "off", "yes please", " "} {
		t.Setenv("SYNC_MONGO_NO_TRANSACTION", value)
		if noTransaction() {
			t.Errorf("%q turned off batch atomicity; only an explicit yes may do that", value)
		}
	}
	for _, value := range []string{"1", "true", "yes", " TRUE ", "Yes"} {
		t.Setenv("SYNC_MONGO_NO_TRANSACTION", value)
		if !noTransaction() {
			t.Errorf("%q was not taken as asking for bare bulk writes", value)
		}
	}
}
