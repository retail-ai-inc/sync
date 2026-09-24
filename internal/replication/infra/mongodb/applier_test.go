package mongodb

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

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

// BulkWrite addresses one collection, so a run spanning several needs one call
// each.
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

// TestAnUnmappedCollectionKeepsItsOwnName covers discovery.
func TestAnUnmappedCollectionKeepsItsOwnName(t *testing.T) {
	a := &Applier{}
	if got := a.targetFor("invoices"); got != "invoices" {
		t.Errorf("targetFor(invoices) = %q, want invoices", got)
	}
}

// TestTheTransactionIsOnByDefault pins the default down.
func TestTheTransactionIsOnByDefault(t *testing.T) {
	var a Applier
	if a.NoTransaction {
		t.Error("the zero value skips the transaction; the default has to be the safe one")
	}
}

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

// TestAnOldTargetIsRecognisedOnce covers the fallback.
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

// TestTheEscapeHatchSaysWhatItCosts covers the one environment variable that
// can turn off the guarantee the rest of this pipeline is built on.
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

// TestTheEscapeHatchIsOffUnlessItIsAskedFor guards the default.
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

// A document read back from the source the long way is masked exactly as one
// that arrived on the stream. Without this a task with field security would
// write the value in the clear whenever a change could not be applied as a
// delta.
func TestADocumentReadBackIsMaskedLikeOneFromTheStream(t *testing.T) {
	a := &Applier{Mask: func(database, collection string, value interface{}) interface{} {
		document, ok := value.(bson.M)
		if !ok {
			return value
		}
		document["card"] = "****"
		return document
	}}

	masked := a.mask("shop", "payments", bson.M{"card": "4111111111111111"})
	if masked["card"] != "****" {
		t.Errorf("card = %v, want it masked", masked["card"])
	}

	// No masking configured leaves the document as it is, rather than as
	// nothing.
	plain := (&Applier{}).mask("shop", "payments", bson.M{"card": "4111"})
	if plain["card"] != "4111" {
		t.Errorf("card = %v, want it untouched", plain["card"])
	}
}

// TestTheSourceIsNeverReadThroughTheTargetsSession records the wiring that made
// the whole-document fallback fail every time it was needed.
//
// The batch's transaction session belongs to the target client. A session used
// on any other client is refused by the driver before a single byte goes out,
// with "session was not created by this client" — and the source reads of the
// fallback were being handed exactly that context. The retry loop then repeated
// it for ever: the task stayed up, the position stopped, and nothing said why.
func TestTheSourceIsNeverReadThroughTheTargetsSession(t *testing.T) {
	// Two clients, as production has: the target starts the session, the source
	// is a different connection. Neither needs a server — the session check
	// happens before any I/O — so server selection is kept short for the reads
	// that do go out.
	quick := options.Client().SetServerSelectionTimeout(300 * time.Millisecond)
	target, err := mongo.Connect(quick.ApplyURI("mongodb://127.0.0.1:1/?directConnection=true"))
	if err != nil {
		t.Fatalf("connect the target: %v", err)
	}
	source, err := mongo.Connect(quick.ApplyURI("mongodb://127.0.0.1:2/?directConnection=true"))
	if err != nil {
		t.Fatalf("connect the source: %v", err)
	}
	t.Cleanup(func() {
		_ = target.Disconnect(context.Background())
		_ = source.Disconnect(context.Background())
	})

	applier := &Applier{
		Client:         target,
		Source:         source,
		TargetDatabase: "shop",
		Mappings: []config.DatabaseMapping{{
			SourceDatabase: "shop", TargetDatabase: "shop",
		}},
	}

	// One change that can only be applied by reading the document whole.
	run := []*domain.Event{{
		NS:      domain.Namespace{DB: "shop", Object: "orders"},
		Op:      domain.OpUpdate,
		Key:     "_id=1",
		Payload: &fullDocumentRead{filter: bson.M{"_id": "1"}, reason: reasonUndescribed},
	}}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, err = applier.Apply(ctx, [][]*domain.Event{run}, domain.Position{})

	// It fails — there is no server on either port — but it must not fail
	// because the source was read with the target's session.
	if err == nil {
		t.Fatal("Apply succeeded against two unreachable servers")
	}
	if strings.Contains(err.Error(), "session was not created by this client") {
		t.Errorf("the source was read through the target's session: %v", err)
	}
	if errors.Is(err, mongo.ErrWrongClient) {
		t.Errorf("the source was read through the target's session: %v", err)
	}
}

func TestASchemaChangeSharingABatchFailsItBeforeAnyWrite(t *testing.T) {
	client, err := mongo.Connect(options.Client().ApplyURI(
		"mongodb://127.0.0.1:1/?directConnection=true&serverSelectionTimeoutMS=200"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	applier := &Applier{Client: client, TargetDatabase: "shop", NoTransaction: true}

	schema := func() *domain.Event {
		return &domain.Event{
			NS: domain.Namespace{DB: "shop", Object: "orders"},
			Op: domain.OpSchema,
			Payload: schemaChange{
				Kind:       "createIndexes",
				Collection: "orders",
				Command: bson.D{{Key: "createIndexes", Value: "orders"}, {Key: "indexes", Value: bson.A{
					bson.D{{Key: "key", Value: bson.D{{Key: "customer", Value: 1}}}, {Key: "name", Value: "customer_1"}},
				}}},
				Describe: "create 1 index(es) on orders",
			},
		}
	}
	for _, c := range []struct {
		name string
		runs [][]*domain.Event
	}{
		{"row after", [][]*domain.Event{{schema(), writeEvent("orders", "a")}}},
		{"row before", [][]*domain.Event{{writeEvent("orders", "a"), schema()}}},
		{"row in another run", [][]*domain.Event{{schema()}, {writeEvent("orders", "a")}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := applier.Apply(ctx, c.runs, domain.Position{})
			// Anything else writes the rows, passes over the DDL and moves the position past it.
			if !domain.IsUnrecoverable(err) {
				t.Errorf("Apply = %v, want the batch refused as a pipeline bug", err)
			}
		})
	}
}
