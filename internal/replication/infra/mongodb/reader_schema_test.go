package mongodb

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

func ordersReader(t *testing.T) *Reader {
	t.Helper()
	r := readerFor([]config.DatabaseMapping{{
		SourceDatabase: "shop",
		Tables:         []config.TableMapping{{SourceTable: "orders"}},
	}})
	r.Logger = quietLog()
	r.conv = &MongoDBSyncer{cfg: r.Config, logger: r.Logger}
	return r
}

func addedIndex(t *testing.T) bson.Raw {
	t.Helper()
	return schemaEvent(t, "createIndexes", bson.D{
		{Key: "indexes", Value: bson.A{
			bson.D{{Key: "key", Value: bson.D{{Key: "customer", Value: 1}}}, {Key: "name", Value: "customer_1"}},
		}},
	})
}

// A failure means a transaction the source committed before a DDL is delivered after it, or in its batch.
func TestASchemaChangeIsDeliveredAfterTheTransactionBeforeIt(t *testing.T) {
	r := ordersReader(t)
	if err := r.take(txEvent(t, "orders", "o1", "s1", 1)); err != nil {
		t.Fatalf("take the transaction: %v", err)
	}
	if err := r.take(addedIndex(t)); err != nil {
		t.Fatalf("take the schema change: %v", err)
	}

	if len(r.ready) != 2 {
		t.Fatalf("handed over %d events, want the transaction's and the schema change's", len(r.ready))
	}
	if tx := r.ready[0]; tx.Op != domain.OpInsert || !tx.EndsTransaction {
		t.Errorf("first = %v (ends=%v), want the transaction's insert, sealed", tx.Op, tx.EndsTransaction)
	}
	ddl := r.ready[1]
	if ddl.Op != domain.OpSchema || !ddl.EndsTransaction {
		t.Errorf("second = %v (ends=%v), want the schema change as its own boundary", ddl.Op, ddl.EndsTransaction)
	}
	if change, ok := ddl.Payload.(schemaChange); !ok || change.Kind != "createIndexes" || change.Collection != "orders" {
		t.Errorf("payload = %#v, want the createIndexes on orders", ddl.Payload)
	}
	if ddl.Pos.Payload == "" {
		t.Error("the schema change carries no position, so it could not be checkpointed")
	}
	if len(r.open) != 0 || r.openID != "" {
		t.Errorf("a transaction is still open (%d events, id %q)", len(r.open), r.openID)
	}
}

// A failure means a created collection reaches the target without its options, or with an _id index it cannot take.
func TestACreatedCollectionIsDeliveredWithItsOptions(t *testing.T) {
	r := ordersReader(t)
	raw := schemaEvent(t, "create", bson.D{
		{Key: "idIndex", Value: bson.D{{Key: "v", Value: 2}, {Key: "key", Value: bson.D{{Key: "_id", Value: 1}}}}},
		{Key: "validator", Value: bson.D{{Key: "total", Value: bson.D{{Key: "$gte", Value: 0}}}}},
	})
	if err := r.take(raw); err != nil {
		t.Fatalf("take: %v", err)
	}

	if len(r.ready) != 1 {
		t.Fatalf("handed over %d events, want the create", len(r.ready))
	}
	change, ok := r.ready[0].Payload.(schemaChange)
	if !ok {
		t.Fatalf("payload is a %T, want a schema change", r.ready[0].Payload)
	}
	keys := make([]string, 0, len(change.Command))
	for _, element := range change.Command {
		keys = append(keys, element.Key)
	}
	if len(keys) != 2 || keys[0] != "create" || change.Command[0].Value != "orders" || keys[1] != "validator" {
		t.Errorf("command = %v, want create orders with its validator and no idIndex", change.Command)
	}
}

// A failure means a destructive change to a mapped collection is replicated, or passed over, unattended.
func TestADestructiveChangeToAMappedCollectionStopsTheReader(t *testing.T) {
	for _, kind := range []string{"drop", "rename"} {
		t.Run(kind, func(t *testing.T) {
			r := ordersReader(t)
			err := r.take(schemaEvent(t, kind, nil))
			if !domain.IsUnrecoverable(err) {
				t.Fatalf("take = %v, want an unrecoverable stop", err)
			}
			if len(r.ready) != 0 {
				t.Errorf("%d events were handed over for a change that was refused", len(r.ready))
			}
		})
	}
}

// A failure means a sharding change is applied to the target, or holds up the transaction around it.
func TestAShardingChangeIsPassedOver(t *testing.T) {
	r := ordersReader(t)
	if err := r.take(txEvent(t, "orders", "o1", "s1", 1)); err != nil {
		t.Fatalf("take the transaction: %v", err)
	}
	if err := r.take(schemaEvent(t, "shardCollection", nil)); err != nil {
		t.Fatalf("take = %v, want the change passed over", err)
	}
	if len(r.ready) != 0 {
		t.Errorf("%d events were handed over, want none", len(r.ready))
	}
	if len(r.open) != 1 {
		t.Errorf("the open transaction holds %d events, want its 1", len(r.open))
	}
}

// A failure means another collection's schema changes stop, or reshape, this task's target.
func TestASchemaChangeToAnUnmappedCollectionIsIgnored(t *testing.T) {
	for _, kind := range []string{"createIndexes", "drop"} {
		t.Run(kind, func(t *testing.T) {
			r := ordersReader(t)
			raw := rawEvent(t, bson.D{
				{Key: "_id", Value: bson.D{{Key: "_data", Value: "8264"}}},
				{Key: "operationType", Value: kind},
				{Key: "ns", Value: bson.D{{Key: "db", Value: "shop"}, {Key: "coll", Value: "customers"}}},
			})
			if err := r.take(raw); err != nil {
				t.Fatalf("take = %v, want it ignored", err)
			}
			if len(r.ready) != 0 {
				t.Errorf("%d events were handed over, want none", len(r.ready))
			}
		})
	}
}
