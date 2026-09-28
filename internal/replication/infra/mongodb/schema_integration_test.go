//go:build integration

package mongodb

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/test/harness"
)

// schemaApplier writes schema changes to the test target under a mapped name.
func schemaApplier(t *testing.T, source, target string) *Applier {
	t.Helper()
	tgt := connect(t, harness.MongoTarget)
	t.Cleanup(func() {
		_ = tgt.Database(targetDB).Collection(source).Drop(context.Background())
		_ = tgt.Database(targetDB).Collection(target).Drop(context.Background())
	})
	return &Applier{
		Client:         tgt,
		TargetDatabase: targetDB,
		Mappings: []config.DatabaseMapping{{
			Tables: []config.TableMapping{{SourceTable: source, TargetTable: target}},
		}},
	}
}

func plannedSchemaEvent(t *testing.T, collection, kind string, description bson.D) *domain.Event {
	t.Helper()
	change, decision, reason := planSchemaChange(schemaEvent(t, kind, description), collection)
	if decision != ddlApply {
		t.Fatalf("a %s was not planned to be applied: %s", kind, reason)
	}
	return &domain.Event{
		NS:              domain.Namespace{DB: sourceDB, Object: collection},
		Op:              domain.OpSchema,
		Payload:         change,
		EndsTransaction: true,
	}
}

func TestAnIndexIsCreatedAndDroppedOnTheMappedCollectionAndSurvivesAReplay(t *testing.T) {
	source := harness.UniqueName("orders")
	archive := source + "_archive"
	applier := schemaApplier(t, source, archive)
	ctx := context.Background()

	spec := bson.D{
		{Key: "v", Value: 2},
		{Key: "key", Value: bson.D{{Key: "customer", Value: 1}}},
		{Key: "name", Value: "customer_1"},
	}
	create := plannedSchemaEvent(t, source, "createIndexes", bson.D{{Key: "indexes", Value: bson.A{spec}}})
	drop := plannedSchemaEvent(t, source, "dropIndexes", bson.D{{Key: "indexes", Value: bson.A{spec}}})

	// Applied twice, as a replayed batch applies it; the second must not fail the batch.
	for pass := 1; pass <= 2; pass++ {
		if _, err := applier.Apply(ctx, [][]*domain.Event{{create}}, domain.Position{}); err != nil {
			t.Fatalf("create the index (pass %d): %v", pass, err)
		}
	}
	if !indexesOn(t, applier.Client, targetDB, archive)["customer_1"] {
		t.Errorf("%s has no customer_1 after the createIndexes was applied", archive)
	}
	names, err := applier.Client.Database(targetDB).ListCollectionNames(ctx, bson.M{"name": source})
	if err != nil {
		t.Fatalf("list the target's collections: %v", err)
	}
	if len(names) != 0 {
		t.Errorf("the index was also created on %s, the source's own name", source)
	}

	for pass := 1; pass <= 2; pass++ {
		if _, err := applier.Apply(ctx, [][]*domain.Event{{drop}}, domain.Position{}); err != nil {
			t.Fatalf("drop the index (pass %d): %v", pass, err)
		}
	}
	if indexesOn(t, applier.Client, targetDB, archive)["customer_1"] {
		t.Errorf("%s still has customer_1 after the dropIndexes was applied", archive)
	}
}

func TestASchemaChangeTheTargetRefusesFailsTheBatch(t *testing.T) {
	source := harness.UniqueName("orders")
	applier := schemaApplier(t, source, source+"_archive")

	modify := plannedSchemaEvent(t, source, "modify", bson.D{
		{Key: "validator", Value: bson.D{{Key: "customer", Value: bson.D{{Key: "$exists", Value: true}}}}},
	})
	// Nil here moves the position past a schema change the target never took.
	if _, err := applier.Apply(context.Background(), [][]*domain.Event{{modify}}, domain.Position{}); err == nil {
		t.Error("a collMod on a collection the target does not have was reported applied")
	}
}
