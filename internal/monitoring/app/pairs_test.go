package app

import (
	"testing"

	"github.com/retail-ai-inc/sync/internal/platform/config"
)

// Which tables the reconciler compares. It follows the same rule replication
// itself does: the ones the task lists, or every one the source has when it
// lists none. Getting this wrong is quiet in both directions -- comparing a
// table nobody replicates reports differences that are meant to be there, and
// missing one leaves it unverified.

func pairsOf(tables ...[2]string) []config.TableMapping {
	mappings := make([]config.TableMapping, 0, len(tables))
	for _, pair := range tables {
		mappings = append(mappings, config.TableMapping{
			SourceTable: pair[0], TargetTable: pair[1],
		})
	}
	return mappings
}

func TestConfiguredPairsTakesTheNamesTheTaskGives(t *testing.T) {
	task := config.SyncConfig{Mappings: []config.DatabaseMapping{{
		Tables: pairsOf([2]string{"orders", "orders_bk"}, [2]string{"payments", "payments_bk"}),
	}}}

	pairs := configuredPairs(task)

	if len(pairs) != 2 {
		t.Fatalf("got %d pairs, want 2: %v", len(pairs), pairs)
	}
	if pairs[0].source != "orders" || pairs[0].target != "orders_bk" {
		t.Errorf("first pair = %v", pairs[0])
	}
}

// TestAMappingWithNoTargetComparesTheSameName, which is what the task form
// produces when the two sides match. Skipping it would leave the table
// unverified, and defaulting the target to empty would compare against a table
// called "".
func TestAMappingWithNoTargetComparesTheSameName(t *testing.T) {
	task := config.SyncConfig{Mappings: []config.DatabaseMapping{{
		Tables: pairsOf([2]string{"orders", ""}),
	}}}

	pairs := configuredPairs(task)

	if len(pairs) != 1 {
		t.Fatalf("got %d pairs, want 1", len(pairs))
	}
	if pairs[0].target != "orders" {
		t.Errorf("target = %q, want the source's name", pairs[0].target)
	}
}

// TestAMappingWithNoSourceIsSkipped: there is nothing to read from, so a pair
// built from it would compare an empty table name against something.
func TestAMappingWithNoSourceIsSkipped(t *testing.T) {
	task := config.SyncConfig{Mappings: []config.DatabaseMapping{{
		Tables: pairsOf([2]string{"", "orders_bk"}, [2]string{"payments", "payments_bk"}),
	}}}

	pairs := configuredPairs(task)

	if len(pairs) != 1 || pairs[0].source != "payments" {
		t.Errorf("got %v, want only the pair with a source", pairs)
	}
}

// TestATaskThatListsNoTablesConfiguresNoPairs is the signal to go and discover
// them. Answering with an empty-but-present list would compare nothing and
// report the task as verified.
func TestATaskThatListsNoTablesConfiguresNoPairs(t *testing.T) {
	for name, task := range map[string]config.SyncConfig{
		"no mappings":            {},
		"mapping with no tables": {Mappings: []config.DatabaseMapping{{}}},
		"tables with no names":   {Mappings: []config.DatabaseMapping{{Tables: pairsOf([2]string{"", ""})}}},
	} {
		t.Run(name, func(t *testing.T) {
			if pairs := configuredPairs(task); len(pairs) != 0 {
				t.Errorf("got %v, want nothing so the tables are discovered", pairs)
			}
		})
	}
}

func TestPairsAcrossSeveralMappingsAreAllKept(t *testing.T) {
	task := config.SyncConfig{Mappings: []config.DatabaseMapping{
		{Tables: pairsOf([2]string{"orders", ""})},
		{Tables: pairsOf([2]string{"payments", ""}, [2]string{"refunds", ""})},
	}}

	if pairs := configuredPairs(task); len(pairs) != 3 {
		t.Errorf("got %d pairs across two mappings, want 3: %v", len(pairs), pairs)
	}
}

// TestSamePairsPairsEachNameWithItself is what a task listing no tables
// replicates into, so it is what the comparison has to follow.
func TestSamePairsPairsEachNameWithItself(t *testing.T) {
	pairs := samePairs([]string{"orders", "payments"})

	if len(pairs) != 2 {
		t.Fatalf("got %d pairs, want 2", len(pairs))
	}
	for _, pair := range pairs {
		if pair.source != pair.target {
			t.Errorf("%v does not compare a table against itself", pair)
		}
	}
}

func TestSamePairsOfNothing(t *testing.T) {
	if pairs := samePairs(nil); len(pairs) != 0 {
		t.Errorf("got %v", pairs)
	}
}
