package mysql

import (
	"testing"

	"github.com/retail-ai-inc/sync/internal/platform/config"
)

// Which objects get counted. The periodic monitor loops over the tables a task
// names and does nothing for one that names none -- which is every task on this
// deployment, because they replicate whole databases, and it is why the row
// count history is empty for them. This resolution follows the rule replication
// itself follows instead.

func mappingOf(tables ...[2]string) []config.DatabaseMapping {
	mapped := make([]config.TableMapping, 0, len(tables))
	for _, pair := range tables {
		mapped = append(mapped, config.TableMapping{SourceTable: pair[0], TargetTable: pair[1]})
	}
	return []config.DatabaseMapping{{Tables: mapped}}
}

// TestATaskThatNamesNoTablesIsDiscovered is the case that produced nothing at
// all before.
func TestATaskThatNamesNoTablesIsDiscovered(t *testing.T) {
	for name, cfg := range map[string]config.SyncConfig{
		"no mappings":              {},
		"a mapping with no tables": {Mappings: []config.DatabaseMapping{{}}},
		"tables with no names":     {Mappings: mappingOf([2]string{"", ""})},
	} {
		t.Run(name, func(t *testing.T) {
			pairs, discovered := tablePairs(cfg)
			if !discovered {
				t.Error("a task naming no tables was not marked for discovery, so " +
					"nothing would be counted")
			}
			if len(pairs) != 0 {
				t.Errorf("pairs = %v", pairs)
			}
		})
	}
}

func TestATaskThatNamesTablesIsNotDiscovered(t *testing.T) {
	pairs, discovered := tablePairs(config.SyncConfig{
		Mappings: mappingOf([2]string{"orders", "orders_bk"}),
	})
	if discovered {
		t.Error("a task that names its tables had them discovered instead")
	}
	if len(pairs) != 1 || pairs[0][0] != "orders" || pairs[0][1] != "orders_bk" {
		t.Errorf("pairs = %v", pairs)
	}
}

// TestAMappingWithNoTargetCountsTheSameName, which is what the task form
// produces when both sides share a name. Counting against "" would report every
// such table as missing.
func TestAMappingWithNoTargetCountsTheSameName(t *testing.T) {
	pairs, _ := tablePairs(config.SyncConfig{Mappings: mappingOf([2]string{"orders", ""})})
	if len(pairs) != 1 || pairs[0][1] != "orders" {
		t.Errorf("pairs = %v, want the source's name on both sides", pairs)
	}
}

// TestQuoteNameClosesAnIdentifier. A table name cannot be a bound parameter, so
// it is quoted; a name carrying a backtick would otherwise end the quoting and
// become part of the statement. These come from the source's own catalogue,
// which is exactly where an unusual name would come from.
func TestQuoteNameClosesAnIdentifier(t *testing.T) {
	for in, want := range map[string]string{
		"orders":       "`orders`",
		"order`s":      "`order``s`",
		"a``b":         "`a````b`",
		"":             "``",
		"tenant-trial": "`tenant-trial`",
	} {
		if got := quoteName(in); got != want {
			t.Errorf("quoteName(%q) = %q, want %q", in, got, want)
		}
	}
}
