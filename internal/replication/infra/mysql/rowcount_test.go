package mysql

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
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

// Counting both sides. The counts are read when somebody asks and are used to
// decide whether a region is safe to promote, so a side that could not be read
// must be reported as unknown rather than as zero -- a missing table and an
// empty one are different problems.

func countReply(match string, n int64) reply {
	return reply{match: match, columns: []string{"COUNT(*)"},
		rows: [][]driver.Value{{n}}}
}

func TestBothSidesAreCountedForEveryPair(t *testing.T) {
	fake := &fakeDB{replies: []reply{
		countReply("`shop`.`orders`", 100),
		countReply("`shop_bk`.`orders_bk`", 100),
		countReply("`shop`.`payments`", 40),
		countReply("`shop_bk`.`payments`", 39),
	}}
	db := fake.open(t)

	counts := fill(context.Background(), domain.RowCounts{Engine: "mysql"},
		[][2]string{{"orders", "orders_bk"}, {"payments", "payments"}},
		db, db, "shop", "shop_bk")

	if len(counts.Objects) != 2 {
		t.Fatalf("counted %d objects, want 2", len(counts.Objects))
	}
	if !counts.Objects[0].Agrees() {
		t.Errorf("matching counts disagreed: %+v", counts.Objects[0])
	}
	if counts.Objects[1].Agrees() {
		t.Errorf("counts one apart agreed: %+v", counts.Objects[1])
	}
	if got := counts.Difference(); got != 1 {
		t.Errorf("Difference() = %d, want 1", got)
	}
}

// TestASideThatCannotBeCountedIsUnknown covers the answer that matters. Zero
// would read as an empty table, and an unreachable target would then agree with
// an empty source.
func TestASideThatCannotBeCountedIsUnknown(t *testing.T) {
	fake := &fakeDB{replies: []reply{
		countReply("`shop`.`orders`", 100),
		{match: "`shop_bk`.`orders`", err: errors.New("Table 'shop_bk.orders' doesn't exist")},
	}}
	db := fake.open(t)

	counts := fill(context.Background(), domain.RowCounts{},
		[][2]string{{"orders", "orders"}}, db, db, "shop", "shop_bk")

	object := counts.Objects[0]
	if object.TargetRows >= 0 {
		t.Errorf("an unreadable target counted as %d", object.TargetRows)
	}
	if object.SourceRows != 100 {
		t.Errorf("the readable side was lost: %d", object.SourceRows)
	}
	if object.Agrees() {
		t.Error("a pair with an uncounted side agreed")
	}
	if object.Note == "" {
		t.Error("nothing said why the side could not be counted")
	}
	if !strings.Contains(object.Note, "target") {
		t.Errorf("the note does not say which side: %q", object.Note)
	}
}

func TestBothSidesFailingIsReportedForBoth(t *testing.T) {
	fake := &fakeDB{replies: []reply{
		{match: "`shop`", err: errors.New("source is away")},
		{match: "`shop_bk`", err: errors.New("target is away")},
	}}
	db := fake.open(t)

	counts := fill(context.Background(), domain.RowCounts{},
		[][2]string{{"orders", "orders"}}, db, db, "shop", "shop_bk")

	object := counts.Objects[0]
	if object.SourceRows >= 0 || object.TargetRows >= 0 {
		t.Errorf("a side was counted despite failing: %+v", object)
	}
	if !strings.Contains(object.Note, "source") || !strings.Contains(object.Note, "target") {
		t.Errorf("the note does not name both sides: %q", object.Note)
	}
}

// TestTheCountNamesTheTableItAsksFor: the schema and table are quoted into the
// statement because a table name cannot be bound, so the wrong pair would be
// counted silently.
func TestTheCountNamesTheTableItAsksFor(t *testing.T) {
	fake := &fakeDB{replies: []reply{countReply("COUNT", 1)}}
	db := fake.open(t)

	if _, err := countTable(context.Background(), db, "shop", "orders"); err != nil {
		t.Fatalf("countTable: %v", err)
	}

	statements := fake.statements()
	if len(statements) != 1 {
		t.Fatalf("ran %d statements", len(statements))
	}
	if !strings.Contains(statements[0], "`shop`.`orders`") {
		t.Errorf("the statement does not name the quoted table: %q", statements[0])
	}
}

func TestCountingNothingIsAnEmptyReport(t *testing.T) {
	fake := &fakeDB{}
	db := fake.open(t)

	counts := fill(context.Background(), domain.RowCounts{Engine: "mysql"},
		nil, db, db, "shop", "shop_bk")

	if len(counts.Objects) != 0 || counts.Difference() != 0 {
		t.Errorf("counting no pairs produced %+v", counts)
	}
	if len(fake.statements()) != 0 {
		t.Errorf("statements were run with no pairs: %v", fake.statements())
	}
}
