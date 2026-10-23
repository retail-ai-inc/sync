package ddlack

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/retail-ai-inc/sync/internal/platform/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

func useTempDB(t *testing.T) {
	t.Helper()
	t.Setenv("SYNC_DB_PATH", filepath.Join(t.TempDir(), "sync.db"))
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		t.Fatalf("open temp sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
}

const dropColumn = "ALTER TABLE tenant.RetailerSmartCartAppVersions DROP COLUMN TabletModel"

func TestNothingIsAllowedPastWithoutAnAcknowledgement(t *testing.T) {
	useTempDB(t)

	allowed, err := Consume(context.Background(), 41, dropColumn)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if allowed {
		t.Fatal("a statement nobody acknowledged was allowed past")
	}
}

func TestAnAcknowledgementIsSpentOnce(t *testing.T) {
	useTempDB(t)
	ctx := context.Background()

	if _, err := Add(ctx, 41, dropColumn, "jack"); err != nil {
		t.Fatalf("add: %v", err)
	}

	allowed, err := Consume(ctx, 41, dropColumn)
	if err != nil || !allowed {
		t.Fatalf("first consume: allowed=%v err=%v", allowed, err)
	}

	// The same statement arriving again is a second destructive change, and the
	// operator has only agreed to one.
	again, err := Consume(ctx, 41, dropColumn)
	if err != nil {
		t.Fatalf("second consume: %v", err)
	}
	if again {
		t.Fatal("one acknowledgement allowed the same statement past twice")
	}
}

func TestAnAcknowledgementIsOnlyForItsOwnTask(t *testing.T) {
	useTempDB(t)
	ctx := context.Background()

	if _, err := Add(ctx, 41, dropColumn, "jack"); err != nil {
		t.Fatalf("add: %v", err)
	}

	allowed, err := Consume(ctx, 39, dropColumn)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if allowed {
		t.Fatal("task 39 spent task 41's acknowledgement")
	}
}

func TestTheStatementIsMatchedPastHowItWasCopied(t *testing.T) {
	useTempDB(t)
	ctx := context.Background()

	// What an operator pastes back carries the line break and the indentation a
	// terminal put there.
	if _, err := Add(ctx, 41, "  ALTER TABLE tenant.Versions\n    DROP COLUMN TabletModel  ", "jack"); err != nil {
		t.Fatalf("add: %v", err)
	}

	allowed, err := Consume(ctx, 41, "ALTER TABLE tenant.Versions DROP COLUMN TabletModel")
	if err != nil || !allowed {
		t.Fatalf("consume: allowed=%v err=%v", allowed, err)
	}
}

func TestADifferentStatementIsNotAllowedPast(t *testing.T) {
	useTempDB(t)
	ctx := context.Background()

	if _, err := Add(ctx, 41, dropColumn, "jack"); err != nil {
		t.Fatalf("add: %v", err)
	}

	allowed, err := Consume(ctx, 41,
		"ALTER TABLE tenant.RetailerSmartCartAppVersions DROP COLUMN AppVersion")
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if allowed {
		t.Fatal("acknowledging one column's removal allowed another's")
	}
}

func TestAskingTwiceDoesNotBuyTwoSkips(t *testing.T) {
	useTempDB(t)
	ctx := context.Background()

	first, err := Add(ctx, 41, dropColumn, "jack")
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	second, err := Add(ctx, 41, dropColumn, "jack")
	if err != nil {
		t.Fatalf("add again: %v", err)
	}
	if first.ID != second.ID {
		t.Fatalf("two outstanding acknowledgements for one statement: %d and %d",
			first.ID, second.ID)
	}
}

func TestAnEmptyStatementIsRefused(t *testing.T) {
	useTempDB(t)

	if _, err := Add(context.Background(), 41, "   ", "jack"); err == nil {
		t.Fatal("an acknowledgement that names no statement was recorded")
	}
}

func TestListReportsOutstandingAndSpent(t *testing.T) {
	useTempDB(t)
	ctx := context.Background()

	if _, err := Add(ctx, 41, dropColumn, "jack"); err != nil {
		t.Fatalf("add: %v", err)
	}
	if _, err := Add(ctx, 41, "DROP TABLE tenant.Orders", "jack"); err != nil {
		t.Fatalf("add: %v", err)
	}
	if allowed, err := Consume(ctx, 41, dropColumn); err != nil || !allowed {
		t.Fatalf("consume: allowed=%v err=%v", allowed, err)
	}

	all, err := List(ctx, 41)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("expected both acknowledgements, got %d", len(all))
	}
	// Outstanding first: it is the one that still decides something.
	if all[0].Used() {
		t.Fatal("the spent acknowledgement was reported before the outstanding one")
	}
	if !all[1].Used() || all[1].UsedAt.IsZero() {
		t.Fatalf("the spent one does not say when: %+v", all[1])
	}
	if all[1].CreatedBy != "jack" {
		t.Fatalf("who acknowledged it was not kept: %q", all[1].CreatedBy)
	}
}

func TestRevokeStopsAnUnusedOne(t *testing.T) {
	useTempDB(t)
	ctx := context.Background()

	ack, err := Add(ctx, 41, dropColumn, "jack")
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := Revoke(ctx, 41, ack.ID); err != nil {
		t.Fatalf("revoke: %v", err)
	}

	allowed, err := Consume(ctx, 41, dropColumn)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if allowed {
		t.Fatal("a withdrawn acknowledgement still allowed the statement past")
	}
}

func TestRevokingSomethingThatIsNotThereSaysSo(t *testing.T) {
	useTempDB(t)

	if err := Revoke(context.Background(), 41, 404); err == nil {
		t.Fatal("withdrawing an acknowledgement that does not exist reported success")
	}
}

func TestASpentAcknowledgementCannotBeWithdrawn(t *testing.T) {
	useTempDB(t)
	ctx := context.Background()

	ack, err := Add(ctx, 41, dropColumn, "jack")
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	if allowed, err := Consume(ctx, 41, dropColumn); err != nil || !allowed {
		t.Fatalf("consume: allowed=%v err=%v", allowed, err)
	}

	// Withdrawing it would erase the record that it was used, which is the only
	// evidence a destructive statement was passed over.
	if err := Revoke(ctx, 41, ack.ID); err == nil {
		t.Fatal("a spent acknowledgement was withdrawn")
	}
}
