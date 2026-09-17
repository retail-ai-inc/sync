package app

import (
	"context"
	"strings"
	"testing"
)

const mysqlTask = `{"type":"mysql","taskName":"trial",
	"sourceConn":{"host":"h","port":3306,"user":"u","password":"p","database":"shop"},
	"targetConn":{"host":"h","port":3306,"user":"u","password":"p","database":"shop_bk"}}`

func TestAcknowledgingLetsTheTaskPastThatStatementOnce(t *testing.T) {
	db := useTempTaskDB(t)
	id := itoa(insertTask(t, db, 1, mysqlTask))
	ctx := context.Background()

	if _, err := AcknowledgeDDL(ctx, id, "ALTER TABLE orders DROP COLUMN email", "jack"); err != nil {
		t.Fatalf("acknowledge: %v", err)
	}

	all, err := DDLAcknowledgements(ctx, id)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 1 || all[0].Used() {
		t.Fatalf("acknowledgements = %+v, want one outstanding", all)
	}
	if all[0].CreatedBy != "jack" {
		t.Errorf("who acknowledged it = %q", all[0].CreatedBy)
	}
}

// An acknowledgement recorded against a task that is not there would sit unused
// while the task everybody meant stayed blocked.
func TestAcknowledgingAnUnknownTaskIsRefused(t *testing.T) {
	useTempTaskDB(t)

	if _, err := AcknowledgeDDL(context.Background(), "404",
		"ALTER TABLE orders DROP COLUMN email", "jack"); err == nil {
		t.Fatal("an acknowledgement was recorded for a task that does not exist")
	}
}

func TestAcknowledgingNeedsATaskId(t *testing.T) {
	useTempTaskDB(t)

	_, err := AcknowledgeDDL(context.Background(), "orders", "ALTER TABLE orders DROP COLUMN email", "")
	if err == nil || !strings.Contains(err.Error(), "not a task id") {
		t.Fatalf("error = %v, want one naming the id", err)
	}
}

func TestAcknowledgingNothingIsRefused(t *testing.T) {
	db := useTempTaskDB(t)
	id := itoa(insertTask(t, db, 1, mysqlTask))

	if _, err := AcknowledgeDDL(context.Background(), id, "  ", "jack"); err == nil {
		t.Fatal("an acknowledgement that names no statement was recorded")
	}
}

func TestWithdrawingAnAcknowledgement(t *testing.T) {
	db := useTempTaskDB(t)
	id := itoa(insertTask(t, db, 1, mysqlTask))
	ctx := context.Background()

	ack, err := AcknowledgeDDL(ctx, id, "DROP TABLE orders", "jack")
	if err != nil {
		t.Fatalf("acknowledge: %v", err)
	}
	if err := RevokeDDLAcknowledgement(ctx, id, ack.ID); err != nil {
		t.Fatalf("revoke: %v", err)
	}

	all, err := DDLAcknowledgements(ctx, id)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 0 {
		t.Fatalf("acknowledgements = %+v, want none left", all)
	}
}

func TestListingForAnUnknownTaskIsRefused(t *testing.T) {
	useTempTaskDB(t)

	if _, err := DDLAcknowledgements(context.Background(), "404"); err == nil {
		t.Fatal("listing reported success for a task that does not exist")
	}
}

func TestWithdrawingForAnUnknownTaskIsRefused(t *testing.T) {
	useTempTaskDB(t)

	if err := RevokeDDLAcknowledgement(context.Background(), "404", 1); err == nil {
		t.Fatal("withdrawing reported success for a task that does not exist")
	}
}
