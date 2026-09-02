package mysql

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
)

func insertEvent(values ...interface{}) *canal.RowsEvent {
	return &canal.RowsEvent{
		Table:  sourceTable("orders", "id", "customer", "email"),
		Action: canal.InsertAction,
		Rows:   [][]interface{}{values},
	}
}

// TestNothingLandsBeforeTheTransactionEnds pins the buffering. Row events are
// held until the XID that closes the source transaction, so a reader on the
// target never sees half of one.
func TestNothingLandsBeforeTheTransactionEnds(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("orders", "orders"))

	if err := h.OnRow(insertEvent("1", "Ada", "a@x")); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	if err := h.OnRow(insertEvent("2", "Grace", "g@x")); err != nil {
		t.Fatalf("OnRow: %v", err)
	}

	if got := rows(t, db); len(got) != 0 {
		t.Fatalf("target holds %v before the transaction closed", got)
	}

	if err := h.OnXID(nil, mysql.Position{}); err != nil {
		t.Fatalf("OnXID: %v", err)
	}
	if got := rows(t, db); len(got) != 2 {
		t.Errorf("target holds %v after the commit, want both rows", got)
	}
}

// TestAFailedStatementRollsBackItsTransaction is the guarantee the payment
// ledger needs: a debit whose matching credit cannot be applied must not be
// visible on its own.
func TestAFailedStatementRollsBackItsTransaction(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("orders", "orders"))

	if err := h.OnRow(insertEvent("1", "Ada", "a@x")); err != nil {
		t.Fatalf("the good row: %v", err)
	}
	if err := h.OnRow(&canal.RowsEvent{
		Table:  sourceTable("orders", "id", "missing_column"),
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{"2", "x"}},
	}); err != nil {
		t.Fatalf("buffering the bad row: %v", err)
	}

	err := h.OnXID(nil, mysql.Position{})
	if err == nil {
		t.Fatal("OnXID reported nothing for a transaction it could not apply")
	}
	if !strings.Contains(err.Error(), "source transaction") {
		t.Errorf("error = %v, want the transaction named", err)
	}
	if got := rows(t, db); len(got) != 0 {
		t.Errorf("target holds %v; the good row of a rolled-back transaction "+
			"survived", got)
	}
	if atomic.LoadInt32(&h.lastExecError) != 1 {
		t.Error("the error flag was not raised")
	}
}

// Canal is stopping and the offset is frozen, so the statements will arrive
// again on the next run; replaying them here would only mix them into an
// unrelated transaction.
func TestTheBufferIsClearedByAFailure(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("orders", "orders"))

	_ = h.OnRow(&canal.RowsEvent{
		Table:  sourceTable("orders", "id", "missing_column"),
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{"1", "x"}},
	})
	if err := h.OnXID(nil, mysql.Position{}); err == nil {
		t.Fatal("the bad transaction was not reported")
	}

	if err := h.OnRow(insertEvent("2", "Grace", "g@x")); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	if err := h.OnXID(nil, mysql.Position{}); err != nil {
		t.Fatalf("the following transaction failed too: %v", err)
	}
	if got := rows(t, db); len(got) != 1 || got[0] != "2|Grace|g@x" {
		t.Errorf("target holds %v, want only the second transaction", got)
	}
}

// A source transaction larger than the cap is applied in more than one target
// transaction, which gives up atomicity for that transaction rather than
// buffering an unbounded stretch of the binlog.
func TestAnOversizedTransactionIsSplit(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("orders", "orders"))
	h.pendingLimit = 2

	for _, id := range []string{"1", "2", "3"} {
		if err := h.OnRow(insertEvent(id, "Ada", "a@x")); err != nil {
			t.Fatalf("OnRow %s: %v", id, err)
		}
	}

	// The first two statements hit the cap and were applied on their own.
	if got := rows(t, db); len(got) != 2 {
		t.Errorf("target holds %v before the XID, want the first two rows", got)
	}
	if err := h.OnXID(nil, mysql.Position{}); err != nil {
		t.Fatalf("OnXID: %v", err)
	}
	if got := rows(t, db); len(got) != 3 {
		t.Errorf("target holds %v after the commit, want all three", got)
	}
}

func TestTheDefaultBufferCapIsThePackageOne(t *testing.T) {
	if got := (&MyEventHandler{}).maxPending(); got != maxPendingStatements {
		t.Errorf("maxPending() = %d, want %d", got, maxPendingStatements)
	}
}

// TestOnPosSyncedDrainsAnUnterminatedTransaction covers the source that never
// sends an XID — a non-transactional engine, or a stream that stops mid
// transaction.
func TestOnPosSyncedDrainsAnUnterminatedTransaction(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("orders", "orders"))

	if err := h.OnRow(insertEvent("1", "Ada", "a@x")); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	if err := h.OnPosSynced(nil, mysql.Position{Name: "binlog.1", Pos: 4}, nil, true); err != nil {
		t.Fatalf("OnPosSynced: %v", err)
	}

	if got := rows(t, db); len(got) != 1 {
		t.Errorf("target holds %v, want the buffered row applied", got)
	}
}

// TestAFlushWithNoTargetConnectionIsReported covers the window between the
// health check closing a connection and opening its replacement.
func TestAFlushWithNoTargetConnectionIsReported(t *testing.T) {
	h := newHandler(t, nil, mapTable("orders", "orders"))

	if err := h.OnRow(insertEvent("1", "Ada", "a@x")); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	err := h.OnXID(nil, mysql.Position{})
	if err == nil {
		t.Fatal("flushing with no connection reported nothing")
	}
	if !strings.Contains(err.Error(), "no target connection") {
		t.Errorf("error = %v", err)
	}
}

// TestAnEmptyTransactionIsANoOp covers the XID that follows a transaction whose
// rows were all filtered out by the table mapping.
func TestAnEmptyTransactionIsANoOp(t *testing.T) {
	h := newHandler(t, nil, nil)

	if err := h.OnXID(nil, mysql.Position{}); err != nil {
		t.Errorf("OnXID with nothing buffered: %v", err)
	}
}

// TestTheTargetConnectionCanBeReplaced pins the accessor the health check uses.
// It exists so the swap takes the same lock the statement buffer does.
func TestTheTargetConnectionCanBeReplaced(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, nil, mapTable("orders", "orders"))

	if err := h.OnRow(insertEvent("1", "Ada", "a@x")); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	h.setTargetDB(db)
	if err := h.OnXID(nil, mysql.Position{}); err != nil {
		t.Fatalf("OnXID: %v", err)
	}

	if got := rows(t, db); len(got) != 1 {
		t.Errorf("target holds %v, want the row on the replacement connection", got)
	}
}
