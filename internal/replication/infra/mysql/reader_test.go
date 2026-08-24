package mysql

import (
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

func readerFor(mappings []config.DatabaseMapping, sourceDSN string) *Reader {
	return &Reader{Config: config.SyncConfig{
		Type:             "mysql",
		SourceConnection: sourceDSN,
		Mappings:         mappings,
	}}
}

// TestOneStreamCoversEveryMappedDatabase is the point of the single-stream
// design on the MySQL side.
//
// The binlog is one log per server, but the include list used to be built from
// the single database named in the connection string. Replicating three
// databases off one server therefore meant three tasks, three canal instances
// and three binlog dump connections all reading the same bytes — the source did
// the work once for each of them.
func TestOneStreamCoversEveryMappedDatabase(t *testing.T) {
	r := readerFor([]config.DatabaseMapping{
		{SourceDatabase: "shop", Tables: []config.TableMapping{{SourceTable: "orders"}}},
		{SourceDatabase: "ledger", Tables: []config.TableMapping{{SourceTable: "entries"}}},
		{SourceDatabase: "audit", Tables: []config.TableMapping{{SourceTable: "trail"}}},
	}, "u:p@tcp(h:3306)/shop")

	includes, err := r.includeTables()
	if err != nil {
		t.Fatalf("includeTables: %v", err)
	}

	joined := strings.Join(includes, " ")
	for _, want := range []string{`shop\.orders`, `ledger\.entries`, `audit\.trail`} {
		if !strings.Contains(joined, want) {
			t.Errorf("include list %v is missing %s", includes, want)
		}
	}
}

// TestAMappingWithNoTablesTakesTheWholeDatabase covers discovery: a table
// created after the task started used simply not to be replicated, with no
// warning anywhere, which looks exactly like everything working.
func TestAMappingWithNoTablesTakesTheWholeDatabase(t *testing.T) {
	r := readerFor([]config.DatabaseMapping{{SourceDatabase: "shop"}}, "u:p@tcp(h:3306)/shop")

	includes, err := r.includeTables()
	if err != nil {
		t.Fatalf("includeTables: %v", err)
	}
	if len(includes) != 1 || includes[0] != `shop\..*` {
		t.Errorf("include list = %v, want the whole database", includes)
	}
}

// TestAMappingWithoutADatabaseFallsBackToTheConnection covers the shape a
// single-database task has had all along, so upgrading does not change it.
func TestAMappingWithoutADatabaseFallsBackToTheConnection(t *testing.T) {
	r := readerFor([]config.DatabaseMapping{
		{Tables: []config.TableMapping{{SourceTable: "orders"}}},
	}, "u:p@tcp(h:3306)/shop")

	includes, err := r.includeTables()
	if err != nil {
		t.Fatalf("includeTables: %v", err)
	}
	if len(includes) != 1 || includes[0] != `shop\.orders` {
		t.Errorf("include list = %v, want shop.orders", includes)
	}
}

// TestTheSamePatternIsNotListedTwice keeps two mappings of one table from
// making canal read it twice.
func TestTheSamePatternIsNotListedTwice(t *testing.T) {
	r := readerFor([]config.DatabaseMapping{
		{SourceDatabase: "shop", Tables: []config.TableMapping{{SourceTable: "orders", TargetTable: "a"}}},
		{SourceDatabase: "shop", Tables: []config.TableMapping{{SourceTable: "orders", TargetTable: "b"}}},
	}, "u:p@tcp(h:3306)/shop")

	includes, err := r.includeTables()
	if err != nil {
		t.Fatalf("includeTables: %v", err)
	}
	if len(includes) != 1 {
		t.Errorf("include list = %v, want one pattern for one source table", includes)
	}
}

// TestATaskWithNothingToReadIsRefused covers a configuration that names no
// database anywhere. Starting anyway would replicate nothing and report success.
func TestATaskWithNothingToReadIsRefused(t *testing.T) {
	r := readerFor([]config.DatabaseMapping{
		{Tables: []config.TableMapping{{SourceTable: "orders"}}},
	}, "u:p@tcp(h:3306)/")

	if _, err := r.includeTables(); !domain.IsUnrecoverable(err) {
		t.Fatalf("includeTables returned %v, want an unrecoverable error", err)
	}
}

// ------------------------------------------------------------- record keys

func eventFor(action string, pk []int, rows ...[]interface{}) *canal.RowsEvent {
	table := &schema.Table{
		Schema:    "shop",
		Name:      "orders",
		Columns:   []schema.TableColumn{{Name: "id"}, {Name: "customer"}},
		PKColumns: pk,
	}
	return &canal.RowsEvent{Table: table, Action: action, Rows: rows}
}

// TestTheRecordKeyComesFromThePrimaryKey is what keeps two changes to one row
// from being reordered against each other: an insert followed by a delete
// leaves nothing behind, and the same two the other way round leave the row.
func TestTheRecordKeyComesFromThePrimaryKey(t *testing.T) {
	e := eventFor(canal.InsertAction, []int{0},
		[]interface{}{7, "ada"},
		[]interface{}{8, "grace"})

	if got := rowKey(e, 0); !strings.HasPrefix(got, "7") {
		t.Errorf("rowKey(0) = %q, want it built from id 7", got)
	}
	if got := rowKey(e, 1); !strings.HasPrefix(got, "8") {
		t.Errorf("rowKey(1) = %q, want it built from id 8", got)
	}
}

// TestAnUpdateKeysOnTheRowPairs covers the before/after layout: the nth
// statement was built from the nth pair, so it has to key on the nth pair.
func TestAnUpdateKeysOnTheRowPairs(t *testing.T) {
	e := eventFor(canal.UpdateAction, []int{0},
		[]interface{}{7, "ada"}, []interface{}{7, "ADA"},
		[]interface{}{8, "grace"}, []interface{}{8, "GRACE"})

	if got := rowKey(e, 0); !strings.HasPrefix(got, "7") {
		t.Errorf("rowKey(0) = %q, want the first pair's key", got)
	}
	if got := rowKey(e, 1); !strings.HasPrefix(got, "8") {
		t.Errorf("rowKey(1) = %q, want the second pair's key", got)
	}
}

// TestACompositeKeyUsesEveryColumn keeps two rows that share one key column
// from colliding into a single ordering group.
func TestACompositeKeyUsesEveryColumn(t *testing.T) {
	e := eventFor(canal.InsertAction, []int{0, 1},
		[]interface{}{7, "ada"},
		[]interface{}{7, "grace"})

	if rowKey(e, 0) == rowKey(e, 1) {
		t.Error("two rows sharing only the first key column produced the same key")
	}
}

// TestAKeylessTableHasNoRecordKey means such an event acts as a barrier rather
// than joining an ordering group it does not belong to.
func TestAKeylessTableHasNoRecordKey(t *testing.T) {
	e := eventFor(canal.InsertAction, nil, []interface{}{7, "ada"})

	if got := rowKey(e, 0); got != "" {
		t.Errorf("rowKey = %q, want empty for a table with no key", got)
	}
}

// ------------------------------------------------------------- operations

func TestTheOperationIsReadOffTheStatement(t *testing.T) {
	cases := map[string]domain.Op{
		"INSERT INTO a.b (x) VALUES (?)":   domain.OpInsert,
		"REPLACE INTO a.b (x) VALUES (?)":  domain.OpInsert,
		"UPDATE a.b SET x = ? WHERE y = ?": domain.OpUpdate,
		"DELETE FROM a.b WHERE y = ?":      domain.OpDelete,
		"ALTER TABLE a.b ADD COLUMN x INT": domain.OpSchema,
	}
	for query, want := range cases {
		if got := opOf(query); got != want {
			t.Errorf("opOf(%q) = %v, want %v", query, got, want)
		}
	}
}

// ------------------------------------------------------- column reordering

func parseOne(t *testing.T, query string) ast.StmtNode {
	t.Helper()
	stmts, _, err := parser.New().Parse(query, "", "")
	if err != nil || len(stmts) != 1 {
		t.Fatalf("parse %q: %v", query, err)
	}
	return stmts[0]
}

// TestAMoveIsRecognised covers the one schema change that corrupts rows without
// saying anything.
//
// A ROW binlog event carries positions, not names, and the names come from
// asking the source for its current shape. A statement that moves a column
// without changing how many there are therefore makes every row read before it,
// since the stream resumed, decode one or more columns out of place — and write
// cleanly, with the row counts still agreeing afterwards.
func TestAMoveIsRecognised(t *testing.T) {
	moving := []string{
		"ALTER TABLE orders MODIFY COLUMN amount DECIMAL(12,2) AFTER customer",
		"ALTER TABLE orders CHANGE COLUMN amount total DECIMAL(12,2) FIRST",
		"ALTER TABLE orders ADD COLUMN channel VARCHAR(16) AFTER customer",
	}
	for _, query := range moving {
		if !reordersColumns(parseOne(t, query)) {
			t.Errorf("%q was not recognised as moving a column", query)
		}
	}
}

// TestAChangeThatKeepsTheOrderIsNotAMove keeps the check from stopping tasks for
// the statements that are already either loud or harmless.
func TestAChangeThatKeepsTheOrderIsNotAMove(t *testing.T) {
	harmless := []string{
		// Appended at the end: the column count changes, so a row read before it
		// fails to apply rather than applying wrongly.
		"ALTER TABLE orders ADD COLUMN channel VARCHAR(16) NOT NULL DEFAULT 'web'",
		// A rename leaves every value in the position it was read from.
		"ALTER TABLE orders CHANGE COLUMN amount total DECIMAL(12,2)",
		// A widened type, same position.
		"ALTER TABLE orders MODIFY COLUMN customer VARCHAR(128)",
		"ALTER TABLE orders ADD INDEX idx_customer (customer)",
		"CREATE TABLE orders (id BIGINT PRIMARY KEY)",
	}
	for _, query := range harmless {
		if reordersColumns(parseOne(t, query)) {
			t.Errorf("%q was treated as moving a column", query)
		}
	}
}

// TestAMoveOnlyMattersAfterRowsHaveBeenApplied covers the condition that makes
// it a problem: a stream that resumed, and rows already handed over.
func TestAMoveOnlyMattersAfterRowsHaveBeenApplied(t *testing.T) {
	r := &Reader{Config: config.SyncConfig{Type: "mysql", SourceConnection: "u:p@tcp(h:3306)/shop"}}
	const move = "ALTER TABLE orders MODIFY COLUMN amount DECIMAL(12,2) AFTER customer"

	// Started at the end of the log: nothing older than the statement was read.
	r.resumed = false
	r.appliedSince = map[string]bool{"shop.orders": true}
	if err := r.checkReordering("shop", move); err != nil {
		t.Errorf("a fresh stream was stopped: %v", err)
	}

	// Resumed, but nothing applied for that table yet.
	r.resumed = true
	r.appliedSince = map[string]bool{}
	if err := r.checkReordering("shop", move); err != nil {
		t.Errorf("a resumed stream with nothing applied was stopped: %v", err)
	}

	// Resumed, and rows for another table applied — not this one's problem.
	r.appliedSince = map[string]bool{"shop.payments": true}
	if err := r.checkReordering("shop", move); err != nil {
		t.Errorf("another table's rows stopped this one: %v", err)
	}

	// Resumed, and rows for this table applied: those rows are wrong.
	r.appliedSince = map[string]bool{"shop.orders": true}
	err := r.checkReordering("shop", move)
	if !domain.IsUnrecoverable(err) {
		t.Fatalf("checkReordering returned %v, want an unrecoverable error", err)
	}
	if !strings.Contains(err.Error(), "shop.orders") {
		t.Errorf("error = %v, want it to name the table that needs copying again", err)
	}
	if !strings.Contains(err.Error(), "copy") {
		t.Errorf("error = %v, want it to say what to do", err)
	}
}
