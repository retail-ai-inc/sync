package mysql

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
)

func readerFor(mappings []config.DatabaseMapping, sourceDSN string) *Reader {
	return &Reader{Config: config.SyncConfig{
		Type:             "mysql",
		SourceConnection: sourceDSN,
		Mappings:         mappings,
	}}
}

// The binlog is one log per server, but the include list used to be built from
// the single database named in the connection string.
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

func parseOne(t *testing.T, query string) ast.StmtNode {
	t.Helper()
	stmts, _, err := parser.New().Parse(query, "", "")
	if err != nil || len(stmts) != 1 {
		t.Fatalf("parse %q: %v", query, err)
	}
	return stmts[0]
}

// A ROW binlog event carries positions, not names, and the names come from
// asking the source for its current shape.
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
	read := time.Now()
	// A statement older than the shape that was read for the table: the case
	// this refuses. A live one is covered by the test below.
	stale := read.Add(-time.Hour)

	// Started at the end of the log: nothing older than the statement was read.
	r.resumed = false
	r.appliedSince = map[string]time.Time{"shop.orders": read}
	if err := r.checkReordering("shop", move, stale); err != nil {
		t.Errorf("a fresh stream was stopped: %v", err)
	}

	// Resumed, but nothing applied for that table yet.
	r.resumed = true
	r.appliedSince = map[string]time.Time{}
	if err := r.checkReordering("shop", move, stale); err != nil {
		t.Errorf("a resumed stream with nothing applied was stopped: %v", err)
	}

	// Resumed, and rows for another table applied — not this one's problem.
	r.appliedSince = map[string]time.Time{"shop.payments": read}
	if err := r.checkReordering("shop", move, stale); err != nil {
		t.Errorf("another table's rows stopped this one: %v", err)
	}

	// Resumed, and rows for this table applied: those rows are wrong.
	r.appliedSince = map[string]time.Time{"shop.orders": read}
	err := r.checkReordering("shop", move, stale)
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

// TestAMoveMadeWhileTheStreamIsRunningIsNotARefusal records what stopped the
// staging MySQL task on 2026-09-14: an ALTER ran on the source nine hours into
// a run and every row handed over until then was refused as wrongly decoded.
// They were not. canal read the table's shape when the first of those rows
// arrived, which was before the statement ran, so the rows carry the shape
// they were written under and the statement is simply propagated.
func TestAMoveMadeWhileTheStreamIsRunningIsNotARefusal(t *testing.T) {
	r := &Reader{Config: config.SyncConfig{Type: "mysql", SourceConnection: "u:p@tcp(h:3306)/shop"}}
	const move = "ALTER TABLE orders ADD COLUMN tablet VARCHAR(48) NULL AFTER customer"

	r.resumed = true
	shapeRead := time.Now().Add(-9 * time.Hour)
	r.appliedSince = map[string]time.Time{"shop.orders": shapeRead}

	if err := r.checkReordering("shop", move, time.Now()); err != nil {
		t.Errorf("a statement that ran while the stream was live was refused: %v", err)
	}

	// The other side of the same comparison: a statement that ran before the
	// shape was read is still refused, because those rows really were decoded
	// against the shape it produced.
	if err := r.checkReordering("shop", move, shapeRead.Add(-time.Hour)); !domain.IsUnrecoverable(err) {
		t.Errorf("a statement older than the shape read for the table returned %v", err)
	}

	// A statement whose timestamp is missing keeps the old, conservative
	// answer: with nothing to compare, refusing is the safe side.
	if err := r.checkReordering("shop", move, time.Time{}); !domain.IsUnrecoverable(err) {
		t.Errorf("a statement with no timestamp returned %v", err)
	}
}

// TestClockSkewDoesNotDecideTheVerdict covers the margin: the two timestamps
// come from different clocks, so a statement that ran a moment before the
// shape was read is treated as the live case rather than as a corruption.
func TestClockSkewDoesNotDecideTheVerdict(t *testing.T) {
	r := &Reader{Config: config.SyncConfig{Type: "mysql", SourceConnection: "u:p@tcp(h:3306)/shop"}}
	const move = "ALTER TABLE orders MODIFY COLUMN amount DECIMAL(12,2) AFTER customer"

	r.resumed = true
	read := time.Now()
	r.appliedSince = map[string]time.Time{"shop.orders": read}

	if err := r.checkReordering("shop", move, read.Add(-schemaReadSkew/2)); err != nil {
		t.Errorf("a statement within the skew margin was refused: %v", err)
	}
	if err := r.checkReordering("shop", move, read.Add(-2*schemaReadSkew)); !domain.IsUnrecoverable(err) {
		t.Errorf("a statement well outside the margin returned %v", err)
	}
}

// TestHandingOverAfterCloseDoesNotPanic is a regression test for a crash that
// took the whole process down, every other task with it.  canal.Close calls
// OnPosSynced one last time on its way out.
func TestHandingOverAfterCloseDoesNotPanic(t *testing.T) {
	r := &Reader{Config: config.SyncConfig{
		Type:             "mysql",
		SourceConnection: "u:p@tcp(h:3306)/shop",
	}}
	r.out = make(chan *domain.Event, 1)
	r.fail = make(chan error, 1)
	r.done = make(chan struct{})
	r.tx = []*domain.Event{{}, {}, {}}

	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Two handovers: the first may still fit in the buffered channel, the
	// second cannot, so this reaches the case that used to block or panic.
	for i := 0; i < 2; i++ {
		r.tx = []*domain.Event{{}, {}, {}}
		err := r.handOver(mysql.Position{Name: "binlog.000001", Pos: 4}, nil, nil)
		if err != nil && !errors.Is(err, errReaderClosed) {
			t.Fatalf("handOver after Close: %v, want errReaderClosed or nil", err)
		}
	}
}

// TestNextReportsAClosedReaderRatherThanBlocking is the other half: once the
// reader is closed, a caller still waiting in Next has to be told, not left
// there until its context expires.
func TestNextReportsAClosedReaderRatherThanBlocking(t *testing.T) {
	r := &Reader{}
	r.out = make(chan *domain.Event, 1)
	r.fail = make(chan error, 1)
	r.done = make(chan struct{})

	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	event, err := r.Next(ctx)
	if err == nil {
		t.Fatalf("Next returned %v and no error after the reader was closed", event)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("Next waited for its context instead of noticing the reader had closed")
	}
}

// TestACheckpointWithoutAGTIDKeepsTheOneBefore covers the position degrading
// from GTID to file and offset.  canal does not carry a GTID set on every
// position it reports.
func TestACheckpointWithoutAGTIDKeepsTheOneBefore(t *testing.T) {
	r := &Reader{source: "10.0.0.1:3306/bench", flavor: "mysql"}

	set, err := mysql.ParseGTIDSet("mysql", "e67b8f4b-a2d6-11f1-9406-42010a400002:1-624")
	if err != nil {
		t.Fatalf("ParseGTIDSet: %v", err)
	}
	first, err := r.encode(mysql.Position{Name: "mysql-bin.000005", Pos: 100}, set)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if !strings.Contains(first, "1-624") {
		t.Fatalf("first checkpoint = %s, want it to carry the GTID", first)
	}

	// The same stream, one position later, reported without a GTID set.
	second, err := r.encode(mysql.Position{Name: "mysql-bin.000005", Pos: 900}, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if !strings.Contains(second, "1-624") {
		t.Errorf("second checkpoint = %s, want it to keep the GTID it already had", second)
	}
	if !strings.Contains(second, "900") {
		t.Errorf("second checkpoint = %s, want the newer file offset", second)
	}
}

// TestAPositionInsideATransactionIsNotHandedOver is the silent row loss this
// package existed with until it was measured.  canal reports a position at the
// BEGIN of every transaction, and go-mysql has already added that
// transaction's GTID to the set by then — it adds it when it reads the GTID
// event, which comes before the rows.
func TestAPositionInsideATransactionIsNotHandedOver(t *testing.T) {
	r := &Reader{
		source: "10.0.0.1:3306/bench", flavor: "mysql",
		out: make(chan *domain.Event, 4), done: make(chan struct{}),
	}

	set, err := mysql.ParseGTIDSet("mysql", "e67b8f4b-a2d6-11f1-9406-42010a400002:1-624")
	if err != nil {
		t.Fatalf("ParseGTIDSet: %v", err)
	}

	// The GTID event opens transaction 624. Its rows have not been read.
	if err := r.OnGTID(nil, nil); err != nil {
		t.Fatalf("OnGTID: %v", err)
	}
	// The BEGIN that follows it, carrying a set that already counts 624.
	if err := r.OnPosSynced(nil, mysql.Position{Name: "mysql-bin.000005", Pos: 100}, set, false); err != nil {
		t.Fatalf("OnPosSynced: %v", err)
	}

	select {
	case e := <-r.out:
		t.Fatalf("handed over %+v from inside a transaction; a restart would resume "+
			"past rows it never read", e)
	default:
	}

	// The transaction ends, and now the position may move.
	if err := r.OnXID(nil, mysql.Position{Name: "mysql-bin.000005", Pos: 400}); err != nil {
		t.Fatalf("OnXID: %v", err)
	}
	if err := r.OnPosSynced(nil, mysql.Position{Name: "mysql-bin.000005", Pos: 400}, set, false); err != nil {
		t.Fatalf("OnPosSynced after the transaction: %v", err)
	}
	select {
	case e := <-r.out:
		if !e.Heartbeat {
			t.Fatalf("event after the transaction = %+v, want the heartbeat", e)
		}
	default:
		t.Fatal("nothing handed over after the transaction ended, so the position never moves")
	}
}

// A failure means a restart before the next DML replays the ALTER, which the target refuses.
func TestAPositionAfterASchemaChangeCountsIt(t *testing.T) {
	r := readerWithMappings(t, mapTable("orders", "orders"))
	r.source, r.flavor = "10.0.0.1:3306/shop", "mysql"
	r.lastGTID = "e67b8f4b-a2d6-11f1-9406-42010a400002:1-10"

	withDDL, err := mysql.ParseGTIDSet("mysql", "e67b8f4b-a2d6-11f1-9406-42010a400002:1-11")
	if err != nil {
		t.Fatalf("ParseGTIDSet: %v", err)
	}
	pos := mysql.Position{Name: "mysql-bin.000005", Pos: 900}

	// canal's order for one DDL: its GTID event, the statement, then the synced position.
	if err := r.OnGTID(nil, nil); err != nil {
		t.Fatalf("OnGTID: %v", err)
	}
	ddl := query("ALTER TABLE orders ADD COLUMN note TEXT")
	ddl.GSet = withDDL
	if err := r.OnDDL(nil, pos, ddl); err != nil {
		t.Fatalf("OnDDL: %v", err)
	}
	if err := r.OnPosSynced(nil, pos, withDDL, true); err != nil {
		t.Fatalf("OnPosSynced: %v", err)
	}
	later := mysql.Position{Name: "mysql-bin.000006", Pos: 4}
	if err := r.OnPosSynced(nil, later, withDDL, true); err != nil {
		t.Fatalf("OnPosSynced: %v", err)
	}

	events := handedOver(r)
	var change, last *domain.Event
	for _, e := range events {
		if e.Op == domain.OpSchema && !e.Heartbeat {
			change = e
		}
		if !e.Pos.IsZero() {
			last = e
		}
	}
	if change == nil {
		t.Fatalf("handed over %d events and no schema change", len(events))
	}

	resumesAt := func(e *domain.Event) binlogCheckpoint {
		t.Helper()
		var cp binlogCheckpoint
		if _, err := checkpoint.Decode(e.Pos.Payload, &cp); err != nil {
			t.Fatalf("decode %q: %v", e.Pos.Payload, err)
		}
		return cp
	}
	if cp := resumesAt(change); cp.gtidSet() == nil || !cp.gtidSet().Contain(withDDL) || cp.Pos != pos.Pos {
		t.Errorf("the schema change carries %+v, want GTID %s at offset %d: a restart "+
			"from it is sent the ALTER again", cp, withDDL, pos.Pos)
	}
	if cp := resumesAt(last); cp.Name != later.Name || cp.gtidSet() == nil || !cp.gtidSet().Contain(withDDL) {
		t.Errorf("the newest position is %+v, want %s with GTID %s: the reader is still "+
			"inside the schema change and drops every position after it", cp, later.Name, withDDL)
	}
}

// A failure means a restart between MariaDB's CREATE ... SELECT and its rows skips the rows.
func TestASchemaChangeFollowedByRowsInItsTransactionDoesNotEndIt(t *testing.T) {
	r := readerWithMappings(t, mapTable("orders", "orders"))
	r.source, r.flavor = "10.0.0.1:3306/shop", "mariadb"
	r.lastGTID = "0-1-10"

	withGroup, err := mysql.ParseGTIDSet("mariadb", "0-1-11")
	if err != nil {
		t.Fatalf("ParseGTIDSet: %v", err)
	}
	pos := mysql.Position{Name: "mysql-bin.000005", Pos: 900}

	// Not standalone: the group ends with its own COMMIT, after the rows.
	if err := r.OnGTID(nil, &replication.MariadbGTIDEvent{}); err != nil {
		t.Fatalf("OnGTID: %v", err)
	}
	ddl := query("ALTER TABLE orders ADD COLUMN note TEXT")
	ddl.GSet = withGroup
	if err := r.OnDDL(nil, pos, ddl); err != nil {
		t.Fatalf("OnDDL: %v", err)
	}
	if err := r.OnPosSynced(nil, pos, withGroup, true); err != nil {
		t.Fatalf("OnPosSynced: %v", err)
	}

	for _, e := range handedOver(r) {
		if e.Pos.IsZero() {
			continue
		}
		var cp binlogCheckpoint
		if _, err := checkpoint.Decode(e.Pos.Payload, &cp); err != nil {
			t.Fatalf("decode %q: %v", e.Pos.Payload, err)
		}
		if cp.gtidSet() != nil && cp.gtidSet().Contain(withGroup) {
			t.Errorf("handed over %+v before the group's rows were read", cp)
		}
	}
}

// TestTheCapturedTableCountIsPublished is Debezium's CapturedTables.
func TestTheCapturedTableCountIsPublished(t *testing.T) {
	r := readerFor([]config.DatabaseMapping{
		{Tables: []config.TableMapping{
			{SourceTable: "orders", TargetTable: "orders"},
			{SourceTable: "users", TargetTable: "users"},
		}},
		{Tables: []config.TableMapping{
			{SourceTable: "payments", TargetTable: "payments"},
		}},
	}, "root:pw@tcp(10.0.0.1:3306)/bench")

	if got := r.capturedTables(); got != 3 {
		t.Errorf("capturedTables() = %d, want 3 across both mappings", got)
	}
}

// The offset moves with every transaction and is published as a number.
func TestTheSourceInfoIsPublishedOnlyWhenTheLogFileChanges(t *testing.T) {
	r := &Reader{
		source: "10.0.0.1:3306/bench", flavor: "mysql",
		out: make(chan *domain.Event, 8), done: make(chan struct{}),
		Labels: metrics.Labels{"task": t.Name()},
	}
	defer metrics.Default.Forget(r.Labels)

	first := mysql.Position{Name: "mysql-bin.000005", Pos: 100}
	if err := r.OnPosSynced(nil, first, nil, false); err != nil {
		t.Fatalf("OnPosSynced: %v", err)
	}
	if r.lastLogFile != "mysql-bin.000005" {
		t.Fatalf("lastLogFile = %q, want the file just published", r.lastLogFile)
	}

	// The same file, a later offset: nothing new to publish about the file.
	if err := r.OnPosSynced(nil, mysql.Position{Name: "mysql-bin.000005", Pos: 900}, nil, false); err != nil {
		t.Fatalf("OnPosSynced: %v", err)
	}
	if r.lastLogFile != "mysql-bin.000005" {
		t.Errorf("lastLogFile = %q, want it unchanged", r.lastLogFile)
	}

	// A rotation is the case worth publishing again.
	if err := r.OnPosSynced(nil, mysql.Position{Name: "mysql-bin.000006", Pos: 4}, nil, false); err != nil {
		t.Fatalf("OnPosSynced: %v", err)
	}
	if r.lastLogFile != "mysql-bin.000006" {
		t.Errorf("lastLogFile = %q, want the rotated file", r.lastLogFile)
	}
}

// A file and offset recorded against another server is worse than no position:
// read successfully, against data it does not describe. The old syncer checked
// this and the pipeline did not.
func TestAPositionFromAnotherServerIsRefused(t *testing.T) {
	stored := &binlogCheckpoint{
		Name:   "binlog.000012",
		Pos:    41017161,
		Source: "10.0.0.1:3306/shop",
	}

	err := resumableHere(stored, "10.0.0.2:3306/shop")
	if err == nil {
		t.Fatal("a file-and-offset position from another server was accepted")
	}
	if !domain.IsUnrecoverable(err) {
		t.Errorf("err = %v, want it unrecoverable: retrying reads the same wrong bytes", err)
	}
	for _, want := range []string{"10.0.0.1:3306/shop", "10.0.0.2:3306/shop"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not name %q, so nobody can act on it: %v", want, err)
		}
	}
}

// A GTID set names transactions rather than a place in one server's log, so it
// survives the failover this deployment exists for. Discarding it because the
// endpoint moved would force a full re-copy every time Osaka took over.
func TestAGTIDPositionSurvivesTheServerChanging(t *testing.T) {
	stored := &binlogCheckpoint{
		Name:   "binlog.000012",
		Pos:    41017161,
		GTID:   "e67b8f4b-a2d6-11f1-9406-42010a400002:1-624",
		Source: "10.0.0.1:3306/shop",
	}

	if err := resumableHere(stored, "10.0.0.2:3306/shop"); err != nil {
		t.Errorf("a GTID position was refused after a failover: %v", err)
	}
}

// The same source, and no source at all — a checkpoint from a build that did
// not record one — both resume.
func TestAPositionFromThisServerResumes(t *testing.T) {
	here := "10.0.0.1:3306/shop"
	for _, stored := range []*binlogCheckpoint{
		{Name: "binlog.000012", Pos: 41017161, Source: here},
		{Name: "binlog.000012", Pos: 41017161},
		{},
	} {
		if err := resumableHere(stored, here); err != nil {
			t.Errorf("resumableHere(%+v) = %v", stored, err)
		}
	}
}

// The shape the UI's "replicate the whole database" switch saves: one mapping
// carrying no tables. It is the contract between the form and the reader, so it
// is pinned here -- a table list that arrives empty must mean everything, not
// nothing.
func TestTheWholeDatabaseSwitchesShapeTakesEverything(t *testing.T) {
	r := readerFor([]config.DatabaseMapping{{Tables: []config.TableMapping{}}},
		"u:p@tcp(h:3306)/shop")

	includes, err := r.includeTables()
	if err != nil {
		t.Fatalf("includeTables: %v", err)
	}
	if len(includes) != 1 || includes[0] != `shop\..*` {
		t.Errorf("include list = %v, want every table of shop", includes)
	}
}
