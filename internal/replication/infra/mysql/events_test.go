package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/schema"
	_ "github.com/mattn/go-sqlite3"
	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/sirupsen/logrus"
)

const ordersSchema = `CREATE TABLE orders (id TEXT, customer TEXT, email TEXT)`

// targetDB stands in for the replication target. The handler builds parameterised
// SQL and hands it to a *sql.DB, so SQLite can execute it — the placeholder
// syntax is the same and "main" is SQLite's own schema name, so the generated
// "main.orders" resolves.
func sqliteTarget(t *testing.T, schemaSQL string) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "target.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	if _, err := db.Exec(schemaSQL); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	return db
}

// targetDSN names the database the handler will address. Only the part after
// the slash is read.
const targetDSN = "u:p@tcp(127.0.0.1:3306)/main"

func newHandler(t *testing.T, db *sql.DB, mappings []config.DatabaseMapping) *MyEventHandler {
	t.Helper()

	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	return &MyEventHandler{
		targetDB:         db,
		mappings:         mappings,
		logger:           logger,
		TargetConnection: targetDSN,
		dialect:          dialectSQLite,
	}
}

// sourceTable describes the replicated table as the binlog reader would, with
// the first column as the primary key.
func sourceTable(name string, columns ...string) *schema.Table {
	cols := make([]schema.TableColumn, len(columns))
	for i, c := range columns {
		cols[i] = schema.TableColumn{Name: c}
	}
	return &schema.Table{Schema: "shop", Name: name, Columns: cols, PKColumns: []int{0}}
}

func mapTable(source, target string) []config.DatabaseMapping {
	return []config.DatabaseMapping{{
		Tables: []config.TableMapping{{SourceTable: source, TargetTable: target}},
	}}
}

func securedTable(source, target string, fields ...string) []config.DatabaseMapping {
	secured := make([]interface{}, len(fields))
	for i, f := range fields {
		secured[i] = map[string]interface{}{"field": f, "securityType": "masked"}
	}
	return []config.DatabaseMapping{{
		Tables: []config.TableMapping{{
			SourceTable:     source,
			TargetTable:     target,
			SecurityEnabled: true,
			FieldSecurity:   secured,
		}},
	}}
}

// apply feeds a row event through the handler the way canal does: the rows
// arrive first and are buffered, then the XID that ends the source transaction
// commits them on the target.
func apply(h *MyEventHandler, e *canal.RowsEvent) error {
	if err := h.OnRow(e); err != nil {
		return err
	}
	return h.OnXID(nil, mysql.Position{})
}

func rows(t *testing.T, db *sql.DB) []string {
	t.Helper()

	res, err := db.Query(`SELECT COALESCE(id,'<null>'), COALESCE(customer,'<null>'),
		COALESCE(email,'<null>') FROM orders ORDER BY rowid`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer res.Close()

	var out []string
	for res.Next() {
		var a, b, c string
		if err := res.Scan(&a, &b, &c); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, strings.Join([]string{a, b, c}, "|"))
	}
	return out
}

// -------------------------------------------------------------- dispatch

func TestOnRowAppliesAnInsert(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("orders", "orders"))

	err := apply(h, &canal.RowsEvent{
		Table:  sourceTable("orders", "id", "customer", "email"),
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{"1", "Ada", "ada@example.com"}},
	})
	if err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	if got := rows(t, db); len(got) != 1 || got[0] != "1|Ada|ada@example.com" {
		t.Errorf("rows = %v", got)
	}
}

func TestOnRowAppliesEveryRowOfABatch(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("orders", "orders"))

	if err := apply(h, &canal.RowsEvent{
		Table:  sourceTable("orders", "id", "customer", "email"),
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{"1", "Ada", "x"}, {"2", "Grace", "y"}},
	}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	if got := rows(t, db); len(got) != 2 {
		t.Errorf("rows = %v, want both", got)
	}
}

func TestOnRowRenamesTheTargetTable(t *testing.T) {
	db := sqliteTarget(t, `CREATE TABLE orders_archive (id TEXT, customer TEXT, email TEXT)`)
	h := newHandler(t, db, mapTable("orders", "orders_archive"))

	if err := apply(h, &canal.RowsEvent{
		Table:  sourceTable("orders", "id", "customer", "email"),
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{"1", "Ada", "x"}},
	}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}

	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM orders_archive`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Errorf("orders_archive holds %d rows", n)
	}
}

func TestOnRowSkipsAnUnmappedTable(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("customers", "customers"))

	if err := apply(h, &canal.RowsEvent{
		Table:  sourceTable("orders", "id", "customer", "email"),
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{"1", "Ada", "x"}},
	}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	if got := rows(t, db); len(got) != 0 {
		t.Errorf("rows = %v, want the event skipped", got)
	}
}

// TestTheMappingLookupIgnoresTheSourceDatabase records that the table is matched
// by name alone: the event's schema is read for logging and never compared. So a
// task watching two source databases that both have an "orders" table
// replicates both into the same target table.
func TestTheMappingLookupIgnoresTheSourceDatabase(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("orders", "orders"))

	other := sourceTable("orders", "id", "customer", "email")
	other.Schema = "a_completely_different_database"

	if err := apply(h, &canal.RowsEvent{
		Table: other, Action: canal.InsertAction,
		Rows: [][]interface{}{{"1", "Ada", "x"}},
	}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	if got := rows(t, db); len(got) != 1 {
		t.Fatalf("rows = %v; the source database appears to be compared now, so "+
			"assert that instead", got)
	}
}

// TestTheFirstMatchingMappingWins records that the lookup stops at the first
// mapping naming the table, so a second mapping for the same source table is
// silently unreachable — a table cannot be fanned out to two targets.
func TestTheFirstMatchingMappingWins(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, []config.DatabaseMapping{
		{Tables: []config.TableMapping{{SourceTable: "orders", TargetTable: "orders"}}},
		{Tables: []config.TableMapping{{SourceTable: "orders", TargetTable: "orders_copy"}}},
	})

	if err := apply(h, &canal.RowsEvent{
		Table:  sourceTable("orders", "id", "customer", "email"),
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{"1", "Ada", "x"}},
	}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	// The second mapping's table does not exist, so had it been used the insert
	// would have failed; one row in "orders" proves only the first was applied.
	if got := rows(t, db); len(got) != 1 {
		t.Errorf("rows = %v", got)
	}
}

func TestOnRowAppliesAnUpdate(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Ada','x')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	h := newHandler(t, db, mapTable("orders", "orders"))

	if err := apply(h, &canal.RowsEvent{
		Table:  sourceTable("orders", "id", "customer", "email"),
		Action: canal.UpdateAction,
		Rows: [][]interface{}{
			{"1", "Ada", "x"},   // before
			{"1", "Grace", "y"}, // after
		},
	}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	if got := rows(t, db); len(got) != 1 || got[0] != "1|Grace|y" {
		t.Errorf("rows = %v", got)
	}
}

// TestAnUpdateMatchesOnTheOldPrimaryKey records that a change to the key column
// itself is handled: the WHERE clause carries the old value, so the row is found
// and its key rewritten.
func TestAnUpdateMatchesOnTheOldPrimaryKey(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Ada','x')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	h := newHandler(t, db, mapTable("orders", "orders"))

	if err := apply(h, &canal.RowsEvent{
		Table:  sourceTable("orders", "id", "customer", "email"),
		Action: canal.UpdateAction,
		Rows:   [][]interface{}{{"1", "Ada", "x"}, {"9", "Ada", "x"}},
	}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	if got := rows(t, db); len(got) != 1 || got[0] != "9|Ada|x" {
		t.Errorf("rows = %v", got)
	}
}

// TestAnOddUpdateBatchPanics records that the update loop trusts the binlog to
// deliver before/after rows in pairs and indexes Rows[i+1] without checking. A
// malformed or truncated event therefore takes the process down rather than
// being rejected.
func TestAnOddUpdateBatchPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("an odd update batch did not panic; the length appears to be " +
				"checked now, so assert that instead")
		}
	}()

	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("orders", "orders"))
	_ = apply(h, &canal.RowsEvent{
		Table:  sourceTable("orders", "id", "customer", "email"),
		Action: canal.UpdateAction,
		Rows:   [][]interface{}{{"1", "Ada", "x"}},
	})
}

func TestOnRowAppliesADelete(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Ada','x'),('2','Grace','y')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	h := newHandler(t, db, mapTable("orders", "orders"))

	if err := apply(h, &canal.RowsEvent{
		Table:  sourceTable("orders", "id", "customer", "email"),
		Action: canal.DeleteAction,
		Rows:   [][]interface{}{{"1", "Ada", "x"}},
	}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	if got := rows(t, db); len(got) != 1 || got[0] != "2|Grace|y" {
		t.Errorf("rows = %v", got)
	}
}

// TestTheDeleteMatchesOnTheKeyAlone records that only the primary key columns
// reach the WHERE clause, so a target row that has drifted in its other columns
// is still deleted. That is the opposite trade-off from the PostgreSQL syncer,
// which matches on every column.
func TestTheDeleteMatchesOnTheKeyAlone(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Drifted','z')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	h := newHandler(t, db, mapTable("orders", "orders"))

	if err := apply(h, &canal.RowsEvent{
		Table:  sourceTable("orders", "id", "customer", "email"),
		Action: canal.DeleteAction,
		Rows:   [][]interface{}{{"1", "Ada", "x"}},
	}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	if got := rows(t, db); len(got) != 0 {
		t.Errorf("rows = %v, want the row deleted on its key", got)
	}
}

// TestAnUnknownActionIsIgnored records that the switch has no default, so an
// action the reader invents is dropped without a log line.
func TestAnUnknownActionIsIgnored(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("orders", "orders"))

	if err := apply(h, &canal.RowsEvent{
		Table:  sourceTable("orders", "id", "customer", "email"),
		Action: "truncate",
		Rows:   [][]interface{}{{"1", "Ada", "x"}},
	}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	if got := rows(t, db); len(got) != 0 {
		t.Errorf("rows = %v", got)
	}
}

// -------------------------------------------------------- keyless tables

// TestAnUpdateWithNoPrimaryKeyIsSkipped records the guard against an
// unqualified UPDATE: with no key columns the statement would rewrite every row,
// so the change is dropped instead. A keyless source table therefore never
// receives updates — silently, since the call reports nothing.
func TestAnUpdateWithNoPrimaryKeyIsSkipped(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Ada','x')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	h := newHandler(t, db, mapTable("orders", "orders"))
	table := sourceTable("orders", "id", "customer", "email")
	table.PKColumns = nil

	if err := apply(h, &canal.RowsEvent{
		Table: table, Action: canal.UpdateAction,
		Rows: [][]interface{}{{"1", "Ada", "x"}, {"1", "Grace", "y"}},
	}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	if got := rows(t, db); got[0] != "1|Ada|x" {
		t.Errorf("rows = %v, want the update skipped", got)
	}
}

func TestADeleteWithNoPrimaryKeyIsSkipped(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Ada','x')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	h := newHandler(t, db, mapTable("orders", "orders"))
	table := sourceTable("orders", "id", "customer", "email")
	table.PKColumns = nil

	if err := apply(h, &canal.RowsEvent{
		Table: table, Action: canal.DeleteAction,
		Rows: [][]interface{}{{"1", "Ada", "x"}},
	}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	if got := rows(t, db); len(got) != 1 {
		t.Errorf("rows = %v, want the delete skipped", got)
	}
}

// TestAnInsertWithNoPrimaryKeyStillRuns records the asymmetry: inserts need no
// key, so a keyless table accumulates rows in the target that no later update or
// delete can ever reach.
func TestAnInsertWithNoPrimaryKeyStillRuns(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("orders", "orders"))
	table := sourceTable("orders", "id", "customer", "email")
	table.PKColumns = nil

	if err := apply(h, &canal.RowsEvent{
		Table: table, Action: canal.InsertAction,
		Rows: [][]interface{}{{"1", "Ada", "x"}},
	}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	if got := rows(t, db); len(got) != 1 {
		t.Errorf("rows = %v", got)
	}
}

// ------------------------------------------------------------- masking

func TestAnInsertMasksASecuredField(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, securedTable("orders", "orders", "email"))

	if err := apply(h, &canal.RowsEvent{
		Table:  sourceTable("orders", "id", "customer", "email"),
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{"1", "Ada", "ada@example.com"}},
	}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}

	got := rows(t, db)
	if len(got) != 1 {
		t.Fatalf("rows = %v", got)
	}
	if strings.Contains(got[0], "ada@example.com") {
		t.Errorf("row = %q, want the address masked", got[0])
	}
	if !strings.HasPrefix(got[0], "1|Ada|") {
		t.Errorf("row = %q, want the other columns untouched", got[0])
	}
}

// TestAnUpdateAlsoMasks records that the MySQL syncer applies masking on both
// paths, unlike the PostgreSQL one where only inserts are masked.
func TestAnUpdateAlsoMasks(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Ada','masked')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	h := newHandler(t, db, securedTable("orders", "orders", "email"))

	if err := apply(h, &canal.RowsEvent{
		Table:  sourceTable("orders", "id", "customer", "email"),
		Action: canal.UpdateAction,
		Rows: [][]interface{}{
			{"1", "Ada", "masked"},
			{"1", "Ada", "grace@example.com"},
		},
	}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	if got := rows(t, db); strings.Contains(got[0], "grace@example.com") {
		t.Errorf("row = %q, want the new address masked too", got[0])
	}
}

// TestTheDeleteKeyIsNotMasked records that the delete path passes the raw key
// values through. That is what makes deletes work at all when the key itself is
// a secured field — but it also means the raw value reaches the target's query
// log.
func TestTheDeleteKeyIsNotMasked(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Ada','x')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	h := newHandler(t, db, securedTable("orders", "orders", "id"))

	if err := apply(h, &canal.RowsEvent{
		Table:  sourceTable("orders", "id", "customer", "email"),
		Action: canal.DeleteAction,
		Rows:   [][]interface{}{{"1", "Ada", "x"}},
	}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	if got := rows(t, db); len(got) != 0 {
		t.Errorf("rows = %v, want the row deleted with the raw key", got)
	}
}

// ---------------------------------------------------------- error flag

// failingEvent is a row event the target cannot apply, because it names a
// column the target table does not have.
func failingEvent() *canal.RowsEvent {
	return &canal.RowsEvent{
		Table:  sourceTable("orders", "id", "missing_column"),
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{"1", "x"}},
	}
}

// TestAFailedStatementIsReportedToCanal pins the contract that keeps the offset
// honest: the failure reaches canal, which stops rather than reading on past a
// row that never landed. The statement is buffered by OnRow and rejected when
// the transaction is applied, so the error surfaces from OnXID.
func TestAFailedStatementIsReportedToCanal(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("orders", "orders"))

	err := apply(h, failingEvent())
	if err == nil {
		t.Fatal("the handler swallowed a statement the target could not apply")
	}
	if !strings.Contains(err.Error(), "main.orders") {
		t.Errorf("error = %v, want the target table named", err)
	}
	if atomic.LoadInt32(&h.lastExecError) != 1 {
		t.Error("the error flag was not raised")
	}
}

// TestTheFirstFailureOfABatchIsReported covers a multi-row event: the remaining
// rows are still attempted, so the target is as complete as it can be, but the
// failure is not lost.
func TestTheFirstFailureOfABatchIsReported(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("orders", "orders"))

	err := apply(h, &canal.RowsEvent{
		Table:  sourceTable("orders", "id", "missing_column"),
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{"1", "x"}, {"2", "y"}},
	})
	if err == nil {
		t.Fatal("the handler reported nothing for a batch where every row failed")
	}
}

// TestTheErrorFlagIsStickyAcrossEvents pins the other half. The flag guards the
// position saver, so a later success must not clear it: the offset that follows
// the failure is still an offset the target never caught up to.
func TestTheErrorFlagIsStickyAcrossEvents(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("orders", "orders"))

	if err := apply(h, failingEvent()); err == nil {
		t.Fatal("the failing event was not reported")
	}

	if err := apply(h, &canal.RowsEvent{
		Table:  sourceTable("orders", "id", "customer", "email"),
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{"2", "Ada", "y"}},
	}); err != nil {
		t.Fatalf("the following event failed too: %v", err)
	}

	if atomic.LoadInt32(&h.lastExecError) != 1 {
		t.Error("a later success cleared the error flag, which would let the " +
			"position advance past the row that was lost")
	}
}

// TestThePositionIsNotWrittenAfterAFailure closes the loop between the two: the
// saver consults the flag and leaves the file untouched, so a restart replays
// from the last offset that was fully applied.
func TestThePositionIsNotWrittenAfterAFailure(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("orders", "orders"))
	h.positionSaverPath = filepath.Join(t.TempDir(), "pos.json")

	if err := h.OnPosSynced(nil, mysql.Position{Name: "binlog.1", Pos: 100}, nil, false); err != nil {
		t.Fatalf("OnPosSynced before any failure: %v", err)
	}
	if err := apply(h, failingEvent()); err == nil {
		t.Fatal("the failing event was not reported")
	}
	if err := h.OnPosSynced(nil, mysql.Position{Name: "binlog.1", Pos: 200}, nil, true); err != nil {
		t.Fatalf("OnPosSynced after a failure: %v", err)
	}

	got := newSyncer(t).loadBinlogPosition(h.positionSaverPath)
	if got == nil {
		t.Fatal("no position was stored at all")
	}
	if got.Pos != 100 {
		t.Errorf("stored offset %d, want the pre-failure 100: the saver advanced "+
			"past a row that never reached the target", got.Pos)
	}
}

// ------------------------------------------------------ position saving

func TestOnPosSyncedWritesThePosition(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "pos.json")
	h := newHandler(t, nil, nil)
	h.positionSaverPath = path

	pos := mysql.Position{Name: "binlog.000004", Pos: 1234}
	if err := h.OnPosSynced(nil, pos, nil, false); err != nil {
		t.Fatalf("OnPosSynced: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	var got mysql.Position
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got != pos {
		t.Errorf("stored %+v, want %+v", got, pos)
	}
}

// TestOnPosSyncedWithNoPathIsANoOp records that an unset position path turns
// the saver off silently, so the task restarts from wherever the server offers.
func TestOnPosSyncedWithNoPathIsANoOp(t *testing.T) {
	h := newHandler(t, nil, nil)

	if err := h.OnPosSynced(nil, mysql.Position{Name: "binlog.1", Pos: 1}, nil, false); err != nil {
		t.Fatalf("OnPosSynced with no path: %v", err)
	}
}

func TestOnPosSyncedReportsAnUnwritablePath(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocker, nil, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	h := newHandler(t, nil, nil)
	h.positionSaverPath = filepath.Join(blocker, "pos.json")

	if err := h.OnPosSynced(nil, mysql.Position{Name: "binlog.1", Pos: 1}, nil, false); err == nil {
		t.Fatal("OnPosSynced to an unwritable path returned no error")
	}
}

const sampleGTID = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"

// TestTheSavedPositionCarriesTheGTIDSet pins what makes a checkpoint survive a
// failover. The file-and-offset pair only means something on the server that
// produced it; the GTID set names the transactions and stays meaningful when
// the source is failed over to a replica with its own binlog files.
func TestTheSavedPositionCarriesTheGTIDSet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pos.json")
	h := newHandler(t, nil, nil)
	h.positionSaverPath = path
	h.flavor = mysql.MySQLFlavor

	gtid, err := mysql.ParseMysqlGTIDSet(sampleGTID)
	if err != nil {
		t.Fatalf("ParseMysqlGTIDSet: %v", err)
	}
	if err := h.OnPosSynced(nil, mysql.Position{Name: "binlog.1", Pos: 4}, gtid, false); err != nil {
		t.Fatalf("OnPosSynced: %v", err)
	}

	cp := newSyncer(t).loadCheckpoint(path)
	if cp == nil {
		t.Fatal("loadCheckpoint returned nil for a file the saver wrote")
	}
	if cp.GTID != sampleGTID {
		t.Errorf("stored GTID %q, want %q", cp.GTID, sampleGTID)
	}
	if cp.Flavor != mysql.MySQLFlavor {
		t.Errorf("stored flavor %q, want %q", cp.Flavor, mysql.MySQLFlavor)
	}
	if got := cp.gtidSet(); got == nil || got.String() != sampleGTID {
		t.Errorf("the stored set did not parse back: %v", got)
	}
}

// TestACheckpointWithNoGTIDSetFallsBackToTheOffset covers the source that has
// GTIDs turned off, and files written before they were recorded.
func TestACheckpointWithNoGTIDSetFallsBackToTheOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pos.json")
	h := newHandler(t, nil, nil)
	h.positionSaverPath = path

	want := mysql.Position{Name: "binlog.000007", Pos: 990}
	if err := h.OnPosSynced(nil, want, nil, false); err != nil {
		t.Fatalf("OnPosSynced: %v", err)
	}

	cp := newSyncer(t).loadCheckpoint(path)
	if cp == nil {
		t.Fatal("loadCheckpoint returned nil")
	}
	if cp.gtidSet() != nil {
		t.Errorf("a set was recorded for a source that sent none: %q", cp.GTID)
	}
	if cp.position() != want {
		t.Errorf("position %+v, want %+v", cp.position(), want)
	}
}

// TestAPositionFileFromAnOlderBuildStillLoads pins the file format's
// compatibility: the two fields keep the capitalised names mysql.Position
// marshals to, so an existing deployment resumes rather than restarting from
// the current end of the binlog.
func TestAPositionFileFromAnOlderBuildStillLoads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pos.json")
	if err := os.WriteFile(path, []byte(`{"Name":"binlog.000003","Pos":154}`), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	cp := newSyncer(t).loadCheckpoint(path)
	if cp == nil {
		t.Fatal("loadCheckpoint returned nil for a file in the old format")
	}
	if want := (mysql.Position{Name: "binlog.000003", Pos: 154}); cp.position() != want {
		t.Errorf("position %+v, want %+v", cp.position(), want)
	}
	if cp.gtidSet() != nil {
		t.Error("a GTID set was invented for a file that has none")
	}
}

// TestAnUnreadableGTIDSetFallsBackToTheOffset records that a corrupt set does
// not stop the task: the offset is still there, and resuming from it is better
// than not resuming at all.
func TestAnUnreadableGTIDSetFallsBackToTheOffset(t *testing.T) {
	cp := &binlogCheckpoint{Name: "binlog.1", Pos: 4, GTID: "not-a-gtid-set"}

	if got := cp.gtidSet(); got != nil {
		t.Errorf("gtidSet() = %v for an unparseable set", got)
	}
}

// TestAMissingFlavourReadsAsMySQL covers a checkpoint whose flavour was not
// recorded, which is the shape an older build wrote.
func TestAMissingFlavourReadsAsMySQL(t *testing.T) {
	cp := &binlogCheckpoint{Name: "binlog.1", Pos: 4, GTID: sampleGTID}

	got := cp.gtidSet()
	if got == nil {
		t.Fatal("gtidSet() = nil for a set with no flavour recorded")
	}
	if got.String() != sampleGTID {
		t.Errorf("gtidSet() = %q, want %q", got, sampleGTID)
	}
}

// TestTheSavedPositionRoundTrips closes the loop with the loader, which is what
// makes a restart resume where the stream stopped.
func TestTheSavedPositionRoundTrips(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pos.json")
	h := newHandler(t, nil, nil)
	h.positionSaverPath = path
	want := mysql.Position{Name: "binlog.000009", Pos: 4711}

	if err := h.OnPosSynced(nil, want, nil, true); err != nil {
		t.Fatalf("OnPosSynced: %v", err)
	}
	got := newSyncer(t).loadBinlogPosition(path)
	if got == nil {
		t.Fatal("loadBinlogPosition returned nil for a file the saver wrote")
	}
	if *got != want {
		t.Errorf("loaded %+v, wrote %+v", *got, want)
	}
}

func TestTheHandlerNamesItself(t *testing.T) {
	if got := newHandler(t, nil, nil).String(); got != "MyEventHandler" {
		t.Errorf("String() = %q", got)
	}
}

// -------------------------------------------------------- batch insert

func TestBatchInsertWritesEveryRow(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	s := newSyncer(t)

	err := s.batchInsert(context.Background(), db, "main", "orders",
		[]string{"id", "customer", "email"},
		[][]interface{}{{"1", "Ada", "x"}, {"2", "Grace", "y"}})
	if err != nil {
		t.Fatalf("batchInsert: %v", err)
	}
	if got := rows(t, db); len(got) != 2 {
		t.Errorf("rows = %v", got)
	}
}

func TestBatchInsertOnAnEmptyBatchIsANoOp(t *testing.T) {
	s := newSyncer(t)

	// A nil database proves nothing is executed.
	if err := s.batchInsert(context.Background(), nil, "main", "orders",
		[]string{"id"}, nil); err != nil {
		t.Fatalf("batchInsert on an empty batch: %v", err)
	}
}

func TestBatchInsertReportsAFailingStatement(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	s := newSyncer(t)

	err := s.batchInsert(context.Background(), db, "main", "orders",
		[]string{"id", "missing_column"}, [][]interface{}{{"1", "x"}})
	if err == nil || !strings.Contains(err.Error(), "batchInsert Exec") {
		t.Fatalf("err = %v, want an exec failure", err)
	}
}

// TestBatchInsertMasksInPlace records that the initial-sync path rewrites the
// caller's own slice when masking is enabled, rather than building a copy the
// way the binlog path does. The rows the caller read from the source are
// modified underneath it, so anything that inspects them afterwards — a retry,
// a row count, a log line — sees the masked values, not what the source held.
func TestBatchInsertMasksInPlace(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	s := newSyncer(t)
	s.cfg = config.SyncConfig{Mappings: securedTable("orders", "orders", "email")}

	batch := [][]interface{}{{"1", "Ada", "ada@example.com"}}
	if err := s.batchInsert(context.Background(), db, "main", "orders",
		[]string{"id", "customer", "email"}, batch); err != nil {
		t.Fatalf("batchInsert: %v", err)
	}

	if batch[0][2] == "ada@example.com" {
		t.Fatal("the caller's slice was left alone; the copy appears to be made " +
			"now, so assert that instead")
	}
	if got := rows(t, db); strings.Contains(got[0], "ada@example.com") {
		t.Errorf("row = %q, want the address masked", got[0])
	}
}
