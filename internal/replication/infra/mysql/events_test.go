package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"
	_ "github.com/mattn/go-sqlite3"
	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/sirupsen/logrus"
)

const ordersSchema = `CREATE TABLE orders (id TEXT, customer TEXT, email TEXT)`

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

// targetDSN names the database the handler will address.
const targetDSN = "u:p@tcp(127.0.0.1:3306)/main"

// sourceSchema is the database on the source that the fixtures replicate from.
const sourceSchema = "shop"

func newHandler(t *testing.T, db *sql.DB, mappings []config.DatabaseMapping) *MyEventHandler {
	t.Helper()

	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	return &MyEventHandler{
		mappings:         mappings,
		logger:           logger,
		TargetConnection: targetDSN,
		sourceDatabase:   sourceSchema,
		dialect:          dialectSQLite,
	}
}

// quietLogger is a logger the assertions do not have to read.
func quietLogger() *logrus.Logger {
	l := logrus.New()
	l.SetLevel(logrus.PanicLevel)
	return l
}

// sourceTable describes the replicated table as the binlog reader would.
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

// insertEvent is one INSERT against the orders fixture.
func insertEvent(values ...interface{}) *canal.RowsEvent {
	return &canal.RowsEvent{
		Table:  sourceTable("orders", "id", "customer", "email"),
		Action: canal.InsertAction,
		Rows:   [][]interface{}{values},
	}
}

// apply renders one row event and writes what it produced in a single
// transaction, which is what the pipeline's applier does with a batch.
func apply(db *sql.DB, h *MyEventHandler, e *canal.RowsEvent) error {
	var pending []statement
	previous := h.sink
	h.sink = func(stmt *statement) error {
		pending = append(pending, *stmt)
		return nil
	}
	defer func() { h.sink = previous }()

	if err := h.OnRow(e); err != nil {
		return err
	}
	if len(pending) == 0 {
		return nil
	}
	if db == nil {
		return fmt.Errorf("apply %d statements: no target connection", len(pending))
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	for _, stmt := range pending {
		if _, err := tx.Exec(stmt.query, stmt.args...); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("%s: %w", stmt.query, err)
		}
	}
	return tx.Commit()
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

func TestOnRowAppliesAnInsert(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("orders", "orders"))

	err := apply(db, h, &canal.RowsEvent{
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

	if err := apply(db, h, &canal.RowsEvent{
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

	if err := apply(db, h, &canal.RowsEvent{
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

	if err := apply(db, h, &canal.RowsEvent{
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

// The table used to be matched by name alone — the event's schema was read and
// then only used for logging — so two databases that both have an "orders"
// table were replicated into the same target table, one over the other.
func TestAMappingThatNamesItsDatabaseIsHeldToIt(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, []config.DatabaseMapping{{
		SourceDatabase: "shop",
		Tables:         []config.TableMapping{{SourceTable: "orders", TargetTable: "orders"}},
	}})

	other := sourceTable("orders", "id", "customer", "email")
	other.Schema = "a_completely_different_database"

	if err := apply(db, h, &canal.RowsEvent{
		Table: other, Action: canal.InsertAction,
		Rows: [][]interface{}{{"1", "Ada", "x"}},
	}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	if got := rows(t, db); len(got) != 0 {
		t.Errorf("rows = %v, want the other database's table left alone", got)
	}
}

// TestAMappingWithNoDatabaseMatchesAnyOfThem is the other half: a task with one
// source database does not name it in its mappings, and those must go on
// matching.
func TestAMappingWithNoDatabaseMatchesAnyOfThem(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("orders", "orders"))

	if err := apply(db, h, &canal.RowsEvent{
		Table:  sourceTable("orders", "id", "customer", "email"),
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{"1", "Ada", "x"}},
	}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	if got := rows(t, db); len(got) != 1 {
		t.Errorf("rows = %v, want the row replicated", got)
	}
}

// The lookup used to stop at the first mapping naming the table, so the second
// target silently received nothing and the configuration that asked for it
// looked like it had been accepted.
func TestATableCanBeFannedOutToTwoTargets(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	if _, err := db.Exec(`CREATE TABLE orders_copy (id TEXT PRIMARY KEY, customer TEXT, email TEXT)`); err != nil {
		t.Fatalf("create second target: %v", err)
	}
	h := newHandler(t, db, []config.DatabaseMapping{
		{Tables: []config.TableMapping{{SourceTable: "orders", TargetTable: "orders"}}},
		{Tables: []config.TableMapping{{SourceTable: "orders", TargetTable: "orders_copy"}}},
	})

	if err := apply(db, h, &canal.RowsEvent{
		Table:  sourceTable("orders", "id", "customer", "email"),
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{"1", "Ada", "x"}},
	}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}

	if got := rows(t, db); len(got) != 1 {
		t.Errorf("orders = %v, want the row", got)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM orders_copy`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Errorf("orders_copy holds %d rows, want 1", n)
	}
}

func TestOnRowAppliesAnUpdate(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Ada','x')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	h := newHandler(t, db, mapTable("orders", "orders"))

	if err := apply(db, h, &canal.RowsEvent{
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

	if err := apply(db, h, &canal.RowsEvent{
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

// The loop trusted the binlog to deliver before/after rows in pairs and
// indexed Rows[i+1] without checking, so an odd count panicked — inside the
// canal callback, where nothing recovers it, taking the whole process down.
func TestAnOddUpdateBatchIsRejected(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("orders", "orders"))

	err := apply(db, h, &canal.RowsEvent{
		Table:  sourceTable("orders", "id", "customer", "email"),
		Action: canal.UpdateAction,
		Rows:   [][]interface{}{{"1", "Ada", "x"}},
	})
	if err == nil {
		t.Fatal("an odd update batch was accepted")
	}
	if !strings.Contains(err.Error(), "before/after pairs") {
		t.Errorf("err = %v, want it to say what was wrong", err)
	}
}

func TestOnRowAppliesADelete(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Ada','x'),('2','Grace','y')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	h := newHandler(t, db, mapTable("orders", "orders"))

	if err := apply(db, h, &canal.RowsEvent{
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
// reach the WHERE clause, so a target row that has drifted in its other
// columns is still deleted.
func TestTheDeleteMatchesOnTheKeyAlone(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Drifted','z')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	h := newHandler(t, db, mapTable("orders", "orders"))

	if err := apply(db, h, &canal.RowsEvent{
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

// TestAnUnknownActionStopsReplication covers an action this does not
// understand.
func TestAnUnknownActionStopsReplication(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("orders", "orders"))

	err := apply(db, h, &canal.RowsEvent{
		Table:  sourceTable("orders", "id", "customer", "email"),
		Action: "truncate",
		Rows:   [][]interface{}{{"1", "Ada", "x"}},
	})

	if !domain.IsUnrecoverable(err) {
		t.Fatalf("OnRow returned %v, want an unrecoverable error", err)
	}
	if got := rows(t, db); len(got) != 0 {
		t.Errorf("rows = %v, want nothing written", got)
	}
}

func keylessTable() *schema.Table {
	table := sourceTable("orders", "id", "customer", "email")
	table.PKColumns = nil
	return table
}

// TestAnUpdateWithNoPrimaryKeyStopsReplication covers a table whose rows cannot
// be addressed on the target.
func TestAnUpdateWithNoPrimaryKeyStopsReplication(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Ada','x')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	h := newHandler(t, db, mapTable("orders", "orders"))

	err := apply(db, h, &canal.RowsEvent{
		Table: keylessTable(), Action: canal.UpdateAction,
		Rows: [][]interface{}{{"1", "Ada", "x"}, {"1", "Grace", "y"}},
	})

	if !domain.IsUnrecoverable(err) {
		t.Fatalf("OnRow returned %v, want an unrecoverable error", err)
	}
	if got := rows(t, db); got[0] != "1|Ada|x" {
		t.Errorf("rows = %v, want the target untouched", got)
	}
}

func TestADeleteWithNoPrimaryKeyStopsReplication(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Ada','x')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	h := newHandler(t, db, mapTable("orders", "orders"))

	err := apply(db, h, &canal.RowsEvent{
		Table: keylessTable(), Action: canal.DeleteAction,
		Rows: [][]interface{}{{"1", "Ada", "x"}},
	})

	if !domain.IsUnrecoverable(err) {
		t.Fatalf("OnRow returned %v, want an unrecoverable error", err)
	}
	if got := rows(t, db); len(got) != 1 {
		t.Errorf("rows = %v, want the row still there", got)
	}
}

// TestTheKeylessRefusalNamesTheTable is what makes the stop actionable: an
// operator woken by it has to know which table to fix.
func TestTheKeylessRefusalNamesTheTable(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("orders", "orders"))

	err := apply(db, h, &canal.RowsEvent{
		Table: keylessTable(), Action: canal.DeleteAction,
		Rows: [][]interface{}{{"1", "Ada", "x"}},
	})

	if err == nil || !strings.Contains(err.Error(), "shop.orders") {
		t.Errorf("error = %v, want it to name shop.orders", err)
	}
	if err == nil || !strings.Contains(err.Error(), "allowKeyless") {
		t.Errorf("error = %v, want it to name the way out", err)
	}
}

// TestAllowKeylessRestoresTheBestEffortCopy covers the deliberate opt-out.
func TestAllowKeylessRestoresTheBestEffortCopy(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Ada','x')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	h := newHandler(t, db, mapTable("orders", "orders"))
	h.allowKeyless = true

	if err := apply(db, h, &canal.RowsEvent{
		Table: keylessTable(), Action: canal.DeleteAction,
		Rows: [][]interface{}{{"1", "Ada", "x"}},
	}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	if got := rows(t, db); len(got) != 1 {
		t.Errorf("rows = %v, want the delete skipped rather than refused", got)
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

	if err := apply(db, h, &canal.RowsEvent{
		Table: table, Action: canal.InsertAction,
		Rows: [][]interface{}{{"1", "Ada", "x"}},
	}); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	if got := rows(t, db); len(got) != 1 {
		t.Errorf("rows = %v", got)
	}
}

func TestAnInsertMasksASecuredField(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, securedTable("orders", "orders", "email"))

	if err := apply(db, h, &canal.RowsEvent{
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

	if err := apply(db, h, &canal.RowsEvent{
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

// The delete path passes raw key values through, which is what makes deletes
// work when the key is a secured field — and means the raw value reaches the
// target's query log.
func TestTheDeleteKeyIsNotMasked(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	if _, err := db.Exec(`INSERT INTO orders VALUES ('1','Ada','x')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	h := newHandler(t, db, securedTable("orders", "orders", "id"))

	if err := apply(db, h, &canal.RowsEvent{
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

// failingEvent is a row event the target cannot apply, because it names a
// column the target table does not have.
func failingEvent() *canal.RowsEvent {
	return &canal.RowsEvent{
		Table:  sourceTable("orders", "id", "missing_column"),
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{"1", "x"}},
	}
}

// A statement the target refuses has to reach the caller, which stops rather
// than reading on past a row that never landed.
func TestAFailedStatementIsReported(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("orders", "orders"))

	err := apply(db, h, failingEvent())
	if err == nil {
		t.Fatal("the handler swallowed a statement the target could not apply")
	}
	if !strings.Contains(err.Error(), "main.orders") {
		t.Errorf("error = %v, want the target table named", err)
	}
}

// TestTheFirstFailureOfABatchIsReported covers a multi-row event: the remaining
// rows are still attempted, so the target is as complete as it can be, but the
// failure is not lost.
func TestTheFirstFailureOfABatchIsReported(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("orders", "orders"))

	err := apply(db, h, &canal.RowsEvent{
		Table:  sourceTable("orders", "id", "missing_column"),
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{"1", "x"}, {"2", "y"}},
	})
	if err == nil {
		t.Fatal("the handler reported nothing for a batch where every row failed")
	}
}

const sampleGTID = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"

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

// TestBatchInsertLeavesTheCallersRowsAlone covers a shared slice being written
// to.
func TestBatchInsertLeavesTheCallersRowsAlone(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	s := newSyncer(t)
	s.cfg = config.SyncConfig{Mappings: securedTable("orders", "orders", "email")}

	batch := [][]interface{}{{"1", "Ada", "ada@example.com"}}
	if err := s.batchInsert(context.Background(), db, "main", "orders",
		[]string{"id", "customer", "email"}, batch); err != nil {
		t.Fatalf("batchInsert: %v", err)
	}

	if batch[0][2] != "ada@example.com" {
		t.Errorf("the caller's row now reads %v, want what it read from the source", batch[0][2])
	}
	if got := rows(t, db); strings.Contains(got[0], "ada@example.com") {
		t.Errorf("row = %q, want the address masked on the target", got[0])
	}
}

// TestAGeneratedColumnIsLeftOutOfTheStatement: the primary key is addressed by
// position in the full row, so the generated column has to be dropped from the
// column list without renumbering the indexes the key is found by. The
// generated column sits before the key here on purpose -- with it after, a
// renumbering is indistinguishable from the correct answer.
func TestAGeneratedColumnIsLeftOutOfTheStatement(t *testing.T) {
	table := sourceTable("users", "NameHashed", "Id", "Name", "Email")
	table.Columns[0].IsVirtual = true
	table.PKColumns = []int{1}

	h := &MyEventHandler{mappings: mapTable("users", "users"), logger: quietLogger()}
	cols := []string{"NameHashed", "Id", "Name", "Email"}
	newRow := []interface{}{"deadbeef", 7, "ann", "ann@example.com"}
	oldRow := []interface{}{"cafe", 7, "old", "old@example.com"}

	insert, err := h.buildStatement("INSERT", "shop_bk", "users", cols, table, newRow, nil)
	if err != nil {
		t.Fatalf("build the insert: %v", err)
	}
	if strings.Contains(insert.query, "NameHashed") {
		t.Errorf("insert sets the generated column: %s", insert.query)
	}
	if len(insert.args) != 3 {
		t.Errorf("insert args = %v, want the three writable columns", insert.args)
	}
	if insert.args[0] != 7 {
		t.Errorf("insert args = %v, want the generated column's value dropped with it", insert.args)
	}

	update, err := h.buildStatement("UPDATE", "shop_bk", "users", cols, table, newRow, oldRow)
	if err != nil {
		t.Fatalf("build the update: %v", err)
	}
	if strings.Contains(update.query, "`NameHashed` = ?") {
		t.Errorf("update sets the generated column: %s", update.query)
	}
	if !strings.Contains(update.query, "WHERE `Id` = ?") {
		t.Errorf("update addresses the wrong column: %s", update.query)
	}
	if got := update.args[len(update.args)-1]; got != 7 {
		t.Errorf("key argument = %v, want the old row's Id", got)
	}

	del, err := h.buildStatement("DELETE", "shop_bk", "users", cols, table, newRow, nil)
	if err != nil {
		t.Fatalf("build the delete: %v", err)
	}
	if !strings.Contains(del.query, "WHERE `Id` = ?") || del.args[0] != 7 {
		t.Errorf("delete = %s args %v", del.query, del.args)
	}
}

// TestARowWiderThanTheSchemaIsRefused covers the DROP COLUMN half of the
// schema-change hazard.
//
// canal names the values from the source's current shape; the values came off
// the binlog under the shape they were written with. After a DROP the row is
// one value too wide, and pairing by position wrote every value after the
// dropped column under its neighbour's name while the extra one was discarded
// -- silently, with the row counts still agreeing.
func TestARowWiderThanTheSchemaIsRefused(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("orders", "orders"))

	// Three columns now; the row was written when there were four.
	err := apply(db, h, &canal.RowsEvent{
		Table:  sourceTable("orders", "id", "customer", "email"),
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{"1", "Ada", "dropped", "ada@example.com"}},
	})
	if err == nil {
		t.Fatalf("a row one value too wide was applied: %v", rows(t, db))
	}
	if !domain.IsUnrecoverable(err) {
		t.Errorf("error = %v, want it reported as needing intervention", err)
	}
	for _, want := range []string{"4 values", "3 columns", "orders"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %v, want it to carry %q", err, want)
		}
	}
	if got := rows(t, db); len(got) != 0 {
		t.Errorf("the target was written anyway: %v", got)
	}
}

// And the same for a row that is too narrow, which is what an ADD COLUMN
// replayed from an older position produces. It used to surface as an opaque
// argument-count error from the driver.
func TestARowNarrowerThanTheSchemaIsRefused(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("orders", "orders"))

	err := apply(db, h, &canal.RowsEvent{
		Table:  sourceTable("orders", "id", "customer", "email"),
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{"1", "Ada"}},
	})
	if !domain.IsUnrecoverable(err) {
		t.Fatalf("error = %v, want it reported as needing intervention", err)
	}
	if !strings.Contains(err.Error(), "2 values") {
		t.Errorf("error = %v, want it to say how wide the row was", err)
	}
}
