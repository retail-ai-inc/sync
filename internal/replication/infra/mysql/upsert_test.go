package mysql

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/go-mysql-org/go-mysql/canal"
)

// keyedSchema has the primary key the production target has. The shared
// ordersSchema deliberately has none, because several tests exercise what the
// handler does without one; an upsert only means anything when there is a key
// to conflict on.
const keyedSchema = `CREATE TABLE orders (id TEXT PRIMARY KEY, customer TEXT, email TEXT)`

func keyedTarget(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "target.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	if _, err := db.Exec(keyedSchema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	return db
}

// ------------------------------------------------------ statement rendering

func TestTheMySQLInsertIsAnUpsert(t *testing.T) {
	got := upsertStatement(dialectMySQL, "shop", "orders", []string{"id", "customer"}, 1)

	want := "INSERT INTO shop.orders (id, customer) VALUES (?,?) " +
		"ON DUPLICATE KEY UPDATE id = VALUES(id), customer = VALUES(customer)"
	if got != want {
		t.Errorf("statement =\n%s\nwant\n%s", got, want)
	}
}

func TestTheMySQLUpsertCarriesEveryRow(t *testing.T) {
	got := upsertStatement(dialectMySQL, "shop", "orders", []string{"id"}, 3)

	want := "INSERT INTO shop.orders (id) VALUES (?), (?), (?) " +
		"ON DUPLICATE KEY UPDATE id = VALUES(id)"
	if got != want {
		t.Errorf("statement =\n%s\nwant\n%s", got, want)
	}
}

// TestTheSQLiteUpsertNeedsNoConflictTarget records why the SQLite flavour is
// spelled the way it is: the binlog does not always name a primary key, and
// SQLite's ON CONFLICT ... DO UPDATE will not parse without one.
func TestTheSQLiteUpsertNeedsNoConflictTarget(t *testing.T) {
	got := upsertStatement(dialectSQLite, "main", "orders", []string{"id", "customer"}, 2)

	want := "INSERT OR REPLACE INTO main.orders (id, customer) VALUES (?,?), (?,?)"
	if got != want {
		t.Errorf("statement =\n%s\nwant\n%s", got, want)
	}
}

func TestAnUnsetDialectRendersMySQL(t *testing.T) {
	if got := (&MyEventHandler{}).flavour(); got != dialectMySQL {
		t.Errorf("handler flavour = %q, want %q", got, dialectMySQL)
	}
	if got := (&MySQLSyncer{}).flavour(); got != dialectMySQL {
		t.Errorf("syncer flavour = %q, want %q", got, dialectMySQL)
	}
}

func TestAnEmptyBatchStillRendersOneRow(t *testing.T) {
	got := upsertStatement(dialectSQLite, "main", "orders", []string{"id"}, 0)

	if want := "INSERT OR REPLACE INTO main.orders (id) VALUES (?)"; got != want {
		t.Errorf("statement = %q, want %q", got, want)
	}
}

// ------------------------------------------------------------ idempotency

// TestAReplayedInsertDoesNotLoseTheRow is the point of the whole change. The
// binlog position is written periodically, so a restart replays the last
// stretch of events. Under a plain INSERT the replay raised a duplicate-key
// error, which RetryDBOperation does not retry, so the row was dropped and the
// error flag raised.
func TestAReplayedInsertDoesNotLoseTheRow(t *testing.T) {
	db := keyedTarget(t)
	h := newHandler(t, db, mapTable("orders", "orders"))

	event := &canal.RowsEvent{
		Table:  sourceTable("orders", "id", "customer", "email"),
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{"1", "Ada", "ada@example.com"}},
	}
	if err := h.OnRow(event); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if err := h.OnRow(event); err != nil {
		t.Fatalf("replayed insert: %v", err)
	}

	if got := rows(t, db); len(got) != 1 || got[0] != "1|Ada|ada@example.com" {
		t.Errorf("target holds %v, want the single row once", got)
	}
	if atomic.LoadInt32(&h.lastExecError) != 0 {
		t.Error("the replayed insert raised the error flag")
	}
}

// TestAReplayedInsertCarriesTheNewerRow covers the other half: when the replayed
// event holds a changed row — the same key written twice at the source — the
// later values have to win rather than being rejected as a duplicate.
func TestAReplayedInsertCarriesTheNewerRow(t *testing.T) {
	db := keyedTarget(t)
	h := newHandler(t, db, mapTable("orders", "orders"))

	table := sourceTable("orders", "id", "customer", "email")
	if err := h.OnRow(&canal.RowsEvent{
		Table: table, Action: canal.InsertAction,
		Rows: [][]interface{}{{"1", "Ada", "old@example.com"}},
	}); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if err := h.OnRow(&canal.RowsEvent{
		Table: table, Action: canal.InsertAction,
		Rows: [][]interface{}{{"1", "Ada", "new@example.com"}},
	}); err != nil {
		t.Fatalf("second insert: %v", err)
	}

	if got := rows(t, db); len(got) != 1 || got[0] != "1|Ada|new@example.com" {
		t.Errorf("target holds %v, want the newer row", got)
	}
}

// TestAResumedSnapshotDoesNotLoseRows is the same guarantee for the initial
// copy, which re-reads rows it already wrote when it is interrupted and
// restarted.
func TestAResumedSnapshotDoesNotLoseRows(t *testing.T) {
	db := keyedTarget(t)
	s := newSyncer(t)
	cols := []string{"id", "customer", "email"}
	batch := [][]interface{}{{"1", "Ada", "a@x"}, {"2", "Grace", "g@x"}}

	if err := s.batchInsert(context.Background(), db, "main", "orders", cols, batch); err != nil {
		t.Fatalf("first copy: %v", err)
	}
	if err := s.batchInsert(context.Background(), db, "main", "orders", cols, batch); err != nil {
		t.Fatalf("resumed copy: %v", err)
	}

	if got := rows(t, db); len(got) != 2 {
		t.Errorf("target holds %v, want the two rows once each", got)
	}
}
