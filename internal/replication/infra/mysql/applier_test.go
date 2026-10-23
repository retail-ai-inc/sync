package mysql

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
)

// The applier is driven against SQLite here.

func applierTarget(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "target.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	if _, err := db.Exec(`CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	return db
}

func newApplier(t *testing.T, db *sql.DB) (*Applier, *checkpoint.SQLStore) {
	t.Helper()

	store := &checkpoint.SQLStore{DB: db, TaskID: 1}
	if err := store.Ensure(context.Background()); err != nil {
		t.Fatalf("ensure the checkpoint table: %v", err)
	}

	log := logrus.New()
	log.SetLevel(logrus.PanicLevel)
	return &Applier{DB: db, Checkpoints: store, Logger: log}, store
}

func insertEventFor(id int, customer string) *domain.Event {
	return &domain.Event{
		NS:      domain.Namespace{DB: "shop", Object: "orders"},
		Op:      domain.OpInsert,
		Key:     customer,
		Payload: statement{query: "INSERT INTO orders (id, customer) VALUES (?, ?)", args: []interface{}{id, customer}},
	}
}

func rowCount(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM orders").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

// TestABatchAndItsPositionCommitTogether is the guarantee the applier exists
// for.
func TestABatchAndItsPositionCommitTogether(t *testing.T) {
	db := applierTarget(t)
	applier, store := newApplier(t, db)

	runs := [][]*domain.Event{{insertEventFor(1, "ada"), insertEventFor(2, "grace")}}
	committed, err := applier.Apply(context.Background(), runs, domain.Position{Payload: "pos-1"})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if !committed {
		t.Error("Apply reported it did not record the position, so the runner would write it separately")
	}
	if got := rowCount(t, db); got != 2 {
		t.Errorf("target holds %d rows, want 2", got)
	}
	got, err := store.Load(context.Background(), "")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got != "pos-1" {
		t.Errorf("stored position = %q, want pos-1", got)
	}
}

// TestAFailedBatchLeavesNothingBehind covers the atomicity the design depends
// on.
func TestAFailedBatchLeavesNothingBehind(t *testing.T) {
	db := applierTarget(t)
	applier, store := newApplier(t, db)

	// The second statement violates the primary key, so the batch cannot be
	// applied. The first must not survive it.
	runs := [][]*domain.Event{{
		insertEventFor(1, "ada"),
		insertEventFor(1, "duplicate"),
	}}

	if _, err := applier.Apply(context.Background(), runs, domain.Position{Payload: "pos-1"}); err == nil {
		t.Fatal("Apply returned nil for a batch that cannot be applied")
	}

	if got := rowCount(t, db); got != 0 {
		t.Errorf("target holds %d rows after a failed batch, want 0", got)
	}
	got, err := store.Load(context.Background(), "")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got != "" {
		t.Errorf("stored position = %q after a failed batch, want none", got)
	}
}

// If the position is written on its own connection after the data has
// committed, a failure at that moment leaves the rows on the target with the
// position still pointing before them — so a restart replays them, and on a
// table without a primary key that means duplicates.
func TestAFailedPositionWriteRollsBackTheData(t *testing.T) {
	db := applierTarget(t)
	applier, _ := newApplier(t, db)

	if _, err := db.Exec("DROP TABLE _sync_checkpoint"); err != nil {
		t.Fatalf("drop the checkpoint table: %v", err)
	}

	runs := [][]*domain.Event{{insertEventFor(1, "ada")}}
	if _, err := applier.Apply(context.Background(), runs, domain.Position{Payload: "pos-1"}); err == nil {
		t.Fatal("Apply returned nil when the position could not be recorded")
	}

	if got := rowCount(t, db); got != 0 {
		t.Errorf("target holds %d rows, want 0: the data has to roll back with the position", got)
	}
}

// TestTheFailureRollsBackEarlierRunsToo covers a batch split into several runs
// because it touches the same record twice.
func TestTheFailureRollsBackEarlierRunsToo(t *testing.T) {
	db := applierTarget(t)
	applier, _ := newApplier(t, db)

	runs := [][]*domain.Event{
		{insertEventFor(1, "ada")},
		{insertEventFor(2, "grace")},
		{insertEventFor(1, "duplicate")},
	}

	if _, err := applier.Apply(context.Background(), runs, domain.Position{Payload: "pos-1"}); err == nil {
		t.Fatal("Apply returned nil for a batch that cannot be applied")
	}
	if got := rowCount(t, db); got != 0 {
		t.Errorf("target holds %d rows, want 0: the successful runs have to roll back too", got)
	}
}

// TestWithoutACheckpointStoreTheRunnerIsToldToRecordIt covers a target that
// cannot take part in the transaction.
func TestWithoutACheckpointStoreTheRunnerIsToldToRecordIt(t *testing.T) {
	db := applierTarget(t)
	applier, _ := newApplier(t, db)
	applier.Checkpoints = nil

	committed, err := applier.Apply(context.Background(),
		[][]*domain.Event{{insertEventFor(1, "ada")}},
		domain.Position{Payload: "pos-1"})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if committed {
		t.Error("Apply claimed it recorded the position with no store to record it in")
	}
	if got := rowCount(t, db); got != 1 {
		t.Errorf("target holds %d rows, want 1", got)
	}
}

// TestAnEventWithoutAStatementIsRefused covers a programming error rather than a
// database one: retrying it would fail identically for as long as anybody let
// it, and applying the rest of the batch would tear it.
func TestAnEventWithoutAStatementIsRefused(t *testing.T) {
	db := applierTarget(t)
	applier, _ := newApplier(t, db)

	runs := [][]*domain.Event{{
		insertEventFor(1, "ada"),
		{NS: domain.Namespace{DB: "shop", Object: "orders"}, Op: domain.OpInsert, Payload: "not a statement"},
	}}

	_, err := applier.Apply(context.Background(), runs, domain.Position{Payload: "pos-1"})
	if !domain.IsUnrecoverable(err) {
		t.Fatalf("Apply returned %v, want an unrecoverable error", err)
	}
	if got := rowCount(t, db); got != 0 {
		t.Errorf("target holds %d rows, want 0", got)
	}
}

// TestAnEmptyBatchIsNotATransaction keeps the applier from opening and
// committing a transaction for a batch that holds only heartbeats.
func TestAnEmptyBatchIsNotATransaction(t *testing.T) {
	db := applierTarget(t)
	applier, store := newApplier(t, db)

	committed, err := applier.Apply(context.Background(), nil, domain.Position{Payload: "pos-1"})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if committed {
		t.Error("Apply claimed it recorded the position for a batch it did not write")
	}
	got, err := store.Load(context.Background(), "")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got != "" {
		t.Errorf("stored position = %q, want none", got)
	}
}
