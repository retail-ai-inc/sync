//go:build integration

package mysql

import (
	"context"
	"testing"

	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
	"github.com/retail-ai-inc/sync/test/harness"
)

// The arithmetic is the server's: GTID_SUBTRACT on the source, over sets the
// test spells out, so the count needs no transactions to have happened.
func TestTheServerSubtractsTheAppliedSetFromItsOwn(t *testing.T) {
	source := open(t, harness.MySQLSource, sourceDB)
	ctx := context.Background()

	const server = "4a4eec89-4e4a-11ec-bafd-42010a76c007"
	got, err := distance(ctx, source,
		binlogHead{File: "mysql-bin.000009", Position: 9000, GTIDSet: server + ":1-44979211"},
		&binlogCheckpoint{Name: "mysql-bin.000009", Pos: 4000, GTID: server + ":1-44964644", Flavor: "mysql"})
	if err != nil {
		t.Fatalf("distance: %v", err)
	}
	if got.transactions != 44979211-44964644 {
		t.Errorf("transactions behind = %d, want %d", got.transactions, 44979211-44964644)
	}
	if got.bytes != 5000 {
		t.Errorf("bytes behind = %d, want 5000 within one file", got.bytes)
	}

	// Caught up: the applied set holds everything the source has executed.
	level, err := distance(ctx, source,
		binlogHead{GTIDSet: server + ":1-500"},
		&binlogCheckpoint{GTID: server + ":1-500", Flavor: "mysql"})
	if err != nil {
		t.Fatalf("distance: %v", err)
	}
	if level.transactions != 0 {
		t.Errorf("caught up reports %d behind, want 0", level.transactions)
	}
}

// End to end against the two containers: the position the target recorded,
// the head the source reports, and the number between them -- which, with no
// writer running, is however many transactions the source has executed since
// the recorded set, and never negative.
func TestTheBacklogIsMeasuredFromTheTargetsRecordedPosition(t *testing.T) {
	source := open(t, harness.MySQLSource, sourceDB)
	target := open(t, harness.MySQLTarget, targetDB)
	ctx := context.Background()

	store := &checkpoint.SQLStore{DB: target, Schema: targetDB, TaskID: 990001}
	t.Cleanup(func() { _ = store.Purge(ctx) })

	if _, err := measureBacklog(ctx, source, store); err == nil {
		t.Fatal("a task with no recorded position reported a backlog")
	}

	// Record the source's own head as applied: nothing is behind.
	head, err := readBinlogHead(ctx, source)
	if err != nil {
		t.Fatalf("readBinlogHead: %v", err)
	}
	if head.GTIDSet == "" {
		t.Skip("the source runs without GTIDs, so there is no set to compare")
	}
	payload, err := checkpoint.Encode(&binlogCheckpoint{
		Name: head.File, Pos: head.Position, GTID: head.GTIDSet, Flavor: "mysql"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := store.Save(ctx, "", payload); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, err := measureBacklog(ctx, source, store)
	if err != nil {
		t.Fatalf("measureBacklog: %v", err)
	}
	if got.transactions < 0 {
		t.Fatalf("transactions behind = %d, want it measured", got.transactions)
	}
	if got.appliedOffset != int64(head.Position) {
		t.Errorf("applied offset = %d, want the recorded %d", got.appliedOffset, head.Position)
	}
}
