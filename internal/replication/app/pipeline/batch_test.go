package pipeline

import (
	"testing"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// keysOf renders the runs as the records they touch, in order, so a test can
// say what it means without walking pointers.
func keysOf(runs [][]*domain.Event) [][]string {
	out := make([][]string, 0, len(runs))
	for _, run := range runs {
		var keys []string
		for _, e := range run {
			if e.Op == domain.OpSchema {
				keys = append(keys, "DDL")
				continue
			}
			keys = append(keys, e.Key)
		}
		out = append(out, keys)
	}
	return out
}

func equal(got [][]string, want [][]string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if len(got[i]) != len(want[i]) {
			return false
		}
		for j := range got[i] {
			if got[i][j] != want[i][j] {
				return false
			}
		}
	}
	return true
}

// TestDistinctRecordsShareOneRun is what makes batching worth anything: a run
// holds no record twice, so an applier may write it in any order.
func TestDistinctRecordsShareOneRun(t *testing.T) {
	runs := orderedRuns([]*domain.Event{
		event("orders", "1", "p1"),
		event("orders", "2", "p2"),
		event("orders", "3", "p3"),
	})

	if got := keysOf(runs); !equal(got, [][]string{{"1", "2", "3"}}) {
		t.Errorf("runs = %v, want one run holding all three", got)
	}
}

// TestTheSameRecordTwiceIsSplit covers the ordering that matters.
func TestTheSameRecordTwiceIsSplit(t *testing.T) {
	first := event("orders", "1", "p1")
	second := event("orders", "1", "p2")
	second.Op = domain.OpDelete

	runs := orderedRuns([]*domain.Event{first, second})

	if got := keysOf(runs); !equal(got, [][]string{{"1"}, {"1"}}) {
		t.Fatalf("runs = %v, want the record in two runs", got)
	}
	if runs[0][0].Op != domain.OpInsert || runs[1][0].Op != domain.OpDelete {
		t.Error("the two events were reordered; the insert has to be applied first")
	}
}

// TestTheSameKeyInDifferentNamespacesDoesNotClash covers the single-stream
// consequence: one batch now carries several tables, and two tables may both
// have a row with id 1.
func TestTheSameKeyInDifferentNamespacesDoesNotClash(t *testing.T) {
	runs := orderedRuns([]*domain.Event{
		event("orders", "1", "p1"),
		event("payments", "1", "p2"),
	})

	if got := keysOf(runs); !equal(got, [][]string{{"1", "1"}}) {
		t.Errorf("runs = %v, want one run: different tables, different records", got)
	}
}

// TestADDLIsABarrier covers the statement that changes the shape of a table
// rather than one row in it.
func TestADDLIsABarrier(t *testing.T) {
	runs := orderedRuns([]*domain.Event{
		event("orders", "1", "p1"),
		event("orders", "", "p2", schema),
		event("orders", "2", "p3"),
	})

	if got := keysOf(runs); !equal(got, [][]string{{"1"}, {"DDL"}, {"2"}}) {
		t.Errorf("runs = %v, want the DDL alone between the two rows", got)
	}
}

// TestABatchIsOnlyFullAtATransactionBoundary is the rule stated on its own.
func TestABatchIsOnlyFullAtATransactionBoundary(t *testing.T) {
	limits := Limits{MaxEvents: 2}
	var b batch

	b.add(event("orders", "1", "p1", midTransaction))
	b.add(event("orders", "2", "p2", midTransaction))
	if b.full(limits) {
		t.Error("the batch reported itself full inside a source transaction")
	}

	b.add(event("orders", "3", "p3"))
	if !b.full(limits) {
		t.Error("the batch is over its limit and the transaction has ended; it should be full")
	}
}

func TestASizeLimitAlsoWaitsForTheBoundary(t *testing.T) {
	limits := Limits{MaxEvents: 1000, MaxBytes: 10}
	var b batch

	big := event("orders", "1", "p1", midTransaction)
	big.Bytes = 100
	b.add(big)
	if b.full(limits) {
		t.Error("the batch reported itself full inside a source transaction")
	}

	b.add(event("orders", "2", "p2"))
	if !b.full(limits) {
		t.Error("the batch is over its size limit at a boundary; it should be full")
	}
}

// TestHeartbeatsAreDroppedBeforeApplying keeps the syncer's own bookkeeping out
// of the payment data.
func TestHeartbeatsAreDroppedBeforeApplying(t *testing.T) {
	kept := applicable([]*domain.Event{
		event("orders", "1", "p1"),
		event("", "", "hb", heartbeat),
		event("orders", "2", "p3"),
	})

	if len(kept) != 2 {
		t.Fatalf("kept %d events, want the two real changes", len(kept))
	}
	for _, e := range kept {
		if e.Heartbeat {
			t.Error("a heartbeat survived into the batch to be applied")
		}
	}
}

// TestOverrunningCountsOnlyTheOpenTransaction makes sure the safety valve
// measures the right thing: many small transactions are fine, one huge one is
// not.
func TestOverrunningCountsOnlyTheOpenTransaction(t *testing.T) {
	limits := Limits{MaxTransactionEvents: 3}
	var b batch

	for i := 0; i < 20; i++ {
		b.add(event("orders", "k", "p"))
		if b.overrunning(limits) {
			t.Fatalf("complete transactions counted towards the limit at event %d", i)
		}
	}

	for i := 0; i < 4; i++ {
		b.add(event("orders", "k", "p", midTransaction))
	}
	if !b.overrunning(limits) {
		t.Error("one transaction of four events passed a limit of three")
	}
}
