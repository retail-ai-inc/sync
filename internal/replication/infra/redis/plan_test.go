package redis

import (
	"testing"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

func markersWith(pairs map[int]int64) []int64 {
	m := make([]int64, 16384)
	for slot, at := range pairs {
		m[slot] = at
	}
	return m
}

func commandAt(slot int, offset int64, args ...string) *domain.Event {
	raw := make([][]byte, 0, len(args))
	for _, a := range args {
		raw = append(raw, []byte(a))
	}
	return &domain.Event{Payload: &command{args: raw, slot: slot, offset: offset}}
}

func repairAt(slot int, offset int64, key string) *domain.Event {
	return &domain.Event{Payload: &valueRepair{key: []byte(key), slot: slot, offset: offset}}
}

// After a restart the stream is re-read from a floor that is deliberately
// behind the truth, so commands the target already has arrive again.
func TestACommandAlreadyOnTheTargetIsSkipped(t *testing.T) {
	a := &Applier{}
	jobs, err := a.plan([]*domain.Event{
		commandAt(7, 100, "INCR", "counter"), // at the marker: already applied
		commandAt(7, 150, "INCR", "counter"), // past it: has to be applied
	}, markersWith(map[int]int64{7: 100}))
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	if len(jobs) != 1 {
		t.Fatalf("planned %d jobs, want 1", len(jobs))
	}
	if got := len(jobs[0].commands); got != 1 {
		t.Fatalf("slot 7 carries %d commands, want only the one past the marker", got)
	}
	if jobs[0].commands[0].offset != 150 {
		t.Errorf("kept the command at offset %d, want the one at 150",
			jobs[0].commands[0].offset)
	}
	if a.Skipped() != 1 {
		t.Errorf("Skipped() = %d, want 1 — a skip nobody counts is a skip nobody "+
			"can tell from a loss", a.Skipped())
	}
}

// Keys in different slots cannot share a transaction on a cluster, so the
// batch is grouped by slot and each group commits with its own marker.
func TestEachSlotBecomesItsOwnTransaction(t *testing.T) {
	a := &Applier{}
	jobs, err := a.plan([]*domain.Event{
		commandAt(1, 10, "SET", "a", "1"),
		commandAt(2, 20, "SET", "b", "2"),
		commandAt(1, 30, "SET", "a", "3"),
	}, markersWith(nil))
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	if len(jobs) != 2 {
		t.Fatalf("planned %d jobs, want one per slot", len(jobs))
	}
	if jobs[0].slot != 1 || jobs[1].slot != 2 {
		t.Errorf("slots came out as %d, %d — the order the stream had must be kept",
			jobs[0].slot, jobs[1].slot)
	}
	if len(jobs[0].commands) != 2 {
		t.Errorf("slot 1 carries %d commands, want both of its own",
			len(jobs[0].commands))
	}
}

// TestARepairedKeyIsOnlyCopiedOnce keeps a batch from reading and writing the
// same value several times. A repair is "copy this key whole", so doing it
// twice in one batch costs a round trip and changes nothing.
func TestARepairedKeyIsOnlyCopiedOnce(t *testing.T) {
	a := &Applier{}
	jobs, err := a.plan([]*domain.Event{
		repairAt(3, 10, "user:1"),
		repairAt(3, 20, "user:1"),
		repairAt(3, 30, "user:2"),
	}, markersWith(nil))
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	if len(jobs) != 1 {
		t.Fatalf("planned %d jobs, want 1", len(jobs))
	}
	if got := len(jobs[0].repairs); got != 2 {
		t.Errorf("slot 3 repairs %d keys, want 2 distinct ones", got)
	}
}

// TestARepairWithNoOffsetIsAlwaysDone: a repair that did not come from the
// stream carries no offset, and comparing it against a marker would drop it.
func TestARepairWithNoOffsetIsAlwaysDone(t *testing.T) {
	a := &Applier{}
	jobs, err := a.plan([]*domain.Event{
		repairAt(4, 0, "orphan"),
	}, markersWith(map[int]int64{4: 9999}))
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	if len(jobs) != 1 || len(jobs[0].repairs) != 1 {
		t.Fatalf("a repair with no offset was dropped against a marker of 9999")
	}
}

// TestABatchCarryingSomethingElseIsRefused. Silently ignoring a payload this
// applier cannot write would leave a hole in the target that nothing reports.
func TestABatchCarryingSomethingElseIsRefused(t *testing.T) {
	a := &Applier{}
	_, err := a.plan([]*domain.Event{{Payload: "a string"}}, markersWith(nil))
	if err == nil {
		t.Fatal("a payload the applier cannot write was accepted")
	}
}

// TestTheBatchEndIsTheFurthestOffsetItReached, which is what the metadata floor
// advances to once the batch has landed.
func TestTheBatchEndIsTheFurthestOffsetItReached(t *testing.T) {
	got := endOf([]*domain.Event{
		commandAt(1, 10, "SET", "a", "1"),
		repairAt(2, 40, "b"),
		commandAt(3, 25, "SET", "c", "1"),
	})
	if got != 40 {
		t.Errorf("endOf = %d, want 40 — the floor must not advance past what "+
			"the batch actually carried, nor stop short of it", got)
	}
}

// TestFlatteningRunsKeepsTheStreamsOrder. The Redis path applies in stream
// order; a flatten that reordered would apply a delete before the write it was
// meant to undo.
func TestFlatteningRunsKeepsTheStreamsOrder(t *testing.T) {
	first := commandAt(1, 10, "SET", "a", "1")
	second := commandAt(2, 20, "SET", "b", "2")
	third := commandAt(1, 30, "DEL", "a")

	got := flatten([][]*domain.Event{{first}, {second, third}})
	if len(got) != 3 || got[0] != first || got[1] != second || got[2] != third {
		t.Errorf("flatten changed the order of the run")
	}

	single := [][]*domain.Event{{first, second}}
	if out := flatten(single); len(out) != 2 || out[0] != first {
		t.Errorf("a single run was not returned as it was")
	}
}
