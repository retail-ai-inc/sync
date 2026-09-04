package redis

import (
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// Wrapping what the replication link delivers as events the pipeline can
// order. The pipeline decides where to cut a batch and what a batch may hold
// from these fields alone, so a wrong one here is a batch cut in a place the
// applier cannot order around.

func TestAFlushIsAlwaysABarrier(t *testing.T) {
	at := time.Unix(1757000000, 0)
	event := flushEvent(&flush{args: [][]byte{[]byte("FLUSHDB")}, db: 3, offset: 900}, at)

	if !event.EndsTransaction {
		t.Error("a flush did not end its block; a batch could then be cut with " +
			"commands on both sides of it, which the applier cannot order around")
	}
	if event.Key != "FLUSHDB" {
		t.Errorf("Key = %q, want the command name", event.Key)
	}
	if event.NS.DB != "3" {
		t.Errorf("NS.DB = %q, want the database the flush emptied", event.NS.DB)
	}
	if !event.SourceTime.Equal(at) {
		t.Errorf("SourceTime = %v, want the source's own time", event.SourceTime)
	}
	if _, ok := event.Payload.(*flush); !ok {
		t.Errorf("Payload is %T, not the flush itself", event.Payload)
	}
}

func TestAFlushWithNoArgumentsHasNoName(t *testing.T) {
	if got := (&flush{}).name(); got != "" {
		t.Errorf("name() = %q on a flush with no arguments", got)
	}
}

func TestAFlushRendersItsArgumentsForTheTarget(t *testing.T) {
	f := &flush{args: [][]byte{[]byte("FLUSHDB"), []byte("ASYNC")}}

	args := f.arguments()

	if len(args) != 2 {
		t.Fatalf("arguments() = %v", args)
	}
	if string(args[0].([]byte)) != "FLUSHDB" || string(args[1].([]byte)) != "ASYNC" {
		t.Errorf("arguments() = %v", args)
	}
}

func TestACommandRendersItsArgumentsForTheTarget(t *testing.T) {
	c := &command{args: [][]byte{[]byte("SET"), []byte("k"), []byte("v")}}

	args := c.arguments()

	if len(args) != 3 {
		t.Fatalf("arguments() = %v", args)
	}
	if string(args[2].([]byte)) != "v" {
		t.Errorf("the value did not survive: %v", args)
	}
}

// TestAnEmptyCommandRendersAnEmptyArgumentList rather than nil: the client is
// handed this directly, and a nil there is a command with no name.
func TestAnEmptyCommandRendersAnEmptyArgumentList(t *testing.T) {
	if args := (&command{}).arguments(); args == nil {
		t.Error("arguments() = nil")
	} else if len(args) != 0 {
		t.Errorf("arguments() = %v", args)
	}
}

// TestARepairIsAnUpdateOfOneKey. A repair copies a key by value, which lands on
// the target as a write of that key -- so it is ordered against the commands
// touching the same key, and the key has to be on the event for that to happen.
func TestARepairIsAnUpdateOfOneKey(t *testing.T) {
	at := time.Unix(1757000000, 0)
	event := repairEvent(&valueRepair{key: []byte("order:1"), db: 2}, at, true)

	if event.Op != domain.OpUpdate {
		t.Errorf("Op = %v, want an update", event.Op)
	}
	if event.Key != "order:1" {
		t.Errorf("Key = %q", event.Key)
	}
	if event.NS.DB != "2" {
		t.Errorf("NS.DB = %q", event.NS.DB)
	}
	if !event.EndsTransaction {
		t.Error("EndsTransaction was not carried through")
	}
}

func TestARepairInTheMiddleOfABlockDoesNotEndIt(t *testing.T) {
	event := repairEvent(&valueRepair{key: []byte("k")}, time.Now(), false)
	if event.EndsTransaction {
		t.Error("a repair inside a MULTI block ended it, so the block could be " +
			"cut in half")
	}
}

// TestAHeartbeatChangesNothingAndCarriesTheSourcesTime. A stream that delivers
// nothing looks exactly like a source nobody is writing to, which is the one
// failure monitoring cannot otherwise see; the master's ping is the proof, and
// it is only proof if the time on it is the source's.
func TestAHeartbeatChangesNothingAndCarriesTheSourcesTime(t *testing.T) {
	at := time.Unix(1757000000, 0)
	event := heartbeatEvent(at)

	if !event.Heartbeat {
		t.Error("the event is not marked as a heartbeat, so it would be applied")
	}
	if event.Payload != nil {
		t.Errorf("a heartbeat carries a payload: %v", event.Payload)
	}
	if !event.SourceTime.Equal(at) {
		t.Errorf("SourceTime = %v, want the source's own time", event.SourceTime)
	}
	if !event.EndsTransaction {
		t.Error("a heartbeat did not end its block")
	}
}

func TestACommandEventCarriesItsSizeAndBlockBoundary(t *testing.T) {
	cmd := &command{args: [][]byte{[]byte("SET"), []byte("k"), []byte("value")}, db: 1}

	inside := commandEvent(cmd, []byte("k"), time.Now(), false)
	if inside.EndsTransaction {
		t.Error("a command inside a MULTI block ended it")
	}
	if inside.Bytes != 3+1+5 {
		t.Errorf("Bytes = %d, want the summed argument lengths", inside.Bytes)
	}

	last := commandEvent(cmd, []byte("k"), time.Now(), true)
	if !last.EndsTransaction {
		t.Error("the last command of a block did not end it")
	}
}

// The applier's and the checkpoint store's own defaults.

func TestTheApplierConcurrencyFallsBackToADefault(t *testing.T) {
	if got := (&Applier{}).concurrency(); got != defaultConcurrency {
		t.Errorf("concurrency() = %d, want %d", got, defaultConcurrency)
	}
	if got := (&Applier{Concurrency: 4}).concurrency(); got != 4 {
		t.Errorf("concurrency() = %d, want 4", got)
	}
}

// TestTheApplierConcurrencyIsNeverZero: it is the width of a worker pool, and
// zero workers applies nothing while reporting no error.
func TestTheApplierConcurrencyIsNeverZero(t *testing.T) {
	for _, configured := range []int{0, -1, -100} {
		if got := (&Applier{Concurrency: configured}).concurrency(); got <= 0 {
			t.Errorf("concurrency() = %d for a configured %d", got, configured)
		}
	}
}

func TestRepairsInCountsAcrossEveryJob(t *testing.T) {
	jobs := []work{
		{repairs: []repairKey{{}, {}}},
		{},
		{repairs: []repairKey{{}}},
	}
	if got := repairsIn(jobs); got != 3 {
		t.Errorf("repairsIn = %d, want 3", got)
	}
	if got := repairsIn(nil); got != 0 {
		t.Errorf("repairsIn(nil) = %d", got)
	}
}

// TestUnreadMarkersDefaultToTheStreamStart. A slot with no marker of its own
// has applied everything up to the position the stream resumed from -- not
// zero, which would replay the whole stream into that slot.
func TestUnreadMarkersDefaultToTheStreamStart(t *testing.T) {
	checkpoints := &Checkpoints{TaskID: 42, Shard: "0"}

	markers := checkpoints.markersFor(1276319)

	if len(markers) != SlotCount {
		t.Fatalf("got %d markers, want one per slot (%d)", len(markers), SlotCount)
	}
	for slot, marker := range markers {
		if marker != 1276319 {
			t.Fatalf("slot %d starts at %d, want the stream's resume point 1276319",
				slot, marker)
		}
	}
}

// TestMarkersAreOnlyDefaultedOnce: the second call must not overwrite what was
// read from the target, or every slot would look unapplied after a reload.
func TestMarkersAreOnlyDefaultedOnce(t *testing.T) {
	checkpoints := &Checkpoints{TaskID: 42, Shard: "0"}

	first := checkpoints.markersFor(100)
	first[7] = 900

	second := checkpoints.markersFor(500)
	if second[7] != 900 {
		t.Errorf("slot 7 was reset to %d; a second call re-defaulted the markers",
			second[7])
	}
	if second[0] != 100 {
		t.Errorf("slot 0 = %d, want the first call's start", second[0])
	}
}
