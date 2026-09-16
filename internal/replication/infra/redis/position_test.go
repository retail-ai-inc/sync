package redis

import (
	"math"
	"strings"
	"testing"
)

// Every field has to come back: the replication id decides whether a partial
// resync is even offered, and the value-phase window decides whether a change
// is replayed by command or by re-reading the key.
func TestAPositionSurvivesBeingWrittenDownAndReadBack(t *testing.T) {
	want := streamPosition{
		ReplID:     "8a1c0f2b7d4e",
		Offset:     126895460724,
		Phase:      phaseValue,
		ValueUntil: 126895000000,
	}

	payload, err := want.encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := decodePosition(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != want {
		t.Errorf("round trip lost something:\n got %+v\nwant %+v", got, want)
	}
}

// A Redis replication offset passes 2^53 on a busy source, and a decoder that
// reads it as a float would round it — resuming a few bytes off, which is
// either a repeated command or a skipped one.
func TestALargeOffsetIsNotRounded(t *testing.T) {
	const offset int64 = 9007199254740993 // 2^53 + 1, the first integer a float64 cannot hold

	payload, err := streamPosition{ReplID: "abc", Offset: offset}.encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := decodePosition(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Offset != offset {
		t.Errorf("offset came back as %d, want %d — a rounded offset resumes in "+
			"the wrong place", got.Offset, offset)
	}
}

// TestAnEmptyPositionAsksForEverything: nothing recorded means no copy has been
// made, and the answer is a full one rather than a guess.
func TestAnEmptyPositionAsksForEverything(t *testing.T) {
	got, err := decodePosition("")
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !got.IsZero() {
		t.Errorf("%+v is not zero, so a fresh task would resume from nowhere", got)
	}
}

// TestAPositionWithNoReplicationIdIsRefused is the case that would otherwise
// resume from an offset belonging to a different history.
func TestAPositionWithNoReplicationIdIsRefused(t *testing.T) {
	_, err := decodePosition(`{"offset":12345}`)
	if err == nil {
		t.Fatal("a position with no replication id was accepted")
	}
	if !strings.Contains(err.Error(), "replication id") {
		t.Errorf("error %q does not say what is missing", err)
	}
}

// TestUnreadablePositionIsRefused: a corrupted position must stop the task, not
// silently become "start from the beginning" — that would re-copy a live
// payment database without anybody asking for it.
func TestUnreadablePositionIsRefused(t *testing.T) {
	_, err := decodePosition("{not json")
	if err == nil {
		t.Fatal("a corrupted position was accepted")
	}
}

// TestTheValuePhaseEndsWhereTheCopyDid pins the rule that keeps an initial
// copy from double-applying.
func TestTheValuePhaseEndsWhereTheCopyDid(t *testing.T) {
	p := streamPosition{ReplID: "abc", Phase: phaseValue, ValueUntil: 1000}

	if !p.inValuePhase(999) {
		t.Error("an offset inside the copy's window is not in the value phase, so " +
			"a command from before the key was read would be replayed and applied twice")
	}
	if p.inValuePhase(1000) {
		t.Error("the offset the copy finished at is still in the value phase")
	}
	if p.inValuePhase(1001) {
		t.Error("an offset past the copy's window is still in the value phase")
	}

	streaming := streamPosition{ReplID: "abc", Phase: phaseCommand, ValueUntil: 1000}
	if streaming.inValuePhase(999) {
		t.Error("a position past the copy reports the value phase, which would keep " +
			"re-reading every key for ever")
	}
}

// TestEachTaskAndShardGetsItsOwnMetadataKey keeps two tasks writing to the same
// target from overwriting each other's position — the failure that leaves both
// resuming from the other's offset.
func TestEachTaskAndShardGetsItsOwnMetadataKey(t *testing.T) {
	seen := map[string]string{}
	for _, c := range []struct{ task, shard string }{
		{"51", "0-5460"}, {"51", "5461-10922"}, {"52", "0-5460"},
	} {
		task := 51
		if c.task == "52" {
			task = 52
		}
		key := metaKey(task, c.shard)
		if other, clash := seen[key]; clash {
			t.Fatalf("task %s shard %s produced %q, already used by %s",
				c.task, c.shard, key, other)
		}
		seen[key] = c.task + "/" + c.shard
		if !strings.Contains(key, c.task) || !strings.Contains(key, c.shard) {
			t.Errorf("key %q does not identify task %s shard %s", key, c.task, c.shard)
		}
	}
}

// The value phase has to cover the whole of the first copy.
//
// The copy is taken with SCAN, so it is a smear: a key read early may have
// changed before a key read late. Everything inside that smear is applied by
// re-reading the key's value, because replaying the command instead could
// apply it twice — an INCR already in the copy, a DEL of a key the copy then
// re-adds. The window used to be closed at the offset the copy STARTED at,
// which left every command issued during the copy on the replay path.
func TestTheValuePhaseCoversTheWholeCopy(t *testing.T) {
	const copyStarted, copyFinished = 1000, 5000

	// While the copy runs the end is not known, so nothing is excluded.
	open := streamPosition{Phase: phaseValue, ValueUntil: math.MaxInt64}
	for _, offset := range []int64{copyStarted, copyStarted + 1, copyFinished - 1} {
		if !open.inValuePhase(offset) {
			t.Errorf("offset %d during the copy is not applied by value", offset)
		}
	}

	// Once the copy has finished the window closes where it really ended.
	closed := streamPosition{Phase: phaseValue, ValueUntil: copyFinished}
	if !closed.inValuePhase(copyFinished - 1) {
		t.Error("an offset from inside the copy window is replayed by command")
	}
	if closed.inValuePhase(copyFinished) {
		t.Error("an offset after the copy is still applied by value")
	}
	if closed.inValuePhase(copyFinished + 1) {
		t.Error("an offset after the copy is still applied by value")
	}
}
