package redis

import (
	"context"
	goredis "github.com/redis/go-redis/v9"
	"strings"
	"testing"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

func cmd(parts ...string) *Command {
	args := make([][]byte, 0, len(parts))
	for _, p := range parts {
		args = append(args, []byte(p))
	}
	return &Command{Args: args}
}

// table is the command specification a server would have given us.
func table() *commandTable {
	return &commandTable{
		keyAt:   map[string]int{"set": 1, "del": 1, "hset": 1, "eval": 0, "get": 1},
		movable: map[string]bool{"eval": true},
	}
}

// Emptying the source empties the target with it. This was refused for a long
// time, because a FLUSHALL typed by mistake in Tokyo is indistinguishable from
// here and would destroy the copy in Osaka at the same moment. A copy that is
// meant to be what the source is has the opposite problem: refusing it means
// the two diverge the first time the source clears a cache, and sources do that
// as a matter of routine.
func TestEmptyingTheSourceIsCarriedAsABarrier(t *testing.T) {
	for _, name := range []string{"FLUSHALL", "flushall", "FLUSHDB", "SWAPDB"} {
		t.Run(name, func(t *testing.T) {
			class, key, err := table().classify(context.Background(), nil, cmd(name))
			if err != nil {
				t.Fatalf("classify: %v", err)
			}
			if class != classFlush {
				t.Errorf("%s classified as %v, want the flush class", name, class)
			}
			if key != nil {
				t.Errorf("%s produced key %q, want none: it belongs to no slot", name, key)
			}
		})
	}
}

// A PING is proof the link is alive; REPLCONF is the protocol talking to
// itself; PUBLISH is not state at all — forwarding it would deliver the
// message twice to anything listening in both regions.
func TestTheCommandsThatCarryNoDataAreRecognised(t *testing.T) {
	for _, c := range []struct {
		name string
		want classification
	}{
		{"PING", classHeartbeat},
		{"MULTI", classTransactionBegin},
		{"EXEC", classTransactionEnd},
		{"REPLCONF", classIgnored},
		{"PUBLISH", classIgnored},
		{"SPUBLISH", classIgnored},
		// SELECT is not ignored: it says which database follows it.
		{"SELECT", classSelect},
	} {
		t.Run(c.name, func(t *testing.T) {
			class, _, err := table().classify(context.Background(), nil, cmd(c.name, "x"))
			if err != nil {
				t.Fatalf("classify: %v", err)
			}
			if class != c.want {
				t.Errorf("%s classified as %v, want %v", c.name, class, c.want)
			}
		})
	}
}

// TestAWriteReportsTheKeyThatNamesItsSlot is what decides which slot's marker
// moves.
func TestAWriteReportsTheKeyThatNamesItsSlot(t *testing.T) {
	class, key, err := table().classify(context.Background(), nil, cmd("SET", "user:1", "v"))
	if err != nil {
		t.Fatalf("classify: %v", err)
	}
	if class != classWrite {
		t.Errorf("SET classified as %v, want write", class)
	}
	if string(key) != "user:1" {
		t.Errorf("key = %q, want user:1", key)
	}
}

// TestACommandTheTargetDoesNotKnowIsRefusedLoudly: guessing which key an
// unknown command touches would put its marker in the wrong slot, and applying
// it would fail anyway.
func TestACommandTheTargetDoesNotKnowIsRefusedLoudly(t *testing.T) {
	class, _, err := table().classify(context.Background(), nil, cmd("JSON.SET", "doc", "$", "1"))
	if class != classRefused {
		t.Errorf("classified as %v, want refused", class)
	}
	if err == nil {
		t.Fatal("an unknown command was refused silently")
	}
	if !domain.IsUnrecoverable(err) {
		t.Errorf("error is %v, want an unrecoverable one — retrying cannot teach "+
			"the target a command it does not have", err)
	}
	if !strings.Contains(err.Error(), "JSON.SET") {
		t.Errorf("error %q does not name the command", err)
	}
}

// TestACommandMissingItsKeyIsRefused guards against a truncated or malformed
// command silently becoming a no-op.
func TestACommandMissingItsKeyIsRefused(t *testing.T) {
	class, _, err := table().classify(context.Background(), nil, cmd("SET"))
	if class != classRefused {
		t.Errorf("classified as %v, want refused", class)
	}
	if err == nil {
		t.Fatal("a command with no key was accepted")
	}
}

// TestAnEmptyCommandIsIgnored: the stream carries protocol noise, and an empty
// command is not something to stop replication over.
func TestAnEmptyCommandIsIgnored(t *testing.T) {
	class, key, err := table().classify(context.Background(), nil, cmd())
	if err != nil {
		t.Fatalf("classify: %v", err)
	}
	if class != classIgnored {
		t.Errorf("classified as %v, want ignored", class)
	}
	if key != nil {
		t.Errorf("key = %q, want none", key)
	}
}

// TestTheDatabaseIsReadFromSelect: a standalone server interleaves every
// database into one replication stream, so which one a command belongs to is
// only knowable from the SELECT before it.
func TestTheDatabaseIsReadFromSelect(t *testing.T) {
	for _, c := range []struct {
		arg  string
		want int
		bad  bool
	}{
		{"0", 0, false},
		{"1", 1, false},
		{"11", 11, false},
		{"-1", 0, true},
		{"x", 0, true},
	} {
		got, err := selectedDB(cmd("SELECT", c.arg))
		if c.bad {
			if err == nil {
				t.Errorf("SELECT %q was accepted as database %d", c.arg, got)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Errorf("SELECT %q = %d, %v; want %d", c.arg, got, err, c.want)
		}
	}
	if _, err := selectedDB(cmd("SELECT")); err == nil {
		t.Error("SELECT with no database was accepted")
	}
}

// A flush belongs to no key, so it belongs to no slot, and the ordinary path
// applies slots concurrently. It has to be recognised as a batch that cannot be
// applied that way.
func TestABatchWithAFlushIsRecognised(t *testing.T) {
	plain := []*domain.Event{
		{Payload: &command{args: [][]byte{[]byte("set"), []byte("k")}, slot: 1}},
	}
	if containsFlush(plain) {
		t.Error("a batch of ordinary writes was taken for one containing a flush")
	}

	withFlush := append(plain,
		&domain.Event{Payload: &flush{args: [][]byte{[]byte("flushdb")}, db: 2}})
	if !containsFlush(withFlush) {
		t.Error("a batch containing a flush was not recognised, so it would be " +
			"applied slot by slot and a write could land beside the flush")
	}
}

// TestABatchOfOnlyAFlushCarriesItsOffset: a batch with no offset is refused and
// held, so a flush that did not report one stopped the stream behind it.
func TestABatchOfOnlyAFlushCarriesItsOffset(t *testing.T) {
	events := []*domain.Event{
		{Payload: &flush{args: [][]byte{[]byte("flushdb")}, db: 5, offset: 4242}},
	}
	if got := endOf(events); got != 4242 {
		t.Errorf("batch end = %d, want 4242: a batch that reports none is refused", got)
	}
}

// TestTheStateIsRestoredAfterTheFlushNotBefore: the whole point is the order.
// A position written before the flush is erased by it, which leaves the task
// with no resume point and the target unclaimed -- the same hole the restore
// exists to close.
func TestTheStateIsRestoredAfterTheFlushNotBefore(t *testing.T) {
	var order []string
	applier := &Applier{
		Positions:     &Checkpoints{TaskID: 42, Shard: "0"},
		BookkeepingDB: 0,
		RestoreState: func(_ context.Context, _ goredis.Pipeliner, position string) {
			order = append(order, "restore:"+position)
		},
	}

	// A recorder standing in for the transaction, so the order is what is
	// asserted rather than the effect.
	events := []*domain.Event{
		{Payload: &command{args: [][]byte{[]byte("set"), []byte("k")}, db: 1, slot: 1, offset: 10}},
		{Payload: &flush{args: [][]byte{[]byte("flushdb")}, db: 0, offset: 20}},
	}
	if !containsFlush(events) {
		t.Fatal("the batch was not recognised as containing a flush")
	}
	if endOf(events) != 20 {
		t.Fatalf("batch end = %d, want the flush's offset", endOf(events))
	}
	if applier.RestoreState == nil {
		t.Fatal("no restore hook, so a flush of the bookkeeping database loses the position")
	}
	applier.RestoreState(context.Background(), nil, "encoded-position")
	if len(order) != 1 || order[0] != "restore:encoded-position" {
		t.Errorf("restore recorded %v, want the position it was given", order)
	}
}

// TestAWriteAfterAFlushInTheSameBatchIsNotSkipped is the regression the review
// found: a flush does not stand alone in its batch, and resetting every marker
// to the batch's end made plan drop everything that came after it -- silently,
// reporting success, with the resume floor then moved past the loss.
func TestAWriteAfterAFlushInTheSameBatchIsNotSkipped(t *testing.T) {
	a := &Applier{Positions: &Checkpoints{TaskID: 1, Shard: "0"}}
	markers := make([]int64, SlotCount)

	after := &command{args: [][]byte{[]byte("set"), []byte("k")}, slot: 7, offset: 120}
	batch := []*domain.Event{
		{Payload: &command{args: [][]byte{[]byte("set"), []byte("k")}, slot: 7, offset: 100}},
		{Payload: &flush{args: [][]byte{[]byte("flushall")}, offset: 110}},
		{Payload: after},
	}
	if endOf(batch) != 120 {
		t.Fatalf("batch end = %d, want the last event's offset", endOf(batch))
	}

	// What the flush does to the markers, at its own offset rather than the
	// batch's.
	a.forgetMarkers(markers, &flush{offset: 110})

	jobs, err := a.plan([]*domain.Event{{Payload: after}}, markers)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("the write after the flush planned %d jobs, want 1: it would be "+
			"dropped with the batch reported as applied", len(jobs))
	}
	if a.Skipped() != 0 {
		t.Errorf("skipped = %d, want none", a.Skipped())
	}
}

// A segment records how far it reached, not how far the batch reaches.
func TestASegmentIsMarkedAtItsOwnEnd(t *testing.T) {
	segment := []*domain.Event{
		{Payload: &command{args: [][]byte{[]byte("set")}, slot: 1, offset: 100}},
	}
	whole := append(append([]*domain.Event{}, segment...),
		&domain.Event{Payload: &command{args: [][]byte{[]byte("set")}, slot: 1, offset: 200}})

	if endOf(segment) != 100 {
		t.Errorf("segment end = %d, want 100", endOf(segment))
	}
	if endOf(whole) != 200 {
		t.Errorf("batch end = %d, want 200", endOf(whole))
	}
	if endOf(segment) == endOf(whole) {
		t.Error("a segment cannot be marked at the batch's end: the rest has not landed")
	}
}

// The position restored after a flush names the flush, not the end of the batch
// it arrived in -- a slot with no marker is taken to have applied up to the
// floor, and the flush has just removed every marker.
func TestTheRestoredPositionNamesTheFlush(t *testing.T) {
	original, err := streamPosition{ReplID: "abc", Offset: 200, Phase: phaseCommand}.encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	moved, err := positionAt(original, 110)
	if err != nil {
		t.Fatalf("positionAt: %v", err)
	}
	got, err := decodePosition(moved)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Offset != 110 {
		t.Errorf("offset = %d, want the flush's 110 rather than the batch's 200", got.Offset)
	}
	if got.ReplID != "abc" || got.Phase != phaseCommand {
		t.Errorf("the rest of the position changed: %+v", got)
	}
}

// A flush reaches one node of a cluster and empties what that node holds --
// its slots, and no more. Carrying it to every master of the target empties
// the other shards' data too, which the source still has. Verified on a real
// three-master pair: flushing one source master left the whole target empty.
func TestAShardsFlushCoversItsOwnSlotsOnly(t *testing.T) {
	for _, c := range []struct {
		shard      string
		start, end int
		ok         bool
	}{
		{"0-5460", 0, 5460, true},
		{"5461-10922", 5461, 10922, true},
		{"10923-16383", 10923, 16383, true},
		{"0", 0, 0, false},            // one server: no range, the whole database
		{"5461-10922-x", 0, 0, false}, // not a range
		{"10922-5461", 0, 0, false},   // backwards
		{"0-16384", 0, 0, false},      // past the end
	} {
		start, end, ok := slotRange(c.shard)
		if ok != c.ok {
			t.Errorf("slotRange(%q) ok = %v, want %v", c.shard, ok, c.ok)
			continue
		}
		if ok && (start != c.start || end != c.end) {
			t.Errorf("slotRange(%q) = %d-%d, want %d-%d", c.shard, start, end, c.start, c.end)
		}
	}

	// The range has to be the one the key's slot is tested against, so a key
	// belonging to another shard survives its neighbour's flush.
	mine := SlotOf([]byte("clustertest:1"))
	start, end, _ := slotRange("0-5460")
	if (mine >= start && mine <= end) == (mine > 5460) {
		t.Errorf("slot %d was placed on the wrong side of 0-5460", mine)
	}
}

// A failure means a flush or a sweep of a master owning several ranges misses one of them or reaches past them.
func TestAShardOfSeveralRangesReachesEachOfThem(t *testing.T) {
	spans, ok := slotRanges("0-100,5461-10922")
	if !ok {
		t.Fatal("slotRanges(\"0-100,5461-10922\") reported no range")
	}
	for slot, want := range map[int]bool{0: true, 100: true, 101: false, 5460: false,
		5461: true, 10922: true, 10923: false} {
		if got := spans.has(slot); got != want {
			t.Errorf("slot %d in 0-100,5461-10922 = %v, want %v", slot, got, want)
		}
	}
	for _, name := range []string{"0", "", "0-100,", ",0-100", "0-100,x", "0-100,5461-16384"} {
		if _, ok := slotRanges(name); ok {
			t.Errorf("slotRanges(%q) reported a range", name)
		}
	}
}
