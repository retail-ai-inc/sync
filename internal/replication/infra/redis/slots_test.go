package redis

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

// TestSlotOfAgreesWithTheServer pins the hash against values read from a real
// Redis 8.10.1 with CLUSTER KEYSLOT.
func TestSlotOfAgreesWithTheServer(t *testing.T) {
	cases := map[string]int{
		"foo":               12182,
		"bar":               5061,
		"hello":             866,
		"123456789":         12739,
		"user:1001":         5712,
		"{u1000}.following": 4847,
		"{u1000}.followers": 4847,
		"{}foo":             9500,
		"a{}b":              13694,
		"":                  0,
	}
	for key, want := range cases {
		if got := SlotOf([]byte(key)); got != want {
			t.Errorf("SlotOf(%q) = %d, want %d", key, got, want)
		}
	}
}

// TestABracedTagDecidesTheSlot covers the property the position marker relies
// on: what is inside the braces is the only thing hashed.
func TestABracedTagDecidesTheSlot(t *testing.T) {
	tagged := SlotOf([]byte("{foo}:anything:at:all"))
	bare := SlotOf([]byte("foo"))
	if tagged != bare {
		t.Errorf("{foo}… hashed to %d but foo hashed to %d; a tag must decide the slot alone",
			tagged, bare)
	}
}

// TestAnEmptyTagIsNotATag covers the one edge in the server's rule: braces with
// nothing between them are ordinary characters.
func TestAnEmptyTagIsNotATag(t *testing.T) {
	if got, want := SlotOf([]byte("{}foo")), 9500; got != want {
		t.Errorf("SlotOf({}foo) = %d, want %d — an empty tag must hash the whole key", got, want)
	}
	if got, want := SlotOf([]byte("a{}b")), 13694; got != want {
		t.Errorf("SlotOf(a{}b) = %d, want %d", got, want)
	}
}

func TestAnUnclosedTagIsNotATag(t *testing.T) {
	if SlotOf([]byte("{foo")) == SlotOf([]byte("foo")) {
		t.Error("an unclosed brace was treated as a tag; the server hashes the whole key")
	}
}

// TestCRC16IsXMODEM pins the hash itself against the standard check value, so a
// change to the implementation cannot pass by agreeing with itself.
func TestCRC16IsXMODEM(t *testing.T) {
	if got, want := crc16([]byte("123456789")), uint16(0x31C3); got != want {
		t.Errorf("crc16(123456789) = %#04x, want %#04x", got, want)
	}
	if got := crc16(nil); got != 0 {
		t.Errorf("crc16(nil) = %#04x, want 0", got)
	}
}

// TestEverySlotHasATag is the table's basic obligation: a position marker has to
// be placeable in any slot, because the source decides which slots see writes.
func TestEverySlotHasATag(t *testing.T) {
	tags := SlotTags()
	for slot := 0; slot < SlotCount; slot++ {
		if tags[slot] == "" {
			t.Fatalf("slot %d has no tag, so its position could not be recorded", slot)
		}
	}
}

func TestEveryTagHashesToItsOwnSlot(t *testing.T) {
	tags := SlotTags()
	for slot := 0; slot < SlotCount; slot++ {
		key := "{" + tags[slot] + "}:__off:1"
		if got := SlotOf([]byte(key)); got != slot {
			t.Fatalf("the marker for slot %d, %q, lives in slot %d instead", slot, key, got)
		}
	}
}

// TestTheSlotTagTableIsStable is the most important test in this file, and the
// least obvious. These tags name the keys that hold how far each slot has been
// applied.
func TestTheSlotTagTableIsStable(t *testing.T) {
	const want = "6b728ddecb7be57061f8d2dbdbfe6db2d82aaabcccdc8ed4eb5e8f0231c325a0"

	digest := sha256.New()
	for _, tag := range SlotTags() {
		digest.Write([]byte(tag))
		digest.Write([]byte{0})
	}
	got := hex.EncodeToString(digest.Sum(nil))

	if got != want {
		t.Errorf("the slot tag table changed.\n got: %s\nwant: %s\n"+
			"Every recorded per-slot position is named by these tags. Changing them "+
			"abandons all of them and replays applied history. If this change is "+
			"deliberate, it needs a migration that moves the existing markers.", got, want)
	}
}

// TestTheTagsAreShort keeps the markers from bloating the target: one key per
// slot means 16384 of them.
func TestTheTagsAreShort(t *testing.T) {
	for slot, tag := range SlotTags() {
		if len(tag) > 8 {
			t.Errorf("the tag for slot %d is %q, %d bytes; the walk should find short ones",
				slot, tag, len(tag))
		}
	}
}

func TestAMarkerIsRecognisable(t *testing.T) {
	key := OffsetKey(1234, 7)
	if !IsOffsetKey(key) {
		t.Errorf("%q was not recognised as a marker; a reverse reconcile would delete it "+
			"as a key the source does not have", key)
	}
	if !strings.Contains(key, ":__off:7") {
		t.Errorf("OffsetKey(1234, 7) = %q, want the task id in the name so two tasks "+
			"targeting one cluster cannot overwrite each other", key)
	}
}

func TestOrdinaryKeysAreNotMarkers(t *testing.T) {
	for _, key := range []string{"foo", "user:1001", "{u1000}.following", "__off:1", ""} {
		if IsOffsetKey(key) {
			t.Errorf("%q was treated as a marker, so a reconcile would leave a stale key alone", key)
		}
	}
}

func TestTwoTasksDoNotShareAMarker(t *testing.T) {
	if OffsetKey(99, 1) == OffsetKey(99, 2) {
		t.Error("two tasks share one marker, so each would erase the other's progress")
	}
	if SlotOf([]byte(OffsetKey(99, 1))) != SlotOf([]byte(OffsetKey(99, 2))) {
		t.Error("the task id changed the slot; it must only change the name")
	}
}
