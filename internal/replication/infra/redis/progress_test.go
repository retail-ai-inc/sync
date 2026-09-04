package redis

import "testing"

// These cover the comparison a switch-over depends on. Redis is the one engine
// where it is exact: both sides report a byte count in the same stream.

func TestSourceOffsetIsReadFromInfo(t *testing.T) {
	const info = "# Replication\r\nrole:master\r\nmaster_repl_offset:1276319\r\n"
	if got := infoNumber(info, "master_repl_offset"); got != 1276319 {
		t.Errorf("master_repl_offset = %d, want 1276319", got)
	}
}

func TestInfoNumberOfSomethingMissing(t *testing.T) {
	if got := infoNumber("role:master\r\n", "master_repl_offset"); got != 0 {
		t.Errorf("a missing field read as %d, want 0", got)
	}
	if got := infoNumber("master_repl_offset:not a number\r\n", "master_repl_offset"); got != 0 {
		t.Errorf("an unparseable field read as %d, want 0", got)
	}
}

func TestCompareOffsetsBehind(t *testing.T) {
	got := compareOffsets("0-5460", 1276319, 1276000)
	if got.CaughtUp {
		t.Error("a target 319 bytes behind reported caught up")
	}
	if !got.Comparable {
		t.Error("two offsets in the same stream reported as not comparable")
	}
	if got.BehindBytes != 319 {
		t.Errorf("BehindBytes = %d, want 319", got.BehindBytes)
	}
}

func TestCompareOffsetsCaughtUp(t *testing.T) {
	got := compareOffsets("", 1276319, 1276319)
	if !got.CaughtUp {
		t.Error("a target at the source's own offset reported behind")
	}
	if got.BehindBytes != 0 {
		t.Errorf("BehindBytes = %d, want 0", got.BehindBytes)
	}
}

// TestAStoredOffsetPastTheSourceIsNotCaughtUp covers the one wrong answer. A
// source that restarted or failed over starts its offset from zero, so a stored
// offset ahead of it belongs to a history the source has forgotten -- and
// calling that caught up says Osaka holds everything Tokyo had.
func TestAStoredOffsetPastTheSourceIsNotCaughtUp(t *testing.T) {
	got := compareOffsets("0-5460", 4096, 1276319)
	if got.CaughtUp {
		t.Error("a position from a replaced history reported caught up")
	}
	if got.Comparable {
		t.Error("a position from a replaced history reported as comparable")
	}
	if got.BehindBytes >= 0 {
		t.Errorf("BehindBytes = %d; there is no distance between two histories", got.BehindBytes)
	}
	if got.Note == "" {
		t.Error("nothing said why the two cannot be ordered")
	}
}
