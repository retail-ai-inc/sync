package mysql

import (
	"context"
	"strings"
	"testing"
)

// compareBinlog reaches the source only to compare GTID sets. These cover the
// file-and-offset branches, which need nothing.

const sameServer = "10.118.192.8:3306"

func TestNothingAppliedIsNotCaughtUp(t *testing.T) {
	got := compareBinlog(context.Background(), nil,
		binlogHead{File: "mysql-bin.000123", Position: 4096},
		&binlogCheckpoint{}, sameServer)

	if got.CaughtUp || got.Comparable {
		t.Errorf("an empty position reported %+v", got)
	}
	if got.Note == "" {
		t.Error("nothing said why there is no answer")
	}
}

func TestTheSameFileAndOffsetIsCaughtUp(t *testing.T) {
	got := compareBinlog(context.Background(), nil,
		binlogHead{File: "mysql-bin.000123", Position: 4096},
		&binlogCheckpoint{Name: "mysql-bin.000123", Pos: 4096, Source: sameServer},
		sameServer)

	if !got.CaughtUp {
		t.Errorf("a target at the source's own offset reported behind: %+v", got)
	}
	if got.BehindBytes != 0 {
		t.Errorf("BehindBytes = %d, want 0", got.BehindBytes)
	}
}

func TestBehindInTheSameFileIsAByteCount(t *testing.T) {
	got := compareBinlog(context.Background(), nil,
		binlogHead{File: "mysql-bin.000123", Position: 8192},
		&binlogCheckpoint{Name: "mysql-bin.000123", Pos: 4096, Source: sameServer},
		sameServer)

	if got.CaughtUp {
		t.Error("a target 4096 bytes behind reported caught up")
	}
	if got.BehindBytes != 4096 {
		t.Errorf("BehindBytes = %d, want 4096", got.BehindBytes)
	}
}

// TestAnEarlierFileIsNotAByteCount: offsets in different files are not on the
// same scale, and subtracting them would give a number that looks like a
// distance and is not one.
func TestAnEarlierFileIsNotAByteCount(t *testing.T) {
	got := compareBinlog(context.Background(), nil,
		binlogHead{File: "mysql-bin.000124", Position: 100},
		&binlogCheckpoint{Name: "mysql-bin.000123", Pos: 900000, Source: sameServer},
		sameServer)

	if got.CaughtUp {
		t.Error("a target one file behind reported caught up")
	}
	if !got.Comparable {
		t.Error("two files on the same server reported as not comparable")
	}
	if got.BehindBytes >= 0 {
		t.Errorf("BehindBytes = %d, want it reported as not a byte count", got.BehindBytes)
	}
}

// TestAStoredFileLaterThanTheSourceCannotBeOrdered covers a source that was
// reset or restored: the stored position names a file the source does not have,
// and calling that caught up would say Osaka holds what Tokyo had.
func TestAStoredFileLaterThanTheSourceCannotBeOrdered(t *testing.T) {
	got := compareBinlog(context.Background(), nil,
		binlogHead{File: "mysql-bin.000010", Position: 100},
		&binlogCheckpoint{Name: "mysql-bin.000123", Pos: 4096, Source: sameServer},
		sameServer)

	if got.CaughtUp {
		t.Error("a position from a history the source does not have reported caught up")
	}
	if got.Comparable {
		t.Error("a position from another history reported as comparable")
	}
	if got.Note == "" {
		t.Error("nothing said why the two cannot be ordered")
	}
}

// TestAPositionFromAnotherServerCannotBeOrdered is the rule the reader applies
// before resuming, asked here for the same reason: a file and an offset name
// one server's bytes and nowhere else, so ordering them against a different
// server compares two unrelated numbers.
func TestAPositionFromAnotherServerCannotBeOrdered(t *testing.T) {
	got := compareBinlog(context.Background(), nil,
		binlogHead{File: "mysql-bin.000123", Position: 8192},
		&binlogCheckpoint{Name: "mysql-bin.000123", Pos: 4096,
			Source: "10.118.192.9:3306"},
		sameServer)

	if got.CaughtUp || got.Comparable {
		t.Errorf("a position from another server reported %+v", got)
	}
	if !strings.Contains(got.Note, "10.118.192.9:3306") {
		t.Errorf("the note does not name the server it was recorded against: %q", got.Note)
	}
}

func TestAPositionWithNoRecordedServerIsStillCompared(t *testing.T) {
	got := compareBinlog(context.Background(), nil,
		binlogHead{File: "mysql-bin.000123", Position: 4096},
		&binlogCheckpoint{Name: "mysql-bin.000123", Pos: 4096},
		sameServer)

	if !got.CaughtUp {
		t.Errorf("a position stored before the server was recorded was not compared: %+v", got)
	}
}
