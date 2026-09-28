package mysql

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
)

// Reading the source's binary log position. SHOW MASTER STATUS was removed in
// MySQL 8.4 and replaced by SHOW BINARY LOG STATUS, and the columns differ
// between versions, so both spellings and a read by column name are the whole
// of what makes this work across the versions in use.

func TestTheBinaryLogHeadIsReadByColumnName(t *testing.T) {
	fake := &fakeDB{replies: []reply{{
		match:   "SHOW BINARY LOG STATUS",
		columns: []string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"},
		rows: [][]driver.Value{
			{"mysql-bin.000123", "4096", "", "", "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"},
		},
	}}}

	head, err := readBinlogHead(context.Background(), fake.open(t))
	if err != nil {
		t.Fatalf("readBinlogHead: %v", err)
	}
	if head.File != "mysql-bin.000123" || head.Position != 4096 {
		t.Errorf("read %s:%d", head.File, head.Position)
	}
	if !strings.Contains(head.GTIDSet, "3E11FA47") {
		t.Errorf("the GTID set was not read: %q", head.GTIDSet)
	}
}

// TestTheOlderSpellingIsTriedNext: SHOW BINARY LOG STATUS does not exist before
// 8.4, and a task must not stop replicating because the newer name was tried
// first.
func TestTheOlderSpellingIsTriedNext(t *testing.T) {
	fake := &fakeDB{replies: []reply{
		{match: "SHOW BINARY LOG STATUS", err: errors.New("You have an error in your SQL syntax")},
		{match: "SHOW MASTER STATUS",
			columns: []string{"File", "Position"},
			rows:    [][]driver.Value{{"mysql-bin.000009", "155"}}},
	}}

	head, err := readBinlogHead(context.Background(), fake.open(t))
	if err != nil {
		t.Fatalf("readBinlogHead: %v", err)
	}
	if head.File != "mysql-bin.000009" || head.Position != 155 {
		t.Errorf("read %s:%d, want mysql-bin.000009:155", head.File, head.Position)
	}
	if !fake.wasAsked("SHOW MASTER STATUS") {
		t.Error("the older spelling was never tried")
	}
}

// TestGTIDColumnsAreStrippedOfNewlines: a long GTID set is returned across
// several lines, and the newlines would be sent back in a JSON field an
// operator reads during a switch-over.
func TestGTIDColumnsAreStrippedOfNewlines(t *testing.T) {
	fake := &fakeDB{replies: []reply{{
		match:   "SHOW BINARY LOG STATUS",
		columns: []string{"File", "Position", "Executed_Gtid_Set"},
		rows:    [][]driver.Value{{"mysql-bin.1", "1", "uuid-a:1-5,\nuuid-b:1-9"}},
	}}}

	head, err := readBinlogHead(context.Background(), fake.open(t))
	if err != nil {
		t.Fatalf("readBinlogHead: %v", err)
	}
	if strings.Contains(head.GTIDSet, "\n") {
		t.Errorf("the GTID set carries a newline: %q", head.GTIDSet)
	}
}

// TestASourceWithNoBinaryLogIsReportedNotRetried. The statement worked and
// there was no row: the source has the binary log switched off, which is a real
// answer and not something to try the other spelling for.
func TestASourceWithNoBinaryLogIsReportedNotRetried(t *testing.T) {
	fake := &fakeDB{replies: []reply{{
		match:   "SHOW BINARY LOG STATUS",
		columns: []string{"File", "Position"},
	}}}

	_, err := readBinlogHead(context.Background(), fake.open(t))
	if err == nil {
		t.Fatal("a source with no binary log was accepted")
	}
	if !strings.Contains(err.Error(), "switched off") {
		t.Errorf("the error does not say why: %v", err)
	}
	if fake.wasAsked("SHOW MASTER STATUS") {
		t.Error("the older spelling was tried after a clear answer")
	}
}

func TestBothSpellingsFailingIsReported(t *testing.T) {
	fake := &fakeDB{replies: []reply{
		{match: "SHOW BINARY LOG STATUS", err: errors.New("syntax error")},
		{match: "SHOW MASTER STATUS", err: errors.New("access denied")},
	}}

	if _, err := readBinlogHead(context.Background(), fake.open(t)); err == nil {
		t.Fatal("a source that answered neither spelling was accepted")
	}
}

// TestAPositionThatIsNotANumberReadsAsZero rather than failing the whole
// report: the file name is still worth answering with, and a zero offset is
// visibly wrong where a refusal would leave the operator with nothing.
func TestAPositionThatIsNotANumberReadsAsZero(t *testing.T) {
	fake := &fakeDB{replies: []reply{{
		match:   "SHOW BINARY LOG STATUS",
		columns: []string{"File", "Position"},
		rows:    [][]driver.Value{{"mysql-bin.000001", "not a number"}},
	}}}

	head, err := readBinlogHead(context.Background(), fake.open(t))
	if err != nil {
		t.Fatalf("readBinlogHead: %v", err)
	}
	if head.Position != 0 {
		t.Errorf("Position = %d, want 0", head.Position)
	}
	if head.File != "mysql-bin.000001" {
		t.Errorf("the file was lost: %q", head.File)
	}
}

// TestAPositionTooLargeForTheProtocolReadsAsZero rather than wrapping to an
// offset that exists in the file.
func TestAPositionTooLargeForTheProtocolReadsAsZero(t *testing.T) {
	fake := &fakeDB{replies: []reply{{
		match:   "SHOW BINARY LOG STATUS",
		columns: []string{"File", "Position"},
		rows:    [][]driver.Value{{"mysql-bin.000001", "4294967300"}},
	}}}

	head, err := readBinlogHead(context.Background(), fake.open(t))
	if err != nil {
		t.Fatalf("readBinlogHead: %v", err)
	}
	if head.Position != 0 {
		t.Errorf("Position = %d, want 0", head.Position)
	}
}

// TestTheGTIDComparisonAsksTheServer covers the preferred comparison: whether
// the source's set is contained in what the target applied. The server makes
// the judgement, because a GTID set is not something to compare as text.
func TestTheGTIDComparisonAsksTheServer(t *testing.T) {
	for name, c := range map[string]struct {
		answer   driver.Value
		caughtUp bool
	}{
		"contained":     {int64(1), true},
		"not contained": {int64(0), false},
	} {
		t.Run(name, func(t *testing.T) {
			fake := &fakeDB{replies: []reply{{
				match:   "GTID_SUBSET",
				columns: []string{"GTID_SUBSET(?, ?)"},
				rows:    [][]driver.Value{{c.answer}},
			}}}

			got := compareBinlog(context.Background(), fake.open(t),
				binlogHead{File: "mysql-bin.1", Position: 100, GTIDSet: "uuid:1-9"},
				&binlogCheckpoint{Name: "mysql-bin.1", Pos: 50, GTID: "uuid:1-9",
					Source: sameServer},
				sameServer)

			if !got.Comparable {
				t.Fatalf("the GTID comparison was not used: %+v", got)
			}
			if got.CaughtUp != c.caughtUp {
				t.Errorf("CaughtUp = %v, want %v", got.CaughtUp, c.caughtUp)
			}
			if got.BehindBytes >= 0 {
				t.Errorf("BehindBytes = %d; a GTID distance is not a byte count",
					got.BehindBytes)
			}
			if !fake.wasAsked("GTID_SUBSET") {
				t.Error("the server was not asked to compare the sets")
			}
		})
	}
}

// TestGTIDIsPreferredOverFileAndOffset: the file and offset here say the target
// is behind, and the GTID sets say it is caught up. GTID wins, because after a
// failover the file and offset name bytes on a server that is gone.
func TestGTIDIsPreferredOverFileAndOffset(t *testing.T) {
	fake := &fakeDB{replies: []reply{{
		match:   "GTID_SUBSET",
		columns: []string{"c"},
		rows:    [][]driver.Value{{int64(1)}},
	}}}

	got := compareBinlog(context.Background(), fake.open(t),
		binlogHead{File: "mysql-bin.000200", Position: 900, GTIDSet: "uuid:1-9"},
		&binlogCheckpoint{Name: "mysql-bin.000100", Pos: 4, GTID: "uuid:1-9",
			Source: sameServer},
		sameServer)

	if !got.CaughtUp {
		t.Errorf("the file and offset overrode the GTID comparison: %+v", got)
	}
}

// TestAServerThatWillNotCompareFallsBackToFileAndOffset, and says so, so the
// operator knows the answer is only good while the source has not failed over.
func TestAServerThatWillNotCompareFallsBackToFileAndOffset(t *testing.T) {
	fake := &fakeDB{replies: []reply{
		{match: "GTID_SUBSET", err: errors.New("FUNCTION GTID_SUBSET does not exist")},
	}}

	got := compareBinlog(context.Background(), fake.open(t),
		binlogHead{File: "mysql-bin.1", Position: 100, GTIDSet: "uuid:1-9"},
		&binlogCheckpoint{Name: "mysql-bin.1", Pos: 100, GTID: "uuid:1-9",
			Source: sameServer},
		sameServer)

	if !got.CaughtUp {
		t.Errorf("the fallback did not compare the offsets: %+v", got)
	}
	if got.Note == "" {
		t.Error("nothing said the comparison is only good until a failover")
	}
}

// When both spellings fail, the one the server did not recognise says nothing;
// the other failure is what the operator has to act on.
func TestAMissingPrivilegeIsReportedRatherThanTheUnknownSpelling(t *testing.T) {
	denied := "Error 1227 (42000): Access denied; you need (at least one of) the SUPER, REPLICATION CLIENT privilege(s) for this operation"
	unknown := "Error 1064 (42000): You have an error in your SQL syntax; check the manual"

	for name, replies := range map[string][]reply{
		"8.0, newer spelling unknown": {
			{match: "SHOW BINARY LOG STATUS", err: errors.New(unknown)},
			{match: "SHOW MASTER STATUS", err: errors.New(denied)},
		},
		"8.4, older spelling unknown": {
			{match: "SHOW BINARY LOG STATUS", err: errors.New(denied)},
			{match: "SHOW MASTER STATUS", err: errors.New(unknown)},
		},
	} {
		t.Run(name, func(t *testing.T) {
			fake := &fakeDB{replies: replies}
			db := fake.open(t)

			_, err := readBinlogHead(context.Background(), db)
			if err == nil || !strings.Contains(err.Error(), "Access denied") {
				t.Errorf("readBinlogHead reported %v, want the denied privilege", err)
			}

			conn, cerr := db.Conn(context.Background())
			if cerr != nil {
				t.Fatalf("Conn: %v", cerr)
			}
			defer conn.Close()
			_, err = (&MySQLSyncer{}).sourceCheckpoint(context.Background(), conn)
			if err == nil || !strings.Contains(err.Error(), "Access denied") {
				t.Errorf("sourceCheckpoint reported %v, want the denied privilege", err)
			}
		})
	}
}
