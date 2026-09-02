package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
)

// statusConn hands back a pinned connection to a SQLite database, which is what
// readBinlogStatus takes.
func statusConn(t *testing.T) *sql.Conn {
	t.Helper()

	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "status.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("pin connection: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestTheBinlogStatusIsReadByColumnName(t *testing.T) {
	conn := statusConn(t)

	cp, err := readBinlogStatus(context.Background(), conn,
		`SELECT 'binlog.000004' AS File, '154' AS Position,
		        '3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5' AS Executed_Gtid_Set,
		        '' AS Binlog_Do_DB`)
	if err != nil {
		t.Fatalf("readBinlogStatus: %v", err)
	}

	if cp.Name != "binlog.000004" || cp.Pos != 154 {
		t.Errorf("position = %+v, want binlog.000004/154", cp.position())
	}
	if cp.GTID != "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5" {
		t.Errorf("GTID = %q", cp.GTID)
	}
}

// TestTheColumnOrderDoesNotMatter is why the mapping is by name.
func TestTheColumnOrderDoesNotMatter(t *testing.T) {
	conn := statusConn(t)

	cp, err := readBinlogStatus(context.Background(), conn,
		`SELECT '' AS Binlog_Ignore_DB, '900' AS Position, 'binlog.000009' AS File`)
	if err != nil {
		t.Fatalf("readBinlogStatus: %v", err)
	}
	if cp.Name != "binlog.000009" || cp.Pos != 900 {
		t.Errorf("position = %+v", cp.position())
	}
	if cp.GTID != "" {
		t.Errorf("GTID = %q for a server that reported none", cp.GTID)
	}
}

// TestAServerWithNoBinaryLogIsReported covers the configuration replication
// cannot work on at all.
func TestAServerWithNoBinaryLogIsReported(t *testing.T) {
	conn := statusConn(t)

	if _, err := readBinlogStatus(context.Background(), conn,
		`SELECT '' AS File, '0' AS Position WHERE 1 = 0`); err == nil {
		t.Fatal("an empty status returned no error")
	}

	_, err := readBinlogStatus(context.Background(), conn,
		`SELECT '' AS File, '0' AS Position`)
	if err == nil {
		t.Fatal("a blank binlog file returned no error")
	}
	if !strings.Contains(err.Error(), "binary log") {
		t.Errorf("error = %v, want it to name the binary log", err)
	}
}

func TestAnUnparseablePositionIsReported(t *testing.T) {
	conn := statusConn(t)

	if _, err := readBinlogStatus(context.Background(), conn,
		`SELECT 'binlog.1' AS File, 'not a number' AS Position`); err == nil {
		t.Fatal("a non-numeric position returned no error")
	}
}

// TestAMultiLineGTIDSetIsFlattened records the shape MySQL returns when the set
// spans several source servers: the value carries newlines that the parser will
// not accept back.
func TestAMultiLineGTIDSetIsFlattened(t *testing.T) {
	conn := statusConn(t)

	cp, err := readBinlogStatus(context.Background(), conn,
		"SELECT 'binlog.1' AS File, '4' AS Position, "+
			"'3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5,"+"\n"+
			"4f11fa47-71ca-11e1-9e33-c80aa9429562:1-3' AS Executed_Gtid_Set")
	if err != nil {
		t.Fatalf("readBinlogStatus: %v", err)
	}
	if strings.Contains(cp.GTID, "\n") {
		t.Errorf("GTID = %q, still carries a newline", cp.GTID)
	}
	if cp.gtidSet() == nil {
		t.Errorf("the flattened set did not parse: %q", cp.GTID)
	}
}

func TestAnUnwritableCheckpointPathIsReported(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocker, nil, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	store := &checkpoint.FileStore{Path: filepath.Join(blocker, "pos.json")}
	if err := store.Save(context.Background(), "", "{}"); err == nil {
		t.Error("writing under a regular file returned no error")
	}
}

// With anything but FULL the binlog carries only the columns that changed plus
// the primary key, and the driver fills the rest with nils; the UPDATE this
// syncer builds sets every column, so those nils go to the target as NULL over
// values that never changed.
func TestOnlyFullRowImagesAreAccepted(t *testing.T) {
	for _, image := range []string{"FULL", "full", "Full"} {
		if err := requireFullRowImage(image); err != nil {
			t.Errorf("requireFullRowImage(%q) = %v, want it accepted", image, err)
		}
	}

	for _, image := range []string{"MINIMAL", "minimal", "NOBLOB", "noblob"} {
		err := requireFullRowImage(image)
		if err == nil {
			t.Errorf("requireFullRowImage(%q) accepted it", image)
			continue
		}
		if !strings.Contains(err.Error(), "binlog_row_image=FULL") {
			t.Errorf("error = %v, want it to say what to set", err)
		}
		if !strings.Contains(err.Error(), "NULL") {
			t.Errorf("error = %v, want it to say what goes wrong", err)
		}
	}
}

// TestAServerWithNoRowImageSettingIsAccepted covers MySQL before 5.6, which has
// no such variable and always logs whole rows.
func TestAServerWithNoRowImageSettingIsAccepted(t *testing.T) {
	if err := requireFullRowImage(""); err != nil {
		t.Errorf("requireFullRowImage(\"\") = %v", err)
	}
}

// Cloud SQL expires binary logs on a schedule, so a task stopped for longer
// than that comes back to find its offset gone.
func TestAPurgedBinlogIsUnrecoverable(t *testing.T) {
	errors := []string{
		"ERROR 1236 (HY000): Could not find first log file name in binary log index file",
		"could not find next log; the first event could not be read",
		"Error 1236: Cannot replicate because the master purged required binary logs",
		"the binary log is not available",
	}

	for _, text := range errors {
		t.Run(text[:24], func(t *testing.T) {
			reason, lost := positionNoLongerAvailable(fmt.Errorf("%s", text))
			if !lost {
				t.Fatalf("positionNoLongerAvailable(%q) said the position is fine", text)
			}
			if !strings.Contains(reason, "fresh copy") {
				t.Errorf("reason = %q, want it to say what to do", reason)
			}
			if !strings.Contains(reason, text) {
				t.Errorf("reason = %q, want it to carry the original message", reason)
			}
		})
	}
}

// TestAnOrdinaryFailureIsRetryable is the other side: the errors that come and
// go must not stop a task permanently.
func TestAnOrdinaryFailureIsRetryable(t *testing.T) {
	for _, text := range []string{
		"connection reset by peer",
		"dial tcp 10.0.0.1:3306: connect: connection refused",
		"i/o timeout",
		"Error 1045: Access denied for user",
		"",
	} {
		var err error
		if text != "" {
			err = fmt.Errorf("%s", text)
		}
		if _, lost := positionNoLongerAvailable(err); lost {
			t.Errorf("positionNoLongerAvailable(%q) said the position is gone", text)
		}
	}
}
