package mysql

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
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

func TestTheCheckpointFileRoundTrips(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "pos.json")
	want := binlogCheckpoint{
		Name: "binlog.000004", Pos: 154,
		GTID: sampleGTID, Flavor: mysql.MySQLFlavor,
	}

	payload, err := checkpoint.Encode(want)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	store := &checkpoint.FileStore{Path: path}
	if err := store.Save(context.Background(), "", payload); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got := storedCheckpoint(t, path)
	if got == nil {
		t.Fatal("loadCheckpoint returned nil")
	}
	if *got != want {
		t.Errorf("loaded %+v, want %+v", *got, want)
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

// TestTheBinlogConnectionFollowsTheDSN covers the connection database/sql never
// sees: canal dials the source itself, so the DSN's tls parameter has to be
// translated for it or the row events cross the region in the clear.
func TestTheBinlogConnectionFollowsTheDSN(t *testing.T) {
	s := newSyncer(t)

	tests := []struct {
		name       string
		dsn        string
		wantTLS    bool
		wantServer string
		wantSkip   bool
	}{
		{"required", "u:p@tcp(db.example.net:3306)/shop?tls=true", true, "db.example.net", false},
		{"skip verify", "u:p@tcp(db.example.net:3306)/shop?tls=skip-verify", true, "", true},
		{"preferred cannot negotiate here", "u:p@tcp(db:3306)/shop?tls=preferred", false, "", false},
		{"none", "u:p@tcp(db:3306)/shop", false, "", false},
		{"unparseable", "not a dsn", false, "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s.cfg.SourceConnection = tt.dsn
			got := s.sourceTLS()

			if (got != nil) != tt.wantTLS {
				t.Fatalf("sourceTLS() = %v, want TLS: %v", got, tt.wantTLS)
			}
			if got == nil {
				return
			}
			if got.ServerName != tt.wantServer {
				t.Errorf("ServerName = %q, want %q", got.ServerName, tt.wantServer)
			}
			if got.InsecureSkipVerify != tt.wantSkip {
				t.Errorf("InsecureSkipVerify = %v, want %v", got.InsecureSkipVerify, tt.wantSkip)
			}
			if got.MinVersion != tls.VersionTLS12 {
				t.Errorf("MinVersion = %x, want TLS 1.2", got.MinVersion)
			}
		})
	}
}

// A file and offset mean nothing on another server: read against a different
// one they address unrelated bytes, and the read succeeds, so the task resumes
// from somewhere arbitrary with nothing to show that it happened.
func TestACheckpointFromAnotherSourceIsIgnored(t *testing.T) {
	s := newSyncer(t)
	s.cfg.SourceConnection = "u:p@tcp(osaka:3306)/shop"
	path := filepath.Join(t.TempDir(), "pos.json")

	payload, err := checkpoint.Encode(binlogCheckpoint{
		Name: "binlog.000004", Pos: 154, Source: "tokyo:3306/shop",
	})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	store := &checkpoint.FileStore{Path: path}
	if err := store.Save(context.Background(), "", payload); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := s.loadCheckpoint(context.Background(), store)
	if err != nil {
		t.Fatalf("loadCheckpoint: %v", err)
	}
	if got != nil {
		t.Errorf("checkpoint = %+v; an offset from another server was accepted", *got)
	}
}

func TestACheckpointFromTheSameSourceIsUsed(t *testing.T) {
	s := newSyncer(t)
	s.cfg.SourceConnection = "u:p@tcp(tokyo:3306)/shop"
	path := filepath.Join(t.TempDir(), "pos.json")

	payload, err := checkpoint.Encode(binlogCheckpoint{
		Name: "binlog.000004", Pos: 154, Source: "tokyo:3306/shop",
	})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	store := &checkpoint.FileStore{Path: path}
	if err := store.Save(context.Background(), "", payload); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := s.loadCheckpoint(context.Background(), store)
	if err != nil {
		t.Fatalf("loadCheckpoint: %v", err)
	}
	if got == nil {
		t.Fatal("a checkpoint from this task's own source was ignored")
	}
	if got.Pos != 154 {
		t.Errorf("offset = %d", got.Pos)
	}
}

// TestACheckpointWithNoSourceIsStillUsed keeps an existing deployment resuming:
// a checkpoint written before the source was recorded has no way to prove which
// server it came from, and refusing it would re-copy every table on upgrade.
func TestACheckpointWithNoSourceIsStillUsed(t *testing.T) {
	s := newSyncer(t)
	s.cfg.SourceConnection = "u:p@tcp(tokyo:3306)/shop"
	path := filepath.Join(t.TempDir(), "pos.json")

	if err := (&checkpoint.FileStore{Path: path}).Save(context.Background(), "",
		`{"Name":"binlog.000003","Pos":154}`); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := s.loadCheckpoint(context.Background(), &checkpoint.FileStore{Path: path})
	if err != nil {
		t.Fatalf("loadCheckpoint: %v", err)
	}
	if got == nil {
		t.Fatal("a checkpoint from an older build was ignored, which would re-copy every table")
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

// TestMariaDBIsNotAsked records that the check is skipped for MariaDB, which
// has no binlog_row_image setting and logs whole rows unconditionally.
func TestMariaDBIsNotAsked(t *testing.T) {
	s := newSyncer(t)
	s.cfg.Type = "mariadb"

	if err := s.checkRowImage(nil); err != nil {
		t.Errorf("checkRowImage for MariaDB = %v", err)
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
