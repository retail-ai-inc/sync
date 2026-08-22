package mysql

import (
	"context"
	"crypto/tls"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// statusConn hands back a pinned connection to a SQLite database, which is what
// readBinlogStatus takes. The statement is a parameter, so a SELECT that names
// its columns the way SHOW MASTER STATUS does exercises the same mapping.
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

// TestTheColumnOrderDoesNotMatter is why the mapping is by name: the column
// list of SHOW MASTER STATUS has changed across server versions.
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
// cannot work on at all. It has to be an error rather than a zero position,
// which would read as "start from the beginning of a file that does not exist".
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

// -------------------------------------------------------- checkpoint file

func TestTheCheckpointFileRoundTrips(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "pos.json")
	want := binlogCheckpoint{
		Name: "binlog.000004", Pos: 154,
		GTID: sampleGTID, Flavor: mysql.MySQLFlavor,
	}

	if err := writeCheckpoint(path, want); err != nil {
		t.Fatalf("writeCheckpoint: %v", err)
	}
	got := newSyncer(t).loadCheckpoint(path)
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

	if err := writeCheckpoint(filepath.Join(blocker, "pos.json"), binlogCheckpoint{}); err == nil {
		t.Error("writing under a regular file returned no error")
	}
}

// -------------------------------------------------------------- source TLS

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
