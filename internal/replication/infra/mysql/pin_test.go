package mysql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
)

// The coordinates a first copy resumes from have to name the same instant as
// the snapshot it copies. Reading them after the snapshot was opened does not:
// anything committed in between is in neither the copy nor the stream, and no
// row count can see the hole it leaves.

func masterStatus(file string, pos int64, gtid string) [][]driver.Value {
	return [][]driver.Value{{[]byte(file), pos, nil, nil, []byte(gtid)}}
}

// What Cloud SQL answers: its default user has no RELOAD privilege, so the
// global read lock is refused and the pin has to prove itself another way.
var errNoReload = errors.New("Error 1227 (42000): Access denied; you need (at least one of) the RELOAD privilege(s) for this operation")

var masterStatusColumns = []string{
	"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set",
}

func pinner(t *testing.T, db *fakeDB) (*Snapshotter, *logrus.Logger) {
	t.Helper()

	quiet := logrus.New()
	quiet.SetLevel(logrus.ErrorLevel)
	s := &Snapshotter{
		Config: config.SyncConfig{ID: 1, Type: "mysql"},
		Logger: quiet,
	}
	s.syncer = &MySQLSyncer{cfg: s.Config, logger: quiet}
	return s, quiet
}

func TestTheSnapshotIsPinnedToCoordinatesThatAgree(t *testing.T) {
	db := &fakeDB{replies: []reply{
		{match: "FLUSH TABLES WITH READ LOCK"},
		{match: "START TRANSACTION"},
		{match: "UNLOCK TABLES"},
		{match: "SHOW MASTER STATUS", columns: masterStatusColumns,
			sequence: [][][]driver.Value{
				masterStatus("mysql-bin.000001", 4000, "uuid:1-10"),
				masterStatus("mysql-bin.000001", 4000, "uuid:1-10"),
			}},
	}}
	s, _ := pinner(t, db)
	conn, err := db.open(t).Conn(context.Background())
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer conn.Close()

	pinned, err := s.pinConsistently(context.Background(), conn)
	if err != nil {
		t.Fatalf("pinConsistently: %v", err)
	}
	if pinned.GTID != "uuid:1-10" {
		t.Errorf("pinned at %q", pinned.GTID)
	}
	if !db.wasAsked("FLUSH TABLES WITH READ LOCK") {
		t.Error("the snapshot was taken without asking for a read lock")
	}
	if !db.wasAsked("UNLOCK TABLES") {
		t.Error("the read lock was not released")
	}
	// The order is the whole point: lock, read, snapshot, read, unlock.
	statements := strings.Join(db.statements(), " | ")
	lock := strings.Index(statements, "FLUSH TABLES")
	snapshot := strings.Index(statements, "START TRANSACTION")
	unlock := strings.Index(statements, "UNLOCK TABLES")
	if !(lock < snapshot && snapshot < unlock) {
		t.Errorf("statements ran in the wrong order: %s", statements)
	}
}

// The case the old code got wrong: the server commits while the snapshot is
// being taken. The coordinates read afterwards are past the snapshot's view, so
// they must not be accepted.
func TestCoordinatesThatMovedDuringTheSnapshotAreRefused(t *testing.T) {
	// A source under steady write load: every read finds it further along, so
	// no attempt ever sees the two reads agree.
	var moving [][][]driver.Value
	for i := 0; i < 2*pinAttempts+2; i++ {
		moving = append(moving, masterStatus("mysql-bin.000001", int64(4000+100*i),
			fmt.Sprintf("uuid:1-%d", 10+i)))
	}
	db := &fakeDB{replies: []reply{
		{match: "FLUSH TABLES WITH READ LOCK", err: errNoReload},
		{match: "START TRANSACTION"},
		{match: "ROLLBACK"},
		{match: "SHOW MASTER STATUS", columns: masterStatusColumns, sequence: moving},
	}}
	s, _ := pinner(t, db)
	conn, err := db.open(t).Conn(context.Background())
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer conn.Close()

	pinned, err := s.pinConsistently(context.Background(), conn)
	if err == nil {
		t.Fatalf("a snapshot whose coordinates moved was accepted at %v", pinned)
	}
	if !strings.Contains(err.Error(), "committed") {
		t.Errorf("error = %v, want it to say the source committed during the snapshot", err)
	}
	if !db.wasAsked("ROLLBACK") {
		t.Error("the snapshot that could not be pinned was left open")
	}
	// Every attempt is made before giving up.
	tries := 0
	for _, statement := range db.statements() {
		if strings.Contains(statement, "START TRANSACTION") {
			tries++
		}
	}
	if tries != pinAttempts {
		t.Errorf("gave up after %d attempts, want %d", tries, pinAttempts)
	}
}

// A source that settles on the second attempt is pinned, not failed: the window
// is sub-millisecond, so one retry is the common case on a busy server.
func TestASourceThatSettlesIsPinnedOnTheRetry(t *testing.T) {
	db := &fakeDB{replies: []reply{
		{match: "FLUSH TABLES WITH READ LOCK", err: errNoReload},
		{match: "START TRANSACTION"},
		{match: "ROLLBACK"},
		{match: "SHOW MASTER STATUS", columns: masterStatusColumns,
			sequence: [][][]driver.Value{
				masterStatus("mysql-bin.000001", 4000, "uuid:1-10"), // before, attempt 1
				masterStatus("mysql-bin.000001", 9000, "uuid:1-20"), // after, attempt 1: moved
				masterStatus("mysql-bin.000001", 9000, "uuid:1-20"), // before, attempt 2
				masterStatus("mysql-bin.000001", 9000, "uuid:1-20"), // after, attempt 2: settled
			}},
	}}
	s, _ := pinner(t, db)
	conn, err := db.open(t).Conn(context.Background())
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer conn.Close()

	pinned, err := s.pinConsistently(context.Background(), conn)
	if err != nil {
		t.Fatalf("pinConsistently: %v", err)
	}
	if pinned.Pos != 9000 {
		t.Errorf("pinned at %d, want the settled 9000", pinned.Pos)
	}
}
