package checkpoint

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
)

func sqlStore(t *testing.T, taskID int) *SQLStore {
	t.Helper()

	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "target.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	return &SQLStore{DB: db, TaskID: taskID}
}

type stubStore struct {
	payloads map[string]string
	loadErr  error
	saveErr  error
	saves    int
}

func newStub() *stubStore { return &stubStore{payloads: map[string]string{}} }

func (s *stubStore) Load(_ context.Context, key string) (string, error) {
	if s.loadErr != nil {
		return "", s.loadErr
	}
	return s.payloads[key], nil
}

func (s *stubStore) Save(_ context.Context, key, payload string) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	s.saves++
	s.payloads[key] = payload
	return nil
}

func TestTheSQLStoreCreatesItsOwnTable(t *testing.T) {
	s := sqlStore(t, 1)

	payload, err := s.Load(context.Background(), "")
	if err != nil {
		t.Fatalf("Load on a database with no table: %v", err)
	}
	if payload != "" {
		t.Errorf("payload = %q, want empty", payload)
	}
}

func TestTheSQLStoreRoundTrips(t *testing.T) {
	s := sqlStore(t, 7)
	ctx := context.Background()

	if err := s.Save(ctx, "", `{"Name":"binlog.1","Pos":4}`); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := s.Load(ctx, "")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got != `{"Name":"binlog.1","Pos":4}` {
		t.Errorf("payload = %q", got)
	}
}

// TestARefreshReplacesTheRow pins that a checkpoint written every second does
// not accumulate rows.
func TestARefreshReplacesTheRow(t *testing.T) {
	s := sqlStore(t, 1)
	ctx := context.Background()

	for _, payload := range []string{"a", "b", "c"} {
		if err := s.Save(ctx, "", payload); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}

	var rows int
	if err := s.DB.QueryRow("SELECT COUNT(*) FROM _sync_checkpoint").Scan(&rows); err != nil {
		t.Fatalf("count: %v", err)
	}
	if rows != 1 {
		t.Errorf("%d rows, want one", rows)
	}
	if got, _ := s.Load(ctx, ""); got != "c" {
		t.Errorf("payload = %q, want the latest", got)
	}
}

// TestEachKeyIsItsOwnCheckpoint is what lets one task hold a resume token per
// collection and a stream offset per stream.
func TestEachKeyIsItsOwnCheckpoint(t *testing.T) {
	s := sqlStore(t, 1)
	ctx := context.Background()

	if err := s.Save(ctx, "orders", "a"); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := s.Save(ctx, "customers", "b"); err != nil {
		t.Fatalf("Save: %v", err)
	}

	if got, _ := s.Load(ctx, "orders"); got != "a" {
		t.Errorf("orders = %q", got)
	}
	if got, _ := s.Load(ctx, "customers"); got != "b" {
		t.Errorf("customers = %q", got)
	}
}

// TestTwoTasksSharingATargetDoNotCollide covers a target written by more than
// one task, which is how a large database is split up.
func TestTwoTasksSharingATargetDoNotCollide(t *testing.T) {
	first := sqlStore(t, 1)
	second := &SQLStore{DB: first.DB, TaskID: 2}
	ctx := context.Background()

	if err := first.Save(ctx, "", "one"); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := second.Save(ctx, "", "two"); err != nil {
		t.Fatalf("Save: %v", err)
	}

	if got, _ := first.Load(ctx, ""); got != "one" {
		t.Errorf("task 1 = %q", got)
	}
	if got, _ := second.Load(ctx, ""); got != "two" {
		t.Errorf("task 2 = %q", got)
	}
}

func TestTheSQLStoreQualifiesTheTable(t *testing.T) {
	if got := (&SQLStore{}).qualified(); got != "_sync_checkpoint" {
		t.Errorf("qualified() = %q", got)
	}
	if got := (&SQLStore{Schema: "shop"}).qualified(); got != "shop._sync_checkpoint" {
		t.Errorf("qualified() = %q", got)
	}
}

func TestAClosedDatabaseIsReported(t *testing.T) {
	s := sqlStore(t, 1)
	_ = s.DB.Close()

	if _, err := s.Load(context.Background(), ""); err == nil {
		t.Error("Load on a closed database returned no error")
	}
	if err := s.Save(context.Background(), "", "x"); err == nil {
		t.Error("Save on a closed database returned no error")
	}
}

func TestTheFileStoreRoundTrips(t *testing.T) {
	s := &FileStore{Path: filepath.Join(t.TempDir(), "nested", "pos.json")}
	ctx := context.Background()

	if err := s.Save(ctx, "", "payload"); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := s.Load(ctx, "")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got != "payload" {
		t.Errorf("payload = %q", got)
	}
}

func TestTheFileStoreKeepsEachKeyApart(t *testing.T) {
	s := &FileStore{Path: filepath.Join(t.TempDir(), "pos")}
	ctx := context.Background()

	if err := s.Save(ctx, "orders", "a"); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := s.Save(ctx, "customers", "b"); err != nil {
		t.Fatalf("Save: %v", err)
	}

	if got, _ := s.Load(ctx, "orders"); got != "a" {
		t.Errorf("orders = %q", got)
	}
	if got, _ := s.Load(ctx, "customers"); got != "b" {
		t.Errorf("customers = %q", got)
	}
}

func TestAnAbsentFileIsNotAnError(t *testing.T) {
	s := &FileStore{Path: filepath.Join(t.TempDir(), "absent")}

	got, err := s.Load(context.Background(), "")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got != "" {
		t.Errorf("payload = %q", got)
	}
}

func TestAPayloadRoundTrips(t *testing.T) {
	type position struct {
		Name string
		Pos  uint32
	}
	want := position{Name: "binlog.1", Pos: 4}

	payload, err := Encode(want)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	var got position
	found, err := Decode(payload, &got)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !found {
		t.Fatal("Decode reported nothing for a payload it was given")
	}
	if got != want {
		t.Errorf("decoded %+v, want %+v", got, want)
	}
}

// TestAnEmptyPayloadIsNotTheZeroValue pins the distinction the caller relies on:
// "there is no checkpoint" must not read as "the checkpoint says offset zero",
// which would start the stream at the beginning of a binlog file that may no
// longer exist.
func TestAnEmptyPayloadIsNotTheZeroValue(t *testing.T) {
	var got struct{ Pos uint32 }

	found, err := Decode("", &got)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if found {
		t.Error("an empty payload was reported as a checkpoint")
	}
}

func TestAnUnreadablePayloadIsReported(t *testing.T) {
	var got struct{ Pos uint32 }

	if _, err := Decode("not json", &got); err == nil {
		t.Error("Decode accepted a payload it cannot have read")
	}
}

// TestTheTableIsCreatedOncePerProcess: MySQL writes a CREATE TABLE to the
// binary log whether or not there was anything to create, and Save runs per
// source transaction. Issuing it every time put a DDL statement into the
// source's log twenty-five times a second, and every task reading that log
// counted each one as a schema change it had refused to carry.
//
// The creation is not counted directly -- it is observed: the table is taken
// away after the first save, and a store that creates it again would not
// notice.
func TestTheTableIsCreatedOncePerProcess(t *testing.T) {
	store := sqlStore(t, 1)
	ctx := context.Background()

	if err := store.Save(ctx, "shard-0", "position"); err != nil {
		t.Fatalf("first save: %v", err)
	}
	if _, err := store.DB.ExecContext(ctx, "DROP TABLE "+tableName); err != nil {
		t.Fatalf("drop: %v", err)
	}

	err := store.Save(ctx, "shard-0", "position")
	if err == nil {
		t.Error("the second save created the table again, so every save writes a " +
			"CREATE TABLE into the source's binary log")
	}
}

// A deleted task used to leave its position behind for ever: the row in
// sync_tasks went and the row here did not, so a target accumulated the
// positions of every task that had ever pointed at it -- and a new task given
// a reused id would resume from a stranger's offset rather than copying.
func TestPurgeRemovesOnlyThisTasksPositions(t *testing.T) {
	mine := sqlStore(t, 7)
	theirs := &SQLStore{DB: mine.DB, TaskID: 8}

	ctx := context.Background()
	for _, s := range []*SQLStore{mine, theirs} {
		for _, key := range []string{"shard-a", "shard-b"} {
			if err := s.Save(ctx, key, "position-"+key); err != nil {
				t.Fatalf("save: %v", err)
			}
		}
	}

	if err := mine.Purge(ctx); err != nil {
		t.Fatalf("purge: %v", err)
	}

	for _, key := range []string{"shard-a", "shard-b"} {
		got, err := mine.Load(ctx, key)
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if got != "" {
			t.Errorf("task 7 still has %s = %q after being purged", key, got)
		}
		// The neighbour's positions are not this task's to remove.
		other, err := theirs.Load(ctx, key)
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if other != "position-"+key {
			t.Errorf("task 8 lost %s: %q", key, other)
		}
	}
}

// Purging a task that never wrote anything is not an error: a task deleted
// before it ever ran must still be deletable.
func TestPurgingATaskThatNeverRanIsFine(t *testing.T) {
	if err := sqlStore(t, 11).Purge(context.Background()); err != nil {
		t.Errorf("purge of an unused task = %v, want nil", err)
	}
}

// A purge that cannot reach the target used to report success, which is the
// one answer that leaves the positions in place with nothing to show for it.
func TestAPurgeReportsATargetItCannotReach(t *testing.T) {
	db, err := sql.Open("mysql", "root:root@tcp(127.0.0.1:1)/nothing")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	store := &SQLStore{DB: db, Schema: "nothing", TaskID: 4242}
	err = store.Purge(context.Background())
	if err == nil {
		t.Fatal("purging an unreachable target reported success")
	}
	if !strings.Contains(err.Error(), "4242") {
		t.Errorf("the error does not name the task whose positions were left: %v", err)
	}
}

// Deleting a task must still succeed when the purge fails, or an unreachable
// target makes the task undeletable. That is the caller's rule, and this is the
// property the error above depends on.
func TestAPurgeOfAReachableTargetStillSucceeds(t *testing.T) {
	store := sqlStore(t, 7)
	ctx := context.Background()

	if err := store.Save(ctx, "", `{"file":"a","pos":1}`); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := store.Purge(ctx); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	payload, err := store.Load(ctx, "")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if payload != "" {
		t.Errorf("the position survived the purge: %q", payload)
	}
}
