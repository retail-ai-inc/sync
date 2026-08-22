package checkpoint

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

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

// stubStore is a store whose behaviour a test dictates.
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

// ------------------------------------------------------------------- SQL

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

// ------------------------------------------------------------------ file

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

// TestAFileStoreWithNoPathIsANoOp covers a task that configures no path, which
// is the only reason the store is optional at all.
func TestAFileStoreWithNoPathIsANoOp(t *testing.T) {
	s := &FileStore{}
	ctx := context.Background()

	if err := s.Save(ctx, "", "x"); err != nil {
		t.Errorf("Save with no path: %v", err)
	}
	if got, err := s.Load(ctx, ""); err != nil || got != "" {
		t.Errorf("Load with no path = %q, %v", got, err)
	}
}

func TestAnUnwritableFileIsReported(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocker, nil, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	s := &FileStore{Path: filepath.Join(blocker, "pos")}

	if err := s.Save(context.Background(), "", "x"); err == nil {
		t.Error("writing under a regular file returned no error")
	}
}

// ---------------------------------------------------------------- layered

// TestTheFirstStoreWithACheckpointWins is the migration path: a deployment
// upgrading from the file-only version has its position on disk and nothing on
// the target, so the file is read once and every write from then on lands in
// both places.
func TestTheFirstStoreWithACheckpointWins(t *testing.T) {
	target, file := newStub(), newStub()
	file.payloads[""] = "from the file"
	l := &Layered{Stores: []Store{target, file}}

	got, err := l.Load(context.Background(), "")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got != "from the file" {
		t.Errorf("payload = %q", got)
	}

	target.payloads[""] = "from the target"
	if got, _ := l.Load(context.Background(), ""); got != "from the target" {
		t.Errorf("payload = %q; once the target has one it must win, or a syncer "+
			"replaced in the other region reads a stale local file", got)
	}
}

func TestASaveReachesEveryStore(t *testing.T) {
	target, file := newStub(), newStub()
	l := &Layered{Stores: []Store{target, file}}

	if err := l.Save(context.Background(), "", "x"); err != nil {
		t.Fatalf("Save: %v", err)
	}

	if target.payloads[""] != "x" || file.payloads[""] != "x" {
		t.Errorf("target = %q, file = %q", target.payloads[""], file.payloads[""])
	}
}

// TestOneUnreachableStoreDoesNotStopTheOthers matters because the target being
// briefly unreachable must not stop the checkpoint being recorded at all.
func TestOneUnreachableStoreDoesNotStopTheOthers(t *testing.T) {
	target, file := newStub(), newStub()
	target.saveErr = errors.New("connection refused")

	var reported []error
	l := &Layered{Stores: []Store{target, file}, OnError: func(err error) {
		reported = append(reported, err)
	}}

	if err := l.Save(context.Background(), "", "x"); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if file.payloads[""] != "x" {
		t.Error("the reachable store did not take the checkpoint")
	}
	if len(reported) != 1 {
		t.Errorf("%d failures were reported, want one: a degraded layer must be visible", len(reported))
	}
}

func TestASaveThatReachesNothingIsReported(t *testing.T) {
	target := newStub()
	target.saveErr = errors.New("connection refused")
	l := &Layered{Stores: []Store{target}}

	if err := l.Save(context.Background(), "", "x"); err == nil {
		t.Error("Save reported success although no store took the checkpoint")
	}

	if err := (&Layered{}).Save(context.Background(), "", "x"); err == nil {
		t.Error("Save with no stores configured reported success")
	}
}

// TestAnUnreadableStoreIsNotSilentlyAbsent is the distinction that matters most
// here: "there is no checkpoint" and "the checkpoint could not be read" lead to
// opposite decisions, and confusing them either re-copies a whole database or
// skips whatever was in flight.
func TestAnUnreadableStoreIsNotSilentlyAbsent(t *testing.T) {
	target := newStub()
	target.loadErr = errors.New("connection refused")
	l := &Layered{Stores: []Store{target}}

	if _, err := l.Load(context.Background(), ""); err == nil {
		t.Error("Load reported no checkpoint for a store it could not read")
	}
}

// TestAReadableStoreWithNothingIsNotAnError is the other side: an empty target
// really does mean there is no checkpoint.
func TestAReadableStoreWithNothingIsNotAnError(t *testing.T) {
	l := &Layered{Stores: []Store{newStub(), newStub()}}

	got, err := l.Load(context.Background(), "")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got != "" {
		t.Errorf("payload = %q", got)
	}
}

// TestOneUnreadableStoreDoesNotHideAnother covers the target being down while
// the file still has the position.
func TestOneUnreadableStoreDoesNotHideAnother(t *testing.T) {
	target, file := newStub(), newStub()
	target.loadErr = errors.New("connection refused")
	file.payloads[""] = "from the file"
	l := &Layered{Stores: []Store{target, file}}

	got, err := l.Load(context.Background(), "")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got != "from the file" {
		t.Errorf("payload = %q", got)
	}
}

// ---------------------------------------------------------------- payloads

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
