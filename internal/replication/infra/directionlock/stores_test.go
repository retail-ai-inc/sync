package directionlock

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func sqlStore(t *testing.T) *SQLStore {
	t.Helper()

	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "lock.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	return &SQLStore{DB: db, Address: "osaka:3306/shop"}
}

func TestTheSQLStoreCreatesItsOwnTable(t *testing.T) {
	s := sqlStore(t)

	claims, err := s.Claims(context.Background())
	if err != nil {
		t.Fatalf("Claims on a database with no table: %v", err)
	}
	if len(claims) != 0 {
		t.Errorf("claims = %v, want none", claims)
	}
}

func TestTheSQLStoreRoundTripsAClaim(t *testing.T) {
	s := sqlStore(t)
	want := Claim{
		TaskID: 7, Role: RoleTarget, Peer: "tokyo:3306/shop",
		Owner: "syncer-0", UpdatedAt: fixedNow,
	}

	if err := s.Put(context.Background(), want); err != nil {
		t.Fatalf("Put: %v", err)
	}

	claims, err := s.Claims(context.Background())
	if err != nil {
		t.Fatalf("Claims: %v", err)
	}
	if len(claims) != 1 {
		t.Fatalf("claims = %v, want one", claims)
	}
	got := claims[0]
	if got.TaskID != want.TaskID || got.Role != want.Role || got.Peer != want.Peer ||
		got.Owner != want.Owner || !got.UpdatedAt.Equal(want.UpdatedAt) {
		t.Errorf("claim = %+v, want %+v", got, want)
	}
}

// TestARefreshReplacesTheTasksClaim pins that a heartbeat does not accumulate
// rows: one task holds one claim per endpoint.
func TestARefreshReplacesTheTasksClaim(t *testing.T) {
	s := sqlStore(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := s.Put(ctx, Claim{
			TaskID: 1, Role: RoleTarget, Peer: "tokyo:3306/shop",
			Owner: "syncer-0", UpdatedAt: fixedNow.Add(time.Duration(i) * time.Minute),
		}); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	claims, err := s.Claims(ctx)
	if err != nil {
		t.Fatalf("Claims: %v", err)
	}
	if len(claims) != 1 {
		t.Fatalf("claims = %v, want one", claims)
	}
	if want := fixedNow.Add(2 * time.Minute); !claims[0].UpdatedAt.Equal(want) {
		t.Errorf("claim time = %v, want the latest %v", claims[0].UpdatedAt, want)
	}
}

func TestTheSQLStoreKeepsOneClaimPerTask(t *testing.T) {
	s := sqlStore(t)
	ctx := context.Background()

	for _, id := range []int{1, 2, 3} {
		if err := s.Put(ctx, Claim{TaskID: id, Role: RoleTarget, UpdatedAt: fixedNow}); err != nil {
			t.Fatalf("Put %d: %v", id, err)
		}
	}

	claims, err := s.Claims(ctx)
	if err != nil {
		t.Fatalf("Claims: %v", err)
	}
	if len(claims) != 3 {
		t.Errorf("claims = %v, want three", claims)
	}
}

// TestAnUnreadableTimestampIsTreatedAsLongPast keeps a corrupt row from holding
// an endpoint hostage for ever, which would stop replication with no way to
// restart it short of editing the table by hand.
func TestAnUnreadableTimestampIsTreatedAsLongPast(t *testing.T) {
	s := sqlStore(t)
	ctx := context.Background()
	if _, err := s.Claims(ctx); err != nil { // create the table
		t.Fatalf("Claims: %v", err)
	}
	if _, err := s.DB.ExecContext(ctx,
		"INSERT INTO _sync_direction_lock (task_id, role, peer, owner, updated_at) "+
			"VALUES (9, 'source', 'p', 'o', 'not a timestamp')"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	claims, err := s.Claims(ctx)
	if err != nil {
		t.Fatalf("Claims: %v", err)
	}
	if len(claims) != 1 {
		t.Fatalf("claims = %v", claims)
	}
	if claims[0].Fresh(fixedNow, DefaultStaleAfter) {
		t.Error("a row with an unreadable timestamp was reported fresh")
	}
}

func TestTheSQLStoreQualifiesTheTable(t *testing.T) {
	if got := (&SQLStore{}).qualified(); got != "_sync_direction_lock" {
		t.Errorf("qualified() = %q", got)
	}
	if got := (&SQLStore{Schema: "shop"}).qualified(); got != "shop._sync_direction_lock" {
		t.Errorf("qualified() = %q", got)
	}
}

func TestTheSQLStoreNamesItsEndpoint(t *testing.T) {
	if got := (&SQLStore{Address: "osaka:3306/shop"}).Endpoint(); got != "osaka:3306/shop" {
		t.Errorf("Endpoint() = %q", got)
	}
}

// TestAClosedDatabaseIsReported covers the endpoint going away underneath a
// heartbeat.
func TestAClosedDatabaseIsReported(t *testing.T) {
	s := sqlStore(t)
	_ = s.DB.Close()

	if _, err := s.Claims(context.Background()); err == nil {
		t.Error("Claims on a closed database returned no error")
	}
	if err := s.Put(context.Background(), Claim{TaskID: 1}); err == nil {
		t.Error("Put on a closed database returned no error")
	}
}

// TestTheSQLStoreRemovesOneTasksClaim covers the release path against a real
// table.
func TestTheSQLStoreRemovesOneTasksClaim(t *testing.T) {
	s := sqlStore(t)
	ctx := context.Background()

	for _, id := range []int{1, 2} {
		if err := s.Put(ctx, Claim{TaskID: id, Role: RoleTarget, UpdatedAt: fixedNow}); err != nil {
			t.Fatalf("Put %d: %v", id, err)
		}
	}

	if err := s.Remove(ctx, 1); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	claims, err := s.Claims(ctx)
	if err != nil {
		t.Fatalf("Claims: %v", err)
	}
	if len(claims) != 1 || claims[0].TaskID != 2 {
		t.Errorf("claims = %v, want only task 2", claims)
	}
}

// TestRemovingAClaimThatIsNotThereIsFine covers a release after a crash left
// nothing behind, and a second release.
func TestRemovingAClaimThatIsNotThereIsFine(t *testing.T) {
	s := sqlStore(t)

	if err := s.Remove(context.Background(), 99); err != nil {
		t.Errorf("Remove of an absent claim = %v", err)
	}
}

// TestTheSQLStoreSpellsPlaceholdersBothWays pins the one difference between the
// flavours this store runs against.
func TestTheSQLStoreSpellsPlaceholdersBothWays(t *testing.T) {
	if got := (&SQLStore{}).arg(2); got != "?" {
		t.Errorf("arg(2) = %q, want a question mark", got)
	}
	if got := (&SQLStore{NumberedPlaceholders: true}).arg(2); got != "$2" {
		t.Errorf("arg(2) = %q, want $2", got)
	}
}
