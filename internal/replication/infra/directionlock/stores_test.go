package directionlock

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
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

// TestTheLockTableIsCreatedOncePerProcess covers a defect the checkpoint store
// had first, in its sibling here.
//
// MySQL writes a CREATE TABLE to the binary log whether or not the table was
// there to create, and a claim is refreshed on both endpoints every
// HeartbeatInterval. Running the create on every claim therefore put a DDL
// statement into the source's binary log every minute, for ever, from a tool
// that is otherwise only a reader there -- and any task replicating that source
// read its own lock table's DDL back and counted it as a schema change it had
// refused to carry.
func TestTheLockTableIsCreatedOncePerProcess(t *testing.T) {
	store := sqlStore(t)
	ctx := context.Background()

	if err := store.Put(ctx, Claim{TaskID: 1, Role: "target", Peer: "tokyo", Owner: "a"}); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if _, err := store.DB.ExecContext(ctx, "DROP TABLE "+tableName); err != nil {
		t.Fatalf("drop: %v", err)
	}

	if err := store.Put(ctx, Claim{TaskID: 1, Role: "target", Peer: "tokyo", Owner: "a"}); err == nil {
		t.Error("the second claim created the table again, so every heartbeat " +
			"writes a CREATE TABLE into the endpoint's binary log")
	}
}

// TestReadingClaimsDoesNotRecreateTheTableEither: Claims runs on the same
// heartbeat as Put, so leaving the create on the read path would keep the DDL
// flowing at the same rate.
func TestReadingClaimsDoesNotRecreateTheTableEither(t *testing.T) {
	store := sqlStore(t)
	ctx := context.Background()

	if _, err := store.Claims(ctx); err != nil {
		t.Fatalf("first read: %v", err)
	}
	if _, err := store.DB.ExecContext(ctx, "DROP TABLE "+tableName); err != nil {
		t.Fatalf("drop: %v", err)
	}

	if _, err := store.Claims(ctx); err == nil {
		t.Error("reading the claims created the table again")
	}
}

// The claim is what stops two processes writing to one target. It used to be an
// unconditional delete and insert: two replicas starting the same task both
// read no claim, both wrote one, and the second overwrote the first -- leaving
// exactly the pair of writers the lock exists to prevent, with their heartbeats
// overwriting each other for as long as both ran.

func TestASecondOwnerCannotTakeALiveClaim(t *testing.T) {
	store := sqlStore(t)
	now := time.Now()

	if err := store.Put(context.Background(), Claim{
		TaskID: 41, Role: "target", Peer: "tokyo", Owner: "replica-a", UpdatedAt: now,
	}); err != nil {
		t.Fatalf("the first claim was refused: %v", err)
	}

	err := store.Put(context.Background(), Claim{
		TaskID: 41, Role: "target", Peer: "tokyo", Owner: "replica-b", UpdatedAt: now,
	})
	if err == nil {
		t.Fatal("a second owner took a live claim, so two processes would write " +
			"to the same target")
	}
	if !errors.Is(err, ErrClaimHeld) {
		t.Errorf("the refusal is %v, which callers cannot tell from a write failure", err)
	}

	claims, err := store.Claims(context.Background())
	if err != nil {
		t.Fatalf("Claims: %v", err)
	}
	if len(claims) != 1 || claims[0].Owner != "replica-a" {
		t.Errorf("the stored claim is %v, want the first owner's", claims)
	}
}

// TestAnOwnerKeepsRefreshingItsOwnClaim: the heartbeat runs every minute and
// must not lock the owner out of its own claim.
func TestAnOwnerKeepsRefreshingItsOwnClaim(t *testing.T) {
	store := sqlStore(t)
	now := time.Now()

	for i := 0; i < 3; i++ {
		if err := store.Put(context.Background(), Claim{
			TaskID: 41, Role: "target", Peer: "tokyo", Owner: "replica-a",
			UpdatedAt: now.Add(time.Duration(i) * time.Minute),
		}); err != nil {
			t.Fatalf("refresh %d was refused: %v", i, err)
		}
	}

	claims, _ := store.Claims(context.Background())
	if len(claims) != 1 {
		t.Fatalf("the refreshes left %d claims", len(claims))
	}
}

// TestAnAbandonedClaimCanBeTaken: a process that died leaves its claim behind,
// and the task has to be startable again once it is old enough to count as
// gone. Without this the lock would be a permanent lock-out after a crash.
func TestAnAbandonedClaimCanBeTaken(t *testing.T) {
	store := sqlStore(t)
	now := time.Now()

	if err := store.Put(context.Background(), Claim{
		TaskID: 41, Role: "target", Peer: "tokyo", Owner: "replica-a",
		UpdatedAt: now.Add(-2 * ConcurrentAfter),
	}); err != nil {
		t.Fatalf("the first claim was refused: %v", err)
	}

	if err := store.Put(context.Background(), Claim{
		TaskID: 41, Role: "target", Peer: "tokyo", Owner: "replica-b", UpdatedAt: now,
	}); err != nil {
		t.Fatalf("an abandoned claim could not be taken over: %v", err)
	}

	claims, _ := store.Claims(context.Background())
	if len(claims) != 1 || claims[0].Owner != "replica-b" {
		t.Errorf("the claim is %v, want the new owner's", claims)
	}
}

// TestOnlyOneOfTwoConcurrentClaimsSucceeds is the case that motivates all of
// this: both processes start at the same moment and both find no claim.
func TestOnlyOneOfTwoConcurrentClaimsSucceeds(t *testing.T) {
	store := sqlStore(t)
	now := time.Now()

	start := make(chan struct{})
	results := make(chan error, 2)
	for _, owner := range []string{"replica-a", "replica-b"} {
		owner := owner
		go func() {
			<-start
			results <- store.Put(context.Background(), Claim{
				TaskID: 41, Role: "target", Peer: "tokyo", Owner: owner, UpdatedAt: now,
			})
		}()
	}
	close(start)

	taken := 0
	for i := 0; i < 2; i++ {
		if err := <-results; err == nil {
			taken++
		} else if !errors.Is(err, ErrClaimHeld) {
			t.Errorf("a claim failed for an unexpected reason: %v", err)
		}
	}
	if taken != 1 {
		t.Errorf("%d of two concurrent claims succeeded, want exactly one", taken)
	}
}

// TestDifferentTasksDoNotBlockEachOther: the claim is per task, not per
// endpoint, and several tasks share a target.
func TestDifferentTasksDoNotBlockEachOther(t *testing.T) {
	store := sqlStore(t)
	now := time.Now()

	for _, id := range []int{39, 41, 44} {
		if err := store.Put(context.Background(), Claim{
			TaskID: id, Role: "target", Peer: "tokyo", Owner: "replica-a", UpdatedAt: now,
		}); err != nil {
			t.Fatalf("task %d was refused: %v", id, err)
		}
	}

	claims, _ := store.Claims(context.Background())
	if len(claims) != 3 {
		t.Errorf("three tasks left %d claims", len(claims))
	}
}

// TestAHeldClaimIsAConcurrentConflict pins how a refusal reaches the caller.
//
// A store reporting the claim held means another process is running this task,
// which is the conflict that resolves itself: the other only has to finish
// exiting. Marked Concurrent it is not blocking, so the task retries with
// backoff and a rolling restart completes on its own. Left as a bare error it
// would not be a *Conflict at all, IsBlocking would say false for the wrong
// reason, and the message an operator sees would be a write failure.
func TestAHeldClaimIsAConcurrentConflict(t *testing.T) {
	err := claimFailure("osaka:3306/shop", ErrClaimHeld)

	var conflict *Conflict
	if !errors.As(err, &conflict) {
		t.Fatalf("a held claim came back as %T, not a conflict", err)
	}
	if !conflict.Concurrent {
		t.Error("a held claim was not marked concurrent, so the task would stop " +
			"for somebody to decide rather than retrying past a rolling restart")
	}
	if IsBlocking(err) {
		t.Error("a held claim reported itself blocking")
	}
	if conflict.Endpoint != "osaka:3306/shop" {
		t.Errorf("the conflict names %q", conflict.Endpoint)
	}
}

// TestAWriteFailureStaysAWriteFailure: only a held claim is a conflict, so a
// target that cannot be written to is still reported as such.
func TestAWriteFailureStaysAWriteFailure(t *testing.T) {
	err := claimFailure("osaka:3306/shop", errors.New("connection refused"))

	var conflict *Conflict
	if errors.As(err, &conflict) {
		t.Error("an unreachable endpoint was reported as a direction conflict")
	}
	if !strings.Contains(err.Error(), "osaka:3306/shop") {
		t.Errorf("the error does not name the endpoint: %v", err)
	}
}
