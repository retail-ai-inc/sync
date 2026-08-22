package directionlock

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// memoryStore is an in-memory endpoint, which is all the guard needs: the real
// stores are exercised separately against SQLite and the Redis stub.
type memoryStore struct {
	address string
	claims  map[int]Claim
	readErr error
	putErr  error
}

func newStore(address string, claims ...Claim) *memoryStore {
	s := &memoryStore{address: address, claims: map[int]Claim{}}
	for _, c := range claims {
		s.claims[c.TaskID] = c
	}
	return s
}

func (s *memoryStore) Claims(context.Context) ([]Claim, error) {
	if s.readErr != nil {
		return nil, s.readErr
	}
	out := make([]Claim, 0, len(s.claims))
	for _, c := range s.claims {
		out = append(out, c)
	}
	return out, nil
}

func (s *memoryStore) Put(_ context.Context, c Claim) error {
	if s.putErr != nil {
		return s.putErr
	}
	s.claims[c.TaskID] = c
	return nil
}

func (s *memoryStore) Remove(_ context.Context, taskID int) error {
	if s.putErr != nil {
		return s.putErr
	}
	delete(s.claims, taskID)
	return nil
}

func (s *memoryStore) Endpoint() string { return s.address }

var fixedNow = time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)

func guardFor(source, target *memoryStore) *Guard {
	return &Guard{
		TaskID: 1,
		Source: source,
		Target: target,
		Now:    func() time.Time { return fixedNow },
		Owner:  "syncer-osaka-0",
	}
}

func claim(taskID int, role Role, peer string, age time.Duration) Claim {
	return Claim{
		TaskID:    taskID,
		Role:      role,
		Peer:      peer,
		Owner:     "syncer-tokyo-0",
		UpdatedAt: fixedNow.Add(-age),
	}
}

// ---------------------------------------------------------------- acquire

func TestAnUnclaimedPairIsAcquired(t *testing.T) {
	source, target := newStore("tokyo:3306/shop"), newStore("osaka:3306/shop")
	g := guardFor(source, target)

	if err := g.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	if got := source.claims[1]; got.Role != RoleSource || got.Peer != "osaka:3306/shop" {
		t.Errorf("source claim = %+v", got)
	}
	if got := target.claims[1]; got.Role != RoleTarget || got.Peer != "tokyo:3306/shop" {
		t.Errorf("target claim = %+v", got)
	}
	if got := target.claims[1].Owner; got != "syncer-osaka-0" {
		t.Errorf("owner = %q, want the process holding the claim", got)
	}
}

// TestAPromotedTargetIsRefused is the failure this package exists for. Tokyo
// goes down, Osaka is promoted and starts taking payments, Tokyo comes back and
// the old task resumes — overwriting everything Osaka has written since, with
// data that is both older and wrong.
func TestAPromotedTargetIsRefused(t *testing.T) {
	source := newStore("tokyo:3306/shop")
	target := newStore("osaka:3306/shop", claim(2, RoleSource, "tokyo:3306/shop", time.Minute))
	g := guardFor(source, target)

	err := g.Acquire(context.Background())
	if err == nil {
		t.Fatal("Acquire succeeded against a target that is being replicated out of")
	}

	var conflict *Conflict
	if !errors.As(err, &conflict) {
		t.Fatalf("error = %v, want a Conflict", err)
	}
	if conflict.Endpoint != "osaka:3306/shop" {
		t.Errorf("conflict endpoint = %q", conflict.Endpoint)
	}
	if !strings.Contains(err.Error(), "promoted replica") {
		t.Errorf("error = %v, want it to explain the promotion", err)
	}
	if !strings.Contains(err.Error(), "syncer-tokyo-0") {
		t.Errorf("error = %v, want the holder named", err)
	}
	if len(source.claims) != 0 {
		t.Error("a claim was written despite the refusal")
	}
}

// TestASourceThatIsSomebodyElsesTargetIsRefused catches the other half of a
// reversed pair: reading out of a database that a replication task is writing
// into would carry those writes back where they came from.
func TestASourceThatIsSomebodyElsesTargetIsRefused(t *testing.T) {
	source := newStore("tokyo:3306/shop", claim(2, RoleTarget, "osaka:3306/shop", time.Minute))
	target := newStore("osaka:3306/shop")
	g := guardFor(source, target)

	err := g.Acquire(context.Background())
	if err == nil {
		t.Fatal("Acquire succeeded against a source that is being replicated into")
	}
	if !strings.Contains(err.Error(), "back to where they came from") {
		t.Errorf("error = %v", err)
	}
}

// TestTwoTasksWritingOneTargetAreRefused covers the ordinary misconfiguration:
// two tasks pointed at the same target from different sources.
func TestTwoTasksWritingOneTargetAreRefused(t *testing.T) {
	source := newStore("tokyo:3306/shop")
	target := newStore("osaka:3306/shop", claim(2, RoleTarget, "kobe:3306/shop", time.Minute))
	g := guardFor(source, target)

	if err := g.Acquire(context.Background()); err == nil {
		t.Fatal("Acquire succeeded with another task writing the same target")
	}
}

// TestTheSameTaskReacquiresItsOwnClaim is what an ordinary restart does.
func TestTheSameTaskReacquiresItsOwnClaim(t *testing.T) {
	source := newStore("tokyo:3306/shop", claim(1, RoleSource, "osaka:3306/shop", time.Minute))
	target := newStore("osaka:3306/shop", claim(1, RoleTarget, "tokyo:3306/shop", time.Minute))
	g := guardFor(source, target)

	if err := g.Acquire(context.Background()); err != nil {
		t.Fatalf("a task could not reacquire its own claim: %v", err)
	}
	if got := target.claims[1].UpdatedAt; !got.Equal(fixedNow) {
		t.Errorf("the claim was not refreshed: %v", got)
	}
}

// TestAnotherTaskWritingFromTheSameSourceIsAllowed covers a source split across
// several tasks, one per set of tables, which is how a large database is
// usually replicated.
func TestAnotherTaskWritingFromTheSameSourceIsAllowed(t *testing.T) {
	source := newStore("tokyo:3306/shop")
	target := newStore("osaka:3306/shop", claim(2, RoleTarget, "tokyo:3306/shop", time.Minute))
	g := guardFor(source, target)

	if err := g.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
}

// TestAStaleClaimIsIgnored is what lets a task start after the process that
// held the endpoint died without cleaning up.
func TestAStaleClaimIsIgnored(t *testing.T) {
	source := newStore("tokyo:3306/shop")
	target := newStore("osaka:3306/shop", claim(2, RoleSource, "elsewhere", 2*DefaultStaleAfter))
	g := guardFor(source, target)

	if err := g.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire refused because of a claim nothing has refreshed: %v", err)
	}
}

// TestAClaimJustInsideTheWindowStillHolds pins the boundary from the other
// side: a claim expiring while its owner is still running would be worse than
// no claim at all, because two tasks would then write the same target.
func TestAClaimJustInsideTheWindowStillHolds(t *testing.T) {
	source := newStore("tokyo:3306/shop")
	target := newStore("osaka:3306/shop", claim(2, RoleSource, "elsewhere", DefaultStaleAfter-time.Second))
	g := guardFor(source, target)

	if err := g.Acquire(context.Background()); err == nil {
		t.Fatal("a claim inside the staleness window was ignored")
	}
}

func TestTheStalenessWindowIsLongerThanTheHeartbeat(t *testing.T) {
	if DefaultStaleAfter <= HeartbeatInterval {
		t.Fatalf("claims go stale after %v but are refreshed every %v, so a running "+
			"task would lose its own endpoint", DefaultStaleAfter, HeartbeatInterval)
	}
}

// -------------------------------------------------------------- failures

func TestAnUnreadableTargetStopsTheTask(t *testing.T) {
	target := newStore("osaka:3306/shop")
	target.readErr = errors.New("connection refused")
	g := guardFor(newStore("tokyo:3306/shop"), target)

	err := g.Acquire(context.Background())
	if err == nil {
		t.Fatal("Acquire succeeded although the claims could not be read")
	}
	if !strings.Contains(err.Error(), "osaka:3306/shop") {
		t.Errorf("error = %v, want the endpoint named", err)
	}
}

func TestAnUnreadableSourceStopsTheTask(t *testing.T) {
	source := newStore("tokyo:3306/shop")
	source.readErr = errors.New("connection refused")
	g := guardFor(source, newStore("osaka:3306/shop"))

	if err := g.Acquire(context.Background()); err == nil {
		t.Fatal("Acquire succeeded although the source claims could not be read")
	}
}

func TestAnUnwritableClaimStopsTheTask(t *testing.T) {
	target := newStore("osaka:3306/shop")
	target.putErr = errors.New("read only")
	g := guardFor(newStore("tokyo:3306/shop"), target)

	err := g.Acquire(context.Background())
	if err == nil {
		t.Fatal("Acquire succeeded although the claim could not be recorded")
	}
	if !strings.Contains(err.Error(), "record the direction claim") {
		t.Errorf("error = %v", err)
	}
}

// ------------------------------------------------------------- heartbeat

func TestKeepAliveStopsWithTheContext(t *testing.T) {
	g := guardFor(newStore("tokyo:3306/shop"), newStore("osaka:3306/shop"))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		g.KeepAlive(ctx, nil)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("KeepAlive did not stop when the context was cancelled")
	}
}

// ---------------------------------------------------------------- claims

func TestFreshnessIsMeasuredFromTheHeartbeat(t *testing.T) {
	c := claim(1, RoleSource, "peer", time.Minute)

	if !c.Fresh(fixedNow, DefaultStaleAfter) {
		t.Error("a claim refreshed a minute ago was reported stale")
	}
	if c.Fresh(fixedNow.Add(DefaultStaleAfter), DefaultStaleAfter) {
		t.Error("a claim older than the window was reported fresh")
	}
}

// TestAClaimWithNoTimestampIsStale is how a row whose timestamp could not be
// read is treated: it must not hold an endpoint hostage for ever.
func TestAClaimWithNoTimestampIsStale(t *testing.T) {
	if (Claim{}).Fresh(fixedNow, DefaultStaleAfter) {
		t.Error("a claim with no timestamp was reported fresh")
	}
}

func TestTheOwnerDefaultsToTheHostname(t *testing.T) {
	g := guardFor(newStore("a"), newStore("b"))
	g.Owner = ""

	if got := g.owner(); got == "" {
		t.Error("owner() returned nothing; a claim with no owner cannot be traced")
	}
}

func TestTheStalenessWindowCanBeOverridden(t *testing.T) {
	g := guardFor(newStore("a"), newStore("b"))
	if got := g.staleAfter(); got != DefaultStaleAfter {
		t.Errorf("staleAfter() = %v, want the default", got)
	}
	g.StaleAfter = time.Minute
	if got := g.staleAfter(); got != time.Minute {
		t.Errorf("staleAfter() = %v", got)
	}
}

// ---------------------------------------------------------------- release

// TestReleasingLetsTheOppositeDirectionStartAtOnce is what makes a planned
// failover quick. Without it the claims sit there until they go stale, so an
// operator who has stopped Tokyo → Osaka and wants to start Osaka → Tokyo is
// refused for the length of the staleness window — a quarter of an hour of a
// runbook spent waiting for a timeout.
func TestReleasingLetsTheOppositeDirectionStartAtOnce(t *testing.T) {
	tokyo, osaka := newStore("tokyo:3306/shop"), newStore("osaka:3306/shop")

	forward := guardFor(tokyo, osaka)
	if err := forward.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	// The reverse task cannot start while the forward one holds the pair.
	reverse := &Guard{
		TaskID: 2, Source: osaka, Target: tokyo,
		Now: func() time.Time { return fixedNow }, Owner: "syncer-tokyo-0",
	}
	if err := reverse.Acquire(context.Background()); err == nil {
		t.Fatal("the reverse direction was allowed while the forward one held the pair")
	}

	if err := forward.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := reverse.Acquire(context.Background()); err != nil {
		t.Errorf("the reverse direction is still refused after the release: %v", err)
	}
}

func TestReleasingRemovesBothClaims(t *testing.T) {
	source, target := newStore("tokyo:3306/shop"), newStore("osaka:3306/shop")
	g := guardFor(source, target)
	if err := g.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	if err := g.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}

	if len(source.claims) != 0 || len(target.claims) != 0 {
		t.Errorf("claims left: source %v, target %v", source.claims, target.claims)
	}
}

// TestReleasingLeavesOtherTasksAlone covers a source split across several tasks,
// which is how a large database is usually replicated.
func TestReleasingLeavesOtherTasksAlone(t *testing.T) {
	source := newStore("tokyo:3306/shop")
	target := newStore("osaka:3306/shop", claim(2, RoleTarget, "tokyo:3306/shop", time.Minute))
	g := guardFor(source, target)
	if err := g.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	if err := g.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}

	if _, ok := target.claims[2]; !ok {
		t.Error("another task's claim was released too")
	}
}

// TestAFailedReleaseIsReportedButStillTriesBothEnds keeps one unreachable
// endpoint from leaving a claim behind on the other.
func TestAFailedReleaseIsReportedButStillTriesBothEnds(t *testing.T) {
	source, target := newStore("tokyo:3306/shop"), newStore("osaka:3306/shop")
	g := guardFor(source, target)
	if err := g.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	source.putErr = errors.New("connection refused")

	err := g.Release(context.Background())
	if err == nil {
		t.Error("a failed release reported success")
	}
	if len(target.claims) != 0 {
		t.Error("the reachable endpoint's claim was left behind")
	}
}
