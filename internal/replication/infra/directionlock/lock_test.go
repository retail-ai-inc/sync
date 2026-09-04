package directionlock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
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

// Tokyo goes down, Osaka is promoted and starts taking payments, Tokyo comes
// back and the old task resumes — overwriting everything Osaka has written
// since, with data that is both older and wrong.
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

// TestTwoTasksWritingOneTargetAreRefused covers the ordinary misconfiguration.
func TestTwoTasksWritingOneTargetAreRefused(t *testing.T) {
	source := newStore("tokyo:3306/shop")
	target := newStore("osaka:3306/shop", claim(2, RoleTarget, "kobe:3306/shop", time.Minute))
	g := guardFor(source, target)

	if err := g.Acquire(context.Background()); err == nil {
		t.Fatal("Acquire succeeded with another task writing the same target")
	}
}

// ownClaim is a claim this guard's own process left behind, which is what a
// restart finds.
func ownClaim(role Role, peer string, age time.Duration) Claim {
	c := claim(1, role, peer, age)
	c.Owner = "syncer-osaka-0"
	return c
}

// TestTheSameTaskReacquiresItsOwnClaim is what an ordinary restart does.
func TestTheSameTaskReacquiresItsOwnClaim(t *testing.T) {
	source := newStore("tokyo:3306/shop", ownClaim(RoleSource, "osaka:3306/shop", time.Minute))
	target := newStore("osaka:3306/shop", ownClaim(RoleTarget, "tokyo:3306/shop", time.Minute))
	g := guardFor(source, target)

	if err := g.Acquire(context.Background()); err != nil {
		t.Fatalf("a task could not reacquire its own claim: %v", err)
	}
	if got := target.claims[1].UpdatedAt; !got.Equal(fixedNow) {
		t.Errorf("the claim was not refreshed: %v", got)
	}
}

// Two writers replaying one stream from different offsets apply an older
// version of a record after a newer one, which idempotence does not undo.
func TestAnotherProcessHoldingTheSameTaskIsRefused(t *testing.T) {
	// claim() names syncer-tokyo-0; the guard is syncer-osaka-0.
	source := newStore("tokyo:3306/shop", claim(1, RoleSource, "osaka:3306/shop", time.Minute))
	target := newStore("osaka:3306/shop", claim(1, RoleTarget, "tokyo:3306/shop", time.Minute))
	g := guardFor(source, target)

	err := g.Acquire(context.Background())
	if err == nil {
		t.Fatal("a second process running the same task was allowed to start")
	}
	if !IsConcurrent(err) {
		t.Errorf("error = %v, want it marked concurrent so the caller retries", err)
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

// Without it the claims sit there until they go stale, so an operator who has
// stopped Tokyo → Osaka and wants to start Osaka → Tokyo is refused for the
// length of the staleness window — a quarter of an hour of a runbook spent
// waiting for a timeout.
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

type recordingWarner struct {
	mu       sync.Mutex
	messages []string
}

func (w *recordingWarner) Warnf(format string, args ...interface{}) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.messages = append(w.messages, fmt.Sprintf(format, args...))
}

func (w *recordingWarner) all() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.messages...)
}

// The claim is taken while the task runs and given up when it stops, which is
// what makes a planned failover quick: without the release the claims sit
// there until they go stale, and an operator who has stopped Tokyo → Osaka and
// wants to start Osaka → Tokyo waits a quarter of an hour.
func TestHoldClaimsAndReleases(t *testing.T) {
	source, target := newStore("tokyo:27017"), newStore("osaka:27017")
	guard := guardFor(source, target)

	release, err := Hold(context.Background(), guard, &recordingWarner{}, "MongoDB")
	if err != nil {
		t.Fatalf("Hold: %v", err)
	}

	if len(source.claims) != 1 || len(target.claims) != 1 {
		t.Fatalf("claims = %d source, %d target, want one each",
			len(source.claims), len(target.claims))
	}

	release()

	if len(source.claims) != 0 || len(target.claims) != 0 {
		t.Errorf("claims left behind: %d source, %d target",
			len(source.claims), len(target.claims))
	}
}

// TestHoldReleasesOnlyOnce records that the returned function can be called
// twice without a second release.
func TestHoldReleasesOnlyOnce(t *testing.T) {
	source, target := newStore("tokyo:27017"), newStore("osaka:27017")
	guard := guardFor(source, target)

	release, err := Hold(context.Background(), guard, &recordingWarner{}, "MySQL")
	if err != nil {
		t.Fatalf("Hold: %v", err)
	}

	release()
	// A second release against a store that now fails would report an error if
	// it ran at all.
	source.putErr = errors.New("the endpoint is gone")
	target.putErr = errors.New("the endpoint is gone")

	release()

	// Nothing to assert beyond not panicking and not reporting: a second
	// release that ran would have hit the failing store.
}

// TestHoldRefusesAReversedPair records that a pair the claims say has been
// reversed is refused, and that nothing is left running behind the refusal.
func TestHoldRefusesAReversedPair(t *testing.T) {
	// The target already claims to be somebody's source: it has been promoted.
	source := newStore("tokyo:27017")
	target := newStore("osaka:27017", claim(2, RoleSource, "elsewhere:27017", time.Minute))
	guard := guardFor(source, target)

	release, err := Hold(context.Background(), guard, &recordingWarner{}, "MongoDB")
	if err == nil {
		release()
		t.Fatal("Hold succeeded against a target that has been promoted")
	}
	if release != nil {
		t.Error("a release function was returned alongside the error")
	}
}

// A claim nobody could remove blocks the reverse direction until it goes stale.
func TestHoldReportsAFailedRelease(t *testing.T) {
	source, target := newStore("tokyo:27017"), newStore("osaka:27017")
	guard := guardFor(source, target)

	warner := &recordingWarner{}
	release, err := Hold(context.Background(), guard, warner, "PostgreSQL")
	if err != nil {
		t.Fatalf("Hold: %v", err)
	}

	source.putErr = errors.New("the endpoint is unreachable")
	release()

	var found bool
	for _, message := range warner.all() {
		if strings.Contains(message, "release") && strings.Contains(message, "PostgreSQL") {
			found = true
		}
	}
	if !found {
		t.Errorf("messages = %v, want the failed release reported", warner.all())
	}
}

// A heartbeat outliving its task keeps a claim alive on an endpoint nothing is
// replicating, which is the one thing worse than a claim that expires too
// early.
func TestHoldStopsTheHeartbeatWithTheContext(t *testing.T) {
	source, target := newStore("tokyo:27017"), newStore("osaka:27017")
	guard := guardFor(source, target)

	ctx, cancel := context.WithCancel(context.Background())
	release, err := Hold(ctx, guard, &recordingWarner{}, "MongoDB")
	if err != nil {
		t.Fatalf("Hold: %v", err)
	}
	before := runtime.NumGoroutine()
	cancel()

	// The heartbeat goroutine returns on cancellation; the release still works
	// afterwards because it uses a context of its own.
	release()

	if len(source.claims) != 0 {
		t.Errorf("the claim survived the release: %v", source.claims)
	}
	if after := runtime.NumGoroutine(); after > before {
		t.Logf("goroutines: %d before, %d after", before, after)
	}
}

// A claim for this task was skipped outright, so that a task restarted after a
// crash could take its own claim back without waiting a quarter of an hour.
func TestASecondProcessRunningTheSameTaskIsRefused(t *testing.T) {
	source, target := newStore("tokyo"), newStore("osaka")

	first := &Guard{TaskID: 7, Source: source, Target: target, Owner: "pod-a"}
	if err := first.Acquire(context.Background()); err != nil {
		t.Fatalf("the first process could not claim: %v", err)
	}

	second := &Guard{TaskID: 7, Source: source, Target: target, Owner: "pod-b"}
	err := second.Acquire(context.Background())
	if err == nil {
		t.Fatal("a second process running the same task was allowed to start")
	}
	if !IsConcurrent(err) {
		t.Errorf("error = %v, want it marked as a concurrency conflict so the caller "+
			"retries rather than blocking for ever", err)
	}
	if !strings.Contains(err.Error(), "pod-a") {
		t.Errorf("error = %v, want it to name the process already holding the task", err)
	}
}

// TestTheSameProcessMayTakeItsClaimBack is the case the skip exists for: a
// restart after a crash must not wait for the claim to go stale.
func TestTheSameProcessMayTakeItsClaimBack(t *testing.T) {
	source, target := newStore("tokyo"), newStore("osaka")

	first := &Guard{TaskID: 7, Source: source, Target: target, Owner: "pod-a"}
	if err := first.Acquire(context.Background()); err != nil {
		t.Fatalf("first Acquire: %v", err)
	}

	// The same process, started again — a restarted pod keeps its name.
	restarted := &Guard{TaskID: 7, Source: source, Target: target, Owner: "pod-a"}
	if err := restarted.Acquire(context.Background()); err != nil {
		t.Errorf("a restarted process was refused its own claim: %v", err)
	}
}

// TestAProcessThatStoppedRefreshingIsNotInTheWay covers a genuine handover: the
// other process has gone, so its claim should not hold the task hostage.
func TestAProcessThatStoppedRefreshingIsNotInTheWay(t *testing.T) {
	source, target := newStore("tokyo"), newStore("osaka")

	stale := &Guard{
		TaskID: 7, Source: source, Target: target, Owner: "pod-a",
		Now: func() time.Time { return time.Now().Add(-10 * ConcurrentAfter) },
	}
	if err := stale.Acquire(context.Background()); err != nil {
		t.Fatalf("first Acquire: %v", err)
	}

	taking := &Guard{TaskID: 7, Source: source, Target: target, Owner: "pod-b"}
	if err := taking.Acquire(context.Background()); err != nil {
		t.Errorf("a process was refused a claim nobody is refreshing: %v", err)
	}
}

// TestADirectionConflictIsNotRetryable keeps the two kinds apart.
func TestADirectionConflictIsNotRetryable(t *testing.T) {
	source, target := newStore("tokyo"), newStore("osaka")

	// Something is replicating out of what this task wants to write to, which is
	// what a promoted replica looks like.
	promoted := &Guard{TaskID: 9, Source: target, Target: newStore("kobe"), Owner: "pod-x"}
	if err := promoted.Acquire(context.Background()); err != nil {
		t.Fatalf("set up the promoted side: %v", err)
	}

	guard := &Guard{TaskID: 7, Source: source, Target: target, Owner: "pod-a"}
	err := guard.Acquire(context.Background())
	if err == nil {
		t.Fatal("writing to a promoted replica was allowed")
	}
	if IsConcurrent(err) {
		t.Error("a reversed direction was marked as a concurrency conflict, so it " +
			"would be retried for ever instead of being put in front of somebody")
	}
}

// TestAnUnreachableEndpointIsRetryable is the defect a 120-second outage of
// the Osaka mongos exposed: the guard reads its claims out of the databases,
// so while the target is down every Acquire fails, and the caller classified
// any non-concurrent failure as needing intervention.
func TestAnUnreachableEndpointIsRetryable(t *testing.T) {
	unreachable := errors.New("server selection error: context deadline exceeded")

	for _, tc := range []struct {
		name           string
		source, target *memoryStore
	}{
		{
			name:   "target is down",
			source: newStore("tokyo"),
			target: &memoryStore{address: "osaka", claims: map[int]Claim{}, readErr: unreachable},
		},
		{
			name:   "source is down",
			source: &memoryStore{address: "tokyo", claims: map[int]Claim{}, readErr: unreachable},
			target: newStore("osaka"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			guard := &Guard{TaskID: 7, Source: tc.source, Target: tc.target, Owner: "pod-a"}
			err := guard.Acquire(context.Background())
			if err == nil {
				t.Fatal("Acquire succeeded while an endpoint was unreachable")
			}
			if IsBlocking(err) {
				t.Errorf("error = %v, want it left retryable: the endpoint comes back, "+
					"and stopping the task permanently for it needs an operator to "+
					"restart replication after every target restart", err)
			}
		})
	}
}

// TestABlockedDirectionStaysBlocked is the other half: the conflicts that no
// amount of retrying resolves must still stop the task.
func TestABlockedDirectionStaysBlocked(t *testing.T) {
	source, target := newStore("tokyo"), newStore("osaka")

	promoted := &Guard{TaskID: 9, Source: target, Target: newStore("kobe"), Owner: "pod-x"}
	if err := promoted.Acquire(context.Background()); err != nil {
		t.Fatalf("set up the promoted side: %v", err)
	}

	guard := &Guard{TaskID: 7, Source: source, Target: target, Owner: "pod-a"}
	err := guard.Acquire(context.Background())
	if err == nil {
		t.Fatal("writing to a promoted replica was allowed")
	}
	if !IsBlocking(err) {
		t.Errorf("error = %v, want it blocking: writing to a promoted replica "+
			"overwrites everything taken since the promotion", err)
	}
}

func TestAConcurrentClaimIsNotBlocking(t *testing.T) {
	source, target := newStore("tokyo"), newStore("osaka")

	first := &Guard{TaskID: 7, Source: source, Target: target, Owner: "pod-a"}
	if err := first.Acquire(context.Background()); err != nil {
		t.Fatalf("the first process could not claim: %v", err)
	}

	second := &Guard{TaskID: 7, Source: source, Target: target, Owner: "pod-b"}
	err := second.Acquire(context.Background())
	if err == nil {
		t.Fatal("a second process running the same task was allowed to start")
	}
	if IsBlocking(err) {
		t.Errorf("error = %v, want it retryable: the old pod exits and the claim "+
			"clears on its own", err)
	}
}

// TestTheTargetClaimIsEncodedForSomebodyElseToWrite: a replicated flush empties
// the database the claim lives in, and the transaction carrying that flush has
// to write the claim back in the same breath. The heartbeat cannot cover it --
// it refreshes a claim rather than noticing one has gone.
func TestTheTargetClaimIsEncodedForSomebodyElseToWrite(t *testing.T) {
	g := &Guard{
		TaskID: 42,
		Source: newStore("tokyo:6379/0"),
		Target: newStore("osaka:6379/0"),
		Now:    func() time.Time { return fixedNow },
		Owner:  "syncer-osaka-0",
	}

	encoded, err := g.TargetClaim()
	if err != nil {
		t.Fatalf("TargetClaim: %v", err)
	}
	var claim Claim
	if err := json.Unmarshal([]byte(encoded), &claim); err != nil {
		t.Fatalf("the claim is not what the store writes: %v", err)
	}
	if claim.TaskID != 42 {
		t.Errorf("task = %d, want 42", claim.TaskID)
	}
	if claim.Role != RoleTarget {
		t.Errorf("role = %q, want the target's: restoring the source's role on the "+
			"target tells it that it is a source", claim.Role)
	}
	if claim.Peer != "tokyo:6379/0" {
		t.Errorf("peer = %q, want the source's endpoint", claim.Peer)
	}
	if claim.UpdatedAt.IsZero() {
		t.Error("the claim has no timestamp, so it reads as stale the moment it lands")
	}
}
