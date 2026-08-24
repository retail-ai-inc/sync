//go:build integration

package redis

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// The test that decides whether the design works.
//
// Everything else in this package is in service of one claim: that a command
// stream can be replayed into a Redis cluster exactly once, without the atomic
// unit that MySQL and MongoDB have. If that claim is false the replay is silent
// and the damage is a counter that is quietly wrong or a payment queued twice, so
// it is not enough to test the happy path — the crash has to be provoked, over
// and over, while non-idempotent commands are in flight.
//
// This runs against a single server owning every slot. The same test against
// three masters is in cluster_integration_test.go, where the constraint the whole
// design is shaped around actually applies.

const crashTaskID = 1

// TestCommandsAreAppliedExactlyOnceAcrossCrashes kills the pipeline repeatedly
// while non-idempotent commands are in flight, and insists the two sides end up
// identical.
//
// A crash here is the context being cancelled part way through applying a batch,
// followed by every piece of in-memory state being thrown away and rebuilt from
// what survived: the markers in the target and the segments on disk. That is what
// a killed process leaves behind.
func TestCommandsAreAppliedExactlyOnceAcrossCrashes(t *testing.T) {
	sourceAddrs := addrsFrom(t, "SYNC_REDIS_SOURCE")
	source := redisAt(t, sourceAddrs)
	target := redisAt(t, addrsFrom(t, "SYNC_REDIS_TARGET"))
	emptyBoth(t, source, target)
	widenBacklogs(t, source)

	ctx := context.Background()
	commands, err := loadCommandTable(ctx, target)
	if err != nil {
		t.Fatalf("loadCommandTable: %v", err)
	}

	root := t.TempDir()
	for i := 0; i < 50; i++ {
		if err := source.Set(ctx, fmt.Sprintf("seed:%d", i), i, 0).Err(); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	// Reach the command phase before disturbing anything: a crash inside the
	// first copy's window proves nothing, because changes there are applied by
	// re-reading values and that is idempotent either way.
	warmCtx, stopWarm := context.WithCancel(ctx)
	warm := newRig(t, source, target, root, commands, crashTaskID, sourceAddrs[0])
	warmDone := warm.run(warmCtx)
	warm.reachCommandPhase(t, source, target, crashTaskID, 30*time.Second)
	stopWarm()
	warm.wait(t, warmDone, 15*time.Second)
	warm.stop()

	stopWorkload := workload(ctx, source, 40)

	const crashes = 20
	random := rand.New(rand.NewSource(7))
	skipped := 0
	for attempt := 0; attempt < crashes; attempt++ {
		runCtx, kill := context.WithCancel(ctx)
		current := newRig(t, source, target, root, commands, crashTaskID, sourceAddrs[0])
		done := current.run(runCtx)

		// Let it work for a while, then pull the plug mid-flight.
		time.Sleep(time.Duration(80+random.Intn(220)) * time.Millisecond)
		kill()

		for _, err := range current.wait(t, done, 15*time.Second) {
			if domain.IsUnrecoverable(err) {
				t.Fatalf("attempt %d stopped for good: %v", attempt, err)
			}
		}
		skipped += current.skipped()
		current.stop()
	}

	written := stopWorkload()
	t.Logf("%d commands written across %d crashes", written, crashes)

	// Now let it catch up undisturbed.
	runCtx, stopRun := context.WithCancel(ctx)
	final := newRig(t, source, target, root, commands, crashTaskID, sourceAddrs[0])
	done := final.run(runCtx)

	var stopped error
	same, difference := converge(t, source, target, 60*time.Second, func() bool {
		select {
		case stopped = <-done:
			return true
		default:
			return false
		}
	})
	stopRun()
	skipped += final.skipped()
	final.stop()
	if stopped != nil {
		t.Logf("the catch-up run returned: %v", stopped)
	}

	if !same {
		t.Fatalf("the two sides differ after %d crashes:\n%s", crashes, difference)
	}

	// A check that the test tested anything, which it silently failed to before
	// this was here. The value phase applies changes by re-reading the key, which
	// is idempotent however often it happens — so a run that never leaves it
	// would pass with the skip removed entirely, and did.
	phase := recordedPhase(t, target, crashTaskID, "0")
	if phase != phaseCommand {
		t.Errorf("the position is still in the %q phase, so the run never replayed "+
			"a command: everything was applied by re-reading values, which is "+
			"idempotent whatever the position logic does", phase)
	}
	t.Logf("phase %s, %d commands skipped as already applied", phase, skipped)
}

// recordedPhase reads which phase a shard's stored position is in.
func recordedPhase(t *testing.T, target goredis.UniversalClient, taskID int, shard string) string {
	t.Helper()
	payload, err := target.Get(context.Background(), metaKey(taskID, shard)).Result()
	if err != nil {
		t.Fatalf("read the stored position: %v", err)
	}
	position, err := decodePosition(payload)
	if err != nil {
		t.Fatalf("decode the stored position: %v", err)
	}
	return position.Phase
}
