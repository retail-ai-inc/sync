//go:build integration

package redis

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

const repairTaskID = 8851

// A failure means a change made during the first copy lands in the wrong database or loses its expiry.
func TestARepairIsReadFromItsOwnDatabaseWithItsLife(t *testing.T) {
	sourceAddr := addrsFrom(t, "SYNC_REDIS_SOURCE")[0]
	targetAddr := addrsFrom(t, "SYNC_REDIS_TARGET")[0]
	source, target := oneConnection(t, sourceAddr), oneConnection(t, targetAddr)
	emptyBoth(t, source, target)
	ctx := context.Background()

	live, third, gone := "{repair}live", "{repair}third", "{repair}gone"
	sourceThird, targetThird := onDB(t, sourceAddr, 3), onDB(t, targetAddr, 3)
	for _, seed := range []*goredis.StatusCmd{
		source.Set(ctx, live, "live-value", time.Minute),
		sourceThird.Set(ctx, third, "third-value", 0),
		targetThird.Set(ctx, third, "stale-value", 0),
		target.Set(ctx, gone, "deleted-on-the-source", 0),
	} {
		if err := seed.Err(); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	applier := standaloneApplier(t, source, target, repairTaskID, 90)
	batch := []*domain.Event{
		streamedRepair(0, 100, live),
		streamedRepair(3, 110, third),
		streamedRepair(0, 120, gone),
	}
	if _, err := applier.Apply(ctx, [][]*domain.Event{batch}, storedPosition(120)); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if got, err := target.Get(ctx, live).Result(); err != nil || got != "live-value" {
		t.Errorf("%s reads %q (%v) on the target, want the source's value", live, got, err)
	}
	if life, err := target.PTTL(ctx, live).Result(); err != nil || life <= 0 || life > time.Minute {
		t.Errorf("%s has %v (%v) left on the target, want the minute it has on the source",
			live, life, err)
	}
	if got, err := targetThird.Get(ctx, third).Result(); err != nil || got != "third-value" {
		t.Errorf("%s reads %q (%v) in database 3 of the target, want the source's value",
			third, got, err)
	}
	if n, err := target.Exists(ctx, third, gone).Result(); err != nil || n != 0 {
		t.Errorf("database 0 of the target holds %d of %s and %s (%v), want neither",
			n, third, gone, err)
	}

	marker := OffsetKey(SlotOf([]byte(live)), repairTaskID)
	if got, err := target.Get(ctx, marker).Result(); err != nil || got != markerValue("h1", 120) {
		t.Errorf("the slot marker reads %q (%v) in the bookkeeping database, want %q",
			got, err, markerValue("h1", 120))
	}
	for _, key := range []string{marker, metaKey(repairTaskID, "0")} {
		if n, err := targetThird.Exists(ctx, key).Result(); err != nil || n != 0 {
			t.Errorf("%s was written into database 3 of the target", key)
		}
	}
	if n, err := target.Exists(ctx, metaKey(repairTaskID, "0")).Result(); err != nil || n != 1 {
		t.Errorf("the position is not in the bookkeeping database (%v)", err)
	}
	if got, err := source.Get(ctx, live).Result(); err != nil || got != "live-value" {
		t.Errorf("the source connection no longer reads database 0 after the repair: %q (%v)",
			got, err)
	}
}
