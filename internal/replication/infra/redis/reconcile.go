package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
)

// Comparing the two sides, on a timer.
//
// This is the backstop for everything else in this package being wrong. The
// per-slot position, the offset arithmetic, the protocol handling — each is
// tested, and none of them can prove there is no case nobody thought of. What
// makes that survivable is that a difference is found and fixed by something
// that does not share any of their assumptions: read both sides, compare, repair.
//
// It runs in both directions. A key the target is missing is the obvious
// failure; a key only the target has is the quieter one, and worse after a
// failover — a record nobody can explain, which no amount of comparing row counts
// would have found.

// Reconciler compares one shard's keys against the target.
type Reconciler struct {
	// Node is this shard's master on the source. Scanning the cluster client
	// would walk every shard, and each shard has its own reconciler.
	Node   goredis.UniversalClient
	Source goredis.UniversalClient
	Target goredis.UniversalClient

	Shard string
	// Interval is how often to compare. Zero means the default.
	Interval time.Duration
	// Batch is how many keys to compare at a time. Zero means the default.
	Batch int
	// ReadRate caps keys read from the source per second. Zero means no limit.
	ReadRate int
	// Settle is how long to wait before re-reading a key that looked different.
	// It has to be longer than replication takes, or a busy source reports
	// differences that are only changes in flight. Zero means the default.
	Settle time.Duration
	// Repair writes the source's value over the target's when they differ. With
	// it off, differences are counted and reported but not touched, which is
	// what to do while finding out whether the comparison itself is right.
	Repair bool

	// Now asks for a comparison before the timer would. The source's shape
	// changing is what sends one: a slot moving between shards can leave a key
	// missing from the target with nothing to show it, and waiting an hour to
	// find that out is an hour of a disaster-recovery copy being wrong.
	Now <-chan string

	Logger logrus.FieldLogger
	Labels metrics.Labels
}

const (
	defaultReconcileInterval = time.Hour
	defaultReconcileBatch    = 200
	defaultReconcileSettle   = 2 * time.Second
)

func (r *Reconciler) interval() time.Duration {
	if r.Interval > 0 {
		return r.Interval
	}
	return defaultReconcileInterval
}

func (r *Reconciler) batch() int {
	if r.Batch > 0 {
		return r.Batch
	}
	return defaultReconcileBatch
}

func (r *Reconciler) logger() logrus.FieldLogger { return orDefault(r.Logger) }

// Run compares the two sides until the context is cancelled.
//
// A failed pass is logged and the next one is waited for. A comparison that
// cannot run is a gap in assurance, not a reason to stop replicating — and
// stopping replication because the audit failed would turn a small problem into
// an outage.
func (r *Reconciler) Run(ctx context.Context) {
	// The first pass waits, so that a restarting task is not doing a full
	// comparison while it is also catching up.
	ticker := time.NewTicker(r.interval())
	defer ticker.Stop()

	for {
		var because string
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case reason, ok := <-r.Now:
			if !ok {
				r.Now = nil
				continue
			}
			because = reason
		}
		if because != "" {
			r.logger().Infof("[Redis] Comparing shard %s now, because %s",
				r.Shard, because)
		}

		started := time.Now()
		differences, err := r.pass(ctx)
		switch {
		case ctx.Err() != nil:
			return
		case err != nil:
			r.logger().Warnf("[Redis] The comparison of shard %s did not finish: %v",
				r.Shard, err)
			continue
		}
		metrics.SetReconcileDifference(r.Labels, float64(differences))

		if differences == 0 {
			r.logger().Infof("[Redis] Shard %s compared clean in %s",
				r.Shard, time.Since(started).Round(time.Second))
			continue
		}
		r.logger().Warnf("[Redis] Shard %s: %d key(s) differed from the source "+
			"(%s). Every difference here is something the stream should have carried "+
			"and did not, so it is worth understanding rather than only repairing.",
			r.Shard, differences, time.Since(started).Round(time.Second))
	}
}

// pass compares every key once, in both directions.
func (r *Reconciler) pass(ctx context.Context) (int, error) {
	limit := newRateLimiter(r.ReadRate)
	differences := 0

	// Source to target: keys the target is missing or holds differently.
	err := scanOne(ctx, r.Node, r.batch(), func(keys []string) error {
		if err := limit.wait(ctx, len(keys)); err != nil {
			return err
		}
		found, err := r.compare(ctx, keys)
		differences += found
		return err
	})
	if err != nil {
		return differences, err
	}

	// Target to source: keys only the target has.
	ghosts, err := r.ghosts(ctx, limit)
	return differences + ghosts, err
}

// compare checks a page of keys and repairs what differs.
func (r *Reconciler) compare(ctx context.Context, keys []string) (int, error) {
	wanted := make([][]byte, 0, len(keys))
	for _, key := range keys {
		if IsOffsetKey(key) || isMetaKey(key) {
			continue
		}
		wanted = append(wanted, []byte(key))
	}
	if len(wanted) == 0 {
		return 0, nil
	}

	found, err := r.differing(ctx, wanted)
	if err != nil {
		return 0, err
	}
	if len(found) == 0 {
		return 0, nil
	}
	suspect := make([][]byte, 0, len(found))
	for _, value := range found {
		suspect = append(suspect, value.key)
	}

	// Look again before believing it.
	//
	// Both sides are moving: a key can change on the source between the two reads
	// and differ for no reason other than the replication being in flight. On a
	// live source that happens routinely, and a metric that reports a difference
	// every time anybody writes anything is a metric nobody reads. A difference
	// that is still there a moment later is a difference.
	differing, err := r.confirm(ctx, suspect)
	if err != nil || len(differing) == 0 {
		return 0, err
	}

	for _, value := range differing {
		r.logger().Warnf("[Redis] Shard %s: %q still differs from the source after "+
			"a second look", r.Shard, value.key)
	}
	if !r.Repair {
		return len(differing), nil
	}

	pipe := r.Target.Pipeline()
	for _, value := range differing {
		value.queue(ctx, pipe)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != goredis.Nil {
		return len(differing), fmt.Errorf("repair %d keys: %w", len(differing), err)
	}
	metrics.CountValueRepairs(r.Labels, len(differing))
	return len(differing), nil
}

// confirm re-reads the keys that looked different and reports the ones that
// still are.
func (r *Reconciler) confirm(ctx context.Context, keys [][]byte) ([]*repairedValue, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(r.settle()):
	}

	return r.differing(ctx, keys)
}

// differing reads both sides and reports the keys they disagree about, with the
// source's value ready to write over the target's.
//
// The serialised value is compared rather than the value itself, because it is
// one comparison for every type — a string, a hash with per-field expiries, a
// stream, a module type. Two servers of the same version that were given the
// same changes produce the same serialisation; if they ever do not, this reports
// a difference that repairing does not settle, and the metric says so rather
// than the difference going unseen.
func (r *Reconciler) differing(ctx context.Context, keys [][]byte) ([]*repairedValue, error) {
	fromSource, err := readValues(ctx, r.Source, keys)
	if err != nil {
		return nil, err
	}
	fromTarget, err := readValues(ctx, r.Target, keys)
	if err != nil {
		return nil, err
	}

	var found []*repairedValue
	for i := range fromSource {
		if i >= len(fromTarget) {
			break
		}
		if string(fromSource[i].payload) != string(fromTarget[i].payload) {
			found = append(found, fromSource[i])
		}
	}
	return found, nil
}

// settle is how long to wait before looking again, which has to be longer than
// the replication takes.
func (r *Reconciler) settle() time.Duration {
	if r.Settle > 0 {
		return r.Settle
	}
	return defaultReconcileSettle
}

// ghosts finds keys the target has and the source does not.
//
// It only considers the slots this shard owns, so that one shard's reconciler
// cannot decide another shard's keys are ghosts while that shard is still
// catching up.
func (r *Reconciler) ghosts(ctx context.Context, limit *rateLimiter) (int, error) {
	owned, err := r.ownedSlots(ctx)
	if err != nil {
		return 0, err
	}

	found := 0
	err = scanAll(ctx, r.Target, r.batch(), func(keys []string) error {
		if err := limit.wait(ctx, len(keys)); err != nil {
			return err
		}
		var mine []string
		for _, key := range keys {
			if IsOffsetKey(key) || isMetaKey(key) {
				continue
			}
			if owned != nil && !owned[SlotOf([]byte(key))] {
				continue
			}
			mine = append(mine, key)
		}
		if len(mine) == 0 {
			return nil
		}

		// Ask the source which of them it still has.
		suspect, err := absentFrom(ctx, r.Source, mine)
		if err != nil {
			return err
		}
		if len(suspect) == 0 {
			return nil
		}

		// Same second look. A key deleted on the source a moment ago is still on
		// the target until the delete arrives, and that is replication working
		// rather than a ghost.
		gone, err := r.confirmGone(ctx, suspect)
		if err != nil || len(gone) == 0 {
			return err
		}
		found += len(gone)

		for _, key := range gone {
			r.logger().Warnf("[Redis] Shard %s: %q is on the target and still not on "+
				"the source. After a failover that is a record nobody can account for.",
				r.Shard, key)
		}
		if !r.Repair {
			return nil
		}
		removal := r.Target.Pipeline()
		for _, key := range gone {
			removal.Del(ctx, key)
		}
		if _, err := removal.Exec(ctx); err != nil && err != goredis.Nil {
			return fmt.Errorf("remove %d keys the source no longer has: %w", len(gone), err)
		}
		return nil
	})
	return found, err
}

// confirmGone re-checks keys the source appeared not to have.
func (r *Reconciler) confirmGone(ctx context.Context, keys []string) ([]string, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(r.settle()):
	}

	return absentFrom(ctx, r.Source, keys)
}

// absentFrom reports which of the keys a server does not have.
func absentFrom(ctx context.Context, client goredis.UniversalClient, keys []string) ([]string, error) {
	pipe := client.Pipeline()
	exists := make([]*goredis.IntCmd, len(keys))
	for i, key := range keys {
		exists[i] = pipe.Exists(ctx, key)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != goredis.Nil {
		return nil, fmt.Errorf("check %d keys against the source: %w", len(keys), err)
	}

	var absent []string
	for i, check := range exists {
		if count, err := check.Result(); err == nil && count == 0 {
			absent = append(absent, keys[i])
		}
	}
	return absent, nil
}

// ownedSlots is the set of slots this shard's master serves, or nil when the
// source is a single server and owns everything.
func (r *Reconciler) ownedSlots(ctx context.Context) (map[int]bool, error) {
	cluster, ok := r.Source.(*goredis.ClusterClient)
	if !ok {
		return nil, nil
	}
	slots, err := cluster.ClusterSlots(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("ask which slots shard %s owns: %w", r.Shard, err)
	}
	owned := make(map[int]bool)
	for _, slot := range slots {
		if len(slot.Nodes) == 0 {
			continue
		}
		if fmt.Sprintf("%d-%d", slot.Start, slot.End) != r.Shard {
			continue
		}
		for at := int(slot.Start); at <= int(slot.End); at++ {
			owned[at] = true
		}
	}
	if len(owned) == 0 {
		return nil, fmt.Errorf("shard %s owns no slots, so it cannot tell a ghost "+
			"from another shard's key", r.Shard)
	}
	return owned, nil
}
