package redis

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// Watching the source's shape. This exists for one failure that nothing else
// in the design catches.

type topologyWatcher struct {
	Source goredis.UniversalClient
	// Every is how often to look. Zero means the default.
	Every time.Duration
	// OnChange is called with a description of what moved.
	OnChange func(what string)
	// Baseline is the shape the shards were built from. A reshard is judged
	// against it, not the last poll: a range it lacks has no reader however long
	// ago it appeared. Nil means the first shape Run reads.
	Baseline map[string]string

	Logger logrus.FieldLogger

	// shape reads the source's slot ranges and their masters. Nil means
	// ownership of Source.
	shape func(ctx context.Context) (map[string]string, error)
}

const defaultTopologyInterval = 30 * time.Second

// How many consecutive polls must agree before a changed shape is called a
// reshard. One poll is not evidence: a master down with no replica vanishes
// from CLUSTER SLOTS and comes back when it or its replacement does.
const reshardPollsToConfirm = 3

func (w *topologyWatcher) every() time.Duration {
	if w.Every > 0 {
		return w.Every
	}
	return defaultTopologyInterval
}

func (w *topologyWatcher) logger() logrus.FieldLogger { return orDefault(w.Logger) }

// Run watches the source's shape until the context ends, or until the slots
// themselves are rearranged -- which it reports as a retryable error, because
// the task cannot carry on through one and a restart is what covers the ranges
// that appeared.
func (w *topologyWatcher) Run(ctx context.Context) error {
	shape := w.shape
	if shape == nil {
		cluster, ok := w.Source.(*goredis.ClusterClient)
		if !ok {
			// One server owns everything; there is nothing for a slot to move to.
			<-ctx.Done()
			return nil
		}
		shape = func(ctx context.Context) (map[string]string, error) {
			return ownership(ctx, cluster)
		}
	}

	ticker := time.NewTicker(w.every())
	defer ticker.Stop()

	baseline := w.Baseline
	if baseline == nil {
		var err error
		if baseline, err = shape(ctx); err != nil {
			w.logger().Warnf("[Redis] Could not read the source's shape: %v", err)
		}
	}
	previous := baseline

	// pending is the changed shape awaiting confirmation, and settled counts how
	// many consecutive polls have agreed with it.
	var pending map[string]string
	settled := 0

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}

		current, err := shape(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			w.logger().Warnf("[Redis] Could not read the source's shape: %v", err)
			continue
		}
		if baseline == nil {
			baseline, previous = current, current
			continue
		}

		// A shard is named by the slots it owns, so a master that failed over to
		// another address is the same shard and its stream reconnects on its own.
		// Slots moving between shards is a different thing: the shard list this
		// task started with no longer covers the key space, and the ranges that
		// appeared have no reader at all. Nothing here can add one -- the readers
		// were built at start -- so carrying on would replicate part of the
		// cluster and say nothing about the rest.
		added, removed := rangesMoved(baseline, current)
		if added == "" && removed == "" {
			settled = 0
		} else {
			// A master that is down with no replica to take over drops out of
			// CLUSTER SLOTS, and so does one that is briefly unreachable while the
			// command runs. Either reads exactly like a reshard for one poll. The
			// same changed shape has to hold across several before this stops a
			// task for good, because stopping is not something a retry undoes.
			if same := describe(pending, current) == "" && pending != nil; same {
				settled++
			} else {
				settled, pending = 1, current
			}
		}
		if settled >= reshardPollsToConfirm {
			// The error carries the reason; see reshardError for why it is
			// retryable. Logging it here as well told the operator twice.
			return reshardError(added, removed)
		}

		if changes := describe(previous, current); changes != "" {
			w.logger().Warnf("[Redis] The source's shape changed: %s.\n"+
				"A slot moving between shards deletes its keys from one master and "+
				"restores them on another, and those two arrive down different "+
				"connections with no ordering between them — so a delete can land "+
				"after a restore and leave the key missing from the target with "+
				"nothing to show it. A comparison has been started to repair that.\n"+
				"Note also that a shard is identified by the slots it owns, so a "+
				"reshard gives the new shards names this task has no position for: "+
				"they will take a first copy. That is safe but not cheap, and it is "+
				"worth doing a reshard of a replicated cluster deliberately.", changes)
			if w.OnChange != nil {
				w.OnChange(changes)
			}
		}
		previous = current
	}
}

// reshardError is why a task stops when the slots are rearranged.
//
// Deliberately not Unrecoverable. Both stop the shards; the difference is what
// happens next. Unrecoverable leaves the task blocked until somebody notices,
// and the ranges that appeared stay unreplicated for as long as that takes --
// on a disaster-recovery link that is the worse outcome of the two. A restart
// rediscovers the shape and covers them again. It is not cheap: the new shards
// have names this task has no position for, so each takes a first copy. The
// supervisor's backoff is what stops a flapping cluster from doing that
// repeatedly.
func reshardError(added, removed string) error {
	return fmt.Errorf(
		"the source was resharded while this task was running: %s%s. A shard is "+
			"identified by the slots it owns, so the ranges that appeared have no "+
			"reader and nothing is being replicated from them until the task has "+
			"restarted, which it does by itself", added, removed)
}

// rangesMoved reports slot ranges that appeared or vanished. An address
// changing under an unchanged range is a failover, not a reshard.
func rangesMoved(before, after map[string]string) (added, removed string) {
	var appeared, vanished []string
	for id := range after {
		if _, had := before[id]; !had {
			appeared = append(appeared, id)
		}
	}
	for id := range before {
		if _, still := after[id]; !still {
			vanished = append(vanished, id)
		}
	}
	sort.Strings(appeared)
	sort.Strings(vanished)
	if len(appeared) > 0 {
		added = "slots " + strings.Join(appeared, ", ") + " appeared"
	}
	if len(vanished) > 0 {
		if added != "" {
			removed = "; "
		}
		removed += "slots " + strings.Join(vanished, ", ") + " are gone"
	}
	return added, removed
}

func ownership(ctx context.Context, cluster *goredis.ClusterClient) (map[string]string, error) {
	slots, err := cluster.ClusterSlots(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("ask the source which shards it has: %w", err)
	}
	if len(slots) == 0 {
		return nil, fmt.Errorf("the source reported no slots")
	}
	shape := make(map[string]string, len(slots))
	for _, slot := range slots {
		if len(slot.Nodes) == 0 {
			continue
		}
		shape[fmt.Sprintf("%d-%d", slot.Start, slot.End)] = slot.Nodes[0].Addr
	}
	return shape, nil
}

// shapeOf must key each shard as ownership does, or an unchanged cluster reads
// as a reshard on every poll.
func shapeOf(shards []shard) map[string]string {
	shape := make(map[string]string, len(shards))
	for _, sh := range shards {
		shape[sh.id] = sh.addr
	}
	return shape
}

func describe(before, after map[string]string) string {
	var changes []string

	for span, was := range before {
		now, still := after[span]
		switch {
		case !still:
			changes = append(changes, fmt.Sprintf("slots %s are no longer a shard "+
				"of their own (were on %s)", span, was))
		case now != was:
			// A shard whose master moved. The slots did not move, so the keys did
			// not either — this is a failover, and the position still applies.
			changes = append(changes, fmt.Sprintf("slots %s moved from %s to %s",
				span, was, now))
		}
	}
	for span, now := range after {
		if _, existed := before[span]; !existed {
			changes = append(changes, fmt.Sprintf("slots %s are a new shard, on %s",
				span, now))
		}
	}

	sort.Strings(changes)
	return strings.Join(changes, "; ")
}
