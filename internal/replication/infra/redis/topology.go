package redis

import (
	"context"
	"fmt"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
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

	Logger logrus.FieldLogger
}

const defaultTopologyInterval = 30 * time.Second

func (w *topologyWatcher) every() time.Duration {
	if w.Every > 0 {
		return w.Every
	}
	return defaultTopologyInterval
}

func (w *topologyWatcher) logger() logrus.FieldLogger { return orDefault(w.Logger) }

// Run watches the source's shape until the context ends, or until the slots
// themselves are rearranged -- which it reports as an error, because the task
// cannot carry on through one.
func (w *topologyWatcher) Run(ctx context.Context) error {
	cluster, ok := w.Source.(*goredis.ClusterClient)
	if !ok {
		// One server owns everything; there is nothing for a slot to move to.
		<-ctx.Done()
		return nil
	}

	ticker := time.NewTicker(w.every())
	defer ticker.Stop()

	previous, err := ownership(ctx, cluster)
	if err != nil {
		w.logger().Warnf("[Redis] Could not read the source's shape: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}

		current, err := ownership(ctx, cluster)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			w.logger().Warnf("[Redis] Could not read the source's shape: %v", err)
			continue
		}
		if previous == nil {
			previous = current
			continue
		}

		// A shard is named by the slots it owns, so a master that failed over to
		// another address is the same shard and its stream reconnects on its own.
		// Slots moving between shards is a different thing: the shard list this
		// task started with no longer covers the key space, and the ranges that
		// appeared have no reader at all. Nothing here can add one -- the readers
		// were built at start -- so carrying on would replicate part of the
		// cluster and say nothing about the rest.
		if added, removed := rangesMoved(previous, current); added != "" || removed != "" {
			return domain.Unrecoverable(
				"the source was resharded while this task was running: %s%s. A shard is "+
					"identified by the slots it owns, so the ranges that appeared have no "+
					"reader and nothing is being replicated from them. Restart the task: "+
					"it will pick up the new shape, and the new ranges will take a first "+
					"copy each", added, removed)
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
