package redis

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// Removing what the target holds and the source does not.
//
// A copy writes what the source has. Nothing in it mentions a key the source
// used to have and deleted, which is exactly what accumulates while a task is
// stopped -- so a copy made to recover a position leaves those keys behind for
// good, and a count comparison of the two sides would not even show it as a
// difference in the direction that matters.
//
// This walks the target rather than the source, which is the only way to see
// them, and asks the source about each one. It is the reverse of the
// reconciler, which walks the source and repairs values.

// sweepBatch is how many keys are asked about per round trip. The same size as
// the reconciler's, for the same reason: it is one EXISTS pipeline against the
// source and one delete pipeline against the target.
const sweepBatch = 200

// SweepStale removes the target's keys that the source no longer has.
//
// It leaves this task's own bookkeeping alone: the position markers are on the
// target by design and the source has never heard of them.
func (s *Snapshotter) SweepStale(ctx context.Context) error {
	if err := s.sourceHoldsSomething(ctx); err != nil {
		return err
	}
	if start, end, ranged := slotRange(s.Link.shard); ranged {
		return s.sweepSlots(ctx, start, end)
	}
	return s.sweepDatabases(ctx)
}

// sourceHoldsSomething refuses to sweep against a source that reports nothing
// at all.
//
// An empty source and a connection pointing somewhere else look exactly the
// same from here, and the difference is the whole of the standby: one means
// "remove everything", the other means "remove everything for no reason". The
// second is not recoverable, so a source with nothing in it stops the task and
// says what it found instead. A source with keys in it has proved the
// connection, and a part of it that is empty is then a real state to
// replicate.
func (s *Snapshotter) sourceHoldsSomething(ctx context.Context) error {
	keys, err := s.sourceKeys(ctx)
	if err != nil {
		return fmt.Errorf("ask the source how many keys it holds before removing "+
			"anything from the target: %w", err)
	}
	if keys > 0 {
		return nil
	}

	held, err := s.Target.DBSize(ctx).Result()
	if err != nil {
		held = -1
	}
	return domain.Unrecoverable(
		"the source holds no keys for shard %s while the target holds %d, so the "+
			"target is not being swept. A source that reports nothing is as likely to "+
			"be a connection pointing somewhere else as a source that was emptied, "+
			"and emptying the standby on that basis cannot be undone. Check what the "+
			"source is pointed at; if it really was emptied, remove the target's copy "+
			"deliberately",
		s.Link.shard, held)
}

// sourceKeys counts what the whole source holds, not what this shard or this
// database holds.
//
// The question being asked is whether the connection reaches the data, and the
// whole source is what answers it. A shard or a database that is empty while
// the rest of the source is not has been emptied, which is a state to
// replicate; a source that is empty everywhere is the case that is
// indistinguishable from a connection pointing elsewhere.
func (s *Snapshotter) sourceKeys(ctx context.Context) (int64, error) {
	if cluster, ok := s.Source.(*goredis.ClusterClient); ok {
		var total int64
		err := cluster.ForEachMaster(ctx, func(ctx context.Context, node *goredis.Client) error {
			keys, err := node.DBSize(ctx).Result()
			if err != nil {
				return err
			}
			total += keys
			return nil
		})
		return total, err
	}

	source := s.Node
	if source == nil {
		source = s.Source
	}
	keys, err := source.DBSize(ctx).Result()
	if err != nil || keys > 0 {
		return keys, err
	}

	// A standalone source keeps up to sixteen databases and the connection is
	// on one of them, so an empty one says nothing about the rest.
	databases, err := populatedDatabases(ctx, source)
	if err != nil {
		return 0, err
	}
	return int64(len(databases)), nil
}

// sweepSlots handles a cluster, where one shard owns a range of slots and the
// target's own shape may divide them differently.
func (s *Snapshotter) sweepSlots(ctx context.Context, start, end int) error {
	source := s.Node
	if source == nil {
		source = s.Source
	}

	removed := 0
	walk := func(ctx context.Context, node *goredis.Client) error {
		n, err := s.sweepNode(ctx, node, s.Target, source, func(key string) bool {
			slot := SlotOf([]byte(key))
			return slot >= start && slot <= end
		})
		removed += n
		return err
	}

	if cluster, ok := s.Target.(*goredis.ClusterClient); ok {
		if err := cluster.ForEachMaster(ctx, walk); err != nil {
			return err
		}
	} else {
		// A cluster source replicated into one server: everything it holds for
		// this shard is on that server.
		n, err := s.sweepNode(ctx, s.Target, s.Target, source, func(key string) bool {
			slot := SlotOf([]byte(key))
			return slot >= start && slot <= end
		})
		if err != nil {
			return err
		}
		removed = n
	}

	s.logger().Infof("[Redis] Shard %s: removed %d keys the source no longer has",
		s.Link.shard, removed)
	return nil
}

// sweepDatabases handles a standalone server, where the copy walked every
// database that held keys and so does this.
func (s *Snapshotter) sweepDatabases(ctx context.Context) error {
	if s.SourceConn == "" || s.TargetConn == "" {
		// Nothing says which databases to open, so the connection this was
		// given is the whole of it.
		source := s.Node
		if source == nil {
			source = s.Source
		}
		removed, err := s.sweepNode(ctx, s.Target, s.Target, source, nil)
		if err != nil {
			return err
		}
		s.logger().Infof("[Redis] Removed %d keys the source no longer has", removed)
		return nil
	}

	// The target's databases, not the source's: a database the source has
	// emptied since is one the copy would never open, and it is where the keys
	// this is looking for would be.
	target, err := clientOnDB(s.TargetConn, 0)
	if err != nil {
		return fmt.Errorf("open the target to look for what the source no longer has: %w", err)
	}
	databases, err := populatedDatabases(ctx, target)
	target.Close()
	if err != nil {
		return err
	}

	for _, db := range databases {
		sourceDB, err := clientOnDB(s.SourceConn, db)
		if err != nil {
			return fmt.Errorf("open the source on database %d: %w", db, err)
		}
		targetDB, err := clientOnDB(s.TargetConn, db)
		if err != nil {
			sourceDB.Close()
			return fmt.Errorf("open the target on database %d: %w", db, err)
		}
		removed, err := s.sweepNode(ctx, targetDB, targetDB, sourceDB, nil)
		sourceDB.Close()
		targetDB.Close()
		if err != nil {
			return err
		}
		s.logger().Infof("[Redis] Database %d: removed %d keys the source no longer has",
			db, removed)
	}
	return nil
}

// sweepNode scans one target node and deletes the keys the source does not
// have. scan is what is walked, remove is what the delete is addressed
// through -- the two differ for a cluster, where a slot that has moved since
// the scan still has to be reached by key.
func (s *Snapshotter) sweepNode(ctx context.Context, scan, remove,
	source goredis.UniversalClient, mine func(string) bool) (int, error) {

	removed := 0
	var cursor uint64
	for {
		keys, next, err := scan.Scan(ctx, cursor, "*", int64(s.batch())).Result()
		if err != nil {
			return removed, fmt.Errorf("read the target's keys: %w", err)
		}

		candidates := make([]string, 0, len(keys))
		for _, key := range keys {
			if IsOffsetKey(key) || isPositionKey(key) {
				continue
			}
			if mine != nil && !mine(key) {
				continue
			}
			candidates = append(candidates, key)
		}

		for len(candidates) > 0 {
			size := min(sweepBatch, len(candidates))
			n, err := s.removeMissing(ctx, remove, source, candidates[:size])
			if err != nil {
				return removed, err
			}
			removed += n
			candidates = candidates[size:]
		}

		if next == 0 {
			return removed, nil
		}
		cursor = next
	}
}

// removeMissing asks the source about a batch of keys and deletes the ones it
// does not have.
func (s *Snapshotter) removeMissing(ctx context.Context, remove,
	source goredis.UniversalClient, keys []string) (int, error) {

	asking := source.Pipeline()
	answers := make([]*goredis.IntCmd, len(keys))
	for i, key := range keys {
		answers[i] = asking.Exists(ctx, key)
	}
	if _, err := asking.Exec(ctx); err != nil && err != goredis.Nil {
		return 0, fmt.Errorf("ask the source which keys it still has: %w", err)
	}

	deleting := remove.Pipeline()
	doomed := 0
	for i, answer := range answers {
		held, err := answer.Result()
		if err != nil && err != goredis.Nil {
			return 0, fmt.Errorf("ask the source about %s: %w", keys[i], err)
		}
		if held == 0 {
			deleting.Del(ctx, keys[i])
			doomed++
		}
	}
	if doomed == 0 {
		return 0, nil
	}
	if _, err := deleting.Exec(ctx); err != nil && err != goredis.Nil {
		return 0, fmt.Errorf("remove keys the source no longer has: %w", err)
	}
	return doomed, nil
}

// isPositionKey reports whether a key is the one this package writes the
// stream's position into, which the source has never heard of.
func isPositionKey(key string) bool {
	return len(key) > len(positionKeyPrefix) && key[:len(positionKeyPrefix)] == positionKeyPrefix
}

const positionKeyPrefix = "__sync:pos:"
