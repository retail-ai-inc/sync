package redis

import (
	"context"
	"fmt"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// The first copy.
//
// The point is pinned before a key is read and recorded only once every key has
// been read. Pinning afterwards would lose everything written while the copy
// ran; recording it before the copy finished would let an interrupted copy resume
// from a point it never reached.
//
// The copy itself is SCAN and DUMP rather than the data set the source offers
// over the replication connection. That is a deliberate trade: the wire format
// changes with almost every release — new encodings for hashes, lists, streams
// and the module types Redis 8 brought into the core — and a parser that has to
// keep up is one that misreads silently the day it falls behind. SCAN and DUMP
// speak only stable commands.
//
// What it costs is that the copy is a smear rather than a point in time: a key
// read early may have changed before a key read late. The reader resolves that by
// applying changes by value until the stream has passed the end of the copy.

// Snapshotter takes one shard's first copy.
type Snapshotter struct {
	Link *link
	// Node is this shard's master. The copy reads from it rather than from the
	// cluster, because every shard runs its own copy: scanning the cluster from
	// each of them would read the whole key space once per shard, and on a
	// twelve-shard cluster that is twelve times the work and twelve times the
	// load on a live payment database.
	Node   goredis.UniversalClient
	Source goredis.UniversalClient
	Target goredis.UniversalClient

	Logger logrus.FieldLogger
	Labels metrics.Labels

	// Batch is how many keys are read at a time. Zero means the default.
	Batch int
	// ReadRate caps keys read from the source per second, so a first copy
	// cannot become a load test against a live payment database. Zero means no
	// limit.
	ReadRate int
}

const defaultCopyBatch = 200

func (s *Snapshotter) batch() int {
	if s.Batch > 0 {
		return s.Batch
	}
	return defaultCopyBatch
}

func (s *Snapshotter) logger() logrus.FieldLogger { return orDefault(s.Logger) }

// Pin opens the replication connection, which is what fixes the point the copy
// is taken against.
//
// The connection starts filling the buffer immediately, so the source only has to
// hold history for the length of the handshake rather than the length of the
// copy. A copy that takes an hour would otherwise need an hour of backlog.
func (s *Snapshotter) Pin(ctx context.Context) (domain.Position, error) {
	at, err := s.Link.start(ctx, streamPosition{})
	if err != nil {
		return domain.Position{}, err
	}
	payload, err := at.encode()
	if err != nil {
		return domain.Position{}, err
	}
	return domain.Position{Payload: payload}, nil
}

// Copy reads every key from the source and writes it to the target.
func (s *Snapshotter) Copy(ctx context.Context) error {
	var (
		copied int
		limit  = newRateLimiter(s.ReadRate)
		start  = time.Now()
	)

	from := s.Node
	if from == nil {
		from = s.Source
	}
	err := scanOne(ctx, from, s.batch(), func(keys []string) error {
		if err := limit.wait(ctx, len(keys)); err != nil {
			return err
		}
		wanted := make([][]byte, 0, len(keys))
		for _, key := range keys {
			if IsOffsetKey(key) || isMetaKey(key) {
				// This task's own bookkeeping, if the source has ever been a
				// target. Copying it would overwrite the progress of the task
				// writing here.
				continue
			}
			wanted = append(wanted, []byte(key))
		}
		if len(wanted) == 0 {
			return nil
		}

		values, err := readValues(ctx, s.Source, wanted)
		if err != nil {
			return err
		}
		pipe := s.Target.Pipeline()
		for _, value := range values {
			value.queue(ctx, pipe)
		}
		if _, err := pipe.Exec(ctx); err != nil && err != goredis.Nil {
			return fmt.Errorf("write %d copied keys: %w", len(values), err)
		}
		copied += len(values)
		return nil
	})
	if err != nil {
		// An incomplete copy must not leave a position behind: the runner only
		// records one once this returns without error, so returning the failure is
		// what keeps the target from being treated as caught up.
		return fmt.Errorf("copy the source after %d keys: %w", copied, err)
	}

	s.logger().Infof("[Redis] Copied %d keys for shard %s in %s",
		copied, s.Link.shard, time.Since(start).Round(time.Millisecond))
	return nil
}

// scanAll walks every key of a source, whether it is one server or a cluster.
//
// The callback is called one page at a time, never twice at once. That matters
// because ForEachMaster runs its callback against every master in parallel, and
// every caller here accumulates something across pages — a count, a list of
// keys. Leaving the callers to discover that would mean each of them racing on
// its own accumulator, and the symptom is not a crash but a scan that quietly
// returns fewer keys than the server holds, which reads as data missing from the
// source.
func scanAll(ctx context.Context, client goredis.UniversalClient, batch int,
	page func(keys []string) error) error {

	cluster, ok := client.(*goredis.ClusterClient)
	if !ok {
		return scanOne(ctx, client, batch, page)
	}

	// A cluster has no cursor that spans nodes: each master holds its own slots,
	// so each is walked separately.
	var mu sync.Mutex
	return cluster.ForEachMaster(ctx, func(ctx context.Context, node *goredis.Client) error {
		return scanOne(ctx, node, batch, func(keys []string) error {
			mu.Lock()
			defer mu.Unlock()
			return page(keys)
		})
	})
}

func scanOne(ctx context.Context, client goredis.UniversalClient, batch int,
	page func(keys []string) error) error {

	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, "*", int64(batch)).Result()
		if err != nil {
			return fmt.Errorf("scan the source: %w", err)
		}
		if len(keys) > 0 {
			if err := page(keys); err != nil {
				return err
			}
		}
		if next == 0 {
			return nil
		}
		cursor = next
	}
}

// isMetaKey reports whether a key is one of the position metadata keys.
func isMetaKey(key string) bool {
	const prefix = "__sync:pos:"
	return len(key) >= len(prefix) && key[:len(prefix)] == prefix
}

// rateLimiter spreads reads over time, so a first copy cannot become a load test
// against a live payment database.
type rateLimiter struct {
	perSecond int
	allowance float64
	last      time.Time
}

func newRateLimiter(perSecond int) *rateLimiter {
	return &rateLimiter{perSecond: perSecond, last: time.Now()}
}

func (r *rateLimiter) wait(ctx context.Context, n int) error {
	if r.perSecond <= 0 {
		return nil
	}
	now := time.Now()
	r.allowance += now.Sub(r.last).Seconds() * float64(r.perSecond)
	r.last = now
	if r.allowance > float64(r.perSecond) {
		r.allowance = float64(r.perSecond)
	}
	if r.allowance >= float64(n) {
		r.allowance -= float64(n)
		return nil
	}

	short := float64(n) - r.allowance
	pause := time.Duration(short / float64(r.perSecond) * float64(time.Second))
	timer := time.NewTimer(pause)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		r.allowance = 0
		r.last = time.Now()
		return nil
	}
}
