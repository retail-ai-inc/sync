package redis

import (
	"context"
	"fmt"
	intRedis "github.com/retail-ai-inc/sync/internal/platform/dbconn/redis"
	"github.com/retail-ai-inc/sync/internal/replication/infra/directionlock"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/app/pipeline"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// The first copy. The point is pinned before a key is read and recorded only
// once every key has been read.

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

	// SourceConn and TargetConn are the connection strings, used to open a
	// client on a database other than the one the task names. A standalone
	// source holds several, and the copy has to walk all of them.
	SourceConn string
	TargetConn string

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
	return pipeline.CopyBatch(defaultCopyBatch)
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

func (s *Snapshotter) Copy(ctx context.Context) error {
	from := s.Node
	if from == nil {
		from = s.Source
	}

	// A cluster has one database and its client cannot SELECT, so it is copied
	// as it always was: this shard's own master, scanned directly. A standalone
	// server holds up to sixteen databases, and a copy that walks only the one
	// its connection is on leaves the rest out of the standby with nothing
	// saying so.
	//
	// The test is on the source rather than on `from`. Every shard of a cluster
	// is handed its own master as a plain client -- that is the whole point, so
	// that each scans its own slots -- so asking `from` whether it is a cluster
	// answers no for every shard, and all of them took the multi-database path
	// instead: each opened a fresh cluster client, each scanned whichever single
	// node that client picked, and the same keys were copied three times while
	// two thirds of the key space was never read. The copy reported success.
	if _, cluster := s.Source.(*goredis.ClusterClient); cluster || s.SourceConn == "" {
		return s.copyDatabase(ctx, from, s.Source, s.Target, -1)
	}

	databases, err := populatedDatabases(ctx, from)
	if err != nil {
		return err
	}
	if len(databases) == 0 {
		s.logger().Infof("[Redis] The source holds no keys for shard %s", s.Link.shard)
		return nil
	}

	for _, db := range databases {
		source, err := clientOnDB(s.SourceConn, db)
		if err != nil {
			return fmt.Errorf("open the source on database %d: %w", db, err)
		}
		target, err := clientOnDB(s.TargetConn, db)
		if err != nil {
			source.Close()
			return fmt.Errorf("open the target on database %d: %w", db, err)
		}
		err = s.copyDatabase(ctx, source, source, target, db)
		source.Close()
		target.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// copyDatabase copies one database. db is -1 for a source that has only one,
// which is what keeps the cluster path's logs saying what they always said.
func (s *Snapshotter) copyDatabase(ctx context.Context, from, source,
	target goredis.UniversalClient, db int) error {

	var (
		copied int
		limit  = newRateLimiter(s.ReadRate)
		start  = time.Now()
	)

	err := scanOne(ctx, from, s.batch(), func(keys []string) error {
		if err := limit.wait(ctx, len(keys)); err != nil {
			return err
		}
		wanted := make([][]byte, 0, len(keys))
		for _, key := range keys {
			if internalKey(key) {
				continue
			}
			wanted = append(wanted, []byte(key))
		}
		if len(wanted) == 0 {
			return nil
		}

		values, err := readValues(ctx, source, wanted)
		if err != nil {
			return err
		}
		pipe := target.Pipeline()
		for _, value := range values {
			value.queue(ctx, pipe)
		}
		written, err := pipe.Exec(ctx)
		if err != nil && err != goredis.Nil {
			return fmt.Errorf("write %d copied keys: %w", len(values), err)
		}
		// Per command, not just the batch. A cluster pipeline reports a command
		// that a node refused on the command, and returns nothing for the batch,
		// so counting the batch as copied is how a first copy loses keys and says
		// it succeeded.
		for _, cmd := range written {
			if err := cmd.Err(); err != nil && err != goredis.Nil {
				return fmt.Errorf("write a copied key: %w", err)
			}
		}
		copied += len(values)
		// Debezium: RowsScanned. A first copy of a live key space has no total
		// to count down from — SCAN gives no cardinality — so the progress a
		// shard can honestly report is how much it has done and how long it has
		// been at it.
		metrics.SnapshotProgress(s.Labels, len(values), 1, time.Since(start).Seconds())
		return nil
	})
	if err != nil {
		// An incomplete copy must not leave a position behind: the runner only
		// records one once this returns without error, so returning the failure is
		// what keeps the target from being treated as caught up.
		return fmt.Errorf("copy the source after %d keys: %w", copied, err)
	}

	if db < 0 {
		s.logger().Infof("[Redis] Copied %d keys for shard %s in %s",
			copied, s.Link.shard, time.Since(start).Round(time.Millisecond))
	} else {
		s.logger().Infof("[Redis] Copied %d keys from database %d for shard %s in %s",
			copied, db, s.Link.shard, time.Since(start).Round(time.Millisecond))
	}
	return nil
}

// populatedDatabases reports which of a standalone server's databases hold
// keys, in order. INFO keyspace lists only the ones that do, which is what
// keeps this from probing sixteen databases to find two.
func populatedDatabases(ctx context.Context, client goredis.UniversalClient) ([]int, error) {
	info, err := client.Info(ctx, "keyspace").Result()
	if err != nil {
		return nil, fmt.Errorf("ask the source which databases hold keys: %w", err)
	}
	var databases []int
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "db") {
			continue
		}
		colon := strings.Index(line, ":")
		if colon < 0 {
			continue
		}
		db, err := strconv.Atoi(line[2:colon])
		if err != nil {
			continue
		}
		databases = append(databases, db)
	}
	sort.Ints(databases)
	return databases, nil
}

// scanAll walks every key of a source, whether it is one server or a cluster.
// The callback is called one page at a time, never twice at once.
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

// internalKey reports whether a key is this tool's own rather than the data it
// is replicating. Copying one writes one side's replication state onto the
// other: a marker overwrites the progress of the task writing here, and the
// direction lock tells the target it is a source.
//
// The first copy used to walk only the database its connection was on, which
// hid the direction lock in a database nobody scanned. Walking all of them is
// what made the skip necessary rather than incidental.
func internalKey(key string) bool {
	return IsOffsetKey(key) || isMetaKey(key) || key == directionlock.RedisKey
}

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

// clientOnDB opens a client on one database of a connection, whatever database
// the connection string names.
func clientOnDB(connection string, db int) (goredis.UniversalClient, error) {
	u, err := url.Parse(connection)
	if err != nil {
		return nil, fmt.Errorf("read the connection: %w", err)
	}
	u.Path = "/" + strconv.Itoa(db)
	return intRedis.GetRedisClient(u.String())
}
