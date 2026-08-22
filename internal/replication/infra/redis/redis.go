package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/retail-ai-inc/sync/internal/platform/config"
	intRedis "github.com/retail-ai-inc/sync/internal/platform/dbconn/redis"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/platform/resilience"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
	"github.com/retail-ai-inc/sync/internal/replication/infra/directionlock"
	"github.com/retail-ai-inc/sync/internal/replication/infra/security"
	"github.com/sirupsen/logrus"
)

// defaultReconcileInterval is how often the whole keyspace is compared against
// the source when nothing is configured.
//
// Keyspace notifications are fire-and-forget: Redis publishes them with no
// acknowledgement and no replay, so anything published while the subscriber is
// reconnecting is gone. They cannot be the only path a payment keyspace
// reaches the disaster-recovery copy by, which is what the periodic full
// comparison is for.
const defaultReconcileInterval = time.Hour

// streamGroup is the consumer group the stream reader creates on the source.
const streamGroup = "sync_group"

type RedisSyncer struct {
	cfg         config.SyncConfig
	logger      logrus.FieldLogger
	source      goredis.UniversalClient
	target      goredis.UniversalClient
	lastExecErr int32

	positionPath string
	// checkpoints is where the stream offsets are recorded. They go to the
	// target as well as the local file, so a syncer replaced in the other
	// region can find out where to resume from.
	checkpoints checkpoint.Store
	// reconcileEvery is how often the keyspace is fully compared. Zero turns
	// the comparison off.
	reconcileEvery time.Duration
}

func NewRedisSyncer(cfg config.SyncConfig, logger *logrus.Logger) *RedisSyncer {
	interval := cfg.RedisReconcileInterval
	if interval == 0 {
		interval = defaultReconcileInterval
	}
	if interval < 0 {
		interval = 0
	}
	return &RedisSyncer{
		cfg:            cfg,
		logger:         logger.WithField("sync_task_id", cfg.ID),
		positionPath:   cfg.RedisPositionPath,
		reconcileEvery: interval,
	}
}

// streamPair names one stream mapping.
type streamPair struct {
	source string
	target string
}

// streamMappings reports the streams this task replicates.
//
// It used to be one line — cfg.Mappings[0].Tables[0].SourceTable — with no
// bounds check at all, and the configuration loader inserts an empty mapping
// when a task has none, so the index was out of range for every task with no
// tables configured. That panicked in a goroutine with no recover, taking the
// whole syncer process down with it, every other replication task included.
func (r *RedisSyncer) streamMappings() []streamPair {
	var pairs []streamPair
	for _, mapping := range r.cfg.Mappings {
		for _, table := range mapping.Tables {
			if table.SourceTable == "" {
				continue
			}
			target := table.TargetTable
			if target == "" {
				target = table.SourceTable
			}
			pairs = append(pairs, streamPair{source: table.SourceTable, target: target})
		}
	}
	return pairs
}

// Start replicates until the context is cancelled, or until it cannot carry on.
//
// The returned error is what the supervisor decides on: nil or a transient
// failure means try again, an ErrUnrecoverable means stop and tell somebody.
func (r *RedisSyncer) Start(ctx context.Context) error {
	if err := security.CheckKeyForMappings(r.cfg.Mappings); err != nil {
		return domain.Unrecoverable("%v", err)
	}

	r.logger.Info("[Redis] Starting synchronization...")

	var err error
	err = resilience.Retry(ctx, 5, 2*time.Second, 2.0, func() error {
		var connErr error
		r.source, connErr = intRedis.GetRedisClient(r.cfg.SourceConnection)
		return connErr
	})
	if err != nil {
		return fmt.Errorf("connect to the source: %w", err)
	}
	err = resilience.Retry(ctx, 5, 2*time.Second, 2.0, func() error {
		var connErr error
		r.target, connErr = intRedis.GetRedisClient(r.cfg.TargetConnection)
		return connErr
	})
	if err != nil {
		return fmt.Errorf("connect to the target: %w", err)
	}
	defer r.source.Close()
	defer r.target.Close()

	// Nothing is read or written until the direction is agreed. A target that
	// has been promoted, or a source that is itself somebody's target, means the
	// pair has been reversed under us and carrying on would overwrite the newer
	// side with the older one.
	stopGuard, guardErr := r.claimDirection(ctx)
	if guardErr != nil {
		// A reversed direction is not something a retry resolves: somebody has
		// to decide which side is authoritative.
		return domain.Unrecoverable("%v", guardErr)
	}
	defer stopGuard()

	r.checkpoints = r.checkpointStore()

	labels := r.metricLabels()
	metrics.SetTaskUp(labels, true)
	defer metrics.SetTaskUp(labels, false)

	r.checkKeyspaceNotifications(ctx)

	if err := r.doInitialSync(ctx); err != nil {
		r.logger.Errorf("[Redis] doInitialSync error: %v", err)
	}
	r.logger.Info("[Redis] Initial full sync done.")

	r.logger.Info("[Redis] Subscribing keyspace notifications...")
	go r.watchKeyspaceChanges(ctx)
	go r.reconcileLoop(ctx)

	pairs := r.streamMappings()
	if len(pairs) == 0 {
		r.logger.Info("[Redis] No stream mappings configured; replicating the keyspace only.")
	} else {
		// A mapping names a stream to consume with a consumer group. It does not
		// narrow what the keyspace copy covers, which is every key the source
		// holds — worth saying, because a task that lists three keys reads as
		// though it replicates three keys.
		r.logger.Infof("[Redis] %d stream mapping(s) configured. They name streams to "+
			"replicate by consumer group; the rest of the keyspace is replicated "+
			"whole either way, so the mappings do not narrow what is copied.", len(pairs))
	}
	for _, pair := range pairs {
		go r.replicateStream(ctx, pair)
	}

	<-ctx.Done()
	r.logger.Info("[Redis] Synchronization stopped.")
	return nil
}

// sourceDatabase reports the database index the source DSN addresses. The
// keyspace notification channel is per-database, so subscribing to database 0
// regardless — which is what it used to do — meant a task configured against
// any other database saw nothing at all and reported no error.
func (r *RedisSyncer) sourceDatabase() string {
	db := dsn.GetDatabaseName("redis", r.cfg.SourceConnection)
	if db == "" {
		return "0"
	}
	return db
}

// checkKeyspaceNotifications reports whether the source will actually publish
// the events the subscription depends on.
//
// The subscription itself succeeds whether or not the server is configured to
// publish, so a source with notify-keyspace-events unset looks exactly like a
// source with nothing happening on it: the task runs, reports no error, and
// replicates nothing. Memorystore leaves the setting empty by default.
func (r *RedisSyncer) checkKeyspaceNotifications(ctx context.Context) {
	values, err := r.source.ConfigGet(ctx, "notify-keyspace-events").Result()
	if err != nil {
		r.logger.Warnf("[Redis] Could not read notify-keyspace-events (%v); if the "+
			"source does not publish keyspace events, incremental replication will "+
			"be silently empty and only the periodic comparison will carry changes", err)
		return
	}

	flags := values["notify-keyspace-events"]
	switch {
	case !strings.Contains(flags, "K"):
		r.logger.Errorf("[Redis] The source has notify-keyspace-events=%q, which does "+
			"not include K: no keyspace event will ever be published and incremental "+
			"replication will be silently empty. Set it to at least KEA.", flags)
	case !strings.ContainsAny(flags, "A$lshzxeg"):
		r.logger.Errorf("[Redis] The source has notify-keyspace-events=%q, which names "+
			"no event class: changes will not be published. Set it to at least KEA.", flags)
	default:
		r.logger.Infof("[Redis] Source notify-keyspace-events=%q", flags)
	}
}

// ------------------------------------------------------------ full copies

func (r *RedisSyncer) doInitialSync(ctx context.Context) error {
	r.logger.Info("[Redis] Starting initial full sync...")
	return r.scanSource(ctx, func(keys []string) {
		if err := r.copyKeys(ctx, keys); err != nil {
			r.logger.Errorf("[Redis] copyKeys error: %v", err)
			atomic.StoreInt32(&r.lastExecErr, 1)
		}
	})
}

// scanSource walks the whole source keyspace, one page at a time, across every
// master when the source is a cluster.
func (r *RedisSyncer) scanSource(ctx context.Context, page func(keys []string)) error {
	return scanAll(ctx, r.source, page)
}

// scanAll walks a keyspace. A cluster keeps its keys on many nodes and SCAN
// only ever walks the node it reached, so each master is scanned in turn.
func scanAll(ctx context.Context, client goredis.UniversalClient, page func(keys []string)) error {
	// The page size is what the reconciliation's cost is measured in: it reads
	// both sides and writes the differences in one round trip each per page, so
	// across a region boundary the pass costs three round trips per page rather
	// than three per key. Five hundred keys of payment-sized values is a
	// hundred kilobytes or so in flight — small enough not to matter, large
	// enough that a million keys is two thousand pages.
	const batchSize = 500

	scanOne := func(ctx context.Context, c goredis.UniversalClient) error {
		var cursor uint64
		for {
			keys, next, err := c.Scan(ctx, cursor, "*", batchSize).Result()
			if err != nil {
				return fmt.Errorf("SCAN fail at cursor=%d: %v", cursor, err)
			}
			if len(keys) > 0 {
				page(keys)
			}
			cursor = next
			if cursor == 0 {
				return nil
			}
		}
	}

	if cluster, ok := client.(*goredis.ClusterClient); ok {
		return cluster.ForEachMaster(ctx, func(ctx context.Context, node *goredis.Client) error {
			return scanOne(ctx, node)
		})
	}
	return scanOne(ctx, client)
}

func (r *RedisSyncer) copyKeys(ctx context.Context, keys []string) error {
	for _, k := range keys {
		if err := r.copyFullKey(ctx, k); err != nil {
			r.logger.Errorf("[Redis] copyFullKey fail => key=%s, error=%v", k, err)
			atomic.StoreInt32(&r.lastExecErr, 1)
			metrics.Failed(r.metricLabels(), 1)
		} else {
			r.logger.Debugf("[Redis][COPY] key=%s copied successfully", k)
			metrics.Applied(r.metricLabels(), 1)
		}
	}
	return nil
}

// copyFullKey copies one key byte for byte, TTL included.
func (r *RedisSyncer) copyFullKey(ctx context.Context, key string) error {
	ttl, err := r.source.TTL(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("get TTL fail: %v", err)
	}
	if ttl < 0 && ttl != -1 {
		r.logger.Debugf("[Redis] key=%s non-existing or expired => skip copy", key)
		return nil
	}
	dumpedVal, errD := r.source.Dump(ctx, key).Result()
	if errD != nil && errD != goredis.Nil {
		return fmt.Errorf("DUMP fail key=%s: %v", key, errD)
	}
	if dumpedVal == "" {
		r.logger.Debugf("[Redis] key=%s dump is empty => skip copy", key)
		return nil
	}
	var expireMs int64
	if ttl == -1 {
		expireMs = 0
	} else {
		expireMs = ttl.Milliseconds()
		if expireMs < 0 {
			expireMs = 0
		}
	}
	r.logger.Debugf("[Redis][RESTORE] command=\"RESTORE key=%s, expireMs=%d\"", key, expireMs)

	restoreErr := r.target.RestoreReplace(ctx, key, time.Duration(expireMs)*time.Millisecond, dumpedVal).Err()
	if restoreErr != nil {
		if strings.Contains(restoreErr.Error(), "ERR syntax error") {
			// A server too old to know RESTORE REPLACE. The fallback used to be
			// DEL followed by RESTORE, two round trips with a window in between
			// where the target does not have the key at all — on every full copy
			// of every key. The script does both in one step.
			restoreErr = replaceKey.Run(ctx, r.target, []string{key}, expireMs, dumpedVal).Err()
		}
		if restoreErr != nil {
			return fmt.Errorf("RESTORE fail key=%s: %v", key, restoreErr)
		}
	}
	return nil
}

// replaceKey overwrites one key with a dump, atomically. Redis runs a script to
// completion before anything else, so the key is never missing partway through.
var replaceKey = goredis.NewScript(`
redis.call('DEL', KEYS[1])
return redis.call('RESTORE', KEYS[1], ARGV[1], ARGV[2])
`)

// ------------------------------------------------------ keyspace watching

// masterRediscoveryInterval is how often the set of cluster masters is looked
// up again.
//
// A subscription is to one node. After a failover or a resharding the node that
// publishes a given key's events is a different one, and go-redis reconnects a
// dropped subscription to the same address — which by then serves a replica, or
// nothing. The subscription stays open and silent, so the events for that slot
// range simply stop arriving, with no error anywhere. The periodic full
// comparison eventually corrects the data, which is why this is a recovery-point
// problem rather than a data-loss one: seconds become an hour.
const masterRediscoveryInterval = 30 * time.Second

// subscriptionHealthInterval is how often a subscription is asked whether it is
// still there.
const subscriptionHealthInterval = 30 * time.Second

func (r *RedisSyncer) watchKeyspaceChanges(ctx context.Context) {
	pattern := fmt.Sprintf("__keyspace@%s__:*", r.sourceDatabase())

	cluster, isCluster := r.source.(*goredis.ClusterClient)
	if !isCluster {
		r.subscribeKeyspace(ctx, r.source, pattern)
		return
	}

	// A cluster publishes keyspace events on the node that owns the key, so
	// every master needs its own subscription — and the set of masters changes.
	running := map[string]context.CancelFunc{}
	defer func() {
		for _, stop := range running {
			stop()
		}
	}()

	start := func(nodeCtx context.Context, addr string) {
		client := r.nodeClient(cluster, addr)
		go func() {
			defer client.Close()
			r.subscribeKeyspace(nodeCtx, client, pattern)
		}()
	}

	ticker := time.NewTicker(masterRediscoveryInterval)
	defer ticker.Stop()

	for {
		masters, err := clusterMasters(ctx, cluster)
		if err != nil {
			r.logger.Errorf("[Redis] Could not list the cluster masters: %v", err)
		} else {
			r.reconcileSubscriptions(ctx, running, masters, start)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// clusterMasters reports the addresses of the nodes currently serving writes.
func clusterMasters(ctx context.Context, cluster *goredis.ClusterClient) ([]string, error) {
	var addresses []string
	err := cluster.ForEachMaster(ctx, func(_ context.Context, node *goredis.Client) error {
		addresses = append(addresses, node.Options().Addr)
		return nil
	})
	return addresses, err
}

// reconcileSubscriptions starts a watcher for each address that has none and
// stops the ones whose address is no longer serving writes.
func (r *RedisSyncer) reconcileSubscriptions(ctx context.Context, running map[string]context.CancelFunc, want []string, start func(context.Context, string)) {
	wanted := make(map[string]bool, len(want))
	for _, addr := range want {
		wanted[addr] = true
	}

	for addr, stop := range running {
		if wanted[addr] {
			continue
		}
		r.logger.Infof("[Redis] %s no longer serves writes; stopping its keyspace subscription", addr)
		stop()
		delete(running, addr)
	}

	for _, addr := range want {
		if _, already := running[addr]; already {
			continue
		}
		r.logger.Infof("[Redis] Subscribing to keyspace events on %s", addr)
		nodeCtx, stop := context.WithCancel(ctx)
		running[addr] = stop
		start(nodeCtx, addr)
	}
}

// nodeClient opens a connection to one cluster node, carrying the cluster's own
// credentials and transport settings.
//
// A client of its own rather than the one ForEachMaster lends out: that one
// belongs to the cluster client, which closes it when the topology changes —
// underneath a subscription that is meant to outlive the lookup.
func (r *RedisSyncer) nodeClient(cluster *goredis.ClusterClient, addr string) *goredis.Client {
	opts := cluster.Options()
	return goredis.NewClient(&goredis.Options{
		Addr:      addr,
		Username:  opts.Username,
		Password:  opts.Password,
		TLSConfig: opts.TLSConfig,
	})
}

// subscribeKeyspace consumes one node's keyspace notifications.
func (r *RedisSyncer) subscribeKeyspace(ctx context.Context, client goredis.UniversalClient, pattern string) {
	pubsub := client.PSubscribe(ctx, pattern)
	if pubsub == nil {
		r.logger.Error("[Redis] PSubscribe returned nil => no keyspace subscription.")
		return
	}
	defer pubsub.Close()
	r.logger.Infof("[Redis] Keyspace subscription started on %s.", pattern)

	// go-redis reconnects a dropped subscription by itself, so the channel does
	// not close and nothing here notices — the events simply stop arriving and
	// the task goes on reporting no error. The ping is what turns that into a
	// line somebody can see; the periodic comparison is what makes the data
	// right again either way.
	health := time.NewTicker(subscriptionHealthInterval)
	defer health.Stop()
	healthy := true

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("[Redis] Keyspace subscription shutting down.")
			return
		case <-health.C:
			if err := pubsub.Ping(ctx); err != nil {
				if healthy {
					r.logger.Errorf("[Redis] The keyspace subscription on %s is not "+
						"answering (%v). Changes are not arriving; the target will only "+
						"catch up at the next full comparison.", pattern, err)
				}
				healthy = false
				continue
			}
			if !healthy {
				r.logger.Infof("[Redis] The keyspace subscription on %s is answering again.", pattern)
				healthy = true
			}
		case msg, ok := <-pubsub.Channel():
			if !ok {
				r.logger.Warn("[Redis] Keyspace subscription channel closed unexpectedly.")
				return
			}
			r.handleKeyspaceChange(ctx, msg.Channel, msg.Payload)
		}
	}
}

// handleKeyspaceChange applies one notification.
//
// Everything that is not a removal is applied as a full copy. Rebuilding the
// value from its type — GET then SET, HGETALL then HSET — dropped the TTL and
// merged rather than replaced a hash, so a key whose expiry mattered lost it
// and a hash that had fields removed kept them.
func (r *RedisSyncer) handleKeyspaceChange(ctx context.Context, ch, op string) {
	parts := strings.SplitN(ch, ":", 2)
	if len(parts) < 2 {
		r.logger.Debugf("[Redis] invalid keyspace channel => %s", ch)
		return
	}
	key := parts[1]

	switch strings.ToLower(op) {
	case "del", "expired", "evicted":
		r.logger.Debugf("[Redis][DELETE] command=\"DEL key=%s\"", key)
		if err := r.target.Del(ctx, key).Err(); err != nil {
			r.logger.Errorf("[Redis][DELETE] key=%s error=%v", key, err)
			atomic.StoreInt32(&r.lastExecErr, 1)
		} else {
			r.logger.Debugf("[Redis][DELETE] key=%s success", key)
		}

	default:
		r.logger.Debugf("[Redis][UPSERT] command=\"FULLCOPY key=%s\"", key)
		if err := r.copyFullKey(ctx, key); err != nil {
			r.logger.Errorf("[Redis][UPSERT] key=%s error=%v", key, err)
			atomic.StoreInt32(&r.lastExecErr, 1)
		}
	}
}

// --------------------------------------------------------- reconciliation

// reconcileLoop periodically brings the target back in line with the source.
//
// Keyspace notifications are published with no acknowledgement and no replay:
// anything Redis publishes while the subscriber is reconnecting is simply gone,
// and nothing in the protocol reports that it happened. The comparison is
// therefore not an optimisation, it is the only thing that makes the target
// eventually correct.
func (r *RedisSyncer) reconcileLoop(ctx context.Context) {
	if r.reconcileEvery <= 0 {
		r.logger.Warn("[Redis] Periodic reconciliation is disabled; keyspace " +
			"notifications are the only path changes reach the target by, and they " +
			"are lost whenever the subscription drops.")
		return
	}

	ticker := time.NewTicker(r.reconcileEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.reconcile(ctx)
		}
	}
}

// reconcile copies every source key over the target and removes target keys the
// source no longer has.
func (r *RedisSyncer) reconcile(ctx context.Context) {
	start := time.Now()
	var examined, copied, removed int

	if err := r.scanSource(ctx, func(keys []string) {
		n, err := r.reconcileBatch(ctx, keys)
		if err != nil {
			r.logger.Errorf("[Redis] Reconciliation could not compare a batch: %v", err)
			atomic.StoreInt32(&r.lastExecErr, 1)
			return
		}
		examined += len(keys)
		copied += n
	}); err != nil {
		r.logger.Errorf("[Redis] Reconciliation could not read the source: %v", err)
		return
	}

	if err := scanAll(ctx, r.target, func(keys []string) {
		removed += r.removeKeysMissingFromSource(ctx, keys)
	}); err != nil {
		r.logger.Errorf("[Redis] Reconciliation could not read the target: %v", err)
		return
	}

	r.logger.Infof("[Redis] Reconciliation finished in %v: %d keys examined, "+
		"%d copied, %d removed", time.Since(start), examined, copied, removed)
}

// reconcileBatch brings one page of keys in line and reports how many it had to
// copy.
//
// Two things make this affordable. It compares before copying, because a replica
// that is already correct is the normal case and re-sending a whole keyspace
// every pass is not a comparison, it is a re-copy. And it pipelines, because the
// cost is dominated by round trips: key by key, this did TTL, DUMP and RESTORE
// in sequence for every key, which measured 568 keys a second against a server
// on loopback — a million keys would have taken half an hour, and across a
// region boundary far longer than any interval worth setting.
func (r *RedisSyncer) reconcileBatch(ctx context.Context, keys []string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	// One round trip for the source's payloads and expiries, one for the
	// target's payloads.
	sourceDumps, sourceTTLs, err := dumpBatch(ctx, r.source, keys)
	if err != nil {
		return 0, fmt.Errorf("read the source: %w", err)
	}
	targetDumps, _, err := dumpBatch(ctx, r.target, keys)
	if err != nil {
		return 0, fmt.Errorf("read the target: %w", err)
	}

	// The payload is what RESTORE takes, so comparing it compares exactly what
	// would be written. Two servers of different versions can render the same
	// value differently, in which case every key looks changed and this
	// degrades to the copy-everything behaviour it replaces — slower, never
	// wrong.
	pipe := r.target.Pipeline()
	queued := 0
	for i, key := range keys {
		payload := sourceDumps[i]
		if payload == "" {
			// Gone from the source since the scan. The target-side pass removes
			// it; doing it here as well would race with a concurrent write.
			continue
		}
		if targetDumps[i] == payload && sourceTTLs[i] == 0 {
			continue // identical, and neither side expires it
		}
		if targetDumps[i] == payload {
			// Same value, so only the expiry may differ. Setting it is one
			// command rather than re-sending the value.
			pipe.PExpire(ctx, key, time.Duration(sourceTTLs[i])*time.Millisecond)
			queued++
			continue
		}
		pipe.RestoreReplace(ctx, key,
			time.Duration(sourceTTLs[i])*time.Millisecond, payload)
		queued++
	}
	if queued == 0 {
		return 0, nil
	}

	// A RESTORE that fails for one key must not hide the rest, so the errors are
	// read per command rather than from Exec alone.
	results, err := pipe.Exec(ctx)
	if err != nil && err != goredis.Nil {
		// Fall back to one key at a time, which reports precisely and is worth
		// the round trips because it only happens when something is wrong.
		r.logger.Warnf("[Redis] A reconciliation batch failed (%v); retrying it "+
			"key by key", err)
		return r.copyBatchIndividually(ctx, keys)
	}
	written := 0
	for _, result := range results {
		if result.Err() != nil && result.Err() != goredis.Nil {
			r.logger.Warnf("[Redis] Reconciliation could not write a key: %v", result.Err())
			atomic.StoreInt32(&r.lastExecErr, 1)
			metrics.Failed(r.metricLabels(), 1)
			continue
		}
		written++
	}
	metrics.Applied(r.metricLabels(), written)
	return written, nil
}

// copyBatchIndividually is the fallback when a pipelined batch fails as a whole.
func (r *RedisSyncer) copyBatchIndividually(ctx context.Context, keys []string) (int, error) {
	copied := 0
	for _, key := range keys {
		if err := r.copyFullKey(ctx, key); err != nil {
			r.logger.Errorf("[Redis] Reconciliation could not copy key=%s: %v", key, err)
			atomic.StoreInt32(&r.lastExecErr, 1)
			metrics.Failed(r.metricLabels(), 1)
			continue
		}
		copied++
	}
	metrics.Applied(r.metricLabels(), copied)
	return copied, nil
}

// dumpBatch reads the serialised value and the remaining expiry of every key in
// one round trip each.
//
// A missing key comes back as an empty payload and a zero expiry, which is what
// the caller wants to know and is not an error: the keyspace moves while it is
// being walked.
func dumpBatch(ctx context.Context, client goredis.UniversalClient, keys []string) (dumps []string, ttls []int64, err error) {
	pipe := client.Pipeline()
	dumpCmds := make([]*goredis.StringCmd, len(keys))
	ttlCmds := make([]*goredis.DurationCmd, len(keys))
	for i, key := range keys {
		dumpCmds[i] = pipe.Dump(ctx, key)
		ttlCmds[i] = pipe.PTTL(ctx, key)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != goredis.Nil {
		return nil, nil, err
	}

	dumps = make([]string, len(keys))
	ttls = make([]int64, len(keys))
	for i := range keys {
		if payload, err := dumpCmds[i].Result(); err == nil {
			dumps[i] = payload
		}
		// PTTL answers -1 for a key with no expiry and -2 for one that is gone;
		// RESTORE takes 0 for "no expiry", so both become zero.
		if ttl, err := ttlCmds[i].Result(); err == nil && ttl > 0 {
			ttls[i] = ttl.Milliseconds()
		}
	}
	return dumps, ttls, nil
}

// removeKeysMissingFromSource deletes the target keys the source no longer
// holds, which is how a delete lost with a dropped subscription is corrected.
func (r *RedisSyncer) removeKeysMissingFromSource(ctx context.Context, keys []string) int {
	if len(keys) == 0 {
		return 0
	}

	// One round trip to ask the source about the whole page, rather than one per
	// key. On a keyspace of any size the round trips are the entire cost.
	pipe := r.source.Pipeline()
	exists := make([]*goredis.IntCmd, len(keys))
	for i, key := range keys {
		exists[i] = pipe.Exists(ctx, key)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != goredis.Nil {
		r.logger.Errorf("[Redis] Reconciliation could not check a page against the "+
			"source: %v", err)
		return 0
	}

	var gone []string
	for i, key := range keys {
		n, err := exists[i].Result()
		if err != nil {
			r.logger.Errorf("[Redis] Reconciliation could not check key=%s: %v", key, err)
			continue
		}
		if n == 0 {
			gone = append(gone, key)
		}
	}
	if len(gone) == 0 {
		return 0
	}

	// A cluster refuses a multi-key DEL across slots, so delete one command per
	// key but in a single pipeline: the round trips are what cost, not the
	// commands.
	del := r.target.Pipeline()
	for _, key := range gone {
		del.Del(ctx, key)
	}
	if _, err := del.Exec(ctx); err != nil && err != goredis.Nil {
		r.logger.Errorf("[Redis] Reconciliation could not remove keys the source no "+
			"longer has: %v", err)
		return 0
	}
	r.logger.Debugf("[Redis][RECONCILE] removed %d keys the source no longer has",
		len(gone))
	return len(gone)
}

// -------------------------------------------------------- stream watching

// replicateStream mirrors one source stream onto the target.
// streamRetryDelay and maxStreamRetryDelay bound how fast a failing stream read
// is tried again.
const (
	streamRetryDelay    = 500 * time.Millisecond
	maxStreamRetryDelay = 30 * time.Second
)

func (r *RedisSyncer) replicateStream(ctx context.Context, pair streamPair) {
	lastID := r.loadStreamPosition(pair.source)
	if lastID == "" {
		lastID = "0-0"
	}

	err := r.source.XGroupCreateMkStream(ctx, pair.source, streamGroup, lastID).Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		r.logger.Errorf("[Redis] XGroupCreate fail => %v", err)
		return
	}
	r.logger.Infof("[Redis] Using group=%s on stream=%s from lastID=%s", streamGroup, pair.source, lastID)

	r.watchStreamChanges(ctx, pair, streamGroup, lastID)
	r.logger.Infof("[Redis] Stream replication for %s ended.", pair.source)
}

func (r *RedisSyncer) watchStreamChanges(ctx context.Context, pair streamPair, groupName, lastID string) {
	// A failing read used to loop straight back round, so a source that was down
	// was dialled as fast as the failures came back — a tight loop against a
	// server that is already in trouble, and a log line per attempt.
	backoff := streamRetryDelay

	for {
		select {
		case <-ctx.Done():
			return
		default:
			streams, xerr := r.source.XReadGroup(ctx, &goredis.XReadGroupArgs{
				Group:    groupName,
				Consumer: "sync_consumer_1",
				Streams:  []string{pair.source, ">"},
				Count:    10,
				Block:    2000 * time.Millisecond,
			}).Result()
			if xerr != nil && xerr != goredis.Nil {
				if strings.Contains(xerr.Error(), "context canceled") {
					r.logger.Warnf("[Redis] XReadGroup context canceled => %v", xerr)
					return
				}
				r.logger.Errorf("[Redis] XReadGroup error, retrying in %s => %v", backoff, xerr)
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				if backoff *= 2; backoff > maxStreamRetryDelay {
					backoff = maxStreamRetryDelay
				}
				continue
			}
			backoff = streamRetryDelay
			if len(streams) == 0 {
				continue
			}
			for _, st := range streams {
				for _, msg := range st.Messages {
					r.logger.Debugf("[Redis][STREAM] command=\"XADD stream=%s msgID=%s\"", pair.target, msg.ID)
					if err := r.processStreamMessage(ctx, pair.target, msg); err != nil {
						r.logger.Errorf("[Redis] processStreamMessage fail => skip XACK => %v", err)
						atomic.StoreInt32(&r.lastExecErr, 1)
						continue
					}
					r.source.XAck(ctx, pair.source, groupName, msg.ID)
					lastID = msg.ID
					r.saveStreamPosition(pair.source, lastID)
				}
			}
		}
	}
}

// processStreamMessage appends one source entry to the target stream, keeping
// its identifier.
//
// It used to write the entry into a hash called msg:<id> on the target, and to
// decide how by asking the *source* for the type of that hash — a key the
// source has never had. The type came back as "none", the call returned an
// error, the entry was never acknowledged, and the reader read the same entry
// forever. Nothing was replicated and the loop never moved on.
func (r *RedisSyncer) processStreamMessage(ctx context.Context, targetStream string, msg goredis.XMessage) error {
	err := r.target.XAdd(ctx, &goredis.XAddArgs{
		Stream: targetStream,
		ID:     msg.ID,
		Values: msg.Values,
	}).Err()

	if err != nil {
		if isAlreadyAppended(err) {
			// The entry is already on the target, which is what a replay after a
			// restart looks like. Reporting success is what lets the reader
			// acknowledge it and move on.
			r.logger.Debugf("[Redis][STREAM] id=%s is already on %s", msg.ID, targetStream)
			return nil
		}
		r.logger.Errorf("[Redis][STREAM] XADD => id=%s error=%v", msg.ID, err)
		return err
	}

	r.logger.Debugf("[Redis][STREAM] id=%s => appended to %s with %d fields",
		msg.ID, targetStream, len(msg.Values))
	return nil
}

// isAlreadyAppended reports the error Redis returns for an identifier that is
// not greater than the target stream's last one, which is how a replayed entry
// presents.
func isAlreadyAppended(err error) bool {
	return err != nil && strings.Contains(err.Error(), "equal or smaller")
}

// ------------------------------------------------------------- positions

// checkpointStore is where this task records its stream offsets.
//
// It writes to the target as well as the configured file. The file alone was
// the problem: the syncer runs beside the source, so the outage this setup
// exists to survive takes the record of what has been applied with it.
func (r *RedisSyncer) checkpointStore() checkpoint.Store {
	var stores []checkpoint.Store
	if r.target != nil {
		stores = append(stores, &checkpoint.RedisStore{Client: r.target, TaskID: r.cfg.ID})
	}
	if r.positionPath != "" {
		stores = append(stores, &checkpoint.FileStore{Path: r.positionPath})
	}
	return &checkpoint.Layered{
		Stores:  stores,
		OnError: func(err error) { r.logger.Warnf("[Redis] Checkpoint store: %v", err) },
	}
}

func (r *RedisSyncer) loadStreamPosition(stream string) string {
	store := r.checkpoints
	if store == nil {
		store = r.checkpointStore()
	}
	id, err := store.Load(context.Background(), stream)
	if err != nil {
		r.logger.Warnf("[Redis] Could not read the stream position for %s: %v", stream, err)
		return ""
	}
	return strings.TrimSpace(id)
}

func (r *RedisSyncer) saveStreamPosition(stream, id string) {
	store := r.checkpoints
	if store == nil {
		store = r.checkpointStore()
	}
	if err := store.Save(context.Background(), stream, id); err != nil {
		r.logger.Errorf("[Redis] Could not record the stream position for %s: %v", stream, err)
	}
}

// claimDirection records which way this task replicates, on both endpoints, and
// keeps the claims refreshed for as long as it runs.
func (r *RedisSyncer) claimDirection(ctx context.Context) (func(), error) {
	guard := &directionlock.Guard{
		TaskID: r.cfg.ID,
		Source: &directionlock.RedisStore{
			Client:  r.source,
			Address: dsn.Endpoint("redis", r.cfg.SourceConnection),
		},
		Target: &directionlock.RedisStore{
			Client:  r.target,
			Address: dsn.Endpoint("redis", r.cfg.TargetConnection),
		},
	}

	if err := guard.Acquire(ctx); err != nil {
		return nil, err
	}

	heartbeatCtx, stop := context.WithCancel(ctx)
	go guard.KeepAlive(heartbeatCtx, func(err error) {
		r.logger.Warnf("[Redis] Could not refresh the replication direction claim: %v", err)
	})
	return func() {
		stop()
		// Give the release its own deadline: the task's context is already
		// cancelled by the time this runs.
		releaseCtx, cancelRelease := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelRelease()
		if err := guard.Release(releaseCtx); err != nil {
			r.logger.Warnf("[Redis] Could not release the replication direction claim: %v", err)
		}
	}, nil
}

// metricLabels identify this task in the metrics. The endpoints are named
// without their credentials, because the exposition is scraped and stored.
func (r *RedisSyncer) metricLabels() metrics.Labels {
	return metrics.Labels{
		"task":   strconv.Itoa(r.cfg.ID),
		"engine": "redis",
		"source": dsn.Endpoint("redis", r.cfg.SourceConnection),
		"target": dsn.Endpoint("redis", r.cfg.TargetConnection),
	}
}
