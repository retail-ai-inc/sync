package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
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
	r.logger.Info("[Redis] Starting synchronization...")

	var err error
	err = resilience.Retry(5, 2*time.Second, 2.0, func() error {
		var connErr error
		r.source, connErr = intRedis.GetRedisClient(r.cfg.SourceConnection)
		return connErr
	})
	if err != nil {
		return fmt.Errorf("connect to the source: %w", err)
	}
	err = resilience.Retry(5, 2*time.Second, 2.0, func() error {
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
	const batchSize = 100

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
			_ = r.target.Del(ctx, key)
			restoreErr = r.target.Restore(ctx, key, time.Duration(expireMs)*time.Millisecond, dumpedVal).Err()
		}
		if restoreErr != nil {
			return fmt.Errorf("RESTORE fail key=%s: %v", key, restoreErr)
		}
	}
	return nil
}

// ------------------------------------------------------ keyspace watching

func (r *RedisSyncer) watchKeyspaceChanges(ctx context.Context) {
	pattern := fmt.Sprintf("__keyspace@%s__:*", r.sourceDatabase())

	// A cluster publishes keyspace events on the node that owns the key, so a
	// single subscription would only ever see one node's share of them.
	if cluster, ok := r.source.(*goredis.ClusterClient); ok {
		var wg sync.WaitGroup
		err := cluster.ForEachMaster(ctx, func(ctx context.Context, node *goredis.Client) error {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r.subscribeKeyspace(ctx, node, pattern)
			}()
			return nil
		})
		if err != nil {
			r.logger.Errorf("[Redis] Could not subscribe to every master: %v", err)
		}
		wg.Wait()
		return
	}
	r.subscribeKeyspace(ctx, r.source, pattern)
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

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("[Redis] Keyspace subscription shutting down.")
			return
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
	copied, removed := 0, 0

	if err := r.scanSource(ctx, func(keys []string) {
		_ = r.copyKeys(ctx, keys)
		copied += len(keys)
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

	r.logger.Infof("[Redis] Reconciliation finished in %v: %d keys copied, %d removed",
		time.Since(start), copied, removed)
}

// removeKeysMissingFromSource deletes the target keys the source no longer
// holds, which is how a delete lost with a dropped subscription is corrected.
func (r *RedisSyncer) removeKeysMissingFromSource(ctx context.Context, keys []string) int {
	removed := 0
	for _, key := range keys {
		exists, err := r.source.Exists(ctx, key).Result()
		if err != nil {
			r.logger.Errorf("[Redis] Reconciliation could not check key=%s: %v", key, err)
			continue
		}
		if exists > 0 {
			continue
		}
		if err := r.target.Del(ctx, key).Err(); err != nil {
			r.logger.Errorf("[Redis] Reconciliation could not remove key=%s: %v", key, err)
			continue
		}
		r.logger.Debugf("[Redis][RECONCILE] key=%s removed; the source no longer has it", key)
		removed++
	}
	return removed
}

// -------------------------------------------------------- stream watching

// replicateStream mirrors one source stream onto the target.
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
				r.logger.Errorf("[Redis] XReadGroup error => %v", xerr)
				continue
			}
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
	return stop, nil
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
