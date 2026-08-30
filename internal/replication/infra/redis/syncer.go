package redis

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	intRedis "github.com/retail-ai-inc/sync/internal/platform/dbconn/redis"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/platform/resilience"
	"github.com/retail-ai-inc/sync/internal/replication/app/pipeline"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/directionlock"
)

// One task, one pipeline per source shard.
//
// This is the one place the Redis flow differs in shape from MySQL and MongoDB.
// Those have a single log for the whole server — one binlog, one change stream —
// so one reader covers everything. A Redis cluster has a replication stream per
// shard and no way to join them, so a task runs a pipeline for each: its own
// connection, its own buffer, its own position. They are independent, and any one
// of them failing for good stops the task.

// Syncer replicates one Redis task.
type Syncer struct {
	cfg    config.SyncConfig
	logger logrus.FieldLogger
}

// NewSyncer builds the syncer for a task.
func NewSyncer(cfg config.SyncConfig, logger *logrus.Logger) *Syncer {
	return &Syncer{cfg: cfg, logger: logger.WithField("sync_task_id", cfg.ID)}
}

// Start replicates until the context is cancelled, or until it cannot carry on.
//
// The returned error is what the supervisor decides on: nil or a transient
// failure means try again, an ErrUnrecoverable means stop and tell somebody.
func (s *Syncer) Start(ctx context.Context) error {
	labels := s.labels()

	source, target, err := s.connect(ctx)
	if err != nil {
		return err
	}
	defer source.Close()
	defer target.Close()

	// Nothing is read or written until the direction is agreed. A target that
	// has been promoted, or a source that is itself somebody's target, means the
	// pair has been reversed and carrying on would overwrite the newer side with
	// the older one.
	stopGuard, err := s.claimDirection(ctx, source, target)
	if err != nil {
		if directionlock.IsBlocking(err) {
			return domain.Unrecoverable("%v", err)
		}
		// Anything else is transient and the task is restarted for it: another
		// process still finishing its shutdown, or an endpoint that is briefly
		// unreachable — the guard reads its claims from the databases, so an
		// outage on either side fails it while the outage lasts.
		return fmt.Errorf("%w", err)
	}
	defer stopGuard()

	metrics.SetTaskUp(labels, true)
	defer metrics.SetTaskUp(labels, false)

	if err := s.warnAboutUnreplicatedThings(ctx, source, target); err != nil {
		return err
	}

	// The command specifications come from the target: it is the server that has
	// to execute what arrives, so its idea of which key a command touches is the
	// one that matters.
	commands, err := loadCommandTable(ctx, target)
	if err != nil {
		return err
	}

	shards, err := shardsOf(ctx, source, s.sourceAddr())
	if err != nil {
		return err
	}
	s.logger.Infof("[Redis] Replicating %d shard(s)", len(shards))

	// One watcher for the task, fanning out to every shard's comparison.
	triggers := make([]chan string, len(shards))
	for i := range triggers {
		triggers[i] = make(chan string, 1)
	}
	watcher := &topologyWatcher{
		Source: source,
		Logger: s.logger,
		OnChange: func(what string) {
			for _, trigger := range triggers {
				// Never block: a comparison already queued is as good as two.
				select {
				case trigger <- what:
				default:
				}
			}
		},
	}
	go watcher.Run(ctx)

	group, groupCtx := errgroup.WithContext(ctx)
	for i, shard := range shards {
		shard, trigger := shard, triggers[i]
		group.Go(func() error {
			return s.runShard(groupCtx, shard, source, target, commands, trigger)
		})
	}
	return group.Wait()
}

// shard names one source master and how to reach it.
type shard struct {
	// id is stable across restarts, so a position can be found again. The slot
	// range serves: a master's address changes when it fails over, but the slots
	// it owns are what identify it in the cluster.
	id   string
	addr string
}

// shardsOf finds the masters of a source.
//
// A shard is named by the slots it owns rather than by its address, so that a
// shard which fails over to another node keeps its position. The tests use this
// sourceAddr is the address a single-server source is dialled at.
//
// It must be host:port and nothing else. dsn.Endpoint appends the database,
// which is right for a log line and wrong for a dial: a standalone source with
// a database configured stopped the task on every attempt with "lookup
// tcp/6379/0: unknown port". A cluster never showed it, because there the
// addresses come from CLUSTER SLOTS rather than from the configuration.
func (s *Syncer) sourceAddr() string {
	return dsn.HostPort("redis", s.cfg.SourceConnection)
}

// too: the identity a position is filed under has to be the same one in both
// places, or a test would be exercising a shard naming that production does not.
func shardsOf(ctx context.Context, source goredis.UniversalClient, single string) ([]shard, error) {
	cluster, ok := source.(*goredis.ClusterClient)
	if !ok {
		// One server, one stream.
		return []shard{{id: "0", addr: single}}, nil
	}

	slots, err := cluster.ClusterSlots(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("ask the source which shards it has: %w", err)
	}
	seen := make(map[string]bool)
	var found []shard
	for _, slot := range slots {
		if len(slot.Nodes) == 0 {
			continue
		}
		id := strconv.Itoa(int(slot.Start)) + "-" + strconv.Itoa(int(slot.End))
		if seen[id] {
			continue
		}
		seen[id] = true
		found = append(found, shard{id: id, addr: slot.Nodes[0].Addr})
	}
	if len(found) == 0 {
		return nil, fmt.Errorf("the source reported no shards")
	}
	return found, nil
}

// runShard replicates one shard until it stops.
func (s *Syncer) runShard(ctx context.Context, sh shard, source, target goredis.UniversalClient,
	commands *commandTable, compareNow <-chan string) error {

	labels := s.labels()
	labels["shard"] = sh.id

	dir, err := s.bufferDir(sh.id)
	if err != nil {
		return err
	}
	buffer, err := OpenBuffer(BufferOptions{
		Dir:      dir,
		MaxBytes: s.cfg.RedisBufferBytes,
	})
	if err != nil {
		return err
	}
	defer buffer.Close()

	username, password := credentials(s.cfg.SourceConnection)
	connection := &link{
		opts: StreamOptions{
			Addr:     sh.addr,
			Username: username,
			Password: password,
		},
		buffer: buffer,
		shard:  sh.id,
		logger: s.logger,
		labels: labels,
	}
	defer connection.close()

	// A plain connection to this shard's master, used only to ask how much
	// history it keeps. The replication connection cannot answer: once it is a
	// replica link it takes no ordinary commands.
	node := goredis.NewClient(&goredis.Options{
		Addr:     sh.addr,
		Username: username,
		Password: password,
	})
	defer node.Close()

	// The link reports the lag against the source's own offset, which needs a
	// connection that still takes ordinary commands.
	connection.node = node

	positions := &Checkpoints{Target: target, TaskID: s.cfg.ID, Shard: sh.id}

	runner := &pipeline.Runner{
		Reader: &Reader{
			Shard:      sh.id,
			Link:       connection,
			Target:     target,
			Commands:   commands,
			Node:       node,
			Configured: s.cfg.RetentionWindow,
			Logger:     s.logger,
			Labels:     labels,
		},
		Applier: &Applier{
			Target:    target,
			Source:    source,
			Link:      connection,
			Positions: positions,
			Commands:  commands,
			Logger:    s.logger,
			Labels:    labels,
		},
		Snapshotter: &Snapshotter{
			Link:     connection,
			Node:     node,
			Source:   source,
			Target:   target,
			Logger:   s.logger,
			Labels:   labels,
			ReadRate: s.cfg.RedisSourceReadRate,
		},
		Checkpoints:   positions,
		CheckpointKey: sh.id,
		Opts: pipeline.Options{
			FlushInterval: s.cfg.RedisBatchWindow,
			// A command stream cannot be reordered.
			StreamOrder: true,
			Labels:      labels,
			Logger:      s.logger,
			Engine:      "Redis",
		},
	}

	// The comparison runs alongside, and its failures do not stop replication:
	// a gap in assurance is not a reason to create a gap in the copy.
	if s.cfg.RedisReconcileInterval >= 0 {
		reconciler := &Reconciler{
			Node:     node,
			Source:   source,
			Target:   target,
			Shard:    sh.id,
			Interval: s.cfg.RedisReconcileInterval,
			ReadRate: s.cfg.RedisSourceReadRate,
			Repair:   true,
			Now:      compareNow,
			Logger:   s.logger,
			Labels:   labels,
		}
		go reconciler.Run(ctx)
	}

	s.logger.Infof("[Redis] Shard %s: replicating %s", sh.id, sh.addr)
	return runner.Run(ctx)
}

// bufferDir is where one shard's stream is held.
func (s *Syncer) bufferDir(id string) (string, error) {
	root := s.cfg.RedisBufferDir
	if root == "" {
		return "", domain.Unrecoverable(
			"this task has no redis_buffer_dir. The replication stream is written to " +
				"disk so that a target that is briefly unavailable costs a partial " +
				"resync rather than a full one, and there is nowhere to write it")
	}
	// The task id keeps two tasks on one volume apart.
	return filepath.Join(root, strconv.Itoa(s.cfg.ID), sanitise(id)), nil
}

// sanitise keeps a shard identifier usable as a directory name.
func sanitise(id string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= '0' && r <= '9', r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z',
			r == '-', r == '_':
			return r
		}
		return '_'
	}, id)
}

func (s *Syncer) connect(ctx context.Context) (goredis.UniversalClient, goredis.UniversalClient, error) {
	var source, target goredis.UniversalClient

	err := resilience.Retry(ctx, 5, 2*time.Second, 2.0, func() error {
		var connErr error
		source, connErr = intRedis.GetRedisClient(s.cfg.SourceConnection)
		return connErr
	})
	if err != nil {
		return nil, nil, fmt.Errorf("connect to the source: %w", err)
	}
	err = resilience.Retry(ctx, 5, 2*time.Second, 2.0, func() error {
		var connErr error
		target, connErr = intRedis.GetRedisClient(s.cfg.TargetConnection)
		return connErr
	})
	if err != nil {
		source.Close()
		return nil, nil, fmt.Errorf("connect to the target: %w", err)
	}
	return source, target, nil
}

// warnAboutUnreplicatedThings says out loud what this does not carry.
//
// Both of these are silent in production and obvious in hindsight, which is the
// worst combination: the data arrives, the target looks right, and the thing that
// is missing is only discovered by the failover that needed it.
func (s *Syncer) warnAboutUnreplicatedThings(ctx context.Context, source, target goredis.UniversalClient) error {
	// Search indexes are not keys. FT.CREATE builds a definition that lives
	// outside the keyspace, so copying every key still leaves the target unable
	// to answer a single query.
	if indexes, err := source.Do(ctx, "FT._LIST").StringSlice(); err == nil && len(indexes) > 0 {
		s.logger.Warnf("[Redis] The source has %d search index(es) (%s). Index "+
			"definitions are not keys and are not replicated: create them on the "+
			"target as part of its deployment, or a failover will find the data "+
			"present and every query empty.", len(indexes), strings.Join(indexes, ", "))
	}

	// A source that starts its fork immediately never starts the command stream.
	//
	// Setting this to zero reads like an optimisation: do not wait five seconds
	// to batch several replicas into one fork, just go. On Redis 8.10.1 the
	// result is a master that accepts the replica, reports it online, and then
	// sends nothing whatsoever — no commands, not even the periodic ping. The
	// relay notices, because a silent source trips the idle timeout, but it
	// notices a minute later and after every reconnection.
	if delay, err := source.ConfigGet(ctx, "repl-diskless-sync-delay").Result(); err == nil {
		if delay["repl-diskless-sync-delay"] == "0" {
			s.logger.Warnf("[Redis] The source has repl-diskless-sync-delay set to 0. " +
				"On Redis 8.10.1 that makes a master accept a replica and then send it " +
				"nothing, so replication stalls after every full resync. Set it back to " +
				"a non-zero value — the delay it buys costs seconds, and this costs " +
				"the whole stream.")
		}
	}

	// A value serialised by a newer server cannot be restored into an older one,
	// so the target has to be upgraded first.
	sourceVersion := serverVersion(ctx, source)
	targetVersion := serverVersion(ctx, target)
	if sourceVersion != "" && targetVersion != "" && olderThan(targetVersion, sourceVersion) {
		return domain.Unrecoverable(
			"the target runs Redis %s and the source runs %s. RESTORE refuses a value "+
				"serialised by a newer server, so the first copy would fail part way "+
				"through. Upgrade the target first — that is the order for every "+
				"upgrade of this pair, not just this one",
			targetVersion, sourceVersion)
	}
	return nil
}

func serverVersion(ctx context.Context, client goredis.UniversalClient) string {
	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(info, "\n") {
		if value, ok := strings.CutPrefix(strings.TrimSpace(line), "redis_version:"); ok {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

// olderThan compares two dotted versions.
func olderThan(a, b string) bool {
	fieldsA, fieldsB := strings.Split(a, "."), strings.Split(b, ".")
	for i := 0; i < len(fieldsA) && i < len(fieldsB); i++ {
		numA, errA := strconv.Atoi(fieldsA[i])
		numB, errB := strconv.Atoi(fieldsB[i])
		if errA != nil || errB != nil {
			return false
		}
		if numA != numB {
			return numA < numB
		}
	}
	return false
}

// credentials pulls the user and password out of a DSN, for the replication
// connection which is made directly to a shard rather than through the client.
func credentials(connection string) (string, string) {
	parsed, err := url.Parse(connection)
	if err != nil || parsed.User == nil {
		return "", ""
	}
	password, _ := parsed.User.Password()
	return parsed.User.Username(), password
}

// claimDirection records which way this task replicates, on both endpoints, and
// keeps the claims refreshed for as long as it runs.
func (s *Syncer) claimDirection(ctx context.Context, source, target goredis.UniversalClient) (func(), error) {
	guard := &directionlock.Guard{
		TaskID: s.cfg.ID,
		Source: &directionlock.RedisStore{
			Client:  source,
			Address: dsn.Endpoint("redis", s.cfg.SourceConnection),
		},
		Target: &directionlock.RedisStore{
			Client:  target,
			Address: dsn.Endpoint("redis", s.cfg.TargetConnection),
		},
	}
	if err := guard.Acquire(ctx); err != nil {
		return nil, err
	}

	heartbeatCtx, stop := context.WithCancel(ctx)
	go guard.KeepAlive(heartbeatCtx, func(err error) {
		s.logger.Warnf("[Redis] Could not refresh the replication direction claim: %v", err)
	})
	return func() {
		stop()
		// The task's context is already cancelled by the time this runs, so the
		// release needs a deadline of its own.
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := guard.Release(releaseCtx); err != nil {
			s.logger.Warnf("[Redis] Could not release the replication direction claim: %v", err)
		}
	}, nil
}

// labels identify this task in the metrics. The endpoints are named without
// their credentials, because the exposition is scraped and stored.
func (s *Syncer) labels() metrics.Labels {
	return metrics.Labels{
		"task":   strconv.Itoa(s.cfg.ID),
		"engine": "redis",
		"source": dsn.Endpoint("redis", s.cfg.SourceConnection),
		"target": dsn.Endpoint("redis", s.cfg.TargetConnection),
	}
}
