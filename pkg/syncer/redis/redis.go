package redis

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"
	intRedis "github.com/retail-ai-inc/sync/internal/db/redis"
	"github.com/retail-ai-inc/sync/pkg/config"
	"github.com/sirupsen/logrus"
)

type RedisSyncer struct {
	cfg         config.SyncConfig
	logger      *logrus.Logger
	source      *goredis.Client
	target      *goredis.Client
	lastExecErr int32

	// Streams replication checkpoint
	positionPath string
}

func NewRedisSyncer(cfg config.SyncConfig, logger *logrus.Logger) *RedisSyncer {
	return &RedisSyncer{
		cfg:          cfg,
		logger:       logger,
		positionPath: cfg.RedisPositionPath,
	}
}

func (r *RedisSyncer) Start(ctx context.Context) {
	var err error
	r.source, err = intRedis.GetRedisClient(r.cfg.SourceConnection)
	if err != nil {
		r.logger.Fatalf("[Redis] Failed to connect to source: %v", err)
	}
	r.target, err = intRedis.GetRedisClient(r.cfg.TargetConnection)
	if err != nil {
		r.logger.Fatalf("[Redis] Failed to connect to target: %v", err)
	}
	defer func() {
		_ = r.source.Close()
		_ = r.target.Close()
	}()

	r.logger.Info("[Redis] Starting initial full sync...")
	if err := r.initialSync(ctx); err != nil {
		r.logger.Errorf("[Redis] initialSync error: %v", err)
	}
	r.logger.Info("[Redis] Initial sync done.")

	r.logger.Info("[Redis] Subscribing to Keyspace Notifications (for all operations) ...")
	go r.subscribeKeyspace(ctx)

	r.logger.Info("[Redis] Starting stream-based replication...")
	r.streamSync(ctx)
	r.logger.Info("[Redis] Stream-based replication ended.")
}

func (r *RedisSyncer) initialSync(ctx context.Context) error {
	var cursor uint64
	const batchSize = 100

	for {
		keys, nextCursor, err := r.source.Scan(ctx, cursor, "*", batchSize).Result()
		if err != nil {
			if strings.Contains(err.Error(), "context canceled") {
				r.logger.Warnf("[Redis] SCAN got context canceled, normal exit: %v", err)
				return nil
			}
			return fmt.Errorf("SCAN fail at cursor=%d: %v", cursor, err)
		}
		if len(keys) > 0 {
			if e2 := r.copyKeys(ctx, keys); e2 != nil {
				r.logger.Errorf("[Redis] copyKeys error: %v", e2)
			}
		}
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
	return nil
}

func (r *RedisSyncer) copyKeys(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	for _, k := range keys {
		if err := r.doFullCopyOfKey(ctx, k); err != nil {
			r.logger.Errorf("[Redis] doFullCopyOfKey fail key=%s: %v", k, err)
			atomic.StoreInt32(&r.lastExecErr, 1)
		}
	}
	return nil
}

/*
doFullCopyOfKey:
	- Get TTL (if ttl < 0 and not -1, it means the key does not exist or has expired, so do not process)
	- If ttl == -1, treat it as indefinite => use 0 for RESTORE
	- DUMP key => if empty string, it means the key does not exist
	- RESTORE REPLACE on the target side
*/
func (r *RedisSyncer) doFullCopyOfKey(ctx context.Context, key string) error {
	ttl, err := r.source.TTL(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("get TTL fail: %v", err)
	}
	// Check if the key exists or has expired
	if ttl < 0 && ttl != -1 {
		r.logger.Debugf("[Redis] key=%s seems not exist or expired, skip copy", key)
		return nil
	}

	dumpedVal, err := r.source.Dump(ctx, key).Result()
	if err != nil && err != goredis.Nil {
		return fmt.Errorf("DUMP fail for key=%s: %v", key, err)
	}
	if dumpedVal == "" {
		r.logger.Debugf("[Redis] key=%s not exist or empty DUMP in source, skip copy", key)
		return nil
	}

	var expireMs int64
	if ttl == -1 {
		expireMs = 0 // no expiry
	} else {
		expireMs = ttl.Milliseconds()
		if expireMs < 0 {
			expireMs = 0
		}
	}

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

	r.logger.Debugf("[Redis] Copied key=%s using DUMP/RESTORE, TTL=%vms", key, expireMs)
	return nil
}

/*
Incremental Sync: subscribeKeyspace
Capture all types of events: as long as it's not a del, execute doFullCopyOfKey once
*/
func (r *RedisSyncer) subscribeKeyspace(ctx context.Context) {
	pubsub := r.source.PSubscribe(ctx, "__keyspace@0__:*")
	if pubsub == nil {
		r.logger.Error("[Redis] PSubscribe returned nil.")
		return
	}
	defer pubsub.Close()

	r.logger.Info("[Redis] Keyspace subscription started successfully (DB0).")

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
			r.logger.Infof("[Redis] Keyspace notification: channel=%s payload=%s", msg.Channel, msg.Payload)
			r.handleKeyspaceEvent(ctx, msg.Channel, msg.Payload)
		}
	}
}

func (r *RedisSyncer) handleKeyspaceEvent(ctx context.Context, ch, op string) {
	parts := strings.SplitN(ch, ":", 2)
	if len(parts) < 2 {
		r.logger.Warnf("[Redis] invalid keyspace channel: %s", ch)
		return
	}
	key := parts[1]

	r.logger.Infof("[Redis] KeyspaceEvent op=%s key=%s => doFullCopyOfKey", op, key)

	switch strings.ToLower(op) {
	case "del":
		if err2 := r.target.Del(ctx, key).Err(); err2 != nil {
			r.logger.Errorf("[Redis] handleKeyspaceEvent DEL fail key=%s: %v", key, err2)
		} else {
			r.logger.Infof("[Redis] Replicated DEL key=%s", key)
		}
	default:
		if err := r.doFullCopyOfKey(ctx, key); err != nil {
			r.logger.Errorf("[Redis] handleKeyspaceEvent fullCopy fail key=%s: %v", key, err)
		}
	}
}

// 以下与Stream同步相关逻辑不变 (示例略)

// streamSync: 处理自定义场景中 Mappings->StreamName 的增量同步逻辑
func (r *RedisSyncer) streamSync(ctx context.Context) {
	if len(r.cfg.Mappings) == 0 || len(r.cfg.Mappings[0].Tables) == 0 {
		r.logger.Warn("[Redis] No mapping found, skip streamSync.")
		return
	}
	streamName := r.cfg.Mappings[0].Tables[0].SourceTable

	lastID := r.loadStreamPosition()
	if lastID == "" {
		lastID = "0-0"
	}
	r.logger.Infof("[Redis] Using lastID=%s for streamName=%s", lastID, streamName)

	groupName := "sync_group"
	err := r.source.XGroupCreateMkStream(ctx, streamName, groupName, lastID).Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		r.logger.Errorf("[Redis] XGroupCreate fail: %v", err)
		return
	}
	r.logger.Infof("[Redis] Using group=%s on stream=%s", groupName, streamName)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				streams, xerr := r.source.XReadGroup(ctx, &goredis.XReadGroupArgs{
					Group:    groupName,
					Consumer: "sync_consumer_1",
					Streams:  []string{streamName, lastID},
					Count:    10,
					Block:    2000 * time.Millisecond,
				}).Result()
				if xerr != nil && xerr != goredis.Nil {
					if strings.Contains(xerr.Error(), "context canceled") {
						r.logger.Warnf("[Redis] XReadGroup context canceled, normal exit: %v", xerr)
						return
					}
					r.logger.Errorf("[Redis] XReadGroup error: %v", xerr)
					continue
				}
				if len(streams) == 0 {
					continue
				}
				for _, st := range streams {
					for _, msg := range st.Messages {
						r.logger.Infof("[Redis] Received stream msg: id=%s values=%v", msg.ID, msg.Values)
						if err2 := r.applyStreamMsg(ctx, msg); err2 == nil {
							r.source.XAck(ctx, streamName, groupName, msg.ID)
							lastID = msg.ID
							r.saveStreamPosition(lastID)
						} else {
							r.logger.Errorf("[Redis] applyStreamMsg fail => skip XACK: %v", err2)
						}
					}
				}
			}
		}
	}()
	wg.Wait()
	r.logger.Info("[Redis] streamSync goroutine ended.")
}

func (r *RedisSyncer) applyStreamMsg(ctx context.Context, msg goredis.XMessage) error {
	hashKey := fmt.Sprintf("msg:%s", msg.ID)
	pipe := r.target.Pipeline()

	fields := make(map[string]interface{})
	for k, v := range msg.Values {
		fields[k] = v
	}
	r.logger.Infof("[Redis] Writing stream msg id=%s => hashKey=%s fields=%v", msg.ID, hashKey, fields)

	pipe.HSet(ctx, hashKey, fields)
	_, err := pipe.Exec(ctx)
	if err != nil {
		r.logger.Errorf("[Redis] pipeline exec in applyStreamMsg fail: %v", err)
		atomic.StoreInt32(&r.lastExecErr, 1)
		return err
	}
	atomic.StoreInt32(&r.lastExecErr, 0)
	return nil
}

func (r *RedisSyncer) loadStreamPosition() string {
	if r.positionPath == "" {
		return ""
	}
	data, err := os.ReadFile(r.positionPath)
	if err != nil && !os.IsNotExist(err) {
		r.logger.Warnf("[Redis] read position file error: %v", err)
		return ""
	}
	return strings.TrimSpace(string(data))
}

func (r *RedisSyncer) saveStreamPosition(id string) {
	if r.positionPath == "" {
		return
	}
	dir := filepath.Dir(r.positionPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		r.logger.Errorf("[Redis] MkdirAll fail: %v", err)
		return
	}
	if err := os.WriteFile(r.positionPath, []byte(id), 0o644); err != nil {
		r.logger.Errorf("[Redis] write position fail: %v", err)
	}
}