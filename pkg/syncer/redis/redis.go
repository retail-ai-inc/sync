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
	"github.com/retail-ai-inc/sync/pkg/utils"
	"github.com/sirupsen/logrus"
)

type RedisSyncer struct {
	cfg         config.SyncConfig
	logger      logrus.FieldLogger
	source      *goredis.Client
	target      *goredis.Client
	lastExecErr int32

	positionPath string
}

func NewRedisSyncer(cfg config.SyncConfig, logger *logrus.Logger) *RedisSyncer {
	return &RedisSyncer{
		cfg:          cfg,
		logger:       logger.WithField("sync_task_id", cfg.ID),
		positionPath: cfg.RedisPositionPath,
	}
}

func (r *RedisSyncer) Start(ctx context.Context) {
	r.logger.Info("[Redis] Starting synchronization...")

	var err error
	err = utils.Retry(5, 2*time.Second, 2.0, func() error {
		var connErr error
		r.source, connErr = intRedis.GetRedisClient(r.cfg.SourceConnection)
		return connErr
	})
	if err != nil {
		r.logger.Errorf("[Redis] Failed to connect to source after retries: %v", err)
		return
	}
	err = utils.Retry(5, 2*time.Second, 2.0, func() error {
		var connErr error
		r.target, connErr = intRedis.GetRedisClient(r.cfg.TargetConnection)
		return connErr
	})
	if err != nil {
		r.logger.Errorf("[Redis] Failed to connect to target after retries: %v", err)
		return
	}
	defer r.source.Close()
	defer r.target.Close()

	if err := r.doInitialSync(ctx); err != nil {
		r.logger.Errorf("[Redis] doInitialSync error: %v", err)
	}
	r.logger.Info("[Redis] Initial full sync done.")

	r.logger.Info("[Redis] Subscribing keyspace notifications...")
	go r.watchKeyspaceChanges(ctx)

	streamName := r.cfg.Mappings[0].Tables[0].SourceTable
	lastID := r.loadStreamPosition()
	if lastID == "" {
		lastID = "0-0"
	}
	groupName := "sync_group"
	err = r.source.XGroupCreateMkStream(ctx, streamName, groupName, lastID).Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		r.logger.Errorf("[Redis] XGroupCreate fail => %v", err)
		return
	}
	r.logger.Infof("[Redis] Using group=%s on stream=%s from lastID=%s", groupName, streamName, lastID)

	r.logger.Info("[Redis] Starting stream-based replication (if any) ...")
	r.watchStreamChanges(ctx, streamName, groupName, lastID)
	r.logger.Info("[Redis] Stream-based replication ended, synchronization finished.")
}

func (r *RedisSyncer) doInitialSync(ctx context.Context) error {
	r.logger.Info("[Redis] Starting initial full sync...")
	var cursor uint64
	const batchSize = 100

	for {
		keys, nextCursor, err := r.source.Scan(ctx, cursor, "*", batchSize).Result()
		if err != nil {
			return fmt.Errorf("SCAN fail at cursor=%d: %v", cursor, err)
		}
		if len(keys) > 0 {
			if err2 := r.copyKeys(ctx, keys); err2 != nil {
				r.logger.Errorf("[Redis] copyKeys error: %v", err2)
				atomic.StoreInt32(&r.lastExecErr, 1)
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
	for _, k := range keys {
		if err := r.copyFullKey(ctx, k); err != nil {
			r.logger.Errorf("[Redis] copyFullKey fail => key=%s, error=%v", k, err)
			atomic.StoreInt32(&r.lastExecErr, 1)
		} else {
			r.logger.Infof("[Redis][COPY] key=%s copied successfully", k)
		}
	}
	return nil
}

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

func (r *RedisSyncer) watchKeyspaceChanges(ctx context.Context) {
	pubsub := r.source.PSubscribe(ctx, "__keyspace@0__:*")
	if pubsub == nil {
		r.logger.Error("[Redis] PSubscribe returned nil => no keyspace subscription.")
		return
	}
	defer pubsub.Close()
	r.logger.Info("[Redis] Keyspace subscription started on DB0.")

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

func (r *RedisSyncer) handleKeyspaceChange(ctx context.Context, ch, op string) {
	parts := strings.SplitN(ch, ":", 2)
	if len(parts) < 2 {
		r.logger.Debugf("[Redis] invalid keyspace channel => %s", ch)
		return
	}
	key := parts[1]
	srcType, err := r.source.Type(ctx, key).Result()
	if err != nil {
		r.logger.Errorf("[Redis] Failed to get type for key=%s: %v", key, err)
		return
	}

	switch strings.ToLower(op) {
	case "del":
		r.logger.Debugf("[Redis][DELETE] command=\"DEL key=%s\"", key)
		if err2 := r.target.Del(ctx, key).Err(); err2 != nil {
			r.logger.Errorf("[Redis][DELETE] key=%s error=%v", key, err2)
		} else {
			r.logger.Infof("[Redis][DELETE] key=%s success", key)
		}

	case "set":
		switch srcType {
		case "string":
			val, err := r.source.Get(ctx, key).Result()
			if err == nil {
				r.target.Set(ctx, key, val, 0)
			}
		case "hash":
			fields, err := r.source.HGetAll(ctx, key).Result()
			if err == nil {
				r.target.HSet(ctx, key, fields)
			}
		default:
			r.logger.Debugf("[Redis][UPSERT] Unsupported type for key=%s: %s", key, srcType)
		}

	default:
		r.logger.Debugf("[Redis][UPSERT] command=\"FULLCOPY key=%s\"", key)
		r.copyFullKey(ctx, key)
	}
}

func (r *RedisSyncer) watchStreamChanges(ctx context.Context, streamName, groupName, lastID string) {
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
						r.logger.Debugf("[Redis][STREAM] command=\"XINSERT stream=%s msgID=%s\"", streamName, msg.ID)
						if err2 := r.processStreamMessage(ctx, msg); err2 == nil {
							r.source.XAck(ctx, streamName, groupName, msg.ID)
							lastID = msg.ID
							r.saveStreamPosition(lastID)
						} else {
							r.logger.Errorf("[Redis] processStreamMessage fail => skip XACK => %v", err2)
							atomic.StoreInt32(&r.lastExecErr, 1)
						}
					}
				}
			}
		}
	}()
	wg.Wait()
}

func (r *RedisSyncer) processStreamMessage(ctx context.Context, msg goredis.XMessage) error {
	hashKey := fmt.Sprintf("msg:%s", msg.ID)
	pipe := r.target.Pipeline()
	fields := make(map[string]interface{})
	for k, v := range msg.Values {
		fields[k] = v
	}

	// Check the type of the key before deciding whether it's an update or a new key
	srcType, err := r.source.Type(ctx, hashKey).Result()
	if err != nil {
		r.logger.Errorf("[Redis] Failed to get type for key=%s: %v", hashKey, err)
		return err
	}

	// Handle based on the type of the source key
	switch srcType {
	case "string":
		// Use SET to update the value in the target
		pipe.Set(ctx, hashKey, fields, 0)
	case "hash":
		// Use HSET to update the hash in the target
		pipe.HSet(ctx, hashKey, fields)
	default:
		r.logger.Debugf("[Redis][STREAM] Unsupported type for key=%s: %s", hashKey, srcType)
		return fmt.Errorf("unsupported key type %s for key=%s", srcType, hashKey)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		r.logger.Errorf("[Redis][STREAM] processStreamMessage => id=%s error=%v", msg.ID, err)
		return err
	}

	r.logger.Infof("[Redis][STREAM] id=%s => stored as hashKey=%s fieldsCount=%d", msg.ID, hashKey, len(fields))
	return nil
}

func (r *RedisSyncer) loadStreamPosition() string {
	if r.positionPath == "" {
		return ""
	}
	data, err := os.ReadFile(r.positionPath)
	if err != nil {
		r.logger.Infof("[Redis] No stream position file => %v", err)
		return ""
	}
	return strings.TrimSpace(string(data))
}

func (r *RedisSyncer) saveStreamPosition(id string) {
	if r.positionPath == "" {
		return
	}
	dir := filepath.Dir(r.positionPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		r.logger.Errorf("[Redis] mkdir fail: %v", err)
		return
	}
	if err := os.WriteFile(r.positionPath, []byte(id), 0644); err != nil {
		r.logger.Errorf("[Redis] write position fail: %v", err)
	}
}
