package test

import (
	"fmt"
	"strings"
	"testing"
	"time"
	// "github.com/redis/go-redis/v9"
	intRedis "github.com/retail-ai-inc/sync/internal/db/redis"
	"github.com/retail-ai-inc/sync/pkg/config"
)

func testTC07RedisSync(t *testing.T, syncConfigs []config.SyncConfig) {
	var redisTasks []config.SyncConfig
	for _, syncConfig := range syncConfigs {
		if strings.ToLower(syncConfig.Type) == "redis" && syncConfig.Enable {
			redisTasks = append(redisTasks, syncConfig)
		}
	}
	if len(redisTasks) == 0 {
		t.Skip("[TC07] no enabled Redis tasks found.")
	}

	syncConfig := redisTasks[0]

	srcDSN := syncConfig.SourceConnection
	tgtDSN := syncConfig.TargetConnection

	if len(syncConfig.Mappings) == 0 || len(syncConfig.Mappings[0].Tables) == 0 {
		t.Skip("[TC07] no table mapping.")
	}
	sourceTable := syncConfig.Mappings[0].Tables[0].SourceTable
	targetTable := syncConfig.Mappings[0].Tables[0].TargetTable

	srcClient, err := intRedis.GetRedisClient(srcDSN)
	if err != nil {
		t.Fatalf("[TC07] connect src fail: %v", err)
	}

	tgtClient, err := intRedis.GetRedisClient(tgtDSN)
	if err != nil {
		t.Fatalf("[TC07] connect tgt fail: %v", err)
	}

	defer srcClient.Close()
	defer tgtClient.Close()

	time.Sleep(1 * time.Second)

	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("user:%d", i)
		value := fmt.Sprintf("User_%d", i)
		err := srcClient.Set(ctx, key, value, 0).Err()
		if err != nil {
			t.Fatalf("[TC07] insert key=%s fail: %v", key, err)
		}
	}

	time.Sleep(5 * time.Second)
	compareRedisDataConsistency(t, srcClient, tgtClient, sourceTable, targetTable)

	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("user:%d", i)
		newValue := fmt.Sprintf("UpdatedUser_%d", i)
		err := srcClient.Set(ctx, key, newValue, 0).Err()
		if err != nil {
			t.Fatalf("[TC07] update key=%s fail: %v", key, err)
		}
	}
	time.Sleep(2 * time.Second)
	compareRedisDataConsistency(t, srcClient, tgtClient, sourceTable, targetTable)

	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("user:%d", i)
		err := srcClient.Del(ctx, key).Err()
		if err != nil {
			t.Fatalf("[TC07] delete key=%s fail: %v", key, err)
		}
	}
	time.Sleep(2 * time.Second)
	compareRedisDataConsistency(t, srcClient, tgtClient, sourceTable, targetTable)
}
