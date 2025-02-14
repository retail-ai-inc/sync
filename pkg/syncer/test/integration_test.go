package test

import (
	"context"
	// "database/sql"
	// "encoding/json"
	// "fmt"
	"log"
	// "math/rand"
	"os"
	// "strings"
	// "sync"
	"testing"
	"time"
	// "bytes"

	_ "github.com/go-sql-driver/mysql" // MySQL / MariaDB
	_ "github.com/lib/pq"              // PostgreSQL
	_ "github.com/mattn/go-sqlite3"
	// intRedis "github.com/retail-ai-inc/sync/internal/db/redis"
	// goredis "github.com/redis/go-redis/v9"

	// "go.mongodb.org/mongo-driver/bson"
	// "go.mongodb.org/mongo-driver/mongo"
	// mongoopt "go.mongodb.org/mongo-driver/mongo/options"
	// "github.com/sirupsen/logrus"
	"github.com/retail-ai-inc/sync/pkg/config"
	// "github.com/retail-ai-inc/sync/pkg/syncer/common"
	"github.com/retail-ai-inc/sync/pkg/logger"
	"github.com/retail-ai-inc/sync/pkg/syncer"
	"github.com/retail-ai-inc/sync/pkg/utils"
)

type ConnDetail struct {
	User     string `json:"user"`
	Password string `json:"password"`
	Host     string `json:"host"`
	Port     string `json:"port"`
	Database string `json:"database"`
}
type TableMapping struct {
	SourceTable string `json:"sourceTable"`
	TargetTable string `json:"targetTable"`
}
type DbMapping struct {
	Tables []TableMapping `json:"tables"`
}
type TaskConfigJSON struct {
	Type               string      `json:"type"`
	TaskName           string      `json:"taskName"`
	Status             string      `json:"status"`
	SourceConn         ConnDetail  `json:"sourceConn"`
	TargetConn         ConnDetail  `json:"targetConn"`
	Mappings           []DbMapping `json:"mappings"`
	MySQLPositionPath  string      `json:"mysql_position_path"`
	MongoDBResumeToken string      `json:"mongodb_resume_token_path"`
	RedisPositionPath  string      `json:"redis_position_path"`
	PGReplicationSlot  string      `json:"pg_replication_slot"`
	PGPlugin           string      `json:"pg_plugin"`
	PGPositionPath     string      `json:"pg_position_path"`
	PGPublicationNames string      `json:"pg_publication_names"`
}

type SyncTaskRow struct {
	ID      int
	Enable  int
	CfgJSON string
	Parsed  TaskConfigJSON
}

var ctx = context.Background()

// -------------------------------------------------------------------
// TestSyncIntegration
// -------------------------------------------------------------------
func TestSyncIntegration(t *testing.T) {
	log.Println("[TEST] Starting sync as child process...")
	os.Setenv("SYNC_DB_PATH", "../../../sync.db")

	dbPath := os.Getenv("SYNC_DB_PATH")
	testTC01ConfigUpdate(dbPath);

	cfg := config.NewConfig()
	log := logger.InitLogger(cfg.LogLevel)

	for _, sc := range cfg.SyncConfigs {
		if !sc.Enable {
			continue
		}

		switch sc.Type {
		case "mongodb":
			s := syncer.NewMongoDBSyncer(sc, log)
			go s.Start(ctx)
		case "mysql":
			s := syncer.NewMySQLSyncer(sc, log)
			go s.Start(ctx)
		case "mariadb":
			s := syncer.NewMySQLSyncer(sc, log)
			go s.Start(ctx)
		case "postgresql":
			s := syncer.NewPostgreSQLSyncer(sc, log)
			go s.Start(ctx)
		case "redis":
			s := syncer.NewRedisSyncer(sc, log)
			go s.Start(ctx)
		}
	}
	time.Sleep(5 * time.Second)

	// t.Run("TC02_ConfigHotReload", func(tt *testing.T) {
	// 	testTC02ConfigHotReload(tt, sqlitePath)
	// })

	t.Run("TC03_MongoDBSync", func(tt *testing.T) {
		testTC03MongoDBSync(tt, cfg.SyncConfigs)
	})
	t.Run("TC04_MySQLSync", func(tt *testing.T) {
		testTC04MySQLSync(tt, cfg.SyncConfigs)
	})
	t.Run("TC05_MariaDBSync", func(tt *testing.T) {
		testTC05MariaDBSync(tt, cfg.SyncConfigs)
	})
	t.Run("TC06_PostgreSQLSync", func(tt *testing.T) {
		testTC06PostgreSQLSync(tt, cfg.SyncConfigs)
	})

	t.Run("TC07_RedisSync", func(tt *testing.T) {
		testTC07RedisSync(tt, cfg.SyncConfigs)
	})
	// t.Run("TC08_ErrorHandlingAndRetry", func(tt *testing.T) {
	// 	tt.Log("[Manual Test] Temporarily stop some target DB to see if sync logs show retry.")
	// })

	t.Run("TC09_RowCountMonitoring", func(tt *testing.T) {
		utils.StartRowCountMonitoring(ctx, cfg, log, time.Second*1)
	})

	t.Run("TC10_LogHookWriting", func(tt *testing.T) {
		testTC10LogHookWriting(tt)
	})
	// t.Run("TC11_MultiTaskConcurrency", func(tt *testing.T) {
	// 	testTC11MultiTaskConcurrency(tt, tasks)
	// })

	t.Run("TC12_StateStore_SaveLoad", func(tt *testing.T) {
		testTC12StateStore_SaveLoad(tt)
	})

	t.Run("TC13_Utils", func(tt *testing.T) {
		testTC13Utils(tt)
	})

	t.Run("TC14_AuthApi", func(tt *testing.T) {
		testTC14AuthApi(tt)
	})

	t.Run("TC15_SyncApi", func(tt *testing.T) {
		testTC15SyncApi(tt)
	})

	t.Run("TC16_MonitorApi", func(tt *testing.T) {
		testTC16MonitorApi(tt)
	})

	time.Sleep(3 * time.Second)

}
