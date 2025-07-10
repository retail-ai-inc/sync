package test

import (
	"context"
	// "encoding/json"
	// "fmt"
	// "math/rand"
	// "strings"
	// "sync"
	"os"
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
	dbPath := os.Getenv("SYNC_DB_PATH")

	testTC01ConfigUpdate(dbPath)

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

	t.Run("TC03_MongoDBSync", func(t *testing.T) {
		t.Parallel()
		testTC03MongoDBSync(t, cfg.SyncConfigs)
	})
	t.Run("TC04_MySQLSync", func(t *testing.T) {
		t.Parallel()
		testTC04MySQLSync(t, cfg.SyncConfigs)
	})
	t.Run("TC05_MariaDBSync", func(t *testing.T) {
		t.Parallel()
		testTC05MariaDBSync(t, cfg.SyncConfigs)
	})
	t.Run("TC06_PostgreSQLSync", func(t *testing.T) {
		t.Parallel()
		testTC06PostgreSQLSync(t, cfg.SyncConfigs)
	})
	t.Run("TC07_RedisSync", func(t *testing.T) {
		t.Parallel()
		testTC07RedisSync(t, cfg.SyncConfigs)
	})

	t.Run("TC10_LogHookWriting", func(t *testing.T) {
		testTC10LogHookWriting(t)
	})

	t.Run("TC12_StateStore_SaveLoad", func(t *testing.T) {
		testTC12StateStore_SaveLoad(t)
	})

	t.Run("TC13_Utils", func(t *testing.T) {
		testTC13Utils(t)
	})

	t.Run("TC14_AuthApi", func(t *testing.T) {
		t.Parallel()
		testTC14AuthApi(t)
	})

	t.Run("TC15_SyncApi", func(t *testing.T) {
		testTC15SyncApi(t)
	})

	t.Run("TC16_MonitorApi", func(t *testing.T) {
		testTC16MonitorApi(t)
	})

	t.Run("TC17_OAuthConfig", func(t *testing.T) {
		testOAuthConfig(t)
	})

	t.Run("TC18_UserManagement", func(t *testing.T) {
		testUserManagementApi(t)
	})

	// t.Run("TC09_RowCountMonitoring", func(t *testing.T) {
	// 	monitorCtx, cancel := context.WithCancel(ctx)
	// 	utils.StartRowCountMonitoring(monitorCtx, cfg, log, 100*time.Millisecond)
	// 	time.Sleep(time.Millisecond * 100)
	// 	cancel()
	// 	time.Sleep(time.Millisecond * 50)
	// })
	// t.Run("TC19_DBConnections", func(t *testing.T) {
	// 	testDBConnections(t)
	// })

	t.Run("TC25_DBUtils", func(t *testing.T) {
		testDBUtils(t)
	})

	// t.Run("TC20_ErrorHandling", func(t *testing.T) {
	// testTC20ErrorHandling(t, cfg.SyncConfigs)
	// })
	t.Run("TC21_Performance", func(t *testing.T) {
		testTC21Performance(t, cfg.SyncConfigs)
	})
	t.Run("TC22_DataConsistency", func(t *testing.T) {
		testTC22DataConsistency(t, cfg.SyncConfigs)
	})
	t.Run("TC23_ConfigurationChanges", func(t *testing.T) {
		testTC23ConfigurationChanges(t, cfg.SyncConfigs)
	})

	// New test cases for improved coverage
	t.Run("TC26_TimeUtils", func(t *testing.T) {
		TestTimeUtilsIntegration(t)
	})

	t.Run("TC27_MongoDBBuffer", func(t *testing.T) {
		TestMongoDBSyncerCreation(t)
	})

	t.Run("TC28_APIBackup", func(t *testing.T) {
		TestAPIBackupIntegration(t)
	})

	t.Run("TC29_CronManager", func(t *testing.T) {
		TestCronManagerIntegration(t)
	})

	t.Run("TC30_BackupExecutor", func(t *testing.T) {
		TestBackupExecutorIntegration(t)
	})

	time.Sleep(3 * time.Second)

}
