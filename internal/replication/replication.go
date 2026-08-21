package replication

import (
	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/mongodb"
	"github.com/retail-ai-inc/sync/internal/replication/mysql"
	"github.com/retail-ai-inc/sync/internal/replication/postgresql"
	"github.com/retail-ai-inc/sync/internal/replication/redis"
	"github.com/sirupsen/logrus"
)

func NewMongoDBSyncer(cfg config.SyncConfig, globalConfig *config.Config, logger *logrus.Logger) *mongodb.MongoDBSyncer {
	return mongodb.NewMongoDBSyncer(cfg, globalConfig, logger)
}

func NewMySQLSyncer(cfg config.SyncConfig, logger *logrus.Logger) *mysql.MySQLSyncer {
	return mysql.NewMySQLSyncer(cfg, logger)
}

func NewPostgreSQLSyncer(cfg config.SyncConfig, logger *logrus.Logger) *postgresql.PostgreSQLSyncer {
	return postgresql.NewPostgreSQLSyncer(cfg, logger)
}

func NewRedisSyncer(cfg config.SyncConfig, logger *logrus.Logger) *redis.RedisSyncer {
	return redis.NewRedisSyncer(cfg, logger)
}
