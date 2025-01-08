package syncer

import (
	"github.com/retail-ai-inc/sync/pkg/config"
	"github.com/retail-ai-inc/sync/pkg/syncer/mariadb"
	"github.com/retail-ai-inc/sync/pkg/syncer/mongodb"
	"github.com/retail-ai-inc/sync/pkg/syncer/mysql"
	"github.com/retail-ai-inc/sync/pkg/syncer/postgresql"
	"github.com/retail-ai-inc/sync/pkg/syncer/redis"
	"github.com/sirupsen/logrus"
)

func NewMongoDBSyncer(cfg config.SyncConfig, logger *logrus.Logger) *mongodb.MongoDBSyncer {
	return mongodb.NewMongoDBSyncer(cfg, logger)
}

func NewMySQLSyncer(cfg config.SyncConfig, logger *logrus.Logger) *mysql.MySQLSyncer {
	return mysql.NewMySQLSyncer(cfg, logger)
}

func NewMariaDBSyncer(cfg config.SyncConfig, logger *logrus.Logger) *mariadb.MariaDBSyncer {
	return mariadb.NewMariaDBSyncer(cfg, logger)
}

func NewPostgreSQLSyncer(cfg config.SyncConfig, logger *logrus.Logger) *postgresql.PostgreSQLSyncer {
	return postgresql.NewPostgreSQLSyncer(cfg, logger)
}

func NewRedisSyncer(cfg config.SyncConfig, logger *logrus.Logger) *redis.RedisSyncer {
	return redis.NewRedisSyncer(cfg, logger)
}