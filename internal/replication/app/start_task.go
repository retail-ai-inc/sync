package app

import (
	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/infra"
	"github.com/retail-ai-inc/sync/internal/replication/infra/mongodb"
	"github.com/retail-ai-inc/sync/internal/replication/infra/mysql"
	"github.com/retail-ai-inc/sync/internal/replication/infra/postgresql"
	"github.com/retail-ai-inc/sync/internal/replication/infra/redis"
	"github.com/sirupsen/logrus"
)

// StartTask marks a task as running.
//
// It only flips the stored status: the syncer that does the replicating is
// started once, at process start, from the configuration as it was then. A task
// started through this endpoint does not begin replicating until the process
// restarts (T-009).
func StartTask(id string) error { return infra.SetEnable(id, true) }

// StopTask marks a task as stopped. The running syncer is not signalled either
// (T-010).
func StopTask(id string) error { return infra.SetEnable(id, false) }

// The four engine adapters, constructed by the process entry point once per
// configured task.
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
