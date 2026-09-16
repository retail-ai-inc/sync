package app

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	intRedis "github.com/retail-ai-inc/sync/internal/platform/dbconn/redis"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/internal/replication/infra/directionlock"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Promoting a target, and taking it back.
//
// A real failover writes nothing: somebody repoints the application at Osaka
// and no task is involved. When Tokyo comes back the supervisor starts the
// task again, and the only thing that can stop it replicating over everything
// Osaka has taken since is a marker on Osaka that outlives both processes.
// That marker is written here.

// PromoteTarget marks a task's target as promoted, so no task will replicate
// into it until the promotion is cleared.
func PromoteTarget(ctx context.Context, id string, owner string) error {
	return withTargetLock(ctx, id, func(store directionlock.Store) error {
		return directionlock.Promote(ctx, store, owner)
	})
}

// DemoteTarget clears the promotion. It is the deliberate step that lets
// replication into that database again, and it is not taken by accident: a
// process restart does not clear it and neither does time.
func DemoteTarget(ctx context.Context, id string) error {
	return withTargetLock(ctx, id, func(store directionlock.Store) error {
		return directionlock.Demote(ctx, store)
	})
}

// TargetPromotion reports whether a task's target carries a promotion marker.
func TargetPromotion(ctx context.Context, id string) (directionlock.Claim, bool, error) {
	var claim directionlock.Claim
	var promoted bool
	err := withTargetLock(ctx, id, func(store directionlock.Store) error {
		var err error
		claim, promoted, err = directionlock.Promoted(ctx, store)
		return err
	})
	return claim, promoted, err
}

// withTargetLock opens the direction-claim store on a task's target, whatever
// engine it is, and hands it to the caller.
func withTargetLock(ctx context.Context, id string, use func(directionlock.Store) error) error {
	number, err := strconv.Atoi(id)
	if err != nil {
		return fmt.Errorf("%q is not a task id", id)
	}
	cfg, err := config.LoadSyncTask(number)
	if err != nil {
		return err
	}

	switch strings.ToLower(cfg.Type) {
	case "mysql", "mariadb":
		db, err := sql.Open("mysql", cfg.TargetConnection)
		if err != nil {
			return fmt.Errorf("connect to the target: %w", err)
		}
		defer db.Close()
		return use(&directionlock.SQLStore{
			DB:      db,
			Schema:  dsn.GetDatabaseName(cfg.Type, cfg.TargetConnection),
			Address: dsn.Endpoint(cfg.Type, cfg.TargetConnection),
		})

	case "postgresql", "postgres":
		db, err := sql.Open("postgres", cfg.TargetConnection)
		if err != nil {
			return fmt.Errorf("connect to the target: %w", err)
		}
		defer db.Close()
		// No schema: PostgreSQL's schema is not its database, and the engine's
		// own guard addresses the table unqualified for that reason.
		return use(&directionlock.SQLStore{
			DB:                   db,
			Address:              dsn.Endpoint(cfg.Type, cfg.TargetConnection),
			NumberedPlaceholders: true,
		})

	case "mongodb":
		client, err := mongo.Connect(options.Client().ApplyURI(cfg.TargetConnection))
		if err != nil {
			return fmt.Errorf("connect to the target: %w", err)
		}
		defer func() { _ = client.Disconnect(ctx) }()
		return use(&directionlock.MongoStore{
			Database: client.Database(dsn.GetDatabaseName(cfg.Type, cfg.TargetConnection)),
			Address:  dsn.Endpoint(cfg.Type, cfg.TargetConnection),
		})

	case "redis":
		client, err := intRedis.GetRedisClient(cfg.TargetConnection)
		if err != nil {
			return fmt.Errorf("connect to the target: %w", err)
		}
		defer client.Close()
		return use(&directionlock.RedisStore{
			Client:  client,
			Address: dsn.Endpoint("redis", cfg.TargetConnection),
		})
	}

	return fmt.Errorf("a %s task has no direction claim to promote", cfg.Type)
}
