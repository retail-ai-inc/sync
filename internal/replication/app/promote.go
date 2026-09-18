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
	"github.com/retail-ai-inc/sync/internal/replication/infra"
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

// PromoteTarget marks a task's target as promoted and stops every enabled task
// that replicates into it, this one included. It reports the tasks it stopped.
//
// The marker alone fenced only restarts: a task checks the direction when it
// starts and not again, so the task already running went on writing into a
// database the application had taken over, and the promotion looked complete
// while it did. The stop takes effect at the supervisor's next reload.
func PromoteTarget(ctx context.Context, id string, owner string) (stopped []int, err error) {
	promoted, err := loadTask(id)
	if err != nil {
		return nil, err
	}
	if err := withTargetLock(ctx, promoted, func(store directionlock.Store) error {
		return directionlock.Promote(ctx, store, owner)
	}); err != nil {
		return nil, err
	}
	return stopTasksWritingInto(promoted)
}

// StopAfterPromotion is a task that could not be stopped once the marker was
// written. The target is fenced against restarts, but this task may still be
// writing into it until somebody stops it by hand.
type StopAfterPromotion struct {
	TaskID int
	Err    error
}

func (e *StopAfterPromotion) Error() string {
	return fmt.Sprintf("the target is marked as promoted, but task %d, which replicates "+
		"into it, could not be stopped and may still be writing: %v", e.TaskID, e.Err)
}

func (e *StopAfterPromotion) Unwrap() error { return e.Err }

func stopTasksWritingInto(promoted config.SyncConfig) ([]int, error) {
	tasks, err := config.LoadSyncTasks()
	if err != nil {
		return nil, err
	}
	var stopped []int
	for _, id := range tasksWritingInto(tasks, promoted) {
		if err := infra.SetEnable(strconv.Itoa(id), false); err != nil {
			return stopped, &StopAfterPromotion{TaskID: id, Err: err}
		}
		stopped = append(stopped, id)
	}
	return stopped, nil
}

// tasksWritingInto picks the enabled tasks whose target is the promoted
// database: the same endpoint as the direction lock names it, without
// credentials, so two tasks connecting as different users still match.
func tasksWritingInto(tasks []config.SyncConfig, promoted config.SyncConfig) []int {
	target := dsn.Endpoint(promoted.Type, promoted.TargetConnection)
	var ids []int
	for _, task := range tasks {
		if task.Enable && dsn.Endpoint(task.Type, task.TargetConnection) == target {
			ids = append(ids, task.ID)
		}
	}
	return ids
}

// DemoteTarget clears the promotion. It is the deliberate step that lets
// replication into that database again, and it is not taken by accident: a
// process restart does not clear it and neither does time.
func DemoteTarget(ctx context.Context, id string) error {
	cfg, err := loadTask(id)
	if err != nil {
		return err
	}
	return withTargetLock(ctx, cfg, func(store directionlock.Store) error {
		return directionlock.Demote(ctx, store)
	})
}

// TargetPromotion reports whether a task's target carries a promotion marker.
func TargetPromotion(ctx context.Context, id string) (directionlock.Claim, bool, error) {
	var claim directionlock.Claim
	var promoted bool
	cfg, err := loadTask(id)
	if err != nil {
		return claim, false, err
	}
	err = withTargetLock(ctx, cfg, func(store directionlock.Store) error {
		var err error
		claim, promoted, err = directionlock.Promoted(ctx, store)
		return err
	})
	return claim, promoted, err
}

func loadTask(id string) (config.SyncConfig, error) {
	number, err := strconv.Atoi(id)
	if err != nil {
		return config.SyncConfig{}, fmt.Errorf("%q is not a task id", id)
	}
	return config.LoadSyncTask(number)
}

// withTargetLock opens the direction-claim store on a task's target, whatever
// engine it is, and hands it to the caller.
func withTargetLock(ctx context.Context, cfg config.SyncConfig, use func(directionlock.Store) error) error {
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
