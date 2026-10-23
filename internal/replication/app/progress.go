package app

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/mongodb"
	"github.com/retail-ai-inc/sync/internal/replication/infra/mysql"
	"github.com/retail-ai-inc/sync/internal/replication/infra/redis"
)

// TaskProgress reports how far a task's target has been written.
//
// It reads the target and the source directly rather than the metrics a running
// task publishes, which is the whole point: a task that has stopped publishes
// nothing, and a task that has stopped is when somebody is deciding whether the
// other region is safe to promote.
func TaskProgress(ctx context.Context, id string) (domain.Progress, error) {
	number, err := strconv.Atoi(id)
	if err != nil {
		return domain.Progress{}, fmt.Errorf("%q is not a task id", id)
	}

	cfg, err := config.LoadSyncTask(number)
	if err != nil {
		return domain.Progress{}, err
	}

	switch strings.ToLower(cfg.Type) {
	case "mysql", "mariadb":
		return mysql.Progress(ctx, cfg)
	case "mongodb":
		return mongodb.Progress(ctx, cfg)
	case "redis":
		return redis.Progress(ctx, cfg)
	default:
		return domain.Progress{}, fmt.Errorf("a %s task cannot report a position: only "+
			"MySQL, MongoDB and Redis record one", cfg.Type)
	}
}
