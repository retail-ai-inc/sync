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
)

// TaskRowCounts counts every replicated object on both sides, on demand.
//
// Asked for rather than collected. The periodic monitor loops over the tables a
// task names and does nothing at all for one that names none -- which is all
// four tasks here, because they replicate whole databases -- and making it
// discover them instead would put an exact count of every object on a
// monitoring interval. That is affordable for MySQL and not for MongoDB: 116
// collections on both sides of the sharded source took about five minutes,
// because a sharded count has to ask every shard and the metadata shortcut is
// stale. So the count happens when somebody asks for it and waits.
func TaskRowCounts(ctx context.Context, id string) (domain.RowCounts, error) {
	number, err := strconv.Atoi(id)
	if err != nil {
		return domain.RowCounts{}, fmt.Errorf("%q is not a task id", id)
	}

	cfg, err := config.LoadSyncTask(number)
	if err != nil {
		return domain.RowCounts{}, err
	}

	switch strings.ToLower(cfg.Type) {
	case "mysql", "mariadb":
		return mysql.RowCounts(ctx, cfg)
	case "mongodb":
		return mongodb.RowCounts(ctx, cfg)
	default:
		return domain.RowCounts{}, fmt.Errorf("a %s task has no tables to count: only "+
			"MySQL and MongoDB are counted this way", cfg.Type)
	}
}
