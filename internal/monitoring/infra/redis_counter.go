package infra

import (
	"strings"
	// "github.com/sirupsen/logrus"
	"context"

	"sync"

	goredis "github.com/redis/go-redis/v9"
	"github.com/retail-ai-inc/sync/internal/platform/config"
	dbconnredis "github.com/retail-ai-inc/sync/internal/platform/dbconn/redis"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/sirupsen/logrus"
)

func CountAndLogRedis(ctx context.Context, sc config.SyncConfig, log *logrus.Logger) {
	dbType := strings.ToUpper(sc.Type)

	// Through the shared opener, which reads a DSN naming more than one host as
	// a cluster. This used to be ParseURL and NewClient: go-redis's ParseURL
	// takes a single host, so a cluster DSN either failed to parse — and the
	// comparison was skipped with one line in the log — or was silently reduced
	// to its first node.
	srcClient, err := dbconnredis.GetRedisClient(sc.SourceConnection)
	if err != nil {
		log.WithError(err).WithField("db_type", dbType).
			Error("[Monitor] Fail to connect to source Redis")
		return
	}
	defer srcClient.Close()

	tgtClient, err := dbconnredis.GetRedisClient(sc.TargetConnection)
	if err != nil {
		log.WithError(err).WithField("db_type", dbType).
			Error("[Monitor] Fail to connect to target Redis")
		return
	}
	defer tgtClient.Close()

	srcDBName := dsn.GetDatabaseName(sc.Type, sc.SourceConnection)
	tgtDBName := dsn.GetDatabaseName(sc.Type, sc.TargetConnection)

	srcCount, srcErr := keyCount(ctx, srcClient)
	if srcErr != nil {
		log.WithError(srcErr).WithField("db_type", dbType).
			Error("Failed to get source DB size")
		srcCount = -1
	}

	tgtCount, tgtErr := keyCount(ctx, tgtClient)
	if tgtErr != nil {
		log.WithError(tgtErr).WithField("db_type", dbType).
			Error("Failed to get target DB size")
		tgtCount = -1
	}

	// One row. What is being reported is the size of each database, which has
	// nothing to do with how many mappings the task lists — and the loop used to
	// be over the mappings, so three mappings wrote the same row three times and
	// a task with none wrote nothing at all while the sizes were measured and
	// thrown away.
	action := rowCountAction(srcErr == nil, tgtErr == nil)
	log.WithFields(logrus.Fields{
		"db_type":        dbType,
		"src_db":         srcDBName,
		"src_row_count":  srcCount,
		"tgt_db":         tgtDBName,
		"tgt_row_count":  tgtCount,
		"monitor_action": action,
	}).Info(action)

	// Insert into database monitoring_log with sync_task_id
	storeMonitoringLog(sc.ID, dbType, srcDBName, "", srcCount, tgtDBName, "", tgtCount, action)
}

// getRowCount is used by MySQL / MariaDB / PostgreSQL
// func getRowCount(db *sql.DB, table string) int64 {
// 	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
// 	var cnt int64
// 	if err := db.QueryRow(query).Scan(&cnt); err != nil {
// 		return -1
// 	}
// 	return cnt
// }

// keyCount reports how many keys an instance holds.
//
// DBSize asked of a cluster node answers for that node alone, so comparing one
// node of the source against one node of the target says nothing about whether
// the copy is complete — and a three-master pair would have reported roughly a
// third of each side while looking like a healthy match. A cluster is summed
// across its masters; a single instance answers for itself.
func keyCount(ctx context.Context, client goredis.UniversalClient) (int64, error) {
	cluster, isCluster := client.(*goredis.ClusterClient)
	if !isCluster {
		return client.DBSize(ctx).Result()
	}

	var (
		mu    sync.Mutex
		total int64
	)
	// ForEachMaster visits the masters concurrently, so the running total needs
	// the lock even though each node is asked once.
	err := cluster.ForEachMaster(ctx, func(ctx context.Context, node *goredis.Client) error {
		size, err := node.DBSize(ctx).Result()
		if err != nil {
			return err
		}
		mu.Lock()
		total += size
		mu.Unlock()
		return nil
	})
	if err != nil {
		return 0, err
	}
	return total, nil
}
