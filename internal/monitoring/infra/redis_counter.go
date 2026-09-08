package infra

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

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
	// A side that cannot be reached is still reported, as -1. Returning here
	// wrote no row at all, so the dashboard went on showing the last successful
	// comparison and a target that had been unreachable for hours looked the
	// same as one that matched.
	srcClient, srcConnErr := dbconnredis.GetRedisClient(sc.SourceConnection)
	if srcConnErr != nil {
		log.WithError(srcConnErr).WithField("db_type", dbType).
			Error("[Monitor] Fail to connect to source Redis")
	} else {
		defer srcClient.Close()
	}

	tgtClient, tgtConnErr := dbconnredis.GetRedisClient(sc.TargetConnection)
	if tgtConnErr != nil {
		log.WithError(tgtConnErr).WithField("db_type", dbType).
			Error("[Monitor] Fail to connect to target Redis")
	} else {
		defer tgtClient.Close()
	}

	srcCount, srcNames, srcErr := sizeOrMark(ctx, srcClient, srcConnErr, "source", dbType, log)
	tgtCount, tgtNames, tgtErr := sizeOrMark(ctx, tgtClient, tgtConnErr, "target", dbType, log)

	// What this tool keeps on the target is not replicated data: a slot marker
	// per slot the task has written -- up to sixteen thousand of them -- and a
	// position key per shard. Counted as data they were reported as a
	// difference for ever: fifteen thousand keys on a task whose two ends held
	// exactly the same 20,390.
	if tgtErr == nil {
		own, err := countOwnKeys(ctx, tgtClient)
		switch {
		case err != nil:
			log.WithError(err).WithField("db_type", dbType).
				Warn("[Monitor] Could not count this tool's own keys on the target, " +
					"so they are included in its total")
		case own > 0:
			tgtCount -= own
			if tgtCount < 0 {
				tgtCount = 0
			}
			log.WithFields(logrus.Fields{"db_type": dbType, "own_keys": own}).
				Debug("[Monitor] Left this tool's own keys out of the target's count")
		}
	}

	srcDBName := orConnectionDB(srcNames, sc.Type, sc.SourceConnection)
	tgtDBName := orConnectionDB(tgtNames, sc.Type, sc.TargetConnection)

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

// keyCount reports how many keys an instance holds, and which databases they
// are in.
//
// DBSize asked of a cluster node answers for that node alone, so comparing one
// node of the source against one node of the target says nothing about whether
// the copy is complete — and a three-master pair would have reported roughly a
// third of each side while looking like a healthy match.
//
// DBSize asked of a standalone server answers for the one database the
// connection is on, which is the connection's own — database 0 unless the DSN
// says otherwise. A source keeping its data in databases 1 and 2 therefore
// read as empty: staging has one, and for a fortnight this reported a source
// of one key against a target of five thousand and called it a difference. The
// replication copies every database that holds keys, so the comparison counts
// every database that holds keys.
func keyCount(ctx context.Context, client goredis.UniversalClient) (int64, []int, error) {
	cluster, isCluster := client.(*goredis.ClusterClient)
	if !isCluster {
		return keyspaceCount(ctx, client)
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
		return 0, nil, err
	}
	// A cluster has one database, whatever the connection says.
	return total, []int{0}, nil
}

// keyspaceCount asks a standalone server for every database that holds keys.
//
// One round trip rather than sixteen: INFO keyspace reports the populated
// databases and how many keys each holds, and asking DBSize per database would
// need a connection per database.
func keyspaceCount(ctx context.Context, client goredis.UniversalClient) (int64, []int, error) {
	info, err := client.Info(ctx, "keyspace").Result()
	if err != nil {
		return 0, nil, err
	}
	total, databases := parseKeyspace(info)
	return total, databases, nil
}

// parseKeyspace reads the "db0:keys=1,expires=0,avg_ttl=0" lines of INFO
// keyspace. A server with nothing in it reports no such line, which is a
// count of zero rather than a failure to read.
func parseKeyspace(info string) (int64, []int) {
	var (
		total     int64
		databases []int
	)
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "db") {
			continue
		}
		colon := strings.Index(line, ":")
		if colon < 0 {
			continue
		}
		db, err := strconv.Atoi(line[2:colon])
		if err != nil {
			continue
		}
		for _, field := range strings.Split(line[colon+1:], ",") {
			name, value, ok := strings.Cut(field, "=")
			if !ok || name != "keys" {
				continue
			}
			keys, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				continue
			}
			total += keys
			databases = append(databases, db)
		}
	}
	sort.Ints(databases)
	return total, databases
}

// orConnectionDB names the databases a count covers, falling back to the one
// the connection is on when there is nothing to count. The row says which
// databases were counted rather than which one was connected to: a total of
// every database labelled "0" is how the old count read.
func orConnectionDB(databases []int, kind, connection string) string {
	if len(databases) == 0 {
		return dsn.GetDatabaseName(kind, connection)
	}
	names := make([]string, 0, len(databases))
	for _, db := range databases {
		names = append(names, strconv.Itoa(db))
	}
	return strings.Join(names, ",")
}

// sizeOrMark reports how many keys one end holds, or -1 when it could not be
// asked -- either because connecting failed or because the count did.
//
// -1 rather than nothing: writing no row at all left the dashboard showing the
// last successful comparison, so a target that had been unreachable for hours
// looked the same as one that matched.
func sizeOrMark(ctx context.Context, client goredis.UniversalClient, connectErr error,
	side, dbType string, log *logrus.Logger) (int64, []int, error) {

	if connectErr != nil {
		return -1, nil, connectErr
	}
	count, databases, err := keyCount(ctx, client)
	if err != nil {
		log.WithError(err).WithField("db_type", dbType).
			Errorf("Failed to get %s DB size", side)
		return -1, nil, err
	}
	return count, databases, nil
}

// ownKeyPatterns match the keys this tool writes to a target of its own
// accord. Neither comes from the source, so neither is data: the slot markers
// carry each slot's applied offset, and the position key carries the stream's.
var ownKeyPatterns = []string{"*:__off:*", "__sync:pos:*"}

// countOwnKeys counts them.
//
// By pattern rather than by name: the marker names are derivable -- one per
// slot per task -- but the position keys are named after shards this does not
// know, and a SCAN with a MATCH returns only what matches. It costs a pass
// over the keyspace per pattern, on the interval the comparison runs at, which
// for a target of twenty thousand keys is a few dozen round trips an hour.
func countOwnKeys(ctx context.Context, client goredis.UniversalClient) (int64, error) {
	if cluster, ok := client.(*goredis.ClusterClient); ok {
		var (
			mu    sync.Mutex
			total int64
		)
		err := cluster.ForEachMaster(ctx, func(ctx context.Context, node *goredis.Client) error {
			n, err := countMatching(ctx, node)
			if err != nil {
				return err
			}
			mu.Lock()
			total += n
			mu.Unlock()
			return nil
		})
		return total, err
	}
	return countMatching(ctx, client)
}

// countMatching walks one server once per pattern.
func countMatching(ctx context.Context, client goredis.UniversalClient) (int64, error) {
	var total int64
	for _, pattern := range ownKeyPatterns {
		var cursor uint64
		for {
			keys, next, err := client.Scan(ctx, cursor, pattern, 1000).Result()
			if err != nil {
				return total, fmt.Errorf("scan the target for %s: %w", pattern, err)
			}
			total += int64(len(keys))
			if next == 0 {
				break
			}
			cursor = next
		}
	}
	return total, nil
}
