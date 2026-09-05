//go:build integration

package infra

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	goredis "github.com/redis/go-redis/v9"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/test/harness"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

// seedSQLTable creates a table with a known number of rows, so the counter has
// a number to be right or wrong about.
func seedSQLTable(t *testing.T, driver, dataSource, table string, rows int) {
	t.Helper()
	db, err := sql.Open(driver, dataSource)
	if err != nil {
		t.Fatalf("open %s: %v", driver, err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec("DROP TABLE IF EXISTS " + table)
		_ = db.Close()
	})
	if _, err := db.Exec("DROP TABLE IF EXISTS " + table); err != nil {
		t.Fatalf("drop %s: %v", table, err)
	}
	if _, err := db.Exec("CREATE TABLE " + table + " (id INT PRIMARY KEY)"); err != nil {
		t.Fatalf("create %s: %v", table, err)
	}
	for i := 0; i < rows; i++ {
		statement := "INSERT INTO " + table + " (id) VALUES (?)"
		if driver == "postgres" {
			statement = "INSERT INTO " + table + " (id) VALUES ($1)"
		}
		if _, err := db.Exec(statement, i); err != nil {
			t.Fatalf("seed %s: %v", table, err)
		}
	}
}

func clusterAddrs(t *testing.T, variable string) []string {
	t.Helper()
	value := os.Getenv(variable)
	if value == "" {
		t.Skipf("set %s to the addresses of a Redis cluster", variable)
	}
	return strings.Split(value, ",")
}

// The counters report by logging and by writing a monitoring_log row, so both
// are checked: a counter that logged the comparison and stored nothing left
// the dashboard empty while the log said it had run.

type countedRow struct {
	Source, Target int64
	Action         string
}

func loggedRows(t *testing.T, db *sql.DB) []countedRow {
	t.Helper()
	rows, err := db.Query(
		`SELECT src_row_count, tgt_row_count, monitor_action FROM monitoring_log ORDER BY id`)
	if err != nil {
		t.Fatalf("read monitoring_log: %v", err)
	}
	defer rows.Close()

	var out []countedRow
	for rows.Next() {
		var row countedRow
		if err := rows.Scan(&row.Source, &row.Target, &row.Action); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, row)
	}
	return out
}

func TestTheRedisCounterComparesTheTwoDatabaseSizes(t *testing.T) {
	control := useMonitoringDB(t)
	logger, out := captureLog()

	source := goredis.NewClient(&goredis.Options{Addr: harness.RedisSource})
	defer source.Close()
	ctx := context.Background()
	if err := source.Set(ctx, "counter:probe", "1", 0).Err(); err != nil {
		t.Fatalf("seed the source: %v", err)
	}
	defer source.Del(ctx, "counter:probe")

	CountAndLogRedis(ctx, config.SyncConfig{
		ID: 9201, Type: "redis",
		SourceConnection: "redis://" + harness.RedisSource + "/0",
		TargetConnection: "redis://" + harness.RedisTarget + "/0",
	}, logger)

	rows := loggedRows(t, control)
	// One row whatever the mappings say, because what is measured is the size
	// of each database rather than any one mapping.
	if len(rows) != 1 {
		t.Fatalf("the counter wrote %d monitoring_log rows, want 1", len(rows))
	}
	if rows[0].Source < 1 {
		t.Errorf("the source counted %d keys after one was written", rows[0].Source)
	}
	if rows[0].Source == -1 || rows[0].Target == -1 {
		t.Errorf("a side could not be counted: %+v", rows[0])
	}
	if !strings.Contains(out.String(), "src_row_count") {
		t.Error("the comparison was not logged")
	}
}

// A side that cannot be reached is recorded as -1 rather than not recorded at
// all. Writing no row left the dashboard showing the last successful
// comparison, so an unreachable target looked the same as a matching one.
func TestAnUnreachableTargetIsRecordedAsMinusOne(t *testing.T) {
	control := useMonitoringDB(t)
	logger, out := captureLog()

	CountAndLogRedis(briefCtx(t), config.SyncConfig{
		ID: 9202, Type: "redis",
		SourceConnection: "redis://" + harness.RedisSource + "/0",
		TargetConnection: "redis://127.0.0.1:1/0",
	}, logger)

	rows := loggedRows(t, control)
	if len(rows) != 1 {
		t.Fatalf("an unreachable target wrote %d rows, want 1", len(rows))
	}
	if rows[0].Target != -1 {
		t.Errorf("the unreachable target was recorded as %d, want -1", rows[0].Target)
	}
	// The source was reachable, so its count is real -- the row says which side
	// failed rather than blanking both.
	if rows[0].Source < 0 {
		t.Errorf("the reachable source was recorded as %d", rows[0].Source)
	}
	if rows[0].Action != actionCountFailed {
		t.Errorf("the row is marked %q, want %q", rows[0].Action, actionCountFailed)
	}
	if !strings.Contains(out.String(), "Fail to connect to target Redis") {
		t.Error("nothing in the log says the target could not be reached")
	}
}

// Both ends gone is still one row, so a task whose whole comparison stopped
// working is visible rather than absent.
func TestBothEndsUnreachableStillWritesARow(t *testing.T) {
	control := useMonitoringDB(t)
	logger, _ := captureLog()

	CountAndLogRedis(briefCtx(t), config.SyncConfig{
		ID: 9205, Type: "redis",
		SourceConnection: "redis://127.0.0.1:1/0",
		TargetConnection: "redis://127.0.0.1:2/0",
	}, logger)

	rows := loggedRows(t, control)
	if len(rows) != 1 {
		t.Fatalf("two unreachable ends wrote %d rows, want 1", len(rows))
	}
	if rows[0].Source != -1 || rows[0].Target != -1 {
		t.Errorf("counted %d/%d, want -1/-1", rows[0].Source, rows[0].Target)
	}
}

func TestTheMySQLCounterComparesRealTables(t *testing.T) {
	control := useMonitoringDB(t)
	logger, _ := captureLog()

	table := strings.ReplaceAll(harness.UniqueName("counted"), "-", "_")
	sourceDSN := fmt.Sprintf("root:root@tcp(%s)/source_db", harness.MySQLSource)
	targetDSN := fmt.Sprintf("root:root@tcp(%s)/target_db", harness.MySQLTarget)
	seedSQLTable(t, "mysql", sourceDSN, table, 4)
	seedSQLTable(t, "mysql", targetDSN, table, 2)

	CountAndLogMySQLOrMariaDB(context.Background(), config.SyncConfig{
		ID: 9203, Type: "mysql",
		SourceConnection: sourceDSN, TargetConnection: targetDSN,
		Mappings: []config.DatabaseMapping{{
			SourceDatabase: "source_db", TargetDatabase: "target_db",
			Tables: []config.TableMapping{{SourceTable: table, TargetTable: table}},
		}},
	}, logger)

	rows := loggedRows(t, control)
	if len(rows) != 1 {
		t.Fatalf("the counter wrote %d rows, want 1", len(rows))
	}
	if rows[0].Source != 4 || rows[0].Target != 2 {
		t.Errorf("counted %d/%d, want 4/2", rows[0].Source, rows[0].Target)
	}
}

func TestThePostgresCounterComparesRealTables(t *testing.T) {
	control := useMonitoringDB(t)
	logger, _ := captureLog()

	table := strings.ReplaceAll(harness.UniqueName("counted"), "-", "_")
	sourceDSN := fmt.Sprintf("postgres://root:root@%s/source_db?sslmode=disable", harness.PostgresSource)
	targetDSN := fmt.Sprintf("postgres://root:root@%s/target_db?sslmode=disable", harness.PostgresTarget)
	seedSQLTable(t, "postgres", sourceDSN, table, 3)
	seedSQLTable(t, "postgres", targetDSN, table, 3)

	CountAndLogPostgreSQL(context.Background(), config.SyncConfig{
		ID: 9204, Type: "postgresql",
		SourceConnection: sourceDSN, TargetConnection: targetDSN,
		Mappings: []config.DatabaseMapping{{
			SourceDatabase: "source_db", TargetDatabase: "target_db",
			Tables: []config.TableMapping{{SourceTable: table, TargetTable: table}},
		}},
	}, logger)

	rows := loggedRows(t, control)
	if len(rows) != 1 {
		t.Fatalf("the counter wrote %d rows, want 1", len(rows))
	}
	if rows[0].Source != 3 || rows[0].Target != 3 {
		t.Errorf("counted %d/%d, want 3/3", rows[0].Source, rows[0].Target)
	}
}

func TestKeyCountAddsUpEveryMasterOfACluster(t *testing.T) {
	addrs := clusterAddrs(t, "SYNC_REDIS_SOURCE_CLUSTER")
	cluster := goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: addrs})
	defer cluster.Close()
	ctx := context.Background()

	before, err := keyCount(ctx, cluster)
	if err != nil {
		t.Fatalf("keyCount: %v", err)
	}

	// Spread over the slot space, so more than one master holds some of them.
	// DBSize asked of one node answers for that node alone.
	const written = 50
	for i := 0; i < written; i++ {
		if err := cluster.Set(ctx, fmt.Sprintf("keycount:%d", i), i, 0).Err(); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	defer func() {
		for i := 0; i < written; i++ {
			cluster.Del(ctx, fmt.Sprintf("keycount:%d", i))
		}
	}()

	after, err := keyCount(ctx, cluster)
	if err != nil {
		t.Fatalf("keyCount: %v", err)
	}
	if after-before != written {
		t.Errorf("the cluster counted %d more keys after %d were written, which is "+
			"one node's share rather than the whole cluster", after-before, written)
	}
}
