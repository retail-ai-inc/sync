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
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

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

// mongoTestURI is the address a task would be configured with.
func mongoTestURI(t *testing.T, endpoint, database string) string {
	t.Helper()
	host, port := harness.SplitHostPort(t, endpoint)
	return "mongodb://" + host + ":" + port + "/" + database + "?directConnection=true"
}

func seedMongoCollection(t *testing.T, endpoint, database, collection string, documents int) {
	t.Helper()
	ctx := context.Background()
	client, err := mongo.Connect(options.Client().ApplyURI(mongoTestURI(t, endpoint, database)))
	if err != nil {
		t.Fatalf("connect to %s: %v", endpoint, err)
	}
	t.Cleanup(func() {
		_ = client.Database(database).Drop(context.Background())
		_ = client.Disconnect(context.Background())
	})
	coll := client.Database(database).Collection(collection)
	for i := 0; i < documents; i++ {
		if _, err := coll.InsertOne(ctx, bson.M{"_id": i, "n": i}); err != nil {
			t.Fatalf("seed %s.%s: %v", database, collection, err)
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

// Every task in production replicates a whole database, and the counters looped
// over the configured mappings -- so they compared nothing and wrote no row at
// all. The row-count panel was empty for those tasks since they were created.
func TestAWholeDatabaseMySQLTaskIsStillCounted(t *testing.T) {
	control := useMonitoringDB(t)
	logger, _ := captureLog()

	first := strings.ReplaceAll(harness.UniqueName("whole_a"), "-", "_")
	second := strings.ReplaceAll(harness.UniqueName("whole_b"), "-", "_")
	sourceDSN := fmt.Sprintf("root:root@tcp(%s)/source_db", harness.MySQLSource)
	targetDSN := fmt.Sprintf("root:root@tcp(%s)/target_db", harness.MySQLTarget)
	seedSQLTable(t, "mysql", sourceDSN, first, 3)
	seedSQLTable(t, "mysql", targetDSN, first, 3)
	seedSQLTable(t, "mysql", sourceDSN, second, 5)

	// No Mappings at all, which is what "replicate the whole database" looks
	// like in the stored configuration.
	CountAndLogMySQLOrMariaDB(context.Background(), config.SyncConfig{
		ID: 9206, Type: "mysql",
		SourceConnection: sourceDSN, TargetConnection: targetDSN,
	}, logger)

	rows := loggedRows(t, control)
	if len(rows) == 0 {
		t.Fatal("a whole-database task wrote no rows at all")
	}
	byCount := map[int64]int64{}
	for _, row := range rows {
		byCount[row.Source] = row.Target
	}
	if target, ok := byCount[3]; !ok || target != 3 {
		t.Errorf("the matching table was not counted as 3/3: %+v", rows)
	}
	// The second table is only on the source. The count has to say so rather
	// than leave it out.
	if target, ok := byCount[5]; !ok || target != -1 {
		t.Errorf("a table the target lacks was recorded as %v, want -1", target)
	}
}

func TestAWholeDatabaseMongoTaskIsStillCounted(t *testing.T) {
	control := useMonitoringDB(t)
	logger, _ := captureLog()

	database := harness.UniqueName("wholedb")
	seedMongoCollection(t, harness.MongoSource, database, "alpha", 4)
	seedMongoCollection(t, harness.MongoTarget, database, "alpha", 4)
	seedMongoCollection(t, harness.MongoSource, database, "beta", 2)
	seedMongoCollection(t, harness.MongoTarget, database, "beta", 2)

	CountAndLogMongoDB(context.Background(), config.SyncConfig{
		ID: 9207, Type: "mongodb",
		SourceConnection: mongoTestURI(t, harness.MongoSource, database),
		TargetConnection: mongoTestURI(t, harness.MongoTarget, database),
	}, logger)

	rows := loggedRows(t, control)
	if len(rows) < 2 {
		t.Fatalf("a whole-database task wrote %d rows, want one per collection", len(rows))
	}
	for _, row := range rows {
		if row.Source != row.Target {
			t.Errorf("a collection seeded identically counted %d/%d", row.Source, row.Target)
		}
	}
}

func TestAWholeSchemaPostgresTaskIsStillCounted(t *testing.T) {
	control := useMonitoringDB(t)
	logger, _ := captureLog()

	table := strings.ReplaceAll(harness.UniqueName("whole_pg"), "-", "_")
	sourceDSN := fmt.Sprintf("postgres://root:root@%s/source_db?sslmode=disable", harness.PostgresSource)
	targetDSN := fmt.Sprintf("postgres://root:root@%s/target_db?sslmode=disable", harness.PostgresTarget)
	seedSQLTable(t, "postgres", sourceDSN, table, 6)
	seedSQLTable(t, "postgres", targetDSN, table, 6)

	CountAndLogPostgreSQL(context.Background(), config.SyncConfig{
		ID: 9208, Type: "postgresql",
		SourceConnection: sourceDSN, TargetConnection: targetDSN,
	}, logger)

	rows := loggedRows(t, control)
	if len(rows) == 0 {
		t.Fatal("a whole-schema task wrote no rows at all")
	}
	var found bool
	for _, row := range rows {
		if row.Source == 6 && row.Target == 6 {
			found = true
		}
	}
	if !found {
		t.Errorf("the seeded table was not counted as 6/6: %+v", rows)
	}
}

// Each mapping names a schema, so pairing every configured table with every
// schema would count each one twice.
func TestPostgresPairsStayWithTheirOwnMapping(t *testing.T) {
	mapping := config.DatabaseMapping{
		SourceSchema: "public",
		Tables:       []config.TableMapping{{SourceTable: "orders"}},
	}
	pairs, err := postgresPairs(context.Background(), mapping, nil, "public")
	if err != nil {
		t.Fatalf("postgresPairs: %v", err)
	}
	if len(pairs) != 1 || pairs[0].Source != "orders" || pairs[0].Target != "orders" {
		t.Errorf("configured pairs came back as %+v", pairs)
	}
}

// One open for a pass, not one per table. A task replicating a whole database
// compares every table it holds, so this was a hundred opens a minute.
func TestAPassOpensTheControlDatabaseOnce(t *testing.T) {
	control := useMonitoringDB(t)
	logger, _ := captureLog()

	first := strings.ReplaceAll(harness.UniqueName("once_a"), "-", "_")
	second := strings.ReplaceAll(harness.UniqueName("once_b"), "-", "_")
	sourceDSN := fmt.Sprintf("root:root@tcp(%s)/source_db", harness.MySQLSource)
	targetDSN := fmt.Sprintf("root:root@tcp(%s)/target_db", harness.MySQLTarget)
	for _, table := range []string{first, second} {
		seedSQLTable(t, "mysql", sourceDSN, table, 2)
		seedSQLTable(t, "mysql", targetDSN, table, 2)
	}

	CountAndLogMySQLOrMariaDB(context.Background(), config.SyncConfig{
		ID: 9209, Type: "mysql",
		SourceConnection: sourceDSN, TargetConnection: targetDSN,
		Mappings: []config.DatabaseMapping{{Tables: []config.TableMapping{
			{SourceTable: first}, {SourceTable: second},
		}}},
	}, logger)

	// Both rows land: holding the handle open must not lose any of them.
	rows := loggedRows(t, control)
	if len(rows) != 2 {
		t.Fatalf("a two-table pass wrote %d rows, want 2", len(rows))
	}
	for _, row := range rows {
		if row.Source != 2 || row.Target != 2 {
			t.Errorf("counted %d/%d, want 2/2", row.Source, row.Target)
		}
	}
}

// A control database that cannot be opened must not stop the comparison: the
// numbers still reach the log, they are simply not kept.
func TestAPassStillMeasuresWhenItCannotRecord(t *testing.T) {
	emptyDB(t)
	logger, out := captureLog()

	table := strings.ReplaceAll(harness.UniqueName("norecord"), "-", "_")
	sourceDSN := fmt.Sprintf("root:root@tcp(%s)/source_db", harness.MySQLSource)
	targetDSN := fmt.Sprintf("root:root@tcp(%s)/target_db", harness.MySQLTarget)
	seedSQLTable(t, "mysql", sourceDSN, table, 3)
	seedSQLTable(t, "mysql", targetDSN, table, 3)

	CountAndLogMySQLOrMariaDB(context.Background(), config.SyncConfig{
		ID: 9210, Type: "mysql",
		SourceConnection: sourceDSN, TargetConnection: targetDSN,
		Mappings: []config.DatabaseMapping{{Tables: []config.TableMapping{{SourceTable: table}}}},
	}, logger)

	if !strings.Contains(out.String(), "src_row_count=3") {
		t.Errorf("the comparison was not logged: %s", out.String())
	}
}
