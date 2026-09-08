//go:build integration

package app

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/dsn"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	goredis "github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/retail-ai-inc/sync/internal/monitoring/infra"
	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/test/harness"
	"github.com/sirupsen/logrus"
)

const (
	sourceDB = "source_db"
	targetDB = "target_db"

	// The redis syncer package's integration tests own db 0 and db 1 and
	// flush them. go test runs packages in parallel, so the monitor tests
	// use their own indices to stay out of the way.
	monitorRedisDB = 9
)

func mysqlDSN(t *testing.T, endpoint, database string) string {
	t.Helper()

	host, port := harness.SplitHostPort(t, endpoint)
	return dsn.BuildDSNByType("mysql", map[string]string{
		"user": "root", "password": "root", "host": host, "port": port, "database": database,
	})
}

func openMySQL(t *testing.T, endpoint, database string) *sql.DB {
	t.Helper()

	db, err := sql.Open("mysql", mysqlDSN(t, endpoint, database))
	if err != nil {
		t.Fatalf("open %s: %v", endpoint, err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping %s: %v", endpoint, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func openMongo(t *testing.T, endpoint string) *mongo.Client {
	t.Helper()

	uri := "mongodb://" + endpoint + "/?directConnection=true"
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("connect %s: %v", endpoint, err)
	}
	if err := client.Ping(t.Context(), nil); err != nil {
		t.Fatalf("ping %s: %v", endpoint, err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	return client
}

func openRedis(t *testing.T, endpoint string, dbIndex int) *goredis.Client {
	t.Helper()

	c := goredis.NewClient(&goredis.Options{Addr: endpoint, DB: dbIndex})
	if err := c.Ping(t.Context()).Err(); err != nil {
		t.Fatalf("ping %s: %v", endpoint, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

type monitoringRow struct {
	TaskID   int
	Engine   string
	Object   string
	SrcCount int64
	TgtCount int64
}

// publishedCounts reads back what one task published. The registry is
// process-wide and every one of these tests uses a task id of its own, which
// is what keeps them from reading each other's.
func publishedCounts(t *testing.T, taskID int) []monitoringRow {
	t.Helper()

	task := strconv.Itoa(taskID)
	targets := map[string]int64{}
	for _, sample := range metrics.RowCounts.Snapshot(metrics.TargetRows) {
		if sample.Labels["task"] == task {
			targets[sample.Labels["object"]] = int64(sample.Value)
		}
	}

	var out []monitoringRow
	for _, sample := range metrics.RowCounts.Snapshot(metrics.SourceRows) {
		if sample.Labels["task"] != task {
			continue
		}
		labels := sample.Labels
		t.Cleanup(func() { metrics.ForgetRowCounts(labels) })
		object := sample.Labels["object"]
		out = append(out, monitoringRow{
			TaskID:   taskID,
			Engine:   sample.Labels["engine"],
			Object:   object,
			SrcCount: int64(sample.Value),
			TgtCount: targets[object],
		})
	}
	slices.SortFunc(out, func(a, b monitoringRow) int { return strings.Compare(a.Object, b.Object) })
	return out
}

// measuredAt reports when a task's counts were last published, zero when it
// has published nothing. A gauge is overwritten on each pass, so this is what
// says a pass happened rather than a number of rows.
func measuredAt(t *testing.T, taskID int) float64 {
	t.Helper()

	task := strconv.Itoa(taskID)
	var latest float64
	for _, sample := range metrics.RowCounts.Snapshot(metrics.RowCountMeasuredAt) {
		if sample.Labels["task"] == task && sample.Value > latest {
			latest = sample.Value
		}
	}
	return latest
}

func TestCountAndLogMySQLRecordsBothSides(t *testing.T) {
	useMonitoringDB(t)

	table := harness.UniqueName("mon")
	src := openMySQL(t, harness.MySQLSource, sourceDB)
	tgt := openMySQL(t, harness.MySQLTarget, targetDB)

	ddl := fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY)", table)
	if _, err := src.Exec(ddl); err != nil {
		t.Fatalf("create source table: %v", err)
	}
	if _, err := tgt.Exec(ddl); err != nil {
		t.Fatalf("create target table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = src.Exec("DROP TABLE " + table)
		_, _ = tgt.Exec("DROP TABLE " + table)
	})

	// Three rows at the source, two at the target.
	if _, err := src.Exec(fmt.Sprintf("INSERT INTO %s (id) VALUES (1),(2),(3)", table)); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	if _, err := tgt.Exec(fmt.Sprintf("INSERT INTO %s (id) VALUES (1),(2)", table)); err != nil {
		t.Fatalf("seed target: %v", err)
	}

	sc := config.SyncConfig{
		ID:               101,
		Enable:           true,
		Type:             "mysql",
		SourceConnection: mysqlDSN(t, harness.MySQLSource, sourceDB),
		TargetConnection: mysqlDSN(t, harness.MySQLTarget, targetDB),
		Mappings: []config.DatabaseMapping{{
			Tables: []config.TableMapping{{SourceTable: table, TargetTable: table}},
		}},
	}

	countAndLogTables(t.Context(), sc, quietLogger())

	got := publishedCounts(t, 101)
	if len(got) != 1 {
		t.Fatalf("monitoring_log holds %d rows, want 1: %+v", len(got), got)
	}
	r := got[0]
	if r.TaskID != 101 || r.Engine != "mysql" {
		t.Errorf("row = %+v", r)
	}
	if r.Object != table {
		t.Errorf("object = %q, want %q", r.Object, table)
	}
	if r.SrcCount != 3 || r.TgtCount != 2 {
		t.Errorf("counts = %d -> %d, want 3 -> 2", r.SrcCount, r.TgtCount)
	}
}

// A table named in the configuration but absent from the database yields the
// -1 sentinel, which is stored as though it were a row count.
func TestAMissingTableIsRecordedAsMinusOne(t *testing.T) {
	useMonitoringDB(t)

	sc := config.SyncConfig{
		ID:               102,
		Enable:           true,
		Type:             "mysql",
		SourceConnection: mysqlDSN(t, harness.MySQLSource, sourceDB),
		TargetConnection: mysqlDSN(t, harness.MySQLTarget, targetDB),
		Mappings: []config.DatabaseMapping{{
			Tables: []config.TableMapping{{
				SourceTable: "table_that_does_not_exist",
				TargetTable: "table_that_does_not_exist",
			}},
		}},
	}

	countAndLogTables(t.Context(), sc, quietLogger())

	got := publishedCounts(t, 102)
	if len(got) != 1 {
		t.Fatalf("monitoring_log holds %d rows, want 1", len(got))
	}
	if got[0].SrcCount != -1 || got[0].TgtCount != -1 {
		t.Fatalf("counts = %d -> %d, want -1 -> -1 — the sentinel appears to have been replaced; assert the new signal instead",
			got[0].SrcCount, got[0].TgtCount)
	}
}

// An unreachable source aborts the whole cycle before anything is written, so
// a monitoring gap during an outage is indistinguishable from the monitor not
// running at all: no row, no marker, only a log line.
func TestAnUnreachableSourceWritesNothing(t *testing.T) {
	useMonitoringDB(t)

	sc := config.SyncConfig{
		ID:               103,
		Enable:           true,
		Type:             "mysql",
		SourceConnection: "root:root@tcp(127.0.0.1:1)/source_db",
		TargetConnection: mysqlDSN(t, harness.MySQLTarget, targetDB),
		Mappings: []config.DatabaseMapping{{
			Tables: []config.TableMapping{{SourceTable: "t", TargetTable: "t"}},
		}},
	}

	countAndLogTables(t.Context(), sc, quietLogger())

	if got := publishedCounts(t, 103); len(got) != 0 {
		t.Fatalf("%d rows were written despite an unreachable source — an outage marker appears to have been added; assert it instead", len(got))
	}
}

func TestCountAndLogMongoDBRecordsBothSides(t *testing.T) {
	useMonitoringDB(t)

	collection := harness.UniqueName("mon")
	src := openMongo(t, harness.MongoSource)
	tgt := openMongo(t, harness.MongoTarget)

	srcColl := src.Database(sourceDB).Collection(collection)
	tgtColl := tgt.Database(targetDB).Collection(collection)
	t.Cleanup(func() {
		_ = srcColl.Drop(context.Background())
		_ = tgtColl.Drop(context.Background())
	})

	if _, err := srcColl.InsertMany(t.Context(), []interface{}{
		bson.M{"n": 1}, bson.M{"n": 2}, bson.M{"n": 3}, bson.M{"n": 4},
	}); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	if _, err := tgtColl.InsertMany(t.Context(), []interface{}{bson.M{"n": 1}}); err != nil {
		t.Fatalf("seed target: %v", err)
	}

	sc := config.SyncConfig{
		ID:               201,
		Enable:           true,
		Type:             "mongodb",
		SourceConnection: "mongodb://" + harness.MongoSource + "/" + sourceDB + "?directConnection=true",
		TargetConnection: "mongodb://" + harness.MongoTarget + "/" + targetDB + "?directConnection=true",
		Mappings: []config.DatabaseMapping{{
			SourceDatabase: sourceDB,
			TargetDatabase: targetDB,
			Tables:         []config.TableMapping{{SourceTable: collection, TargetTable: collection}},
		}},
	}

	countAndLogTables(t.Context(), sc, quietLogger())

	got := publishedCounts(t, 201)
	if len(got) == 0 {
		t.Fatal("monitoring_log is empty")
	}
	var found *monitoringRow
	for i := range got {
		if got[i].Object == collection {
			found = &got[i]
		}
	}
	if found == nil {
		t.Fatalf("no row for collection %q: %+v", collection, got)
	}
	if found.SrcCount != 4 || found.TgtCount != 1 {
		t.Errorf("counts = %d -> %d, want 4 -> 1", found.SrcCount, found.TgtCount)
	}
	if found.Engine != "mongodb" {
		t.Errorf("engine = %q, want mongodb", found.Engine)
	}
}

func TestCountAndLogRedisRecordsDatabaseSizes(t *testing.T) {
	useMonitoringDB(t)

	src := openRedis(t, harness.RedisSource, monitorRedisDB)
	tgt := openRedis(t, harness.RedisTarget, monitorRedisDB)
	emptyServers(t, src, tgt)

	// Four keys in the database the connection names and one in another, because
	// what is counted is the server: the replication copies every database that
	// holds keys, so a comparison of one of them says nothing.
	for i := 0; i < 4; i++ {
		if err := src.Set(t.Context(), fmt.Sprintf("k%d", i), i, 0).Err(); err != nil {
			t.Fatalf("seed source: %v", err)
		}
	}
	elsewhere := openRedis(t, harness.RedisSource, monitorRedisDB+1)
	if err := elsewhere.Set(t.Context(), "k4", 4, 0).Err(); err != nil {
		t.Fatalf("seed another database of the source: %v", err)
	}
	if err := tgt.Set(t.Context(), "k0", 0, 0).Err(); err != nil {
		t.Fatalf("seed target: %v", err)
	}

	sc := config.SyncConfig{
		ID:               301,
		Enable:           true,
		Type:             "redis",
		SourceConnection: fmt.Sprintf("redis://%s/%d", harness.RedisSource, monitorRedisDB),
		TargetConnection: fmt.Sprintf("redis://%s/%d", harness.RedisTarget, monitorRedisDB),
		Mappings: []config.DatabaseMapping{{
			Tables: []config.TableMapping{{SourceTable: "k*", TargetTable: "k*"}},
		}},
	}

	countAndLogTables(t.Context(), sc, quietLogger())

	got := publishedCounts(t, 301)
	if len(got) != 1 {
		t.Fatalf("monitoring_log holds %d rows, want 1: %+v", len(got), got)
	}
	if got[0].SrcCount != 5 || got[0].TgtCount != 1 {
		t.Errorf("counts = %d -> %d, want 5 -> 1: four keys in the database the "+
			"connection names and one in another, all of which the replication "+
			"copies", got[0].SrcCount, got[0].TgtCount)
	}
	// A Redis task compares whole databases and has no object of its own, so
	// the databases it counted are what it is published under.
	if want := fmt.Sprintf("db%d,%d", monitorRedisDB, monitorRedisDB+1); got[0].Object != want {
		t.Errorf("object = %q, want %q", got[0].Object, want)
	}
	if got[0].Engine != "redis" {
		t.Errorf("engine = %q, want redis", got[0].Engine)
	}
}

// emptyServers clears every database of both ends, because what the comparison
// counts is the server rather than one database of it.
func emptyServers(t *testing.T, ends ...*goredis.Client) {
	t.Helper()
	for _, end := range ends {
		if err := end.FlushAll(t.Context()).Err(); err != nil {
			t.Fatalf("empty a test server: %v", err)
		}
	}
}

// The Redis monitor reports DBSize — the size of the whole database — and
// writes one identical row per *mapping* rather than per table. src_table and
// tgt_table are always empty, so the row cannot say which keys were compared,
// and a task with two mappings produces two duplicate rows every cycle.
func TestRedisMonitoringDuplicatesRowsPerMapping(t *testing.T) {
	useMonitoringDB(t)

	src := openRedis(t, harness.RedisSource, monitorRedisDB)
	tgt := openRedis(t, harness.RedisTarget, monitorRedisDB)
	emptyServers(t, src, tgt)
	if err := src.Set(t.Context(), "only", 1, 0).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}

	sc := config.SyncConfig{
		ID:               302,
		Enable:           true,
		Type:             "redis",
		SourceConnection: fmt.Sprintf("redis://%s/%d", harness.RedisSource, monitorRedisDB),
		TargetConnection: fmt.Sprintf("redis://%s/%d", harness.RedisTarget, monitorRedisDB),
		Mappings: []config.DatabaseMapping{
			{Tables: []config.TableMapping{{SourceTable: "a*", TargetTable: "a*"}}},
			{Tables: []config.TableMapping{{SourceTable: "b*", TargetTable: "b*"}}},
			{Tables: []config.TableMapping{{SourceTable: "c*", TargetTable: "c*"}}},
		},
	}

	countAndLogTables(t.Context(), sc, quietLogger())

	got := publishedCounts(t, 302)
	if len(got) != 1 {
		t.Fatalf("%d objects were published, want one measurement of the two "+
			"databases: %v", len(got), got)
	}
	// Not one series per mapping and not one per named key pattern: what is
	// measured is each database's size, published under the databases counted.
	if want := fmt.Sprintf("db%d", monitorRedisDB); got[0].Object != want {
		t.Errorf("object = %q, want %q", got[0].Object, want)
	}
}

// The DBSize calls ran and their results were thrown away, because the only
// write sat inside a loop over the mappings — and a mapping means nothing to a
// Redis task, which replicates the whole keyspace.
func TestARedisTaskWithoutMappingsIsStillMonitored(t *testing.T) {
	useMonitoringDB(t)

	sc := config.SyncConfig{
		ID:               303,
		Enable:           true,
		Type:             "redis",
		SourceConnection: fmt.Sprintf("redis://%s/%d", harness.RedisSource, monitorRedisDB),
		TargetConnection: fmt.Sprintf("redis://%s/%d", harness.RedisTarget, monitorRedisDB),
	}

	countAndLogTables(t.Context(), sc, quietLogger())

	if got := publishedCounts(t, 303); len(got) != 1 {
		t.Errorf("%d rows were written for a task with no mappings, want one", len(got))
	}
}

func TestCountAndLogTablesIgnoresUnknownTypes(t *testing.T) {
	useMonitoringDB(t)

	for _, typ := range []string{"cassandra", "", "sqlite"} {
		sc := config.SyncConfig{ID: 401, Enable: true, Type: typ}
		countAndLogTables(t.Context(), sc, quietLogger())
	}

	if got := publishedCounts(t, 401); len(got) != 0 {
		t.Errorf("%d rows were written for unknown types", len(got))
	}
}

// T-094: countAndLogTables lower-cases the type before dispatching, unlike
// startSyncTasks in cmd/sync, which matches case-sensitively.
func TestTheMonitorAcceptsCasingTheSyncerRejects(t *testing.T) {
	useMonitoringDB(t)

	table := harness.UniqueName("case")
	src := openMySQL(t, harness.MySQLSource, sourceDB)
	tgt := openMySQL(t, harness.MySQLTarget, targetDB)
	ddl := fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY)", table)
	if _, err := src.Exec(ddl); err != nil {
		t.Fatalf("create source table: %v", err)
	}
	if _, err := tgt.Exec(ddl); err != nil {
		t.Fatalf("create target table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = src.Exec("DROP TABLE " + table)
		_, _ = tgt.Exec("DROP TABLE " + table)
	})

	sc := config.SyncConfig{
		ID:               402,
		Enable:           true,
		Type:             "MySQL", // the spelling startSyncTasks drops
		SourceConnection: mysqlDSN(t, harness.MySQLSource, sourceDB),
		TargetConnection: mysqlDSN(t, harness.MySQLTarget, targetDB),
		Mappings: []config.DatabaseMapping{{
			Tables: []config.TableMapping{{SourceTable: table, TargetTable: table}},
		}},
	}

	countAndLogTables(t.Context(), sc, quietLogger())

	got := publishedCounts(t, 402)
	if len(got) != 1 {
		t.Fatalf("the monitor wrote %d rows for type %q — the two dispatchers appear to agree now", len(got), sc.Type)
	}
	if got[0].Engine != "mysql" {
		t.Errorf("engine = %q, want mysql", got[0].Engine)
	}
}

func TestStartRowCountMonitoringMeasuresOnEachTick(t *testing.T) {
	useMonitoringDB(t)

	table := harness.UniqueName("loop")
	src := openMySQL(t, harness.MySQLSource, sourceDB)
	tgt := openMySQL(t, harness.MySQLTarget, targetDB)
	ddl := fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY)", table)
	if _, err := src.Exec(ddl); err != nil {
		t.Fatalf("create source table: %v", err)
	}
	if _, err := tgt.Exec(ddl); err != nil {
		t.Fatalf("create target table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = src.Exec("DROP TABLE " + table)
		_, _ = tgt.Exec("DROP TABLE " + table)
	})

	enabled := config.SyncConfig{
		ID:               501,
		Enable:           true,
		Type:             "mysql",
		SourceConnection: mysqlDSN(t, harness.MySQLSource, sourceDB),
		TargetConnection: mysqlDSN(t, harness.MySQLTarget, targetDB),
		Mappings: []config.DatabaseMapping{{
			Tables: []config.TableMapping{{SourceTable: table, TargetTable: table}},
		}},
	}
	disabled := enabled
	disabled.ID = 502
	disabled.Enable = false

	cfg := &config.Config{SyncConfigs: []config.SyncConfig{enabled, disabled}}

	ctx, cancel := context.WithCancel(t.Context())
	StartRowCountMonitoring(ctx, cfg, quietLogger(), 300*time.Millisecond,
		func() []config.SyncConfig { return cfg.SyncConfigs })

	// The second pass is what is being tested, and a gauge does not accumulate:
	// what moves between passes is the time the measurement was taken.
	var first float64
	harness.Eventually(t, 5*time.Second, func() error {
		first = measuredAt(t, 501)
		if first == 0 {
			return fmt.Errorf("no measurement yet")
		}
		return nil
	})
	harness.Eventually(t, 5*time.Second, func() error {
		if measuredAt(t, 501) <= first {
			return fmt.Errorf("still the measurement taken at %v", first)
		}
		return nil
	})
	cancel()

	if len(publishedCounts(t, 502)) != 0 {
		t.Fatal("a disabled task was monitored")
	}
	for _, r := range publishedCounts(t, 501) {
		if r.TaskID != 501 {
			t.Fatalf("a disabled task was monitored: %+v", r)
		}
	}
}

func TestStartRowCountMonitoringStopsOnCancel(t *testing.T) {
	useMonitoringDB(t)

	sc := config.SyncConfig{
		ID:               601,
		Enable:           true,
		Type:             "redis",
		SourceConnection: fmt.Sprintf("redis://%s/%d", harness.RedisSource, monitorRedisDB),
		TargetConnection: fmt.Sprintf("redis://%s/%d", harness.RedisTarget, monitorRedisDB),
		Mappings: []config.DatabaseMapping{{
			Tables: []config.TableMapping{{SourceTable: "*", TargetTable: "*"}},
		}},
	}

	ctx, cancel := context.WithCancel(t.Context())
	StartRowCountMonitoring(ctx, &config.Config{SyncConfigs: []config.SyncConfig{sc}},
		quietLogger(), 200*time.Millisecond,
		func() []config.SyncConfig { return []config.SyncConfig{sc} })

	harness.Eventually(t, 5*time.Second, func() error {
		if len(publishedCounts(t, 601)) == 0 {
			return fmt.Errorf("nothing written yet")
		}
		return nil
	})

	cancel()
	time.Sleep(600 * time.Millisecond) // two tick intervals
	before := measuredAt(t, 601)
	time.Sleep(600 * time.Millisecond)

	if after := measuredAt(t, 601); after != before {
		t.Errorf("a measurement was taken at %v, after cancellation at %v", after, before)
	}
}

// The ticker fires only after the first interval elapses and nothing was
// recorded before it, so with the production interval such a process produced
// no measurement at all — and a restart is exactly when somebody wants one.
func TestAMeasurementIsTakenAtStartup(t *testing.T) {
	useMonitoringDB(t)

	sc := config.SyncConfig{
		ID:               701,
		Enable:           true,
		Type:             "redis",
		SourceConnection: fmt.Sprintf("redis://%s/%d", harness.RedisSource, monitorRedisDB),
		TargetConnection: fmt.Sprintf("redis://%s/%d", harness.RedisTarget, monitorRedisDB),
		Mappings: []config.DatabaseMapping{{
			Tables: []config.TableMapping{{SourceTable: "*", TargetTable: "*"}},
		}},
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	StartRowCountMonitoring(ctx, &config.Config{SyncConfigs: []config.SyncConfig{sc}},
		quietLogger(), time.Hour,
		func() []config.SyncConfig { return []config.SyncConfig{sc} })

	harness.Eventually(t, 5*time.Second, func() error {
		if n := len(publishedCounts(t, 701)); n == 0 {
			return fmt.Errorf("no measurement was taken before the first tick, an hour away")
		}
		return nil
	})
}

// recordingLogger collects log output so a test can read the fields a summary
// reported. The daily summary writes its numbers to the log and nowhere else.
func recordingLogger() (*logrus.Logger, *bytes.Buffer) {
	var out bytes.Buffer
	l := logrus.New()
	l.SetOutput(&out)
	l.SetLevel(logrus.DebugLevel)
	l.SetFormatter(&logrus.JSONFormatter{})
	return l, &out
}

func postgresDSN(endpoint, database string) string {
	return fmt.Sprintf("postgres://root:root@%s/%s?sslmode=disable", endpoint, database)
}

func openPostgres(t *testing.T, endpoint, database string) *sql.DB {
	t.Helper()

	db, err := sql.Open("postgres", postgresDSN(endpoint, database))
	if err != nil {
		t.Fatalf("open %s: %v", endpoint, err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping %s: %v", endpoint, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestCountAndLogPostgreSQLRecordsBothSides covers the PostgreSQL counter, which
// had no test against a server at all — it is the number an operator reads to
// decide whether the copy is complete, and a wrong one reads as a healthy match.
func TestCountAndLogPostgreSQLRecordsBothSides(t *testing.T) {
	useMonitoringDB(t)

	table := harness.UniqueName("pgmon")
	src := openPostgres(t, harness.PostgresSource, sourceDB)
	tgt := openPostgres(t, harness.PostgresTarget, targetDB)

	ddl := fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY)", table)
	if _, err := src.Exec(ddl); err != nil {
		t.Fatalf("create source table: %v", err)
	}
	if _, err := tgt.Exec(ddl); err != nil {
		t.Fatalf("create target table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = src.Exec("DROP TABLE IF EXISTS " + table)
		_, _ = tgt.Exec("DROP TABLE IF EXISTS " + table)
	})

	if _, err := src.Exec(fmt.Sprintf("INSERT INTO %s (id) VALUES (1),(2),(3)", table)); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	if _, err := tgt.Exec(fmt.Sprintf("INSERT INTO %s (id) VALUES (1),(2)", table)); err != nil {
		t.Fatalf("seed target: %v", err)
	}

	sc := config.SyncConfig{
		ID:               411,
		Enable:           true,
		Type:             "postgresql",
		SourceConnection: postgresDSN(harness.PostgresSource, sourceDB),
		TargetConnection: postgresDSN(harness.PostgresTarget, targetDB),
		Mappings: []config.DatabaseMapping{{
			Tables: []config.TableMapping{{SourceTable: table, TargetTable: table}},
		}},
	}

	countAndLogTables(t.Context(), sc, quietLogger())

	got := publishedCounts(t, 411)
	if len(got) != 1 {
		t.Fatalf("%d objects were published, want 1: %+v", len(got), got)
	}
	r := got[0]
	if r.TaskID != 411 || r.Engine != "postgresql" {
		t.Errorf("row = %+v", r)
	}
	if r.SrcCount != 3 || r.TgtCount != 2 {
		t.Errorf("counts = %d -> %d, want 3 -> 2", r.SrcCount, r.TgtCount)
	}
}

// TestAMissingPostgreSQLTableIsRecordedAsMinusOne records that a table named in
// the configuration but absent from the database is stored as the -1 sentinel
// rather than as zero, which would read as an empty table that is in sync.
func TestAMissingPostgreSQLTableIsRecordedAsMinusOne(t *testing.T) {
	useMonitoringDB(t)

	sc := config.SyncConfig{
		ID:               412,
		Enable:           true,
		Type:             "postgresql",
		SourceConnection: postgresDSN(harness.PostgresSource, sourceDB),
		TargetConnection: postgresDSN(harness.PostgresTarget, targetDB),
		Mappings: []config.DatabaseMapping{{
			Tables: []config.TableMapping{{
				SourceTable: "no_such_table_" + harness.UniqueName("x"),
				TargetTable: "no_such_table_" + harness.UniqueName("y"),
			}},
		}},
	}

	countAndLogTables(t.Context(), sc, quietLogger())

	got := publishedCounts(t, 412)
	if len(got) != 1 {
		t.Fatalf("%d objects were published, want 1: %+v", len(got), got)
	}
	if got[0].SrcCount != -1 || got[0].TgtCount != -1 {
		t.Errorf("counts = %d -> %d, want -1 -> -1", got[0].SrcCount, got[0].TgtCount)
	}
}

// TestAnUnreachablePostgreSQLSourceIsReported records that a source that cannot
// be reached writes nothing rather than a row of zeroes.
func TestAnUnreachablePostgreSQLSourceIsReported(t *testing.T) {
	useMonitoringDB(t)

	sc := config.SyncConfig{
		ID:               413,
		Enable:           true,
		Type:             "postgresql",
		SourceConnection: postgresDSN("127.0.0.1:1", sourceDB),
		TargetConnection: postgresDSN(harness.PostgresTarget, targetDB),
		Mappings: []config.DatabaseMapping{{
			Tables: []config.TableMapping{{SourceTable: "t", TargetTable: "t"}},
		}},
	}

	countAndLogTables(t.Context(), sc, quietLogger())

	if got := publishedCounts(t, 413); len(got) != 0 {
		t.Errorf("%d objects were published for a source that was never reached: %+v", len(got), got)
	}
}

// TestTheServerSideChangeStreamProbeRuns covers the branch that asks MongoDB
// itself what change streams are open.
func TestTheServerSideChangeStreamProbeRuns(t *testing.T) {
	conn := useMonitoringDB(t)

	collection := harness.UniqueName("csprobe")
	src := openMongo(t, harness.MongoSource)
	tgt := openMongo(t, harness.MongoTarget)

	srcColl := src.Database(sourceDB).Collection(collection)
	tgtColl := tgt.Database(targetDB).Collection(collection)
	t.Cleanup(func() {
		_ = srcColl.Drop(context.Background())
		_ = tgtColl.Drop(context.Background())
	})
	if _, err := srcColl.InsertOne(t.Context(), bson.M{"n": 1}); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	const taskID = 205
	labels := metrics.Labels{
		"task": "205", "engine": "mongodb", "collection": collection,
		"source": harness.MongoSource,
	}
	t.Cleanup(func() { metrics.Default.Forget(labels) })
	metrics.SetTaskUp(labels, true)
	metrics.Applied(labels, 3)
	metrics.Failed(labels, 1)

	sc := config.SyncConfig{
		ID:               taskID,
		Enable:           true,
		Type:             "mongodb",
		SourceConnection: "mongodb://" + harness.MongoSource + "/" + sourceDB + "?directConnection=true",
		TargetConnection: "mongodb://" + harness.MongoTarget + "/" + targetDB + "?directConnection=true",
		Mappings: []config.DatabaseMapping{{
			SourceDatabase: sourceDB,
			TargetDatabase: targetDB,
			Tables:         []config.TableMapping{{SourceTable: collection, TargetTable: collection}},
		}},
	}

	countAndLogTables(t.Context(), sc, quietLogger())

	// The statistics row carries what the syncer counted, not zeroes.
	var received, executed int
	err := conn.QueryRow(
		`SELECT received, executed FROM changestream_statistics
		  WHERE task_id = ? AND collection_name = ?`,
		taskID, harness.MongoSource+"."+collection).Scan(&received, &executed)
	if err != nil {
		t.Fatalf("read changestream_statistics: %v", err)
	}
	if executed != 3 {
		t.Errorf("executed = %d, want the 3 the metrics recorded", executed)
	}
	if received != 4 {
		t.Errorf("received = %d, want applied plus failed", received)
	}
}

// TestLogYesterdayMongoDBVolumeCountsTheDayThatEnded covers the daily summary,
// which is the number a switchover decision is read off: how much of
// yesterday's data reached the target.
func TestLogYesterdayMongoDBVolumeCountsTheDayThatEnded(t *testing.T) {
	// The summary reports a difference through the Slack path, which reads the
	// global settings — without a control database of its own it would build one
	// in the working directory.
	useMonitoringDB(t)

	collection := harness.UniqueName("yesterday")
	src := openMongo(t, harness.MongoSource)
	tgt := openMongo(t, harness.MongoTarget)

	srcColl := src.Database(sourceDB).Collection(collection)
	tgtColl := tgt.Database(targetDB).Collection(collection)
	t.Cleanup(func() {
		_ = srcColl.Drop(context.Background())
		_ = tgtColl.Drop(context.Background())
	})

	jst, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		t.Fatalf("load JST: %v", err)
	}
	now := time.Now().In(jst)
	yesterday := now.AddDate(0, 0, -1)
	start := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, jst)
	end := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 23, 59, 59, 999999999, jst)

	// Two documents inside the window and one outside it, so a summary that
	// ignored the range would report three.
	if _, err := srcColl.InsertMany(t.Context(), []interface{}{
		bson.M{"created_at": start.Add(2 * time.Hour)},
		bson.M{"created_at": start.Add(20 * time.Hour)},
		bson.M{"created_at": start.AddDate(0, 0, -3)},
	}); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	if _, err := tgtColl.InsertOne(t.Context(), bson.M{"created_at": start.Add(2 * time.Hour)}); err != nil {
		t.Fatalf("seed target: %v", err)
	}

	sc := config.SyncConfig{
		ID:               206,
		Enable:           true,
		Type:             "mongodb",
		SourceConnection: "mongodb://" + harness.MongoSource + "/" + sourceDB + "?directConnection=true",
		TargetConnection: "mongodb://" + harness.MongoTarget + "/" + targetDB + "?directConnection=true",
		Mappings: []config.DatabaseMapping{{
			SourceDatabase: sourceDB,
			TargetDatabase: targetDB,
			Tables: []config.TableMapping{{
				SourceTable: collection,
				TargetTable: collection,
				CountQuery: map[string]interface{}{
					// The table is part of the condition.
					"conditions": []map[string]interface{}{
						{"field": "created_at", "operator": "dateRange",
							"table": collection, "value": "daily"},
					},
				},
			}},
		}},
	}

	logger, out := recordingLogger()
	infra.LogYesterdayMongoDBVolume(t.Context(), sc, logger, start, end)

	text := out.String()
	if !strings.Contains(text, `"src_yesterday_count":2`) &&
		!strings.Contains(text, `src_yesterday_count=2`) {
		t.Errorf("summary = %q, want 2 source documents inside yesterday", text)
	}
	if !strings.Contains(text, `"tgt_yesterday_count":1`) &&
		!strings.Contains(text, `tgt_yesterday_count=1`) {
		t.Errorf("summary = %q, want 1 target document inside yesterday", text)
	}
}
