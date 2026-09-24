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

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/test/harness"
)

// captureAppLog returns a logger writing into a buffer, which is where these
// report what they could not do.
func captureAppLog() (*logrus.Logger, *bytes.Buffer) {
	var out bytes.Buffer
	logger := logrus.New()
	logger.SetOutput(&out)
	return logger, &out
}

func consistencySourceDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql",
		fmt.Sprintf("root:root@tcp(%s)/source_db", harness.MySQLSource))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func makeTable(t *testing.T, db *sql.DB, prefix string) string {
	t.Helper()
	name := strings.ReplaceAll(harness.UniqueName(prefix), "-", "_")
	if _, err := db.Exec("CREATE TABLE `" + name +
		"` (id INT, part INT, value TEXT, PRIMARY KEY (id, part))"); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS `" + name + "`") })
	return name
}

func TestATaskThatNamesNoTablesComparesWhatTheSourceHolds(t *testing.T) {
	db := consistencySourceDB(t)
	table := makeTable(t, db, "discovered")
	logger, _ := captureAppLog()

	pairs := sqlTablePairs(context.Background(),
		config.SyncConfig{ID: 9401}, db, "source_db", logger)

	var found bool
	for _, pair := range pairs {
		if pair.Source == table {
			found = true
			// Discovered tables are compared against the same name, because
			// nothing said otherwise.
			if pair.Target != table {
				t.Errorf("%s is compared against %q", table, pair.Target)
			}
		}
	}
	if !found {
		t.Errorf("%s was not discovered", table)
	}
}

func TestATaskThatNamesItsTablesIsNotDiscovered(t *testing.T) {
	db := consistencySourceDB(t)
	logger, _ := captureAppLog()

	pairs := sqlTablePairs(context.Background(), config.SyncConfig{
		ID: 9402,
		Mappings: []config.DatabaseMapping{{
			Tables: []config.TableMapping{{SourceTable: "orders", TargetTable: "orders_copy"}},
		}},
	}, db, "source_db", logger)

	if len(pairs) != 1 || pairs[0].Source != "orders" || pairs[0].Target != "orders_copy" {
		t.Errorf("a task that names one table produced %+v", pairs)
	}
}

func TestNothingCanBeDiscoveredThroughADatabaseThatIsNotThere(t *testing.T) {
	db, err := sql.Open("mysql", "root:root@tcp(127.0.0.1:1)/nothing")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	logger, out := captureAppLog()

	if pairs := sqlTablePairs(context.Background(),
		config.SyncConfig{ID: 9403}, db, "nothing", logger); pairs != nil {
		t.Errorf("a source that cannot be read produced %+v", pairs)
	}
	// It reports rather than comparing nothing in silence.
	if !strings.Contains(out.String(), "9403") {
		t.Error("the failure was not logged against the task")
	}
}

func TestAPrimaryKeyIsReturnedWhole(t *testing.T) {
	db := consistencySourceDB(t)
	table := makeTable(t, db, "composite")

	columns, err := primaryKey(context.Background(), db, "source_db", table)
	if err != nil {
		t.Fatalf("primaryKey: %v", err)
	}
	// In order, and both parts: comparing on part of a composite key would
	// report every row sharing the first column as a duplicate.
	if len(columns) != 2 || columns[0] != "id" || columns[1] != "part" {
		t.Errorf("primaryKey = %v, want [id part]", columns)
	}

	// A table whose rows cannot be addressed is refused rather than compared on
	// nothing, which would report every row as a duplicate of every other.
	if _, err := primaryKey(context.Background(), db, "source_db", "no_such_table"); err == nil {
		t.Error("a table with no primary key was accepted for comparison")
	}
}

// verifyDifferences reads what the last check recorded for one table, and forgets
// it afterwards so the registry does not carry it into the next test.
func verifyDifferences(t *testing.T, task config.SyncConfig, table string) float64 {
	t.Helper()
	id := strconv.Itoa(task.ID)
	for _, sample := range metrics.Default.Snapshot(differencesMetric) {
		if sample.Labels["task"] == id && sample.Labels["table"] == table {
			labels := sample.Labels
			t.Cleanup(func() { metrics.Default.Forget(labels) })
			return sample.Value
		}
	}
	t.Fatalf("the check recorded nothing for task %d, table %s", task.ID, table)
	return 0
}

func sqlRows(t *testing.T, db *sql.DB, table string) string {
	t.Helper()
	rows, err := db.Query("SELECT id, part, value FROM `" + table + "` ORDER BY id, part")
	if err != nil {
		t.Fatalf("read %s: %v", table, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id, part int
		var value string
		if err := rows.Scan(&id, &part, &value); err != nil {
			t.Fatalf("scan %s: %v", table, err)
		}
		out = append(out, fmt.Sprintf("(%d,%d,%s)", id, part, value))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read %s: %v", table, err)
	}
	return strings.Join(out, " ")
}

func mongoDocuments(t *testing.T, coll *mongo.Collection) string {
	t.Helper()
	cursor, err := coll.Find(t.Context(), bson.M{},
		options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		t.Fatalf("read %s: %v", coll.Name(), err)
	}
	var docs []bson.M
	if err := cursor.All(t.Context(), &docs); err != nil {
		t.Fatalf("read %s: %v", coll.Name(), err)
	}
	out := make([]string, 0, len(docs))
	for _, doc := range docs {
		fields := make([]string, 0, len(doc))
		for name, value := range doc {
			fields = append(fields, fmt.Sprintf("%s=%v", name, value))
		}
		slices.Sort(fields)
		out = append(out, "{"+strings.Join(fields, ",")+"}")
	}
	return strings.Join(out, " ")
}

// A check with repair off that wrote anyway would upsert and delete on a replica
// nobody asked to be repaired.
func TestTheSQLCheckWritesNothingUnlessRepairIsOn(t *testing.T) {
	useMonitoringDB(t)
	source := openMySQL(t, harness.MySQLSource, sourceDB)
	target := openMySQL(t, harness.MySQLTarget, targetDB)
	table := makeTable(t, source, "gate")
	if _, err := target.Exec("CREATE TABLE `" + table +
		"` (id INT, part INT, value TEXT, PRIMARY KEY (id, part))"); err != nil {
		t.Fatalf("create the target: %v", err)
	}
	t.Cleanup(func() { _, _ = target.Exec("DROP TABLE IF EXISTS `" + table + "`") })

	// The target lacks (3,1), disagrees on (2,1) and holds (4,1) the source does not.
	if _, err := source.Exec("INSERT INTO `" + table +
		"` VALUES (1,1,'a'), (2,1,'b'), (3,1,'c')"); err != nil {
		t.Fatalf("seed the source: %v", err)
	}
	if _, err := target.Exec("INSERT INTO `" + table +
		"` VALUES (1,1,'a'), (2,1,'drifted'), (4,1,'extra')"); err != nil {
		t.Fatalf("seed the target: %v", err)
	}

	task := config.SyncConfig{
		ID: harness.UniqueTaskID(), Type: "mysql", Enable: true,
		SourceConnection: mysqlDSN(t, harness.MySQLSource, sourceDB),
		TargetConnection: mysqlDSN(t, harness.MySQLTarget, targetDB),
		Mappings: []config.DatabaseMapping{{
			SourceDatabase: sourceDB, TargetDatabase: targetDB,
			Tables: []config.TableMapping{{SourceTable: table, TargetTable: table}},
		}},
	}
	cfg := &config.Config{SyncConfigs: []config.SyncConfig{task}}
	n := &recordingNotifier{configured: true}
	untouched := sqlRows(t, target, table)

	t.Setenv("SYNC_VERIFY_REPAIR", "")
	runConsistencyChecks(t.Context(), cfg.SyncConfigs, cfg, n, quiet())

	if got := verifyDifferences(t, task, table); got != 3 {
		t.Errorf("the check found %v differences, want 3", got)
	}
	if len(n.messages) != 1 {
		t.Errorf("%d alerts were sent, want 1", len(n.messages))
	}
	if got := sqlRows(t, target, table); got != untouched {
		t.Fatalf("the target changed with repair off:\nbefore %s\nafter  %s", untouched, got)
	}

	t.Setenv("SYNC_VERIFY_REPAIR", "true")
	runConsistencyChecks(t.Context(), cfg.SyncConfigs, cfg, n, quiet())

	if got, want := sqlRows(t, target, table), sqlRows(t, source, table); got != want {
		t.Errorf("the target after a repair is %s, want the source's %s", got, want)
	}
	runConsistencyChecks(t.Context(), cfg.SyncConfigs, cfg, n, quiet())
	if got := verifyDifferences(t, task, table); got != 0 {
		t.Errorf("%v differences remain after the repair", got)
	}
}

// The same gate on the MongoDB path, with the collections discovered rather than
// named.
func TestTheMongoCheckWritesNothingUnlessRepairIsOn(t *testing.T) {
	useMonitoringDB(t)
	database := harness.UniqueName("gate")
	const collection = "ledger"
	source := openMongo(t, harness.MongoSource).Database(database).Collection(collection)
	target := openMongo(t, harness.MongoTarget).Database(database).Collection(collection)
	t.Cleanup(func() {
		_ = source.Database().Drop(context.Background())
		_ = target.Database().Drop(context.Background())
	})

	if _, err := source.InsertMany(t.Context(), []interface{}{
		bson.D{{Key: "_id", Value: 1}, {Key: "amount", Value: 10}},
		bson.D{{Key: "_id", Value: 2}, {Key: "amount", Value: 20}},
		bson.D{{Key: "_id", Value: 3}, {Key: "amount", Value: 30}},
	}); err != nil {
		t.Fatalf("seed the source: %v", err)
	}
	if _, err := target.InsertMany(t.Context(), []interface{}{
		bson.D{{Key: "_id", Value: 1}, {Key: "amount", Value: 10}},
		bson.D{{Key: "_id", Value: 2}, {Key: "amount", Value: 0}},
		bson.D{{Key: "_id", Value: 4}, {Key: "amount", Value: 40}},
	}); err != nil {
		t.Fatalf("seed the target: %v", err)
	}

	task := config.SyncConfig{
		ID: harness.UniqueTaskID(), Type: "mongodb", Enable: true,
		SourceConnection: "mongodb://" + harness.MongoSource + "/" + database + "?directConnection=true",
		TargetConnection: "mongodb://" + harness.MongoTarget + "/" + database + "?directConnection=true",
	}
	cfg := &config.Config{SyncConfigs: []config.SyncConfig{task}}
	n := &recordingNotifier{configured: true}
	untouched := mongoDocuments(t, target)

	t.Setenv("SYNC_VERIFY_REPAIR", "")
	runConsistencyChecks(t.Context(), cfg.SyncConfigs, cfg, n, quiet())

	if got := verifyDifferences(t, task, collection); got != 3 {
		t.Errorf("the check found %v differences, want 3", got)
	}
	if len(n.messages) != 1 {
		t.Errorf("%d alerts were sent, want 1", len(n.messages))
	}
	if got := mongoDocuments(t, target); got != untouched {
		t.Fatalf("the target changed with repair off:\nbefore %s\nafter  %s", untouched, got)
	}

	t.Setenv("SYNC_VERIFY_REPAIR", "true")
	runConsistencyChecks(t.Context(), cfg.SyncConfigs, cfg, n, quiet())

	if got, want := mongoDocuments(t, target), mongoDocuments(t, source); got != want {
		t.Errorf("the target after a repair is %s, want the source's %s", got, want)
	}
	runConsistencyChecks(t.Context(), cfg.SyncConfigs, cfg, n, quiet())
	if got := verifyDifferences(t, task, collection); got != 0 {
		t.Errorf("%v differences remain after the repair", got)
	}
}
