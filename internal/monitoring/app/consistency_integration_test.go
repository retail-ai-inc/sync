//go:build integration

package app

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/test/harness"
)

// verifyTask describes a comparison of one table against a live MySQL pair.
func verifyTask(t *testing.T, table string) config.SyncConfig {
	t.Helper()

	return config.SyncConfig{
		ID:               harness.UniqueTaskID(),
		Type:             "mysql",
		Enable:           true,
		SourceConnection: mysqlDSN(t, harness.MySQLSource, sourceDB),
		TargetConnection: mysqlDSN(t, harness.MySQLTarget, targetDB),
		Mappings: []config.DatabaseMapping{{
			SourceDatabase: sourceDB, TargetDatabase: targetDB,
			Tables: []config.TableMapping{{SourceTable: table, TargetTable: table}},
		}},
	}
}

// verifyTables creates the same table on both sides and drops them afterwards.
func verifyTables(t *testing.T, table, definition string) (source, target *sql.DB) {
	t.Helper()

	source = openMySQL(t, harness.MySQLSource, sourceDB)
	target = openMySQL(t, harness.MySQLTarget, targetDB)

	for _, db := range []*sql.DB{source, target} {
		if _, err := db.Exec("DROP TABLE IF EXISTS " + table); err != nil {
			t.Fatalf("drop %s: %v", table, err)
		}
		if _, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (%s)", table, definition)); err != nil {
			t.Fatalf("create %s: %v", table, err)
		}
	}
	t.Cleanup(func() {
		for _, db := range []*sql.DB{source, target} {
			_, _ = db.Exec("DROP TABLE IF EXISTS " + table)
		}
	})
	return source, target
}

// differencesFound reports what the comparison recorded for one task, which is
// the number an operator would see on the dashboard.
func differencesFound(t *testing.T, task config.SyncConfig) float64 {
	t.Helper()

	id := fmt.Sprint(task.ID)
	for _, sample := range metrics.Default.Snapshot(differencesMetric) {
		if sample.Labels["task"] == id {
			t.Cleanup(func() { metrics.Default.Forget(sample.Labels) })
			return sample.Value
		}
	}
	t.Fatalf("no comparison was recorded for task %d", task.ID)
	return 0
}

// TestAnIntegerKeyedTableCompares is the case a stubbed comparison cannot check:
// the server orders an integer primary key numerically, and any comparison that
// depends on both sides ordering keys the same way as Go does reports every row
// past the 9/10 boundary as both missing and extra. The rows here cross it.
func TestAnIntegerKeyedTableCompares(t *testing.T) {
	const table = "verify_orders"
	source, target := verifyTables(t, table,
		"id INT PRIMARY KEY, amount VARCHAR(16), note VARCHAR(32)")

	for i := 1; i <= 12; i++ {
		for _, db := range []*sql.DB{source, target} {
			if _, err := db.Exec(
				"INSERT INTO "+table+" (id, amount, note) VALUES (?, ?, ?)",
				i, fmt.Sprint(i*100), "paid"); err != nil {
				t.Fatalf("seed %d: %v", i, err)
			}
		}
	}

	task := verifyTask(t, table)
	checkSQLTask(context.Background(), task, nil, quiet())

	if got := differencesFound(t, task); got != 0 {
		t.Errorf("the comparison found %v differences between two identical tables", got)
	}
}

// TestEachKindOfDivergenceIsReported covers the three things replication can get
// wrong, against a real server: a row that never arrived, a row that was not
// deleted, and a row whose contents drifted.
func TestEachKindOfDivergenceIsReported(t *testing.T) {
	const table = "verify_divergence"
	source, target := verifyTables(t, table, "id INT PRIMARY KEY, amount VARCHAR(16)")

	for i := 1; i <= 12; i++ {
		if _, err := source.Exec(
			"INSERT INTO "+table+" (id, amount) VALUES (?, ?)", i, "100"); err != nil {
			t.Fatalf("seed the source: %v", err)
		}
	}
	// The target is missing 11, holds an extra 99, and disagrees about 10.
	for i := 1; i <= 12; i++ {
		if i == 11 {
			continue
		}
		amount := "100"
		if i == 10 {
			amount = "999"
		}
		if _, err := target.Exec(
			"INSERT INTO "+table+" (id, amount) VALUES (?, ?)", i, amount); err != nil {
			t.Fatalf("seed the target: %v", err)
		}
	}
	if _, err := target.Exec("INSERT INTO " + table + " (id, amount) VALUES (99, '100')"); err != nil {
		t.Fatalf("seed the extra row: %v", err)
	}

	task := verifyTask(t, table)
	checkSQLTask(context.Background(), task, nil, quiet())

	if got := differencesFound(t, task); got != 3 {
		t.Errorf("the comparison found %v differences, want exactly three", got)
	}
}

// TestACompositePrimaryKeyCompares is the shape a payment ledger's tables
// actually have, and the one the comparison used to refuse outright.
func TestACompositePrimaryKeyCompares(t *testing.T) {
	const table = "verify_ledger"
	source, target := verifyTables(t, table,
		"account VARCHAR(16), entry INT, amount VARCHAR(16), PRIMARY KEY (account, entry)")

	for _, account := range []string{"acct-1", "acct-2"} {
		for entry := 1; entry <= 6; entry++ {
			for _, db := range []*sql.DB{source, target} {
				if _, err := db.Exec(
					"INSERT INTO "+table+" (account, entry, amount) VALUES (?, ?, ?)",
					account, entry, "100"); err != nil {
					t.Fatalf("seed %s/%d: %v", account, entry, err)
				}
			}
		}
	}
	// One entry of one account is lost, which must not be confused with the
	// other entries that share its first key column.
	if _, err := target.Exec(
		"DELETE FROM " + table + " WHERE account = 'acct-1' AND entry = 4"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	task := verifyTask(t, table)
	notifier := &recordingNotifier{configured: true}
	checkSQLTask(context.Background(), task, notifier, quiet())

	if got := differencesFound(t, task); got != 1 {
		t.Errorf("the comparison found %v differences, want the one lost entry", got)
	}
	if len(notifier.messages) != 1 {
		t.Fatalf("%d alerts were sent", len(notifier.messages))
	}
	if !strings.Contains(notifier.messages[0], "missing") {
		t.Errorf("the alert does not say what is wrong:\n%s", notifier.messages[0])
	}
}

// TestARepairMakesTheTargetMatch closes the loop against a real server: finding
// out a payment row is missing and then having to put it back by hand is most of
// the work.
func TestARepairMakesTheTargetMatch(t *testing.T) {
	const table = "verify_repair"
	source, target := verifyTables(t, table,
		"account VARCHAR(16), entry INT, amount VARCHAR(16), PRIMARY KEY (account, entry)")

	for entry := 1; entry <= 12; entry++ {
		if _, err := source.Exec(
			"INSERT INTO "+table+" (account, entry, amount) VALUES ('acct-1', ?, ?)",
			entry, fmt.Sprint(entry*100)); err != nil {
			t.Fatalf("seed the source: %v", err)
		}
		if entry == 10 {
			continue // never arrived
		}
		amount := fmt.Sprint(entry * 100)
		if entry == 3 {
			amount = "wrong"
		}
		if _, err := target.Exec(
			"INSERT INTO "+table+" (account, entry, amount) VALUES ('acct-1', ?, ?)",
			entry, amount); err != nil {
			t.Fatalf("seed the target: %v", err)
		}
	}
	if _, err := target.Exec(
		"INSERT INTO " + table + " (account, entry, amount) VALUES ('acct-9', 1, '1')"); err != nil {
		t.Fatalf("seed the extra row: %v", err)
	}

	t.Setenv("SYNC_VERIFY_REPAIR", "true")
	task := verifyTask(t, table)
	checkSQLTask(context.Background(), task, nil, quiet())

	if got := differencesFound(t, task); got != 3 {
		t.Fatalf("the comparison found %v differences, want three", got)
	}

	// Compare again: a repair that worked leaves nothing to find.
	second := verifyTask(t, table)
	checkSQLTask(context.Background(), second, nil, quiet())
	if got := differencesFound(t, second); got != 0 {
		t.Errorf("%v differences remain after the repair", got)
	}

	var amount string
	if err := target.QueryRow(
		"SELECT amount FROM " + table + " WHERE account = 'acct-1' AND entry = 10").
		Scan(&amount); err != nil {
		t.Fatalf("read the repaired row: %v", err)
	}
	if amount != "1000" {
		t.Errorf("the repaired row holds %q", amount)
	}
	var extras int
	if err := target.QueryRow(
		"SELECT COUNT(*) FROM " + table + " WHERE account = 'acct-9'").Scan(&extras); err != nil {
		t.Fatalf("count the extra rows: %v", err)
	}
	if extras != 0 {
		t.Errorf("%d rows the source does not have are still on the target", extras)
	}
}

// ------------------------------------------------------------------ MongoDB

// verifyMongoTask describes a comparison of one collection against a live
// MongoDB pair.
func verifyMongoTask(t *testing.T, collection string) config.SyncConfig {
	t.Helper()

	return config.SyncConfig{
		ID:               harness.UniqueTaskID(),
		Type:             "mongodb",
		Enable:           true,
		SourceConnection: "mongodb://" + harness.MongoSource + "/" + sourceDB + "?directConnection=true",
		TargetConnection: "mongodb://" + harness.MongoTarget + "/" + targetDB + "?directConnection=true",
		Mappings: []config.DatabaseMapping{{
			SourceDatabase: sourceDB, TargetDatabase: targetDB,
			Tables: []config.TableMapping{{SourceTable: collection, TargetTable: collection}},
		}},
	}
}

// verifyCollections empties the same collection on both sides and drops them
// afterwards.
func verifyCollections(t *testing.T, collection string) (source, target *mongo.Collection) {
	t.Helper()

	src := openMongo(t, harness.MongoSource)
	tgt := openMongo(t, harness.MongoTarget)

	source = src.Database(sourceDB).Collection(collection)
	target = tgt.Database(targetDB).Collection(collection)
	t.Cleanup(func() {
		_ = source.Drop(context.Background())
		_ = target.Drop(context.Background())
	})
	return source, target
}

// TestTwoIdenticalCollectionsCompareEqual covers the MongoDB half of the
// periodic comparison, which had no test against a server at all. It is the
// check that answers "is the copy we would switch to actually complete", and a
// comparison that reports differences between two identical collections is as
// useless as one that reports none between two that differ.
func TestTwoIdenticalCollectionsCompareEqual(t *testing.T) {
	collection := harness.UniqueName("verify_mongo")
	source, target := verifyCollections(t, collection)

	var docs []interface{}
	for i := 1; i <= 12; i++ {
		docs = append(docs, bson.M{"_id": i, "amount": i * 100, "note": "paid"})
	}
	if _, err := source.InsertMany(t.Context(), docs); err != nil {
		t.Fatalf("seed the source: %v", err)
	}
	if _, err := target.InsertMany(t.Context(), docs); err != nil {
		t.Fatalf("seed the target: %v", err)
	}

	task := verifyMongoTask(t, collection)
	checkMongoTask(context.Background(), task, nil, quiet())

	if got := differencesFound(t, task); got != 0 {
		t.Errorf("the comparison found %v differences between two identical collections", got)
	}
}

// TestEachKindOfMongoDivergenceIsReported covers the three things replication
// can get wrong: a document that never arrived, one that was not deleted, and
// one whose contents drifted.
func TestEachKindOfMongoDivergenceIsReported(t *testing.T) {
	collection := harness.UniqueName("verify_mongo_diff")
	source, target := verifyCollections(t, collection)

	var sourceDocs []interface{}
	for i := 1; i <= 12; i++ {
		sourceDocs = append(sourceDocs, bson.M{"_id": i, "amount": 100})
	}
	if _, err := source.InsertMany(t.Context(), sourceDocs); err != nil {
		t.Fatalf("seed the source: %v", err)
	}

	// The target is missing 11, holds an extra 99, and disagrees about 10.
	var targetDocs []interface{}
	for i := 1; i <= 12; i++ {
		if i == 11 {
			continue
		}
		amount := 100
		if i == 10 {
			amount = 999
		}
		targetDocs = append(targetDocs, bson.M{"_id": i, "amount": amount})
	}
	targetDocs = append(targetDocs, bson.M{"_id": 99, "amount": 100})
	if _, err := target.InsertMany(t.Context(), targetDocs); err != nil {
		t.Fatalf("seed the target: %v", err)
	}

	task := verifyMongoTask(t, collection)
	checkMongoTask(context.Background(), task, nil, quiet())

	if got := differencesFound(t, task); got != 3 {
		t.Errorf("the comparison found %v differences, want exactly three", got)
	}
}

// TestAMongoRepairMakesTheTargetMatch covers the repair, which only runs when it
// has been turned on. It is what makes the comparison worth running before a
// switchover rather than only worth reading afterwards.
func TestAMongoRepairMakesTheTargetMatch(t *testing.T) {
	t.Setenv("SYNC_VERIFY_REPAIR", "true")

	collection := harness.UniqueName("verify_mongo_repair")
	source, target := verifyCollections(t, collection)

	if _, err := source.InsertMany(t.Context(), []interface{}{
		bson.M{"_id": 1, "amount": 100},
		bson.M{"_id": 2, "amount": 200},
	}); err != nil {
		t.Fatalf("seed the source: %v", err)
	}
	// One document missing and one that drifted.
	if _, err := target.InsertOne(t.Context(), bson.M{"_id": 1, "amount": 999}); err != nil {
		t.Fatalf("seed the target: %v", err)
	}
	// And one the source no longer has.
	if _, err := target.InsertOne(t.Context(), bson.M{"_id": 3, "amount": 300}); err != nil {
		t.Fatalf("seed the extra document: %v", err)
	}

	task := verifyMongoTask(t, collection)
	checkMongoTask(context.Background(), task, nil, quiet())

	var repaired []bson.M
	cursor, err := target.Find(t.Context(), bson.M{})
	if err != nil {
		t.Fatalf("read the target: %v", err)
	}
	if err := cursor.All(t.Context(), &repaired); err != nil {
		t.Fatalf("decode the target: %v", err)
	}

	amounts := map[int32]int32{}
	for _, doc := range repaired {
		id, _ := doc["_id"].(int32)
		amount, _ := doc["amount"].(int32)
		amounts[id] = amount
	}
	if len(amounts) != 2 {
		t.Fatalf("the target holds %d documents after the repair, want 2: %v", len(amounts), amounts)
	}
	if amounts[1] != 100 {
		t.Errorf("document 1 = %d, want the source's 100", amounts[1])
	}
	if amounts[2] != 200 {
		t.Errorf("document 2 = %d, want the one that never arrived", amounts[2])
	}
}

// TestTheCollectionsAreDiscoveredWhenTheTaskListsNone records that a task with
// no table mappings compares everything the source holds, rather than nothing.
// A task configured to copy a whole database would otherwise be verified by a
// comparison that silently checked no collections at all.
func TestTheCollectionsAreDiscoveredWhenTheTaskListsNone(t *testing.T) {
	collection := harness.UniqueName("verify_mongo_discover")
	source, _ := verifyCollections(t, collection)

	if _, err := source.InsertOne(t.Context(), bson.M{"_id": 1}); err != nil {
		t.Fatalf("seed the source: %v", err)
	}

	task := verifyMongoTask(t, collection)
	task.Mappings = nil

	src := openMongo(t, harness.MongoSource)
	pairs := mongoCollectionPairs(context.Background(), task, src.Database(sourceDB), quiet())

	var found bool
	for _, pair := range pairs {
		if pair.source == collection && pair.target == collection {
			found = true
		}
	}
	if !found {
		t.Errorf("the collection just seeded is not among the %d discovered: %v", len(pairs), pairs)
	}
}
