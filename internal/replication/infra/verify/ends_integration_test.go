//go:build integration

package verify

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	_ "github.com/go-sql-driver/mysql"

	"github.com/retail-ai-inc/sync/test/harness"
)

func mongoCollections(t *testing.T) (source, target *mongo.Collection) {
	t.Helper()
	host, port := harness.SplitHostPort(t, harness.MongoTarget)
	client, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://" + host + ":" + port + "/?directConnection=true"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	database := harness.UniqueName("verify")
	t.Cleanup(func() {
		_ = client.Database(database).Drop(context.Background())
		_ = client.Disconnect(context.Background())
	})
	return client.Database(database).Collection("source"),
		client.Database(database).Collection("target")
}

func insert(t *testing.T, coll *mongo.Collection, id int, value string) {
	t.Helper()
	if _, err := coll.InsertOne(context.Background(),
		bson.M{"_id": id, "value": value}); err != nil {
		t.Fatalf("insert %d: %v", id, err)
	}
}

func TestAMongoEndReadsEveryRowInBatches(t *testing.T) {
	source, _ := mongoCollections(t)
	ctx := context.Background()
	for i := 0; i < 7; i++ {
		insert(t, source, i, "v")
	}

	end := &MongoEnd{Coll: source}
	if !strings.HasSuffix(end.Name(), ".source") {
		t.Errorf("Name() = %q, which does not name the collection", end.Name())
	}

	seen := map[string]bool{}
	for {
		batch, err := end.Next(ctx, 3)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if len(batch) == 0 {
			break
		}
		if len(batch) > 3 {
			t.Fatalf("a batch of %d exceeded the limit of 3", len(batch))
		}
		for _, row := range batch {
			if seen[row.Key] {
				t.Fatalf("%s was read twice", row.Key)
			}
			seen[row.Key] = true
		}
	}
	if len(seen) != 7 {
		t.Errorf("read %d rows, want 7", len(seen))
	}

	// Reading past the end stays finished rather than starting again.
	if batch, err := end.Next(ctx, 3); err != nil || len(batch) != 0 {
		t.Errorf("reading a finished end returned %d rows (%v)", len(batch), err)
	}
}

func TestAMongoEndLooksUpOnlyWhatIsAskedFor(t *testing.T) {
	source, _ := mongoCollections(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		insert(t, source, i, fmt.Sprintf("v%d", i))
	}

	end := &MongoEnd{Coll: source}
	first, err := end.Next(ctx, 5)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}

	if empty, err := end.Lookup(ctx, nil); err != nil || len(empty) != 0 {
		t.Errorf("looking up nothing returned %d rows (%v)", len(empty), err)
	}

	wanted := []string{first[0].Key, first[2].Key}
	found, err := end.Lookup(ctx, wanted)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(found) != 2 {
		t.Fatalf("looked up 2 keys and got %d rows", len(found))
	}
	for _, key := range wanted {
		if _, ok := found[key]; !ok {
			t.Errorf("%s was asked for and not returned", key)
		}
	}

	if _, err := end.Lookup(ctx, []string{"not-hex"}); err == nil {
		t.Error("a key that is not a key was looked up")
	}
}

func TestARepairMakesTheTargetMatchTheSource(t *testing.T) {
	source, target := mongoCollections(t)
	ctx := context.Background()

	insert(t, source, 1, "right")
	insert(t, target, 1, "stale")
	insert(t, source, 2, "only-on-source")
	insert(t, target, 3, "only-on-target")

	repairer := &MongoRepairer{Source: source, Target: target}
	fixed, err := repairer.Repair(ctx, []Difference{
		{Key: mustKey(t, 1), Kind: Differing},
		{Key: mustKey(t, 2), Kind: Missing},
		{Key: mustKey(t, 3), Kind: Extra},
	})
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}
	if fixed != 3 {
		t.Errorf("Repair fixed %d of 3 differences", fixed)
	}

	var document struct {
		Value string `bson:"value"`
	}
	if err := target.FindOne(ctx, bson.M{"_id": 1}).Decode(&document); err != nil {
		t.Fatalf("the differing document: %v", err)
	}
	if document.Value != "right" {
		t.Errorf("the differing document reads %q, want the source's value", document.Value)
	}
	if err := target.FindOne(ctx, bson.M{"_id": 2}).Err(); err != nil {
		t.Errorf("the missing document was not written to the target: %v", err)
	}
	if err := target.FindOne(ctx, bson.M{"_id": 3}).Err(); err != mongo.ErrNoDocuments {
		t.Errorf("the target-only document is still there (%v)", err)
	}
}

// A key the source has lost since the comparison means the target should not
// have it either, which is a delete rather than a failed lookup.
func TestARepairOfSomethingTheSourceHasLostRemovesIt(t *testing.T) {
	source, target := mongoCollections(t)
	ctx := context.Background()
	insert(t, target, 9, "left over")

	repairer := &MongoRepairer{Source: source, Target: target}
	fixed, err := repairer.Repair(ctx, []Difference{{Key: mustKey(t, 9), Kind: Missing}})
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}
	if fixed != 1 {
		t.Errorf("Repair fixed %d, want 1", fixed)
	}
	if err := target.FindOne(ctx, bson.M{"_id": 9}).Err(); err != mongo.ErrNoDocuments {
		t.Errorf("the document the source has lost is still on the target (%v)", err)
	}

	if _, err := repairer.Repair(ctx, []Difference{{Key: "not-hex", Kind: Missing}}); err == nil {
		t.Error("a difference naming a key that is not a key was repaired")
	}
}

func mongoKeyOf(id int) (string, error) {
	raw, err := bson.Marshal(bson.D{{Key: "_id", Value: id}})
	if err != nil {
		return "", err
	}
	value := bson.Raw(raw).Lookup("_id")
	return keyFromID(value)
}

func mustKey(t *testing.T, id int) string {
	t.Helper()
	key, err := mongoKeyOf(id)
	if err != nil {
		t.Fatalf("render a key for %d: %v", id, err)
	}
	return key
}

func TestSQLColumnsComeBackInTheServersOwnOrder(t *testing.T) {
	db, err := sql.Open("mysql",
		fmt.Sprintf("root:root@tcp(%s)/source_db", harness.MySQLSource))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	table := strings.ReplaceAll(harness.UniqueName("cols"), "-", "_")
	if _, err := db.Exec("CREATE TABLE `" + table +
		"` (id INT PRIMARY KEY, zebra TEXT, apple TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	defer db.Exec("DROP TABLE IF EXISTS `" + table + "`")

	columns, err := SQLColumns(context.Background(), db, "source_db", table)
	if err != nil {
		t.Fatalf("SQLColumns: %v", err)
	}
	// Declaration order, not alphabetical: both sides have to hash the same
	// thing in the same order.
	want := []string{"id", "zebra", "apple"}
	if len(columns) != len(want) {
		t.Fatalf("read %v, want %v", columns, want)
	}
	for i := range want {
		if columns[i] != want[i] {
			t.Fatalf("read %v, want %v", columns, want)
		}
	}

	if got, err := SQLColumns(context.Background(), db, "source_db", "no_such_table"); err != nil {
		t.Errorf("a table that is not there reported %v", err)
	} else if len(got) != 0 {
		t.Errorf("a table that is not there reported columns %v", got)
	}
}

func TestATotalIsEveryKindOfDifference(t *testing.T) {
	result := Result{Missing: 2, Extra: 3, Differing: 4}
	if result.Total() != 9 {
		t.Errorf("Total() = %d, want 9", result.Total())
	}
	if result.Identical() {
		t.Error("a result with differences reported itself identical")
	}
	if (Result{}).Total() != 0 || !(Result{}).Identical() {
		t.Error("a result with no differences is not identical")
	}
}
