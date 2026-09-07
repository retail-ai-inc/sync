// deltacheck exercises the deployed syncer against the real cluster: it makes
// a collection in the replicated database, fills it, changes it in the ways a
// delta can get wrong, and compares the two sides document by document.
//
// It never drops the source collection: a drop is a schema change the task
// refuses on purpose, and refusing stops replication. It empties it instead.
//
// It is its own module, because it is a tool rather than part of the syncer
// and counting it as uncovered code says nothing about the syncer's tests:
//
//	cd scripts/deltacheck && go run . -uri "$DELTACHECK_URI" -documents 3000
//
// The connection string goes in the environment and not into a file: it holds
// the cluster's password.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var (
	uri        = flag.String("uri", os.Getenv("DELTACHECK_URI"), "connection string for the mongos both databases are on")
	sourceDB   = flag.String("source", "naviee_retail_ai", "the replicated database")
	targetDB   = flag.String("target", "naviee_retail_ai_bk", "the database it is replicated to")
	collection = flag.String("collection", "", "the collection to make (default: a new one named after the time)")
	documents  = flag.Int("documents", 3000, "how many documents to write")
	burst      = flag.Int("burst", 1000, "how many changes to one document, back to back")
	patience   = flag.Duration("patience", 5*time.Minute, "how long to wait for the target to catch up")
	keep       = flag.Bool("keep", false, "leave the documents behind instead of removing them")
)

func main() {
	flag.Parse()
	if *uri == "" {
		fail("no connection string: pass -uri or set DELTACHECK_URI")
	}
	if *collection == "" {
		*collection = fmt.Sprintf("zz_deltacheck_%s", time.Now().UTC().Format("20060102T150405"))
	}

	ctx := context.Background()
	client, err := mongo.Connect(options.Client().ApplyURI(*uri))
	if err != nil {
		fail("connect: %v", err)
	}
	defer client.Disconnect(ctx)
	if err := client.Ping(ctx, nil); err != nil {
		fail("ping: %v", err)
	}

	source := client.Database(*sourceDB).Collection(*collection)
	target := client.Database(*targetDB).Collection(*collection)
	say("collection %s.%s -> %s.%s", *sourceDB, *collection, *targetDB, *collection)

	seed(ctx, client, source)
	waitForCount(ctx, target, int64(*documents), "the first copy")

	change(ctx, source)
	compare(ctx, source, target)

	if !*keep {
		empty(ctx, source, target)
	}
	say("PASS")
}

// seed makes the collection and fills it. The documents carry a nested object,
// an array and a blob, so a delta has something other than flat fields to get
// wrong.
func seed(ctx context.Context, client *mongo.Client, source *mongo.Collection) {
	// Sharded like everything else in this database, so the changes arrive
	// through the same merge every other collection's do.
	err := client.Database("admin").RunCommand(ctx, bson.D{
		{Key: "shardCollection", Value: *sourceDB + "." + *collection},
		{Key: "key", Value: bson.D{{Key: "_id", Value: "hashed"}}},
	}).Err()
	if err != nil {
		say("not sharding the collection (%v); carrying on unsharded", err)
	}

	blob := strings.Repeat("z", 2<<10)
	batch := make([]interface{}, 0, 500)
	for i := 0; i < *documents; i++ {
		batch = append(batch, bson.M{
			"_id":      id(i),
			"seq":      0,
			"a":        0,
			"b":        0,
			"c":        0,
			"round":    0,
			"status":   "new",
			"customer": bson.M{"id": fmt.Sprintf("CUS-%06d", i%500), "tier": "gold"},
			"items":    bson.A{1, 2, 3, 4, 5},
			"blob":     blob,
		})
		if len(batch) == 500 {
			insert(ctx, source, batch)
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		insert(ctx, source, batch)
	}
	say("wrote %d documents to the source", *documents)
}

func insert(ctx context.Context, source *mongo.Collection, batch []interface{}) {
	if _, err := source.InsertMany(ctx, batch); err != nil {
		fail("insert: %v", err)
	}
}

// change makes every kind of change a delta has to carry.
func change(ctx context.Context, source *mongo.Collection) {
	started := time.Now()

	// Many changes to one document, back to back, so that a batch carries more
	// than one of them: that is the only arrangement in which the order inside
	// a batch can be got wrong.
	for i := 1; i <= *burst; i++ {
		update(ctx, source, id(0), bson.M{"$set": bson.M{"seq": i}})
	}
	say("%d changes to one document in %s", *burst, time.Since(started).Round(time.Millisecond))

	// One field of every document, one statement each.
	for i := 0; i < *documents; i++ {
		update(ctx, source, id(i), bson.M{"$set": bson.M{"status": "captured", "a": i}})
	}
	say("one change to each of %d documents", *documents)

	// One statement, every document: updateMany is one change event per
	// document it matched.
	for _, round := range []int{1, 2} {
		if _, err := source.UpdateMany(ctx, bson.M{}, bson.M{"$set": bson.M{"round": round}}); err != nil {
			fail("updateMany round %d: %v", round, err)
		}
	}
	say("two updateMany over %d documents", *documents)

	// A field removed and written again, which must not arrive the other way
	// round.
	update(ctx, source, id(1), bson.M{"$unset": bson.M{"c": ""}})
	update(ctx, source, id(1), bson.M{"$set": bson.M{"c": 99}})

	// Something inside the nested object, addressed by its path.
	update(ctx, source, id(2), bson.M{"$set": bson.M{"customer.tier": "platinum"}})

	// An array shortened, which the event describes by its new length rather
	// than by the elements it dropped.
	update(ctx, source, id(3), bson.M{"$push": bson.M{"items": bson.M{"$each": bson.A{}, "$slice": 2}}})

	// A document changed and removed, then written again under the same _id.
	update(ctx, source, id(4), bson.M{"$set": bson.M{"status": "void"}})
	if _, err := source.DeleteOne(ctx, bson.M{"_id": id(4)}); err != nil {
		fail("delete: %v", err)
	}
	if _, err := source.InsertOne(ctx, bson.M{
		"_id": id(4), "seq": 0, "a": 4, "b": 0, "c": 0, "round": 2,
		"status": "rewritten", "customer": bson.M{"id": "CUS-000004", "tier": "gold"},
		"items": bson.A{1, 2, 3, 4, 5}, "blob": strings.Repeat("z", 2<<10),
	}); err != nil {
		fail("insert again: %v", err)
	}
	say("all changes made in %s", time.Since(started).Round(time.Millisecond))
}

func update(ctx context.Context, source *mongo.Collection, key string, change bson.M) {
	if _, err := source.UpdateOne(ctx, bson.M{"_id": key}, change); err != nil {
		fail("update %s: %v", key, err)
	}
}

// compare waits for the two sides to hold the same documents, field for field.
// A count would pass with a field holding the value it had three changes ago.
func compare(ctx context.Context, source, target *mongo.Collection) {
	deadline := time.Now().Add(*patience)
	var last string
	for {
		want := read(ctx, source)
		got := read(ctx, target)

		if difference := differ(want, got); difference == "" {
			say("the two sides hold the same %d documents, field for field", len(want))
			named(want, got)
			return
		} else {
			last = difference
		}
		if time.Now().After(deadline) {
			fail("the target never caught up: %s", last)
		}
		time.Sleep(2 * time.Second)
	}
}

// named checks the changes this was built around by name, so a failure says
// which one broke rather than which document.
func named(want, got map[string]bson.M) {
	checks := map[string][2]interface{}{
		"the last of a burst of changes to one field": {got[id(0)]["seq"], want[id(0)]["seq"]},
		"a field removed and written again":           {got[id(1)]["c"], want[id(1)]["c"]},
		"the last updateMany":                         {got[id(2)]["round"], want[id(2)]["round"]},
		"a document rewritten under the same _id":     {got[id(4)]["status"], want[id(4)]["status"]},
	}
	for what, pair := range checks {
		if fmt.Sprint(pair[0]) != fmt.Sprint(pair[1]) {
			fail("%s: the target holds %v and the source %v", what, pair[0], pair[1])
		}
		say("%s: %v on both sides", what, pair[0])
	}

	items, _ := got[id(3)]["items"].(bson.A)
	if len(items) != 2 {
		fail("an array shortened to two elements is %v on the target", got[id(3)]["items"])
	}
	say("an array shortened on the source is %d elements on the target", len(items))

	if tier := fmt.Sprint(got[id(2)]["customer"]); !strings.Contains(tier, "platinum") {
		fail("a change inside a nested object did not arrive: %v", got[id(2)]["customer"])
	}
	say("a change addressed by its path inside a nested object arrived")
}

func read(ctx context.Context, coll *mongo.Collection) map[string]bson.M {
	cursor, err := coll.Find(ctx, bson.M{})
	if err != nil {
		fail("read %s: %v", coll.Name(), err)
	}
	defer cursor.Close(ctx)

	out := map[string]bson.M{}
	for cursor.Next(ctx) {
		var document bson.M
		if err := cursor.Decode(&document); err != nil {
			fail("decode: %v", err)
		}
		out[fmt.Sprint(document["_id"])] = document
	}
	if err := cursor.Err(); err != nil {
		fail("read %s: %v", coll.Name(), err)
	}
	return out
}

// differ reports the first difference between the two sides, or "".
func differ(want, got map[string]bson.M) string {
	if len(want) != len(got) {
		return fmt.Sprintf("the source holds %d documents and the target %d", len(want), len(got))
	}
	for key, source := range want {
		replica, ok := got[key]
		if !ok {
			return fmt.Sprintf("the target does not hold %s", key)
		}
		for field, value := range source {
			held, ok := replica[field]
			if !ok {
				return fmt.Sprintf("%s is missing the field %s, which is %v on the source",
					key, field, value)
			}
			if !reflect.DeepEqual(value, held) {
				return fmt.Sprintf("%s.%s is %v on the target and %v on the source",
					key, field, held, value)
			}
		}
		for field := range replica {
			if _, ok := source[field]; !ok {
				return fmt.Sprintf("%s holds %s on the target, which the source does not", key, field)
			}
		}
	}
	return ""
}

// empty removes the documents and leaves the collection. Dropping it at the
// source is a schema change the task refuses, and refusing stops replication.
func empty(ctx context.Context, source, target *mongo.Collection) {
	if _, err := source.DeleteMany(ctx, bson.M{}); err != nil {
		fail("empty the source: %v", err)
	}
	waitForCount(ctx, target, 0, "the deletions")
	say("both sides emptied; the collection itself is left behind on purpose")
}

func waitForCount(ctx context.Context, coll *mongo.Collection, want int64, what string) {
	deadline := time.Now().Add(*patience)
	for {
		n, err := coll.CountDocuments(ctx, bson.M{})
		if err != nil {
			fail("count: %v", err)
		}
		if n == want {
			say("%s: %d documents on the target", what, n)
			return
		}
		if time.Now().After(deadline) {
			fail("%s never arrived: %d of %d documents", what, n, want)
		}
		time.Sleep(2 * time.Second)
	}
}

func id(i int) string { return fmt.Sprintf("deltacheck-%06d", i) }

func say(format string, args ...interface{}) {
	fmt.Printf("%s  %s\n", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...))
}

func fail(format string, args ...interface{}) {
	fmt.Printf("%s  FAIL: %s\n", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...))
	os.Exit(1)
}
