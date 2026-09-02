//go:build integration

package mongodb

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/retail-ai-inc/sync/test/harness"
)

// A batch used to be split into groups that could be applied independently,
// and independence was decided by the _id: two changes to one document kept
// their order, everything else could move.
func TestAUniqueValueHandedFromOneDocumentToAnother(t *testing.T) {
	src, tgt := connect(t, harness.MongoSource), connect(t, harness.MongoTarget)

	collection := harness.UniqueName("handover")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	source := src.Database(sourceDB).Collection(collection)
	target := tgt.Database(targetDB).Collection(collection)
	t.Cleanup(func() {
		clean, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		_ = source.Drop(clean)
		_ = target.Drop(clean)
	})

	// The unique index is what makes the two documents dependent on each other.
	// It goes on both sides: the source to make the handover meaningful, the
	// target to make an out-of-order apply fail rather than quietly pass.
	unique := mongo.IndexModel{
		Keys:    bson.D{{Key: "email", Value: 1}},
		Options: options.Index().SetUnique(true),
	}
	for _, coll := range []*mongo.Collection{source, target} {
		if _, err := coll.Indexes().CreateOne(ctx, unique); err != nil {
			t.Fatalf("create the unique index on %s: %v", coll.Name(), err)
		}
	}
	if _, err := source.InsertOne(ctx, bson.M{"_id": 1, "email": "a@b", "n": 0}); err != nil {
		t.Fatalf("seed the source: %v", err)
	}

	stop := startSyncer(t, syncTask(t, collection))
	defer stop()

	harness.Eventually(t, 45*time.Second, func() error {
		if n := countIn(t, tgt, targetDB, collection, bson.M{}); n != 1 {
			return errCount("the copy", 1, n)
		}
		return nil
	})

	// One transaction, so the three changes land in one batch: a batch is only
	// ever cut on a transaction boundary. The first write is there to make the
	// _id repeat, which is what the split keys on.
	session, err := src.StartSession()
	if err != nil {
		t.Fatalf("start a session: %v", err)
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(sc context.Context) (interface{}, error) {
		if _, err := source.UpdateOne(sc, bson.M{"_id": 1}, bson.M{"$set": bson.M{"n": 1}}); err != nil {
			return nil, err
		}
		if _, err := source.DeleteOne(sc, bson.M{"_id": 1}); err != nil {
			return nil, err
		}
		_, err := source.InsertOne(sc, bson.M{"_id": 2, "email": "a@b", "n": 0})
		return nil, err
	})
	if err != nil {
		t.Fatalf("hand the value over: %v", err)
	}

	harness.Eventually(t, 45*time.Second, func() error {
		n := countIn(t, tgt, targetDB, collection, bson.M{"_id": 2})
		if n != 1 {
			return errCount("the handover", 1, n)
		}
		return nil
	})

	if n := countIn(t, tgt, targetDB, collection, bson.M{"_id": 1}); n != 0 {
		t.Errorf("document 1 is still on the target; the delete did not land")
	}

	var held struct {
		Email string `bson:"email"`
	}
	if err := target.FindOne(ctx, bson.M{"_id": 2}).Decode(&held); err != nil {
		t.Fatalf("document 2 is not on the target: %v", err)
	}
	if held.Email != "a@b" {
		t.Errorf("document 2 holds %q, want the value it was handed", held.Email)
	}
}

func errCount(what string, want, got int64) error {
	return &countMismatch{what: what, want: want, got: got}
}

type countMismatch struct {
	what string
	want int64
	got  int64
}

func (e *countMismatch) Error() string {
	return e.what + ": target holds " + itoa(e.got) + " documents, want " + itoa(e.want)
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}
