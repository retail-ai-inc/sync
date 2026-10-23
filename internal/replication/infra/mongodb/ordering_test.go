package mongodb

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func replaceFor(id interface{}, value string) mongo.WriteModel {
	return mongo.NewReplaceOneModel().
		SetFilter(bson.M{"_id": id}).
		SetReplacement(bson.M{"_id": id, "v": value}).
		SetUpsert(true)
}

func deleteFor(id interface{}) mongo.WriteModel {
	return mongo.NewDeleteOneModel().SetFilter(bson.M{"_id": id})
}

func insertFor(id interface{}) mongo.WriteModel {
	return mongo.NewInsertOneModel().SetDocument(bson.M{"_id": id})
}

// runShape reports how many models each run holds, which is what the splitting
// rule is about.
func runShape(runs [][]mongo.WriteModel) []int {
	shape := make([]int, len(runs))
	for i, run := range runs {
		shape[i] = len(run)
	}
	return shape
}

func TestTheKeyIsReadFromEveryDocumentShape(t *testing.T) {
	want := idOf(bson.M{"_id": "a"})
	if want != "a" {
		t.Fatalf("idOf(bson.M) = %v", want)
	}
	if got := idOf(bson.D{{Key: "v", Value: 1}, {Key: "_id", Value: "a"}}); got != "a" {
		t.Errorf("idOf(bson.D) = %v", got)
	}
	if got := idOf(map[string]interface{}{"_id": "a"}); got != "a" {
		t.Errorf("idOf(map) = %v", got)
	}
	if got := idOf("not a document"); got != nil {
		t.Errorf("idOf(string) = %v, want nil", got)
	}
}

func indexOf(models []mongo.WriteModel, want mongo.WriteModel) int {
	for i, model := range models {
		if model == want {
			return i
		}
	}
	return -1
}

// clusterTime counts whole seconds, so a delay measured from it reports up to a
// second that is not there. wallTime is the primary's own clock at the change.
func TestTheWallTimeIsReadWhenTheServerSendsOne(t *testing.T) {
	at := time.UnixMilli(1757000000123)
	raw, err := bson.Marshal(bson.M{
		"clusterTime": bson.Timestamp{T: uint32(at.Unix()), I: 1},
		"wallTime":    bson.NewDateTimeFromTime(at),
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	wall, ok := eventWallTime(bson.Raw(raw))
	if !ok {
		t.Fatal("wallTime was not read")
	}
	if !wall.Equal(at) {
		t.Errorf("wallTime = %v, want %v", wall, at)
	}
	// The millisecond is the whole point: the ordering clock has already lost it.
	if wall.Nanosecond() == 0 {
		t.Error("the sub-second part was discarded")
	}

	ordering, ok := eventClusterTime(bson.Raw(raw))
	if !ok {
		t.Fatal("clusterTime was not read")
	}
	if ordering.Nanosecond() != 0 {
		t.Error("the ordering clock is expected to be whole seconds")
	}
}

// A server too old to send one has to leave the field empty rather than report
// a zero time, which would read as 1970 and a lag of fifty years.
func TestNoWallTimeIsNotAZeroWallTime(t *testing.T) {
	raw, err := bson.Marshal(bson.M{"clusterTime": bson.Timestamp{T: 1757000000, I: 1}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, ok := eventWallTime(bson.Raw(raw)); ok {
		t.Error("an event with no wallTime reported one")
	}

	zeroed, err := bson.Marshal(bson.M{"wallTime": bson.DateTime(0)})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, ok := eventWallTime(bson.Raw(zeroed)); ok {
		t.Error("a zero wallTime was accepted")
	}
}
