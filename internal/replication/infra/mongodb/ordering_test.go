package mongodb

import (
	"testing"

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
