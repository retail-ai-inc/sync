package mongodb

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
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

// ------------------------------------------------------------- key extraction

func TestTheDocumentKeyComesFromTheFilter(t *testing.T) {
	models := []mongo.WriteModel{
		replaceFor("a", "1"),
		deleteFor("a"),
		mongo.NewUpdateOneModel().SetFilter(bson.M{"_id": "a"}).SetUpdate(bson.M{"$set": bson.M{"v": 1}}),
	}

	first, ok := documentKey(models[0])
	if !ok {
		t.Fatal("the replace model has no key")
	}
	for i, model := range models[1:] {
		got, ok := documentKey(model)
		if !ok {
			t.Fatalf("model %d has no key", i+1)
		}
		if got != first {
			t.Errorf("model %d keyed differently from the replace on the same _id", i+1)
		}
	}
}

func TestTheDocumentKeyComesFromTheInsertedDocument(t *testing.T) {
	got, ok := documentKey(insertFor("a"))
	if !ok {
		t.Fatal("the insert model has no key")
	}
	want, _ := documentKey(deleteFor("a"))
	if got != want {
		t.Error("an insert and a delete of the same document keyed differently")
	}
}

// TestAnObjectIDIsNotTheSameAsItsStringForm pins why the key is the BSON
// encoding rather than the printed value: two documents with different _id
// types must not be treated as one.
func TestAnObjectIDIsNotTheSameAsItsStringForm(t *testing.T) {
	id := primitive.NewObjectID()

	fromID, ok := documentKey(deleteFor(id))
	if !ok {
		t.Fatal("an ObjectID _id produced no key")
	}
	fromString, ok := documentKey(deleteFor(id.Hex()))
	if !ok {
		t.Fatal("a string _id produced no key")
	}
	if fromID == fromString {
		t.Error("an ObjectID and its hex string keyed the same")
	}
}

func TestAModelWithNoIdentifiableDocumentHasNoKey(t *testing.T) {
	for name, model := range map[string]mongo.WriteModel{
		"no _id in the filter": mongo.NewDeleteOneModel().SetFilter(bson.M{"customer": "Ada"}),
		"no _id in the insert": mongo.NewInsertOneModel().SetDocument(bson.M{"v": 1}),
		"a model we never emit": mongo.NewUpdateManyModel().
			SetFilter(bson.M{"_id": "a"}).SetUpdate(bson.M{"$set": bson.M{"v": 1}}),
	} {
		t.Run(name, func(t *testing.T) {
			if _, ok := documentKey(model); ok {
				t.Error("a key was produced for a model that identifies no document")
			}
		})
	}
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

// ---------------------------------------------------------------- run splitting

// TestDistinctDocumentsShareOneRun is the common case, and the reason the batch
// is not simply ordered: writes to unrelated documents still go out together.
func TestDistinctDocumentsShareOneRun(t *testing.T) {
	runs := orderedRuns([]mongo.WriteModel{
		replaceFor("a", "1"), replaceFor("b", "1"), replaceFor("c", "1"),
	})

	if len(runs) != 1 {
		t.Fatalf("runs = %v, want one run of three", runShape(runs))
	}
}

// TestRepeatedWritesToOneDocumentAreSeparated is the defect the split exists
// for: unordered, the server could apply the second replace before the first
// and the target would keep the older value for good.
func TestRepeatedWritesToOneDocumentAreSeparated(t *testing.T) {
	runs := orderedRuns([]mongo.WriteModel{
		replaceFor("a", "first"),
		replaceFor("a", "second"),
		replaceFor("a", "third"),
	})

	if got := runShape(runs); len(got) != 3 {
		t.Fatalf("runs = %v, want three runs of one", got)
	}
	for i, run := range runs {
		replace := run[0].(*mongo.ReplaceOneModel)
		want := []string{"first", "second", "third"}[i]
		if got := replace.Replacement.(bson.M)["v"]; got != want {
			t.Errorf("run %d holds %v, want %q", i, got, want)
		}
	}
}

// TestUnrelatedWritesFillTheEarlierRuns records that the split is not one run
// per model: only the repeats are pushed later.
func TestUnrelatedWritesFillTheEarlierRuns(t *testing.T) {
	runs := orderedRuns([]mongo.WriteModel{
		replaceFor("a", "1"),
		replaceFor("a", "2"),
		replaceFor("b", "1"),
		replaceFor("c", "1"),
	})

	if got := runShape(runs); len(got) != 2 || got[0] != 3 || got[1] != 1 {
		t.Errorf("runs = %v, want [3 1]", got)
	}
}

// TestADeleteAfterAReplaceKeepsItsPlace covers the sequence that matters most:
// a document written and then removed must not come back because the two landed
// the wrong way round.
func TestADeleteAfterAReplaceKeepsItsPlace(t *testing.T) {
	runs := orderedRuns([]mongo.WriteModel{
		replaceFor("a", "1"),
		deleteFor("a"),
	})

	if len(runs) != 2 {
		t.Fatalf("runs = %v, want the delete in its own run", runShape(runs))
	}
	if _, ok := runs[1][0].(*mongo.DeleteOneModel); !ok {
		t.Error("the second run does not hold the delete")
	}
}

// TestAnUnidentifiableModelClosesTheRunsBeforeIt records the barrier: a model
// whose document cannot be named must not be reordered against anything.
func TestAnUnidentifiableModelClosesTheRunsBeforeIt(t *testing.T) {
	unknown := mongo.NewDeleteManyModel().SetFilter(bson.M{"stale": true})
	runs := orderedRuns([]mongo.WriteModel{
		replaceFor("a", "1"),
		unknown,
		replaceFor("b", "1"),
	})

	if got := runShape(runs); len(got) != 3 {
		t.Fatalf("runs = %v, want each model in its own run", got)
	}
	if runs[1][0] != unknown {
		t.Error("the barrier is not in the middle run")
	}
}

func TestAnEmptyBatchHasNoRuns(t *testing.T) {
	if runs := orderedRuns(nil); len(runs) != 0 {
		t.Errorf("runs = %v for an empty batch", runShape(runs))
	}
}

// TestEveryModelSurvivesTheSplit is the invariant that matters beyond ordering:
// splitting must not drop a write.
func TestEveryModelSurvivesTheSplit(t *testing.T) {
	models := []mongo.WriteModel{
		replaceFor("a", "1"), replaceFor("b", "1"), replaceFor("a", "2"),
		deleteFor("c"), insertFor("d"), replaceFor("a", "3"), deleteFor("b"),
	}

	var flattened []mongo.WriteModel
	for _, run := range orderedRuns(models) {
		flattened = append(flattened, run...)
	}

	if len(flattened) != len(models) {
		t.Fatalf("the split produced %d models from %d", len(flattened), len(models))
	}
	// Each document's own writes must stay in source order.
	positions := map[string][]int{}
	for _, model := range flattened {
		if key, ok := documentKey(model); ok {
			positions[key] = append(positions[key], indexOf(models, model))
		}
	}
	for key, order := range positions {
		for i := 1; i < len(order); i++ {
			if order[i] < order[i-1] {
				t.Errorf("document %q had its writes reordered: %v", key, order)
				break
			}
		}
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
