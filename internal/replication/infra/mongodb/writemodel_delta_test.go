package mongodb

import (
	"fmt"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// An update is replicated as the fields it touched. The document it belongs to
// is not in the event and must not be needed: a megabyte document whose status
// flipped used to cost a megabyte through the lookup, over the link and into
// the target for one field.

func deltaOf(t *testing.T, description bson.D) *mongo.UpdateOneModel {
	t.Helper()

	syncer := &MongoDBSyncer{logger: logrus.New()}
	raw := rawEvent(t, append(
		changeDoc("shop", "orders", "update", bson.D{{Key: "_id", Value: "abc"}}),
		bson.E{Key: "updateDescription", Value: description},
	))

	model, err := syncer.convertRawBSONToWriteModel(raw, "shop", "orders")
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	update, ok := model.(*mongo.UpdateOneModel)
	if !ok {
		t.Fatalf("model is a %T, want an UpdateOneModel", model)
	}
	return update
}

func wholeDocumentReadOf(t *testing.T, description interface{}) *fullDocumentRead {
	t.Helper()

	syncer := &MongoDBSyncer{logger: logrus.New()}
	event := changeDoc("shop", "orders", "update", bson.D{{Key: "_id", Value: "abc"}})
	if description != nil {
		event = append(event, bson.E{Key: "updateDescription", Value: description})
	}

	model, err := syncer.convertRawBSONToWriteModel(rawEvent(t, event), "shop", "orders")
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	read, ok := model.(*fullDocumentRead)
	if !ok {
		t.Fatalf("model is a %T, want the change to be read as a whole document", model)
	}
	return read
}

func TestAnUpdateCarriesOnlyTheFieldsItTouched(t *testing.T) {
	update := deltaOf(t, bson.D{
		{Key: "updatedFields", Value: bson.D{{Key: "status", Value: 2}}},
		{Key: "removedFields", Value: bson.A{"note"}},
	})

	rendered := fmt.Sprint(update.Update)
	for _, want := range []string{"$set", "status", "$unset", "note"} {
		if !strings.Contains(rendered, want) {
			t.Errorf("update = %v, want it to carry %s", update.Update, want)
		}
	}
	if update.Upsert != nil && *update.Upsert {
		t.Error("the delta upserts, which would create a document holding only the " +
			"fields that changed")
	}
}

// The event says how long the array is now, not which elements went. Without
// this the target would keep elements the source has dropped, and nothing
// later in the stream would mention that array again.
func TestAnArrayTheSourceShortenedIsShortenedOnTheTarget(t *testing.T) {
	update := deltaOf(t, bson.D{
		{Key: "truncatedArrays", Value: bson.A{
			bson.D{{Key: "field", Value: "items"}, {Key: "newSize", Value: int32(2)}},
		}},
	})

	rendered := fmt.Sprint(update.Update)
	for _, want := range []string{"$push", "items", "$slice", "2"} {
		if !strings.Contains(rendered, want) {
			t.Errorf("update = %v, want it to carry %s", update.Update, want)
		}
	}
}

func TestAnArrayShortenedAlongsideAFieldChangeCarriesBoth(t *testing.T) {
	update := deltaOf(t, bson.D{
		{Key: "updatedFields", Value: bson.D{{Key: "status", Value: 2}}},
		{Key: "truncatedArrays", Value: bson.A{
			bson.D{{Key: "field", Value: "items"}, {Key: "newSize", Value: int32(1)}},
		}},
	})

	rendered := fmt.Sprint(update.Update)
	for _, want := range []string{"$set", "status", "$push", "items"} {
		if !strings.Contains(rendered, want) {
			t.Errorf("update = %v, want it to carry %s", update.Update, want)
		}
	}
}

// Anything that cannot be written as one update is read back as the whole
// document instead. The alternative is writing an update that is nearly the
// change.
func TestAChangeThatCannotBeWrittenAsOneUpdateIsReadWhole(t *testing.T) {
	for name, description := range map[string]interface{}{
		"neither a document nor a description": nil,
		"a description of nothing":             bson.D{},
		"an array shortened to nobody knows what": bson.D{
			{Key: "truncatedArrays", Value: bson.A{
				bson.D{{Key: "field", Value: "items"}},
			}},
		},
		"a field written and something inside it removed": bson.D{
			{Key: "updatedFields", Value: bson.D{{Key: "shipping", Value: bson.M{"to": "Osaka"}}}},
			{Key: "removedFields", Value: bson.A{"shipping.from"}},
		},
		"a field written and its array shortened": bson.D{
			{Key: "updatedFields", Value: bson.D{{Key: "items.0.qty", Value: 1}}},
			{Key: "truncatedArrays", Value: bson.A{
				bson.D{{Key: "field", Value: "items"}, {Key: "newSize", Value: int32(1)}},
			}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			read := wholeDocumentReadOf(t, description)
			if read.reason != reasonUndescribed {
				t.Errorf("reason = %q, want %q", read.reason, reasonUndescribed)
			}
			if read.filter["_id"] != "abc" {
				t.Errorf("filter = %v, want it to address the document", read.filter)
			}
			if read.why == "" {
				t.Error("nothing says why it could not be written as an update, so the " +
					"log would not either")
			}
		})
	}
}

func TestOverlappingPaths(t *testing.T) {
	for name, c := range map[string]struct {
		paths []string
		want  bool
	}{
		"a field and one inside it":  {[]string{"shipping", "shipping.to"}, true},
		"the same field twice":       {[]string{"status", "status"}, true},
		"two elements of an array":   {[]string{"items.0", "items.1"}, false},
		"a field and a longer name":  {[]string{"item", "items"}, false},
		"unrelated fields":           {[]string{"status", "total", "note"}, false},
		"nothing":                    {nil, false},
		"deep inside the same field": {[]string{"a.b", "a.b.c.d"}, true},
	} {
		t.Run(name, func(t *testing.T) {
			got := overlappingPath(c.paths)
			if (got != "") != c.want {
				t.Errorf("overlappingPath(%v) = %q, want overlap=%v", c.paths, got, c.want)
			}
		})
	}
}

// A length the server sent, whichever integer type the driver decoded it into.
// A newSize read as nothing would be a truncation written as "keep the first
// zero elements", which empties the array.
func TestALengthIsReadWhicheverNumberItArrivedAs(t *testing.T) {
	for name, value := range map[string]interface{}{
		"int":     3,
		"int32":   int32(3),
		"int64":   int64(3),
		"float64": float64(3),
	} {
		t.Run(name, func(t *testing.T) {
			got, ok := sizeOf(value)
			if !ok || got != 3 {
				t.Errorf("sizeOf(%v) = %d, %v; want 3, true", value, got, ok)
			}
		})
	}

	for name, value := range map[string]interface{}{
		"nothing":       nil,
		"a string":      "3",
		"half a number": 3.5,
	} {
		t.Run(name, func(t *testing.T) {
			if _, ok := sizeOf(value); ok {
				t.Errorf("sizeOf(%v) reported a length", value)
			}
		})
	}
}
