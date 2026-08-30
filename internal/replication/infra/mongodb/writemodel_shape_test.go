package mongodb

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// TestANestedDocumentIsReadWhicheverShapeTheDriverGaveIt.
//
// The same change stream event reaches this code as bson.M, bson.D, bson.Raw or
// a plain map depending on how it was decoded and which path it came down —
// a document read from the stream, one built for a repair, one round-tripped
// through a checkpoint. A shape this misses returns nil, and a nil update
// document writes nothing while reporting success: the replica quietly stops
// receiving a field, and only a comparison much later finds it.
func TestANestedDocumentIsReadWhicheverShapeTheDriverGaveIt(t *testing.T) {
	raw, err := bson.Marshal(bson.M{"city": "Osaka", "postcode": "530-0001"})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	for _, c := range []struct {
		name  string
		value interface{}
	}{
		{"bson.M, as the stream usually decodes it", bson.M{"city": "Osaka", "postcode": "530-0001"}},
		{"a plain map", map[string]interface{}{"city": "Osaka", "postcode": "530-0001"}},
		{"bson.D, which keeps field order", bson.D{{Key: "city", Value: "Osaka"}, {Key: "postcode", Value: "530-0001"}}},
		{"bson.Raw, undecoded", bson.Raw(raw)},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := documentOf(c.value)
			if got == nil {
				t.Fatal("read as nothing — an update built from this would write no " +
					"fields and still report success")
			}
			if got["city"] != "Osaka" {
				t.Errorf("city = %v, want Osaka", got["city"])
			}
			if got["postcode"] != "530-0001" {
				t.Errorf("postcode = %v, want 530-0001", got["postcode"])
			}
		})
	}
}

// TestReadingADocumentDoesNotShareItsStorage. The map is handed to the driver to
// build an update from; if it aliased the event's own map, a later change to
// either would show up in the other, and the write would carry a value the
// source never had at that point in the stream.
func TestReadingADocumentDoesNotShareItsStorage(t *testing.T) {
	original := bson.M{"city": "Osaka"}

	got := documentOf(original)
	got["city"] = "Tokyo"

	if original["city"] != "Osaka" {
		t.Errorf("the event's own document was changed to %v", original["city"])
	}
}

// TestSomethingThatIsNotADocumentReadsAsNothing, rather than as an empty
// document that would be written as one.
func TestSomethingThatIsNotADocumentReadsAsNothing(t *testing.T) {
	for _, value := range []interface{}{nil, "a string", 42, []int{1, 2}} {
		if got := documentOf(value); got != nil {
			t.Errorf("documentOf(%v) = %v, want nil", value, got)
		}
	}
}

// TestAnUnreadableRawDocumentReadsAsNothing. Corrupted bytes must not become an
// empty update that silently clears nothing and reports success.
func TestAnUnreadableRawDocumentReadsAsNothing(t *testing.T) {
	if got := documentOf(bson.Raw([]byte{1, 2, 3})); got != nil {
		t.Errorf("unreadable bytes read as %v, want nil", got)
	}
}
