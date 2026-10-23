package mongodb

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// The progress of a chunked copy is one thing.
func TestAnIdSurvivesBeingWrittenDownAndReadBack(t *testing.T) {
	oid := bson.NewObjectID()
	for _, c := range []struct {
		name string
		id   interface{}
	}{
		{"an ObjectId, the default", oid},
		{"a string key", "user:42"},
		{"an integer key", int64(9007199254740993)}, // past what a float64 holds
		{"a compound key", bson.D{{Key: "tenant", Value: "a"}, {Key: "seq", Value: int64(7)}}},
		{"a binary key", bson.Binary{Subtype: 0, Data: []byte{1, 2, 3}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			stored, err := encodeID(rawOf(c.id))
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if stored == "" {
				t.Fatal("encoded to nothing, so the copy would restart from the beginning")
			}

			got, err := decodeID(stored)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			// Compare through the same rendering both sides use, which is what
			// the query filter is built from.
			if want, have := rawOf(c.id), rawOf(got); !equalRaw(want, have) {
				t.Errorf("round trip changed the key:\n stored %s\n got    %v (%s)\n want   %v (%s)",
					stored, have.Value, have.Type, want.Value, want.Type)
			}
		})
	}
}

func equalRaw(a, b bson.RawValue) bool {
	if a.Type != b.Type || len(a.Value) != len(b.Value) {
		return false
	}
	for i := range a.Value {
		if a.Value[i] != b.Value[i] {
			return false
		}
	}
	return true
}

// TestNoIdYetEncodesToNothing is the first chunk's case: there is no previous
// key, and inventing one would start the copy in the middle.
func TestNoIdYetEncodesToNothing(t *testing.T) {
	stored, err := encodeID(bson.RawValue{})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if stored != "" {
		t.Errorf("an absent key encoded to %q, want empty", stored)
	}
}

// TestAnUnreadableStoredKeyIsRefused.
func TestAnUnreadableStoredKeyIsRefused(t *testing.T) {
	if _, err := decodeID("{not json"); err == nil {
		t.Fatal("a corrupted key was accepted")
	}
}

// TestAnIdThatCannotBeRenderedIsEmptyRatherThanWrong. rawOf cannot fail loudly
// — it is used where an error has nowhere to go — so it has to return something
// that encodeID then treats as "no progress yet" rather than something that
// looks like a real key.
func TestAnIdThatCannotBeRenderedIsEmptyRatherThanWrong(t *testing.T) {
	raw := rawOf(func() {}) // a function has no BSON representation
	if raw.Type != 0 {
		t.Fatalf("an unrenderable id produced type %s, want the empty value", raw.Type)
	}
	stored, err := encodeID(raw)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if stored != "" {
		t.Errorf("an unrenderable id encoded to %q, want empty", stored)
	}
}
