package dbinspect

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// A collection's shape is inferred by sampling documents, so the type names it
// reports come entirely from these. The switch used to name none of the types
// the driver actually decodes BSON into, so every one fell through to a Go type
// name: an _id came back as "bson.ObjectID" and every array as "bson.A".

func TestEveryBSONTypeIsNamedInItsOwnTerms(t *testing.T) {
	for name, c := range map[string]struct {
		value interface{}
		want  string
	}{
		"object id":   {bson.NewObjectID(), "objectId"},
		"string":      {"a", "string"},
		"symbol":      {bson.Symbol("a"), "string"},
		"javascript":  {bson.JavaScript("1+1"), "string"},
		"int32":       {int32(1), "int"},
		"int64":       {int64(1), "int"},
		"plain int":   {1, "int"},
		"timestamp":   {bson.Timestamp{T: 1, I: 1}, "int"},
		"float64":     {1.5, "float"},
		"float32":     {float32(1.5), "float"},
		"decimal128":  {bson.Decimal128{}, "decimal"},
		"bool":        {true, "bool"},
		"time":        {time.Now(), "date"},
		"datetime":    {bson.DateTime(0), "date"},
		"binary":      {bson.Binary{}, "binary"},
		"regex":       {bson.Regex{}, "regex"},
		"document":    {bson.M{"a": 1}, "object"},
		"plain map":   {map[string]interface{}{"a": 1}, "object"},
		"ordered doc": {bson.D{{Key: "a", Value: 1}}, "object"},
		"array":       {bson.A{1, 2}, "array"},
		"plain slice": {[]interface{}{1, 2}, "array"},
		"null":        {nil, "null"},
		"bson null":   {bson.Null{}, "null"},
	} {
		t.Run(name, func(t *testing.T) {
			if got := getMongoFieldType(c.value); got != c.want {
				t.Errorf("getMongoFieldType(%T) = %q, want %q", c.value, got, c.want)
			}
		})
	}
}

// TestAnUnknownTypeFallsBackToItsGoName rather than to "" or a panic: the
// sample is whatever the collection holds.
func TestAnUnknownTypeFallsBackToItsGoName(t *testing.T) {
	type odd struct{}
	if got := getMongoFieldType(odd{}); got == "" {
		t.Error("an unrecognised value produced no type at all")
	}
}

// TestNestedFieldsAreFlattenedWithTheirPath. The schema is a flat list, so a
// field inside a subdocument has to arrive as "parent.child" -- reported as
// "child" it collides with a top-level field of the same name.
func TestNestedFieldsAreFlattenedWithTheirPath(t *testing.T) {
	fields := map[string]string{}
	extractNestedFields(map[string]interface{}{
		"_id":  bson.NewObjectID(),
		"name": "a",
		"address": bson.M{
			"city": "Tokyo",
			"geo":  bson.M{"lat": 35.6},
		},
	}, "", fields)

	for path, want := range map[string]string{
		"_id":             "objectId",
		"name":            "string",
		"address":         "object",
		"address.city":    "string",
		"address.geo":     "object",
		"address.geo.lat": "float",
	} {
		if got := fields[path]; got != want {
			t.Errorf("field %q = %q, want %q (all: %v)", path, got, want, fields)
		}
	}
}

// TestAnOrderedSubdocumentIsWalkedToo: the driver decodes some documents as
// bson.D rather than bson.M, and a walk that only knew the map form reported
// the subdocument as one opaque field.
func TestAnOrderedSubdocumentIsWalkedToo(t *testing.T) {
	fields := map[string]string{}
	extractNestedFields(map[string]interface{}{
		"payment": bson.D{{Key: "amount", Value: int64(100)}},
	}, "", fields)

	if got := fields["payment.amount"]; got != "int" {
		t.Errorf("payment.amount = %q, want int (all: %v)", got, fields)
	}
}

func TestAPrefixIsCarriedIntoTheNextLevel(t *testing.T) {
	fields := map[string]string{}
	extractNestedFields(map[string]interface{}{"b": "x"}, "a", fields)

	if _, ok := fields["a.b"]; !ok {
		t.Errorf("the prefix was dropped: %v", fields)
	}
}

func TestFieldsAreSortedByName(t *testing.T) {
	schema := &SchemaResponse{Fields: []Field{
		{Name: "zebra"}, {Name: "apple"}, {Name: "mango"},
	}}

	sortFieldsByName(schema)

	for i, want := range []string{"apple", "mango", "zebra"} {
		if schema.Fields[i].Name != want {
			t.Fatalf("fields = %v, want them in name order", schema.Fields)
		}
	}
}

// TestThePrimaryKeyComesFirstWhateverItIsCalled: the list is read by somebody
// choosing a key for a mapping, and a key named "z_id" buried at the bottom of
// forty columns is the one thing they are looking for.
func TestThePrimaryKeyComesFirstWhateverItIsCalled(t *testing.T) {
	schema := &SchemaResponse{Fields: []Field{
		{Name: "amount"},
		{Name: "z_id", IsPrimary: true},
		{Name: "created_at"},
	}}

	sortFieldsByName(schema)

	if !schema.Fields[0].IsPrimary || schema.Fields[0].Name != "z_id" {
		t.Errorf("fields = %v, want the primary key first", schema.Fields)
	}
	if schema.Fields[1].Name != "amount" || schema.Fields[2].Name != "created_at" {
		t.Errorf("the rest are not in name order: %v", schema.Fields)
	}
}

// TestSeveralPrimaryKeyColumnsAreAllListedFirst covers a composite key, which
// a payment ledger's tables commonly have. Within each group the order is by
// name: this list is what the schema endpoint shows, not the key itself, so
// alphabetical is the useful order and the column order of the key is not
// something it carries.
func TestSeveralPrimaryKeyColumnsAreAllListedFirst(t *testing.T) {
	schema := &SchemaResponse{Fields: []Field{
		{Name: "amount"},
		{Name: "entry", IsPrimary: true},
		{Name: "account", IsPrimary: true},
	}}

	sortFieldsByName(schema)

	if !schema.Fields[0].IsPrimary || !schema.Fields[1].IsPrimary {
		t.Fatalf("both key columns should lead: %v", schema.Fields)
	}
	if schema.Fields[2].IsPrimary {
		t.Errorf("a non-key column was listed among the key ones: %v", schema.Fields)
	}
	if schema.Fields[0].Name != "account" || schema.Fields[1].Name != "entry" {
		t.Errorf("the key columns are not in name order: %v", schema.Fields)
	}
}
