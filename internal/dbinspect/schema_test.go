package dbinspect

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestGetMongoFieldType(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
		want  string
	}{
		{"int", int(1), "int"},
		{"int32", int32(1), "int"},
		{"int64", int64(1), "int"},
		{"float32", float32(1.5), "float"},
		{"float64", float64(1.5), "float"},
		{"string", "x", "string"},
		{"bool", true, "bool"},
		{"time", time.Now(), "date"},
		{"bson.M", bson.M{"a": 1}, "object"},
		{"map", map[string]interface{}{"a": 1}, "object"},
		{"slice", []interface{}{1, 2}, "array"},
		{"nil", nil, "null"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := getMongoFieldType(tc.value); got != tc.want {
				t.Errorf("getMongoFieldType(%#v) = %q, want %q", tc.value, got, tc.want)
			}
		})
	}
}

// Anything outside the switch falls through to a Go type name, so the schema a
// client receives leaks driver-internal types instead of a database type.
func TestUnknownMongoTypesLeakGoTypeNames(t *testing.T) {
	tests := []struct {
		value interface{}
		want  string
	}{
		{primitive.NewObjectID(), "primitive.ObjectID"},
		{primitive.NewDateTimeFromTime(time.Now()), "primitive.DateTime"},
		{primitive.Decimal128{}, "primitive.Decimal128"},
		{bson.A{1, 2}, "primitive.A"},
		{bson.D{{Key: "a", Value: 1}}, "primitive.D"},
		{uint8(1), "uint8"},
		{[]byte("x"), "[]uint8"},
	}

	for _, tc := range tests {
		got := getMongoFieldType(tc.value)
		if got != tc.want {
			t.Fatalf("getMongoFieldType(%T) = %q, want %q — the switch appears to have been extended; assert the new database type instead", tc.value, got, tc.want)
		}
	}
}

// _id is the field a MongoDB schema always has, and it is reported as an
// opaque Go type name rather than something a client can map to a column type.
func TestObjectIDIsNotReportedAsAKnownType(t *testing.T) {
	if got := getMongoFieldType(primitive.NewObjectID()); got != "primitive.ObjectID" {
		t.Fatalf("getMongoFieldType(ObjectID) = %q — ObjectID appears to be handled now; assert the intended type instead", got)
	}
}

func TestExtractNestedFieldsFlattensWithDottedPaths(t *testing.T) {
	fields := map[string]string{}
	extractNestedFields(map[string]interface{}{
		"name": "alice",
		"address": map[string]interface{}{
			"city": "Tokyo",
			"geo": map[string]interface{}{
				"lat": 35.6,
			},
		},
	}, "", fields)

	want := map[string]string{
		"name":            "string",
		"address":         "object",
		"address.city":    "string",
		"address.geo":     "object",
		"address.geo.lat": "float",
	}
	if !reflect.DeepEqual(fields, want) {
		t.Errorf("fields = %#v, want %#v", fields, want)
	}
}

func TestExtractNestedFieldsHonoursThePrefix(t *testing.T) {
	fields := map[string]string{}
	extractNestedFields(map[string]interface{}{"id": 1}, "meta", fields)

	if _, ok := fields["meta.id"]; !ok {
		t.Errorf("fields = %#v, want a meta.id entry", fields)
	}
}

func TestExtractNestedFieldsWalksBSONTypes(t *testing.T) {
	fields := map[string]string{}
	extractNestedFields(map[string]interface{}{
		"m": bson.M{"inner": "v"},
		"d": bson.D{{Key: "inner", Value: int32(1)}},
	}, "", fields)

	if fields["m.inner"] != "string" {
		t.Errorf("m.inner = %q, want string (fields = %#v)", fields["m.inner"], fields)
	}
	if fields["d.inner"] != "int" {
		t.Errorf("d.inner = %q, want int (fields = %#v)", fields["d.inner"], fields)
	}
}

// A bson.D is walked into, but the entry recorded for the container itself is
// typed by the switch, which does not list bson.D — so the parent reads as a
// Go type name while its children read as database types.
func TestNestedBSONDContainerIsTypedAsAGoValue(t *testing.T) {
	fields := map[string]string{}
	extractNestedFields(map[string]interface{}{
		"d": bson.D{{Key: "inner", Value: "v"}},
	}, "", fields)

	if fields["d"] != "primitive.D" {
		t.Fatalf("d = %q, want primitive.D — bson.D appears to be typed as an object now", fields["d"])
	}
}

func TestExtractNestedFieldsDoesNotDescendIntoArrays(t *testing.T) {
	fields := map[string]string{}
	extractNestedFields(map[string]interface{}{
		"items": []interface{}{map[string]interface{}{"sku": "A1"}},
	}, "", fields)

	if fields["items"] != "array" {
		t.Errorf("items = %q, want array", fields["items"])
	}
	if _, ok := fields["items.sku"]; ok {
		t.Errorf("fields = %#v — array elements are now descended into; assert the new paths instead", fields)
	}
}

func TestSortFieldsByNamePutsThePrimaryFirst(t *testing.T) {
	schema := SchemaResponse{Fields: []Field{
		{Name: "zeta"},
		{Name: "alpha"},
		{Name: "_id", IsPrimary: true},
		{Name: "mid"},
	}}

	sortFieldsByName(&schema)

	got := make([]string, len(schema.Fields))
	for i, f := range schema.Fields {
		got[i] = f.Name
	}
	want := []string{"_id", "alpha", "mid", "zeta"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("order = %v, want %v", got, want)
	}
}

func TestSortFieldsByNameHandlesEmptyAndSingle(t *testing.T) {
	empty := SchemaResponse{}
	sortFieldsByName(&empty)
	if len(empty.Fields) != 0 {
		t.Errorf("empty schema gained fields: %#v", empty.Fields)
	}

	one := SchemaResponse{Fields: []Field{{Name: "only"}}}
	sortFieldsByName(&one)
	if one.Fields[0].Name != "only" {
		t.Errorf("single field became %q", one.Fields[0].Name)
	}
}

// The comparator reports less(i,j) and less(j,i) both true whenever two fields
// are primary, which is not a strict weak ordering and puts sort.Slice outside
// its contract. A composite primary key — routine in MySQL — therefore has no
// defined order, and the fields are not sorted by name either.
func TestCompositePrimaryKeysHaveNoDefinedOrder(t *testing.T) {
	less := func(a, b Field) bool {
		if a.IsPrimary {
			return true
		}
		if b.IsPrimary {
			return false
		}
		return a.Name < b.Name
	}
	pk1 := Field{Name: "tenant_id", IsPrimary: true}
	pk2 := Field{Name: "order_id", IsPrimary: true}

	if !less(pk1, pk2) || !less(pk2, pk1) {
		t.Fatal("the comparator is now a strict weak ordering — sortFieldsByName appears to be fixed; assert the sorted order instead")
	}

	// Confirm the consequence: primary fields do not come out sorted by name.
	schema := SchemaResponse{Fields: []Field{
		{Name: "zz_pk", IsPrimary: true},
		{Name: "aa_pk", IsPrimary: true},
		{Name: "mm_pk", IsPrimary: true},
	}}
	sortFieldsByName(&schema)

	names := make([]string, len(schema.Fields))
	for i, f := range schema.Fields {
		names[i] = f.Name
	}
	if sort.StringsAreSorted(names) {
		t.Fatalf("primary fields came out sorted (%v) — sortFieldsByName appears to be fixed; assert the sorted order instead", names)
	}
}

func TestGetTableSchemaHandlerRejectsMalformedJSON(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/tables/schema", bytes.NewBufferString("{not json"))
	rec := httptest.NewRecorder()

	GetTableSchemaHandler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestGetTableSchemaHandlerRejectsAnEmptyBody(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/tables/schema", bytes.NewBuffer(nil))
	rec := httptest.NewRecorder()

	GetTableSchemaHandler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestGetTableSchemaHandlerRejectsUnsupportedSourceTypes(t *testing.T) {
	for _, sourceType := range []string{"", "redis", "oracle", "MongoDB", "MySQL"} {
		body, _ := json.Marshal(SchemaRequest{SourceType: sourceType, TableName: "t"})
		req := httptest.NewRequest(http.MethodPost, "/tables/schema", bytes.NewReader(body))
		rec := httptest.NewRecorder()

		GetTableSchemaHandler(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("sourceType %q: status = %d, want 400", sourceType, rec.Code)
			continue
		}
		if got := rec.Header().Get("Content-Type"); got != "application/json" {
			t.Errorf("sourceType %q: Content-Type = %q, want application/json", sourceType, got)
		}
		var resp map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Errorf("sourceType %q: body is not JSON: %v", sourceType, err)
			continue
		}
		if resp["success"] != false {
			t.Errorf("sourceType %q: success = %v, want false", sourceType, resp["success"])
		}
	}
}

// The source type is matched case-sensitively while the rest of the codebase
// stores it lower-cased; a caller that sends the display-cased name gets
// "Unsupported database type" rather than a schema.
func TestSourceTypeMatchingIsCaseSensitive(t *testing.T) {
	body, _ := json.Marshal(SchemaRequest{SourceType: "MongoDB", TableName: "t"})
	req := httptest.NewRequest(http.MethodPost, "/tables/schema", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	GetTableSchemaHandler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d — the source type appears to be normalised now; assert the schema response instead", rec.Code)
	}
}

func TestSchemaFailuresReportAJSONError(t *testing.T) {
	body, _ := json.Marshal(map[string]interface{}{
		"sourceType": "mysql",
		"connection": map[string]string{
			"host": "127.0.0.1", "port": "1", "user": "svc",
			"password": "hunter2", "database": "app",
		},
		"tableName": "t",
	})
	req := httptest.NewRequest(http.MethodPost, "/tables/schema", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	GetTableSchemaHandler(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("body is not JSON: %v (%q)", err, rec.Body.String())
	}
	if resp["success"] != false {
		t.Errorf("success = %v, want false", resp["success"])
	}
	msg, _ := resp["message"].(string)
	if msg == "" {
		t.Errorf("message is empty (body: %s)", rec.Body.String())
	}
	// The DSN is assembled inside the driver, so the supplied password does not
	// reach the client. Guard that, since the message is otherwise verbatim.
	if bytes.Contains(rec.Body.Bytes(), []byte("hunter2")) {
		t.Errorf("the supplied password leaked into the response: %s", rec.Body.String())
	}
}
