package dbinspect

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
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

// TestTheDriversTypesAreNamedAsDatabaseTypes covers what a caller receives. The
// switch named none of the types the driver actually decodes BSON into, so every
// one of them fell through to a Go type name — an _id was "primitive.ObjectID"
// and every array was "primitive.A", because the array case matched a bare
// []interface{} while the driver produces primitive.A. The interface builds
// table mappings out of this, so every schema query returned at least two type
// names it could not map.
func TestTheDriversTypesAreNamedAsDatabaseTypes(t *testing.T) {
	tests := []struct {
		value interface{}
		want  string
	}{
		{primitive.NewObjectID(), "objectId"},
		{primitive.NewDateTimeFromTime(time.Now()), "date"},
		{primitive.Decimal128{}, "decimal"},
		{bson.A{1, 2}, "array"},
		{bson.D{{Key: "a", Value: 1}}, "object"},
		{primitive.Binary{}, "binary"},
		{primitive.Null{}, "null"},
	}

	for _, tc := range tests {
		if got := getMongoFieldType(tc.value); got != tc.want {
			t.Errorf("getMongoFieldType(%T) = %q, want %q", tc.value, got, tc.want)
		}
	}
}

// TestAnUnknownTypeIsStillNamed is the other half: something the switch does not
// list still comes back as something rather than as nothing.
func TestAnUnknownTypeIsStillNamed(t *testing.T) {
	if got := getMongoFieldType(uint8(1)); got == "" {
		t.Error("an unrecognised type was reported as nothing")
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

// TestANestedContainerIsTypedLikeItsChildren covers a bson.D, which is walked
// into. The entry recorded for the container itself was typed by the switch,
// which did not list bson.D — so the parent read as a Go type name while its
// children read as database types.
func TestANestedContainerIsTypedLikeItsChildren(t *testing.T) {
	fields := map[string]string{}
	extractNestedFields(map[string]interface{}{
		"d": bson.D{{Key: "inner", Value: "v"}},
	}, "", fields)

	if fields["d"] != "object" {
		t.Errorf("d = %q, want object", fields["d"])
	}
	if fields["d.inner"] != "string" {
		t.Errorf("d.inner = %q, want string", fields["d.inner"])
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

// TestACompositePrimaryKeyHasADefinedOrder covers the ordinary MySQL case. The
// comparator answered true for both (i,j) and (j,i) when both fields were
// primary keys, which is not a strict weak ordering and is not something
// sort.Slice promises anything about — so a composite key came back in no
// defined order, and those columns were not sorted by name either.
func TestACompositePrimaryKeyHasADefinedOrder(t *testing.T) {
	schema := SchemaResponse{Fields: []Field{
		{Name: "zz_pk", IsPrimary: true},
		{Name: "body"},
		{Name: "aa_pk", IsPrimary: true},
		{Name: "amount"},
		{Name: "mm_pk", IsPrimary: true},
	}}
	sortFieldsByName(&schema)

	names := make([]string, len(schema.Fields))
	for i, f := range schema.Fields {
		names[i] = f.Name
	}

	want := []string{"aa_pk", "mm_pk", "zz_pk", "amount", "body"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Errorf("fields = %v, want %v", names, want)
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
	for _, sourceType := range []string{"", "redis", "oracle", "cassandra"} {
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

// TestTheSourceTypeIsMatchedWithoutRegardToCase covers the name the interface
// displays. The comparison was exact while the rest of the tree stores the type
// lower-cased, so a caller sending "MongoDB" or "MySQL" — which is what it shows
// — was told the database type was unsupported rather than getting a schema.
func TestTheSourceTypeIsMatchedWithoutRegardToCase(t *testing.T) {
	for _, sourceType := range []string{"MongoDB", "MySQL", " postgresql "} {
		body, _ := json.Marshal(SchemaRequest{SourceType: sourceType, TableName: "t"})
		req := httptest.NewRequest(http.MethodPost, "/tables/schema", bytes.NewReader(body))
		rec := httptest.NewRecorder()

		GetTableSchemaHandler(rec, req)

		// Not 400: the type is recognised, so the request gets as far as trying
		// to connect and fails there instead.
		if rec.Code == http.StatusBadRequest {
			t.Errorf("sourceType %q was rejected as unsupported", sourceType)
		}
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
