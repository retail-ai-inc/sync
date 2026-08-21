//go:build integration

package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/test/harness"
)

const (
	schemaSourceDB = "source_db"
	schemaTargetDB = "target_db"
)

func openSchemaMySQL(t *testing.T, endpoint, database string) *sql.DB {
	t.Helper()

	host, port := harness.SplitHostPort(t, endpoint)
	dsn := config.BuildDSNByType("mysql", map[string]string{
		"user": "root", "password": "root", "host": host, "port": port, "database": database,
	})
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open %s: %v", endpoint, err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping %s: %v", endpoint, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// schemaRequestBody builds a POST /api/tables/schema body for the MySQL source.
func schemaRequestBody(t *testing.T, table string) []byte {
	t.Helper()

	host, port := harness.SplitHostPort(t, harness.MySQLSource)
	body, err := json.Marshal(map[string]interface{}{
		"sourceType": "mysql",
		"connection": map[string]string{
			"host": host, "port": port, "user": "root", "password": "root",
			"database": schemaSourceDB,
		},
		"tableName": table,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return body
}

func postSchema(t *testing.T, body []byte) (*httptest.ResponseRecorder, map[string]interface{}) {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/tables/schema", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	GetTableSchemaHandler(rec, req)

	var resp map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("body is not JSON: %v (%q)", err, rec.Body.String())
	}
	return rec, resp
}

func fieldsFrom(t *testing.T, resp map[string]interface{}) []map[string]interface{} {
	t.Helper()

	data, ok := resp["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("data is missing: %#v", resp)
	}
	raw, ok := data["fields"].([]interface{})
	if !ok {
		t.Fatalf("fields is missing: %#v", data)
	}
	out := make([]map[string]interface{}, 0, len(raw))
	for _, f := range raw {
		out = append(out, f.(map[string]interface{}))
	}
	return out
}

func TestGetMySQLSchemaReturnsColumnsAndTypes(t *testing.T) {
	table := harness.UniqueName("schema")
	src := openSchemaMySQL(t, harness.MySQLSource, schemaSourceDB)

	if _, err := src.Exec(fmt.Sprintf(`
		CREATE TABLE %s (
			id     INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			email  VARCHAR(100) NOT NULL,
			amount DECIMAL(10,2),
			note   TEXT,
			active TINYINT(1) DEFAULT 1
		)`, table)); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() { _, _ = src.Exec("DROP TABLE " + table) })

	rec, resp := postSchema(t, schemaRequestBody(t, table))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (body: %s)", rec.Code, rec.Body.String())
	}
	if resp["success"] != true {
		t.Fatalf("success = %v", resp["success"])
	}

	byName := map[string]map[string]interface{}{}
	for _, f := range fieldsFrom(t, resp) {
		byName[f["name"].(string)] = f
	}
	if len(byName) != 5 {
		t.Fatalf("got %d fields, want 5: %v", len(byName), byName)
	}

	// COLUMN_TYPE is reported verbatim, including the width and precision.
	for name, wantType := range map[string]string{
		"id":     "int",
		"email":  "varchar(100)",
		"amount": "decimal(10,2)",
		"note":   "text",
		"active": "tinyint(1)",
	} {
		f, ok := byName[name]
		if !ok {
			t.Errorf("field %q is missing", name)
			continue
		}
		if f["type"] != wantType {
			t.Errorf("%s type = %v, want %v", name, f["type"], wantType)
		}
	}

	if byName["id"]["isPrimary"] != true {
		t.Errorf("id isPrimary = %v, want true", byName["id"]["isPrimary"])
	}
	for _, name := range []string{"email", "amount", "note", "active"} {
		if byName[name]["isPrimary"] != false {
			t.Errorf("%s isPrimary = %v, want false", name, byName[name]["isPrimary"])
		}
	}
}

func TestGetMySQLSchemaPutsThePrimaryKeyFirst(t *testing.T) {
	table := harness.UniqueName("schemaorder")
	src := openSchemaMySQL(t, harness.MySQLSource, schemaSourceDB)

	// Declare the primary key last so ordinal position and sorted order differ.
	if _, err := src.Exec(fmt.Sprintf(
		"CREATE TABLE %s (zeta INT, alpha INT, mid INT, pk INT NOT NULL PRIMARY KEY)", table)); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() { _, _ = src.Exec("DROP TABLE " + table) })

	_, resp := postSchema(t, schemaRequestBody(t, table))

	var order []string
	for _, f := range fieldsFrom(t, resp) {
		order = append(order, f["name"].(string))
	}
	want := []string{"pk", "alpha", "mid", "zeta"}
	for i := range want {
		if i >= len(order) || order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
}

// A composite primary key has no defined order (T-076): sortFieldsByName's
// comparator reports less(i,j) and less(j,i) both true for two primary fields,
// so sort.Slice is outside its contract and the primary columns come out
// neither in declaration order nor sorted by name.
func TestACompositePrimaryKeyHasNoDefinedOrder(t *testing.T) {
	table := harness.UniqueName("schemacomposite")
	src := openSchemaMySQL(t, harness.MySQLSource, schemaSourceDB)

	if _, err := src.Exec(fmt.Sprintf(
		"CREATE TABLE %s (zz_pk INT NOT NULL, aa_pk INT NOT NULL, mm_pk INT NOT NULL, payload INT, PRIMARY KEY (zz_pk, aa_pk, mm_pk))",
		table)); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() { _, _ = src.Exec("DROP TABLE " + table) })

	_, resp := postSchema(t, schemaRequestBody(t, table))

	fields := fieldsFrom(t, resp)
	var primaries []string
	for _, f := range fields {
		if f["isPrimary"] == true {
			primaries = append(primaries, f["name"].(string))
		}
	}
	if len(primaries) != 3 {
		t.Fatalf("got %d primary fields, want 3: %v", len(primaries), primaries)
	}

	sorted := true
	for i := 1; i < len(primaries); i++ {
		if primaries[i-1] > primaries[i] {
			sorted = false
		}
	}
	if sorted {
		t.Fatalf("the primary columns came out sorted (%v) — sortFieldsByName appears to be fixed; assert the sorted order instead", primaries)
	}
}

// A table that does not exist is not an error: INFORMATION_SCHEMA simply
// returns no rows, and the handler answers 200 with an empty field list. The
// only trace is a warn-level log line, so a client cannot distinguish "this
// table has no columns" from "this table does not exist".
func TestAMissingTableLooksLikeAnEmptySchema(t *testing.T) {
	_, resp := postSchema(t, schemaRequestBody(t, "table_that_does_not_exist"))

	if resp["success"] != true {
		t.Fatalf("success = %v — a missing table appears to be reported now; assert the error instead", resp["success"])
	}
	data := resp["data"].(map[string]interface{})
	if data["fields"] != nil {
		t.Fatalf("fields = %#v, want null for a missing table", data["fields"])
	}
}

func TestGetMySQLSchemaRequiresAUser(t *testing.T) {
	host, port := harness.SplitHostPort(t, harness.MySQLSource)
	body, err := json.Marshal(map[string]interface{}{
		"sourceType": "mysql",
		"connection": map[string]string{
			"host": host, "port": port, "user": "", "password": "root",
			"database": schemaSourceDB,
		},
		"tableName": "orders",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	rec, resp := postSchema(t, body)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
	msg, _ := resp["message"].(string)
	if msg == "" {
		t.Errorf("message is empty: %s", rec.Body.String())
	}
}

func TestGetMySQLSchemaReportsBadCredentials(t *testing.T) {
	host, port := harness.SplitHostPort(t, harness.MySQLSource)
	body, err := json.Marshal(map[string]interface{}{
		"sourceType": "mysql",
		"connection": map[string]string{
			"host": host, "port": port, "user": "root", "password": "wrong-password",
			"database": schemaSourceDB,
		},
		"tableName": "orders",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	rec, resp := postSchema(t, body)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
	if resp["success"] != false {
		t.Errorf("success = %v, want false", resp["success"])
	}
	// The supplied password must not be echoed back.
	if bytes.Contains(rec.Body.Bytes(), []byte("wrong-password")) {
		t.Errorf("the supplied password leaked into the response: %s", rec.Body.String())
	}
}

// The seeded fixture tables from init.sql are readable, which also proves the
// dispatch accepts the lower-cased type the rest of the codebase stores.
func TestGetMySQLSchemaOnTheSeededTables(t *testing.T) {
	for _, table := range []string{"orders", "users"} {
		t.Run(table, func(t *testing.T) {
			rec, resp := postSchema(t, schemaRequestBody(t, table))
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d (body: %s)", rec.Code, rec.Body.String())
			}
			if len(fieldsFrom(t, resp)) == 0 {
				t.Errorf("%s returned no fields", table)
			}
		})
	}
}

// ------------------------------------------------------------- MongoDB

func openSchemaMongo(t *testing.T) *mongo.Client {
	t.Helper()

	uri := "mongodb://" + harness.MongoSource + "/?directConnection=true"
	client, err := mongo.Connect(t.Context(), options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("connect %s: %v", harness.MongoSource, err)
	}
	if err := client.Ping(t.Context(), nil); err != nil {
		t.Fatalf("ping %s: %v", harness.MongoSource, err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	return client
}

func mongoSchemaBody(t *testing.T, collection string) []byte {
	t.Helper()

	host, port := harness.SplitHostPort(t, harness.MongoSource)
	body, err := json.Marshal(map[string]interface{}{
		"sourceType": "mongodb",
		"connection": map[string]string{
			"host": host, "port": port, "database": schemaSourceDB,
		},
		"tableName": collection,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return body
}

func TestGetMongoDBSchemaInfersFieldsFromDocuments(t *testing.T) {
	collection := harness.UniqueName("mschema")
	client := openSchemaMongo(t)
	coll := client.Database(schemaSourceDB).Collection(collection)
	t.Cleanup(func() { _ = coll.Drop(context.Background()) })

	if _, err := coll.InsertOne(t.Context(), bson.M{
		"name":   "alice",
		"age":    int32(30),
		"score":  4.5,
		"active": true,
		"tags":   bson.A{"a", "b"},
		"address": bson.M{
			"city": "Tokyo",
			"geo":  bson.M{"lat": 35.6},
		},
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	rec, resp := postSchema(t, mongoSchemaBody(t, collection))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (body: %s)", rec.Code, rec.Body.String())
	}

	byName := map[string]map[string]interface{}{}
	for _, f := range fieldsFrom(t, resp) {
		byName[f["name"].(string)] = f
	}

	for name, wantType := range map[string]string{
		"name":   "string",
		"age":    "int",
		"score":  "float",
		"active": "bool",
		// Not "array": the driver decodes a BSON array into primitive.A, which
		// getMongoFieldType's `case []interface{}` does not match. See
		// TestTheArrayBranchIsDeadForRealDocuments.
		"tags":            "primitive.A",
		"address":         "object",
		"address.city":    "string",
		"address.geo":     "object",
		"address.geo.lat": "float",
	} {
		f, ok := byName[name]
		if !ok {
			t.Errorf("field %q is missing (got %v)", name, byName)
			continue
		}
		if f["type"] != wantType {
			t.Errorf("%s type = %v, want %v", name, f["type"], wantType)
		}
	}

	if byName["_id"]["isPrimary"] != true {
		t.Errorf("_id isPrimary = %v, want true", byName["_id"]["isPrimary"])
	}
	if byName["name"]["isPrimary"] != false {
		t.Errorf("name isPrimary = %v, want false", byName["name"]["isPrimary"])
	}
}

func TestGetMongoDBSchemaMergesFieldsAcrossDocuments(t *testing.T) {
	collection := harness.UniqueName("mmerge")
	client := openSchemaMongo(t)
	coll := client.Database(schemaSourceDB).Collection(collection)
	t.Cleanup(func() { _ = coll.Drop(context.Background()) })

	// Different documents carry different fields; the union must be reported.
	if _, err := coll.InsertMany(t.Context(), []interface{}{
		bson.M{"a": "x"},
		bson.M{"b": int32(1)},
		bson.M{"c": true},
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	_, resp := postSchema(t, mongoSchemaBody(t, collection))

	seen := map[string]bool{}
	for _, f := range fieldsFrom(t, resp) {
		seen[f["name"].(string)] = true
	}
	for _, want := range []string{"a", "b", "c", "_id"} {
		if !seen[want] {
			t.Errorf("field %q is missing: %v", want, seen)
		}
	}
}

// The sample is the last ten documents by natural order. A field that exists
// only in older documents is invisible to the schema, so a collection whose
// shape changed at some point reports only the newest shape — and the UI builds
// table mappings from this list.
func TestOnlyTheNewestTenDocumentsAreSampled(t *testing.T) {
	collection := harness.UniqueName("msample")
	client := openSchemaMongo(t)
	coll := client.Database(schemaSourceDB).Collection(collection)
	t.Cleanup(func() { _ = coll.Drop(context.Background()) })

	// One old document with a field nothing else has.
	if _, err := coll.InsertOne(t.Context(), bson.M{"legacy_field": "present only here"}); err != nil {
		t.Fatalf("insert legacy: %v", err)
	}
	// Twenty newer documents without it.
	var newer []interface{}
	for i := 0; i < 20; i++ {
		newer = append(newer, bson.M{"current_field": int32(i)})
	}
	if _, err := coll.InsertMany(t.Context(), newer); err != nil {
		t.Fatalf("insert newer: %v", err)
	}

	_, resp := postSchema(t, mongoSchemaBody(t, collection))

	seen := map[string]bool{}
	for _, f := range fieldsFrom(t, resp) {
		seen[f["name"].(string)] = true
	}
	if !seen["current_field"] {
		t.Fatalf("current_field is missing, so the sample did not work at all: %v", seen)
	}
	if seen["legacy_field"] {
		t.Fatalf("legacy_field was found — the sample appears to cover the whole collection now; assert the full schema instead")
	}
}

// _id is present in every document and is reported with a Go type name
// (T-081), because getMongoFieldType's switch has no case for ObjectID.
func TestTheMongoIDTypeIsAGoTypeName(t *testing.T) {
	collection := harness.UniqueName("mid")
	client := openSchemaMongo(t)
	coll := client.Database(schemaSourceDB).Collection(collection)
	t.Cleanup(func() { _ = coll.Drop(context.Background()) })

	if _, err := coll.InsertOne(t.Context(), bson.M{"n": int32(1)}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	_, resp := postSchema(t, mongoSchemaBody(t, collection))

	for _, f := range fieldsFrom(t, resp) {
		if f["name"] == "_id" {
			if f["type"] != "primitive.ObjectID" {
				t.Fatalf("_id type = %v — ObjectID appears to be handled now; assert the database type instead", f["type"])
			}
			return
		}
	}
	t.Fatal("_id was not reported at all")
}

// An empty MongoDB collection returns an empty array while an empty MySQL
// result returns null (T-115), so the same endpoint answers with two different
// shapes depending on the engine.
func TestAnEmptyMongoCollectionReturnsAnArrayNotNull(t *testing.T) {
	collection := harness.UniqueName("mempty")
	client := openSchemaMongo(t)
	coll := client.Database(schemaSourceDB).Collection(collection)
	// Create the collection without documents.
	if err := client.Database(schemaSourceDB).CreateCollection(t.Context(), collection); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	t.Cleanup(func() { _ = coll.Drop(context.Background()) })

	rec, resp := postSchema(t, mongoSchemaBody(t, collection))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (body: %s)", rec.Code, rec.Body.String())
	}
	data := resp["data"].(map[string]interface{})
	if data["fields"] == nil {
		t.Fatalf("fields = null — MongoDB now matches MySQL's shape; assert the unified shape instead")
	}
	if n := len(data["fields"].([]interface{})); n != 0 {
		t.Errorf("fields has %d entries, want 0", n)
	}
}

// A collection that does not exist behaves exactly like an empty one: 200 with
// no fields. As with MySQL (T-115) a typo is indistinguishable from an empty
// collection.
func TestAMissingMongoCollectionLooksEmpty(t *testing.T) {
	rec, resp := postSchema(t, mongoSchemaBody(t, "collection_that_does_not_exist"))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d — a missing collection appears to be reported now; assert the error instead", rec.Code)
	}
	if resp["success"] != true {
		t.Fatalf("success = %v, want true", resp["success"])
	}
}

func TestGetMongoDBSchemaReportsAnUnreachableHost(t *testing.T) {
	body, err := json.Marshal(map[string]interface{}{
		"sourceType": "mongodb",
		"connection": map[string]string{
			"host": "127.0.0.1", "port": "1", "database": schemaSourceDB,
		},
		"tableName": "anything",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	rec, resp := postSchema(t, body)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
	if resp["success"] != false {
		t.Errorf("success = %v, want false", resp["success"])
	}
}

// Credentials are applied only when both the user and the password are
// non-empty, so a user configured without a password connects anonymously
// instead of failing or authenticating.
func TestMongoCredentialsAreIgnoredWithoutAPassword(t *testing.T) {
	collection := harness.UniqueName("mnopw")
	client := openSchemaMongo(t)
	coll := client.Database(schemaSourceDB).Collection(collection)
	t.Cleanup(func() { _ = coll.Drop(context.Background()) })

	if _, err := coll.InsertOne(t.Context(), bson.M{"n": int32(1)}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	host, port := harness.SplitHostPort(t, harness.MongoSource)
	body, err := json.Marshal(map[string]interface{}{
		"sourceType": "mongodb",
		"connection": map[string]string{
			"host": host, "port": port, "database": schemaSourceDB,
			"user": "someone", "password": "", // password omitted
		},
		"tableName": collection,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	rec, resp := postSchema(t, body)

	// The server has no authentication enabled, so an anonymous connection
	// succeeds; a URI carrying the username would have been rejected.
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d — the username appears to be applied now; assert the authentication failure instead (body: %s)", rec.Code, rec.Body.String())
	}
	if resp["success"] != true {
		t.Errorf("success = %v, want true", resp["success"])
	}
}

// getMongoFieldType has a `case []interface{}: return "array"` branch, but the
// driver decodes every BSON array into primitive.A — a named type that the case
// does not match. The branch is therefore dead for real documents, and arrays
// are always reported as the Go type name. The unit test that shows "array"
// passes only because it constructs a plain []interface{} by hand.
func TestTheArrayBranchIsDeadForRealDocuments(t *testing.T) {
	collection := harness.UniqueName("marray")
	client := openSchemaMongo(t)
	coll := client.Database(schemaSourceDB).Collection(collection)
	t.Cleanup(func() { _ = coll.Drop(context.Background()) })

	if _, err := coll.InsertOne(t.Context(), bson.M{
		"strings": bson.A{"a", "b"},
		"numbers": bson.A{1, 2, 3},
		"empty":   bson.A{},
		"nested":  bson.A{bson.M{"k": "v"}},
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	_, resp := postSchema(t, mongoSchemaBody(t, collection))

	for _, f := range fieldsFrom(t, resp) {
		name := f["name"].(string)
		switch name {
		case "strings", "numbers", "empty", "nested":
			if f["type"] != "primitive.A" {
				t.Fatalf("%s type = %v — arrays appear to be recognised now; assert \"array\" instead", name, f["type"])
			}
		}
	}
}
