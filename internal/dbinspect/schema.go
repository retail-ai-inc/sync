package dbinspect

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type SchemaRequest struct {
	SourceType string `json:"sourceType"`
	Connection struct {
		Host     string `json:"host"`
		Port     string `json:"port"`
		User     string `json:"user"`
		Password string `json:"password"`
		Database string `json:"database"`
	} `json:"connection"`
	TableName string `json:"tableName"`
}

type Field struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	IsPrimary bool   `json:"isPrimary"`
}

type SchemaResponse struct {
	Fields []Field `json:"fields"`
}

// TableSchemaHandler processes POST requests to get table/collection structure
func GetTableSchemaHandler(w http.ResponseWriter, r *http.Request) {
	var req SchemaRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logrus.Errorf("[Schema] Failed to parse request: %v", err)
		http.Error(w, "Invalid request parameters", http.StatusBadRequest)
		return
	}

	logrus.Infof("[Schema] Received table structure request: %s, %s.%s", req.SourceType, req.Connection.Database, req.TableName)

	var schema SchemaResponse
	var err error

	// Matched without regard to case. It used to be an exact comparison, and the
	// interface displays these names capitalised — so a caller passing "MongoDB"
	// or "MySQL", which is what it shows, was told the database type was
	// unsupported rather than getting a schema.
	switch strings.ToLower(strings.TrimSpace(req.SourceType)) {
	case "mongodb":
		schema, err = getMongoDBSchema(r.Context(), req)
	case "mysql", "mariadb":
		schema, err = getMySQLSchema(r.Context(), req)
	case "postgresql":
		schema, err = getPostgreSQLSchema(r.Context(), req)
	default:
		response := map[string]interface{}{
			"success": false,
			"message": fmt.Sprintf("Unsupported database type: %s", req.SourceType),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(response)
		return
	}

	if err != nil {
		logrus.Errorf("[Schema] Failed to get table structure: %v", err)
		response := map[string]interface{}{
			"success": false,
			"message": fmt.Sprintf("Failed to get table structure: %v", err),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(response)
		return
	}

	// Sort fields before returning results
	sortFieldsByName(&schema)

	response := map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"fields": schema.Fields,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// sortFieldsByName puts the key columns first and orders the rest by name.
//
// The comparison used to answer true for both (i,j) and (j,i) when both were
// primary keys, which is not a strict weak ordering and is not something
// sort.Slice promises anything about. A composite primary key — the ordinary
// case in MySQL — therefore came back in no defined order, and those columns
// were not sorted by name either.
func sortFieldsByName(schema *SchemaResponse) {
	sort.SliceStable(schema.Fields, func(i, j int) bool {
		if schema.Fields[i].IsPrimary != schema.Fields[j].IsPrimary {
			return schema.Fields[i].IsPrimary
		}
		return schema.Fields[i].Name < schema.Fields[j].Name
	})
}

func getMongoDBSchema(c context.Context, req SchemaRequest) (SchemaResponse, error) {
	uri := fmt.Sprintf("mongodb://%s:%s/%s", req.Connection.Host, req.Connection.Port, req.Connection.Database)
	if req.Connection.User != "" && req.Connection.Password != "" {
		escapedUser := url.QueryEscape(req.Connection.User)
		escapedPassword := url.QueryEscape(req.Connection.Password)

		uri = fmt.Sprintf("mongodb://%s:%s@%s:%s/%s?authSource=admin",
			escapedUser, escapedPassword,
			req.Connection.Host, req.Connection.Port, req.Connection.Database)
	}

	// Set connection timeout
	ctx, cancel := context.WithTimeout(c, 30*time.Second)
	defer cancel()

	// Connect to MongoDB - set connection options
	clientOptions := options.Client().
		ApplyURI(uri).
		SetConnectTimeout(10 * time.Second).
		SetServerSelectionTimeout(10 * time.Second).
		SetDirect(true) // Direct mode, don't try to discover replica set

	client, err := mongo.Connect(clientOptions)
	if err != nil {
		return SchemaResponse{}, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}
	defer func() {
		if err := client.Disconnect(ctx); err != nil {
			logrus.Errorf("[MongoDB] Failed to disconnect: %v", err)
		}
	}()

	// Validate connection
	if err := client.Ping(ctx, nil); err != nil {
		return SchemaResponse{}, fmt.Errorf("mongoDB connection test failed: %w", err)
	}

	// Get collection document sample to infer structure
	collection := client.Database(req.Connection.Database).Collection(req.TableName)

	// Get sample documents to extract nested fields (last 10 documents)
	var sampleDocs []bson.M
	// A hundred documents, newest first. It used to be ten, and the shape a
	// collection is reported to have is what the interface builds table mappings
	// out of — so a field that only the older documents carry was invisible, and
	// a collection whose shape had changed reported only its newest form.
	findOptions := options.Find().SetSort(bson.D{{Key: "$natural", Value: -1}}).SetLimit(schemaSampleSize)
	findCursor, err := collection.Find(ctx, bson.M{}, findOptions)
	if err != nil {
		return SchemaResponse{}, fmt.Errorf("failed to query documents: %w", err)
	}
	defer findCursor.Close(ctx)

	if err := findCursor.All(ctx, &sampleDocs); err != nil {
		return SchemaResponse{}, fmt.Errorf("failed to decode documents: %w", err)
	}

	// If collection is empty, return empty field list
	if len(sampleDocs) == 0 {
		logrus.Warnf("[MongoDB] Collection is empty, unable to infer structure: %s.%s", req.Connection.Database, req.TableName)
		return SchemaResponse{Fields: []Field{}}, nil
	}

	// Create field mapping to get unique fields and types from sample documents
	fieldMap := make(map[string]string)

	// Process sample documents to discover all fields (top-level and nested)
	for _, doc := range sampleDocs {
		extractNestedFields(doc, "", fieldMap)
	}

	// Build field response
	var fields []Field
	for field, fieldType := range fieldMap {
		if fieldType == "" {
			fieldType = "unknown" // Set default value for unknown type
		}

		fields = append(fields, Field{
			Name:      field,
			Type:      fieldType,
			IsPrimary: field == "_id",
		})
	}

	return SchemaResponse{Fields: fields}, nil
}

func extractNestedFields(doc map[string]interface{}, prefix string, fields map[string]string) {
	for k, v := range doc {
		fieldName := k
		if prefix != "" {
			fieldName = prefix + "." + k
		}

		fields[fieldName] = getMongoFieldType(v)

		if nested, ok := v.(map[string]interface{}); ok {
			extractNestedFields(nested, fieldName, fields)
		} else if nested, ok := v.(bson.M); ok {
			extractNestedFields(nested, fieldName, fields)
		} else if nested, ok := v.(bson.D); ok {
			nestedMap := make(map[string]interface{})
			for _, elem := range nested {
				nestedMap[elem.Key] = elem.Value
			}
			extractNestedFields(nestedMap, fieldName, fields)
		}
	}
}

// schemaSampleSize is how many documents a collection's shape is inferred from.
// More than this and a large collection makes the endpoint slow; fewer and a
// field that is not on every document goes unnoticed.
const schemaSampleSize = 100

// getMongoFieldType gets MongoDB field type.
//
// The switch used to name none of the types the driver actually decodes BSON
// into, so every one of them fell through and came back as a Go type name: an
// _id was "bson.ObjectID" and every array was "bson.A", because the
// array case matched a bare []interface{} and the driver produces bson.A.
// Every schema query therefore returned at least two type names the caller —
// the interface that builds table mappings out of this — cannot map.
func getMongoFieldType(value interface{}) string {
	switch value.(type) {
	case int, int32, int64, bson.Timestamp:
		return "int"
	case float32, float64:
		return "float"
	case bson.Decimal128:
		return "decimal"
	case string, bson.Symbol, bson.JavaScript:
		return "string"
	case bool:
		return "bool"
	case time.Time, bson.DateTime:
		return "date"
	case bson.ObjectID:
		return "objectId"
	case bson.Binary:
		return "binary"
	case bson.Regex:
		return "regex"
	case bson.M, map[string]interface{}, bson.D:
		return "object"
	case []interface{}, bson.A:
		return "array"
	case nil, bson.Null:
		return "null"
	default:
		return fmt.Sprintf("%T", value)
	}
}

func getMySQLSchema(c context.Context, req SchemaRequest) (SchemaResponse, error) {
	// Check username and password
	if req.Connection.User == "" {
		// If user doesn't provide username, use default or return error
		return SchemaResponse{}, fmt.Errorf("MySQL connection requires a username")
	}

	// Build DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?timeout=10s&parseTime=true&multiStatements=true&charset=utf8mb4",
		req.Connection.User, req.Connection.Password,
		req.Connection.Host, req.Connection.Port,
		req.Connection.Database)

	// Connect to database
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return SchemaResponse{}, fmt.Errorf("failed to connect to MySQL: %w", err)
	}
	defer db.Close()

	// Set database connection parameters
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	// Set timeout
	ctx, cancel := context.WithTimeout(c, 10*time.Second)
	defer cancel()

	// Validate connection
	if err := db.PingContext(ctx); err != nil {
		return SchemaResponse{}, fmt.Errorf("mySQL connection test failed: %w", err)
	}

	// Query table structure
	query := `
		SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_KEY 
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`
	rows, err := db.QueryContext(ctx, query, req.Connection.Database, req.TableName)
	if err != nil {
		return SchemaResponse{}, fmt.Errorf("failed to query table structure: %w", err)
	}
	defer rows.Close()

	var fields []Field
	for rows.Next() {
		var name, colType, colKey string
		if err := rows.Scan(&name, &colType, &colKey); err != nil {
			return SchemaResponse{}, fmt.Errorf("failed to scan results: %w", err)
		}

		field := Field{
			Name:      name,
			Type:      colType,
			IsPrimary: colKey == "PRI",
		}
		fields = append(fields, field)
	}

	if err := rows.Err(); err != nil {
		return SchemaResponse{}, fmt.Errorf("failed to iterate through results: %w", err)
	}

	// INFORMATION_SCHEMA answers a table that does not exist with no rows, which
	// is indistinguishable from a table with no columns — and a table with no
	// columns cannot exist. Saying so is the difference between "you named the
	// wrong table" and "this table is oddly empty".
	if len(fields) == 0 {
		return SchemaResponse{}, fmt.Errorf("%s.%s does not exist",
			req.Connection.Database, req.TableName)
	}

	return SchemaResponse{Fields: fields}, nil
}

func getPostgreSQLSchema(c context.Context, req SchemaRequest) (SchemaResponse, error) {
	// Build connection string
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		req.Connection.Host, req.Connection.Port,
		req.Connection.User, req.Connection.Password,
		req.Connection.Database)

	// Connect to database
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return SchemaResponse{}, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	defer db.Close()

	// Set timeout
	ctx, cancel := context.WithTimeout(c, 10*time.Second)
	defer cancel()

	// Validate connection
	if err := db.PingContext(ctx); err != nil {
		return SchemaResponse{}, fmt.Errorf("postgreSQL connection test failed: %w", err)
	}

	// Query table structure
	query := `
		SELECT 
			a.attname as column_name,
			pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type,
			CASE WHEN 
				(SELECT COUNT(*) FROM pg_constraint WHERE conrelid = a.attrelid AND conkey[1] = a.attnum AND contype = 'p') > 0 
			THEN true ELSE false END as is_primary
		FROM 
			pg_catalog.pg_attribute a
		WHERE 
			a.attrelid = (SELECT oid FROM pg_catalog.pg_class WHERE relname = $1 AND relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'public'))
			AND a.attnum > 0 
			AND NOT a.attisdropped
		ORDER BY a.attnum
	`
	rows, err := db.QueryContext(ctx, query, req.TableName)
	if err != nil {
		return SchemaResponse{}, fmt.Errorf("failed to query table structure: %w", err)
	}
	defer rows.Close()

	var fields []Field
	for rows.Next() {
		var name, dataType string
		var isPrimary bool
		if err := rows.Scan(&name, &dataType, &isPrimary); err != nil {
			return SchemaResponse{}, fmt.Errorf("failed to scan results: %w", err)
		}

		field := Field{
			Name:      name,
			Type:      dataType,
			IsPrimary: isPrimary,
		}
		fields = append(fields, field)
	}

	if err := rows.Err(); err != nil {
		return SchemaResponse{}, fmt.Errorf("failed to iterate through results: %w", err)
	}

	return SchemaResponse{Fields: fields}, nil
}
