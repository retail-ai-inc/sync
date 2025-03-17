package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// SchemaRequest represents a request to get table structure
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

// Field represents table field information
type Field struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	IsPrimary bool   `json:"isPrimary"`
}

// SchemaResponse represents a table structure response
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

	switch req.SourceType {
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

// New function: sort fields by name
func sortFieldsByName(schema *SchemaResponse) {
	// Use sort package to sort Fields by name
	sort.Slice(schema.Fields, func(i, j int) bool {
		// Usually put primary key fields (like _id) at the front
		if schema.Fields[i].IsPrimary {
			return true
		}
		if schema.Fields[j].IsPrimary {
			return false
		}
		// Sort regular fields by name
		return schema.Fields[i].Name < schema.Fields[j].Name
	})
}

// getMongoDBSchema gets MongoDB collection structure
func getMongoDBSchema(c context.Context, req SchemaRequest) (SchemaResponse, error) {
	// Build MongoDB connection URI
	uri := fmt.Sprintf("mongodb://%s:%s/%s", req.Connection.Host, req.Connection.Port, req.Connection.Database)
	if req.Connection.User != "" && req.Connection.Password != "" {
		uri = fmt.Sprintf("mongodb://%s:%s@%s:%s/%s",
			req.Connection.User, req.Connection.Password,
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

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return SchemaResponse{}, fmt.Errorf("Failed to connect to MongoDB: %w", err)
	}
	defer func() {
		if err := client.Disconnect(ctx); err != nil {
			logrus.Errorf("[MongoDB] Failed to disconnect: %v", err)
		}
	}()

	// Validate connection
	if err := client.Ping(ctx, nil); err != nil {
		return SchemaResponse{}, fmt.Errorf("MongoDB connection test failed: %w", err)
	}

	// Get collection document sample to infer structure
	collection := client.Database(req.Connection.Database).Collection(req.TableName)

	// Get sample documents to extract nested fields
	var sampleDocs []bson.M
	findCursor, err := collection.Find(ctx, bson.M{}, options.Find().SetLimit(10))
	if err != nil {
		return SchemaResponse{}, fmt.Errorf("Failed to query documents: %w", err)
	}
	defer findCursor.Close(ctx)

	if err := findCursor.All(ctx, &sampleDocs); err != nil {
		return SchemaResponse{}, fmt.Errorf("Failed to decode documents: %w", err)
	}

	// Use aggregation query to get top-level fields
	pipeline := bson.A{
		bson.D{{"$limit", 100}},
		bson.D{{"$project", bson.D{{"arrayofkeyvalue", bson.D{{"$objectToArray", "$$ROOT"}}}}}},
		bson.D{{"$unwind", "$arrayofkeyvalue"}},
		bson.D{{"$group", bson.D{
			{"_id", nil},
			{"fields", bson.D{{"$addToSet", "$arrayofkeyvalue.k"}}},
			{"fieldTypes", bson.D{{"$push", bson.D{
				{"field", "$arrayofkeyvalue.k"},
				{"value", "$arrayofkeyvalue.v"},
			}}}},
		}}},
	}

	cursor, err := collection.Aggregate(ctx, pipeline)
	if err != nil {
		return SchemaResponse{}, fmt.Errorf("Failed to analyze collection structure: %w", err)
	}
	defer cursor.Close(ctx)

	type AggResult struct {
		Fields     []string `bson:"fields"`
		FieldTypes []struct {
			Field string      `bson:"field"`
			Value interface{} `bson:"value"`
		} `bson:"fieldTypes"`
	}

	var results []AggResult
	if err := cursor.All(ctx, &results); err != nil {
		return SchemaResponse{}, fmt.Errorf("Failed to decode results: %w", err)
	}

	// If collection is empty, return empty field list
	if len(results) == 0 || len(sampleDocs) == 0 {
		logrus.Warnf("[MongoDB] Collection is empty, unable to infer structure: %s.%s", req.Connection.Database, req.TableName)
		return SchemaResponse{Fields: []Field{}}, nil
	}

	// Create field mapping to get unique fields and types
	result := results[0]
	fieldMap := make(map[string]string)

	// First mark all discovered fields
	for _, field := range result.Fields {
		fieldMap[field] = ""
	}

	// Then try to get type for each field
	for _, ft := range result.FieldTypes {
		if fieldMap[ft.Field] == "" && ft.Value != nil {
			fieldMap[ft.Field] = getMongoFieldType(ft.Value)
		}
	}

	// Process sample documents to discover nested fields
	nestedFields := make(map[string]string)
	for _, doc := range sampleDocs {
		extractNestedFields(doc, "", nestedFields)
	}

	// Merge top-level fields and nested fields
	for field, fieldType := range nestedFields {
		if _, exists := fieldMap[field]; !exists {
			fieldMap[field] = fieldType
		}
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

// getMongoFieldType gets MongoDB field type
func getMongoFieldType(value interface{}) string {
	switch value.(type) {
	case int, int32, int64:
		return "int"
	case float32, float64:
		return "float"
	case string:
		return "string"
	case bool:
		return "bool"
	case time.Time:
		return "date"
	case bson.M, map[string]interface{}:
		return "object"
	case []interface{}:
		return "array"
	case nil:
		return "null"
	default:
		return fmt.Sprintf("%T", value)
	}
}

// getMySQLSchema gets MySQL table structure
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
		return SchemaResponse{}, fmt.Errorf("Failed to connect to MySQL: %w", err)
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
		return SchemaResponse{}, fmt.Errorf("MySQL connection test failed: %w", err)
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
		return SchemaResponse{}, fmt.Errorf("Failed to query table structure: %w", err)
	}
	defer rows.Close()

	var fields []Field
	for rows.Next() {
		var name, colType, colKey string
		if err := rows.Scan(&name, &colType, &colKey); err != nil {
			return SchemaResponse{}, fmt.Errorf("Failed to scan results: %w", err)
		}

		field := Field{
			Name:      name,
			Type:      colType,
			IsPrimary: colKey == "PRI",
		}
		fields = append(fields, field)
	}

	if err := rows.Err(); err != nil {
		return SchemaResponse{}, fmt.Errorf("Failed to iterate through results: %w", err)
	}

	// If no fields found, return empty array instead of error
	if len(fields) == 0 {
		logrus.Warnf("[MySQL] Table is empty or does not exist: %s.%s", req.Connection.Database, req.TableName)
	}

	return SchemaResponse{Fields: fields}, nil
}

// getPostgreSQLSchema gets PostgreSQL table structure
func getPostgreSQLSchema(c context.Context, req SchemaRequest) (SchemaResponse, error) {
	// Build connection string
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		req.Connection.Host, req.Connection.Port,
		req.Connection.User, req.Connection.Password,
		req.Connection.Database)

	// Connect to database
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return SchemaResponse{}, fmt.Errorf("Failed to connect to PostgreSQL: %w", err)
	}
	defer db.Close()

	// Set timeout
	ctx, cancel := context.WithTimeout(c, 10*time.Second)
	defer cancel()

	// Validate connection
	if err := db.PingContext(ctx); err != nil {
		return SchemaResponse{}, fmt.Errorf("PostgreSQL connection test failed: %w", err)
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
		return SchemaResponse{}, fmt.Errorf("Failed to query table structure: %w", err)
	}
	defer rows.Close()

	var fields []Field
	for rows.Next() {
		var name, dataType string
		var isPrimary bool
		if err := rows.Scan(&name, &dataType, &isPrimary); err != nil {
			return SchemaResponse{}, fmt.Errorf("Failed to scan results: %w", err)
		}

		field := Field{
			Name:      name,
			Type:      dataType,
			IsPrimary: isPrimary,
		}
		fields = append(fields, field)
	}

	if err := rows.Err(); err != nil {
		return SchemaResponse{}, fmt.Errorf("Failed to iterate through results: %w", err)
	}

	return SchemaResponse{Fields: fields}, nil
}
