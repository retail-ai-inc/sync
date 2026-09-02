package dbinspect

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// TestConnectionHandler POST /api/test-connection
func TestConnectionHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DbType   string `json:"dbType"`
		Host     string `json:"host"`
		Port     string `json:"port"`
		User     string `json:"user"`
		Password string `json:"password"`
		Database string `json:"database"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	var (
		tables []string
		err    error
	)

	switch req.DbType {
	case "mysql", "mariadb":
		// DSN: user:password@tcp(host:port)/database?parseTime=true&loc=Local
		tables, err = sqlTables(
			"mysql",
			fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&loc=Local",
				req.User, req.Password, req.Host, req.Port, req.Database),
			"SHOW TABLES")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

	case "postgresql":
		// DSN: postgres://user:password@host:port/database?sslmode=disable
		tables, err = sqlTables(
			"postgres",
			fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
				req.User, req.Password, req.Host, req.Port, req.Database),
			"SELECT tablename FROM pg_tables WHERE schemaname='public'")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

	case "mongodb":
		// The probe builds its URI the same way a task does, so what it
		// reports is what the task will get. Building it here separately is
		// how the probe kept forcing directConnection after the syncer had
		// stopped.
		uri := dsn.BuildDSNByType("mongodb", map[string]string{
			"user":     req.User,
			"password": req.Password,
			"host":     req.Host,
			"port":     req.Port,
			"database": req.Database,
		})

		// The request's own context, so a client that gives up stops the probe
		// with it. It used to be context.Background(), so a ten-second MongoDB
		// probe ran to completion however long the caller had been gone.
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		client, err := mongo.Connect(options.Client().ApplyURI(uri))
		if err != nil {
			http.Error(w, fmt.Sprintf("MongoDB connection error: %v", err), http.StatusInternalServerError)
			return
		}
		defer func() {
			_ = client.Disconnect(ctx)
		}()

		if err = client.Ping(ctx, nil); err != nil {
			http.Error(w, fmt.Sprintf("MongoDB ping error: %v", err), http.StatusInternalServerError)
			return
		}

		collections, err := client.Database(req.Database).ListCollectionNames(ctx, struct{}{})
		if err != nil {
			http.Error(w, fmt.Sprintf("Listing collections failed: %v", err), http.StatusInternalServerError)
			return
		}
		tables = collections

	case "redis":
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		dbIndex, err := strconv.Atoi(req.Database)
		if err != nil {
			dbIndex = 0
		}

		rdb := redis.NewClient(&redis.Options{
			Addr:     fmt.Sprintf("%s:%s", req.Host, req.Port),
			Username: req.User,
			Password: req.Password,
			DB:       dbIndex,
		})

		if err = rdb.Ping(ctx).Err(); err != nil {
			http.Error(w, fmt.Sprintf("Redis ping error: %v", err), http.StatusInternalServerError)
			return
		}

		resp := map[string]interface{}{
			"success": true,
			"data": map[string]interface{}{
				"tables": []string{},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return

	default:
		http.Error(w, "Unsupported dbType", http.StatusBadRequest)
		return
	}

	resp := map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"tables": tables,
		},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// sqlTables opens one connection, checks it answers, and reads a single column
// of names out of it. The MySQL and PostgreSQL branches of the probe were the
// same twenty-five lines twice over, differing in the driver, the DSN and one
// query.
func sqlTables(driver, dsn, query string) ([]string, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("Error opening connection: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("Ping failed: %v", err)
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("Query failed: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, fmt.Errorf("Scan failed: %v", err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("Query failed: %v", err)
	}
	return tables, nil
}
