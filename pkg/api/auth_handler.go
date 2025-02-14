package api

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
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	access        = ""
	currentUserDB = map[string]interface{}{
		"name":   "Serati Ma",
		"avatar": "https://gw.alipayobjects.com/zos/antfincdn/XAosXuNZyF/BiazfanxmamNRoxxVxka.png",
		"userid": "00000001",
		"email":  "antdesign@alipay.com",
		"access": "admin",
	}
)

// AuthLoginHandler  POST /api/login
func AuthLoginHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username  string `json:"username"`
		Password  string `json:"password"`
		AutoLogin bool   `json:"autoLogin"`
		Type      string `json:"type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	if req.Username == "admin" && req.Password == "admin" {
		access = "admin"
		resp := map[string]interface{}{
			"status":           "ok",
			"type":             req.Type,
			"currentAuthority": "admin",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	} else {
		access = "guest"
		resp := map[string]interface{}{
			"status":           "error",
			"type":             req.Type,
			"currentAuthority": "guest",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}

// AuthCurrentUserHandler  GET /api/currentUser
func AuthCurrentUserHandler(w http.ResponseWriter, r *http.Request) {
	if access == "" || access == "guest" {
		w.WriteHeader(http.StatusUnauthorized)
		resp := map[string]interface{}{
			"data": map[string]interface{}{
				"isLogin": false,
			},
			"errorCode":    "401",
			"errorMessage": "Please log in first!",
			"success":      true,
		}
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	resp := map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"name":   currentUserDB["name"],
			"avatar": currentUserDB["avatar"],
			"userid": currentUserDB["userid"],
			"email":  currentUserDB["email"],
			"access": access,
		},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// AuthLogoutHandler POST /api/logout
func AuthLogoutHandler(w http.ResponseWriter, r *http.Request) {
	access = ""
	resp := map[string]interface{}{
		"data":    map[string]interface{}{},
		"success": true,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

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

	var tables []string

	switch req.DbType {
	case "mysql", "mariadb":
		// DSN: user:password@tcp(host:port)/database?parseTime=true&loc=Local
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&loc=Local",
			req.User, req.Password, req.Host, req.Port, req.Database)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error opening connection: %v", err), http.StatusInternalServerError)
			return
		}
		defer db.Close()

		if err = db.Ping(); err != nil {
			http.Error(w, fmt.Sprintf("Ping failed: %v", err), http.StatusInternalServerError)
			return
		}

		rows, err := db.Query("SHOW TABLES")
		if err != nil {
			http.Error(w, fmt.Sprintf("Query failed: %v", err), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var table string
			if err := rows.Scan(&table); err != nil {
				http.Error(w, fmt.Sprintf("Scan failed: %v", err), http.StatusInternalServerError)
				return
			}
			tables = append(tables, table)
		}

	case "postgresql":
		// DSN: postgres://user:password@host:port/database?sslmode=disable
		dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
			req.User, req.Password, req.Host, req.Port, req.Database)
		db, err := sql.Open("postgres", dsn)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error opening connection: %v", err), http.StatusInternalServerError)
			return
		}
		defer db.Close()

		if err = db.Ping(); err != nil {
			http.Error(w, fmt.Sprintf("Ping failed: %v", err), http.StatusInternalServerError)
			return
		}

		rows, err := db.Query("SELECT tablename FROM pg_tables WHERE schemaname='public'")
		if err != nil {
			http.Error(w, fmt.Sprintf("Query failed: %v", err), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var table string
			if err := rows.Scan(&table); err != nil {
				http.Error(w, fmt.Sprintf("Scan failed: %v", err), http.StatusInternalServerError)
				return
			}
			tables = append(tables, table)
		}

	case "mongodb":
		var uri string
		if req.User != "" && req.Password != "" {
			uri = fmt.Sprintf("mongodb://%s:%s@%s:%s/?directConnection=true", req.User, req.Password, req.Host, req.Port)
		} else {
			uri = fmt.Sprintf("mongodb://%s:%s/?directConnection=true", req.Host, req.Port)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
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
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
