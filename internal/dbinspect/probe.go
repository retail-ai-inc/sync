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
	"github.com/retail-ai-inc/sync/internal/platform/httpx"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// probeTimeout bounds server selection and the dial. A probe answers a person
// waiting in a browser, so it is short.
const probeTimeout = 5 * time.Second

// teardownTimeout is how long the handler waits for the client to close before
// leaving it to finish on its own. Short because it is paid after the answer is
// already decided.
const teardownTimeout = 500 * time.Millisecond

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
		httpx.ErrorJSONStatus(w, http.StatusBadRequest, "the request body could not be read", err)
		return
	}

	// The list endpoints mask stored passwords, so an edit form filled from one
	// carries the mask rather than a password. Probing with it fails as an
	// authentication error, which reads as "the credentials are wrong" when what
	// happened is that none were sent. Saying so is the difference between an
	// operator retyping the password and an operator changing it on the server.
	if req.Password == httpx.RedactedPassword {
		httpx.ErrorJSONStatus(w, http.StatusBadRequest,
			"the password field still holds the mask the task list answers with, "+
				"not a password. Type the password to test this connection; leaving "+
				"the field alone keeps the stored one when the task is saved", nil)
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
			httpx.ErrorJSON(w, "the database refused the connection", err)
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
			httpx.ErrorJSON(w, "the database refused the connection", err)
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

		// The driver's own deadlines, not just the request's. Without them the
		// topology monitor keeps dialling an address that will not answer, and
		// Disconnect waits for it: measured at 30 seconds against an unroutable
		// host, holding the request goroutine long after the caller had gone.
		client, err := mongo.Connect(options.Client().ApplyURI(uri).
			SetServerSelectionTimeout(probeTimeout).
			SetConnectTimeout(probeTimeout))
		if err != nil {
			httpx.ErrorJSON(w, "MongoDB connection error", err)
			return
		}
		defer func() {
			// Disconnect on its own goroutine, and do not wait for it beyond
			// teardownTimeout.
			//
			// It does not honour the context it is given: the topology monitor
			// is still dialling an address that will never answer, and the close
			// waits for that regardless -- measured at 5 seconds with a bounded
			// context and 30 without, on a probe whose caller had already gone.
			// The request must not be held for either.
			//
			// Leaking the goroutine is the lesser cost: it ends when the driver's
			// own dial times out, and a probe is one request by one operator, not
			// something on a hot path.
			done := make(chan struct{})
			go func() {
				defer close(done)
				_ = client.Disconnect(context.WithoutCancel(ctx))
			}()
			select {
			case <-done:
			case <-time.After(teardownTimeout):
			}
		}()

		if err = client.Ping(ctx, nil); err != nil {
			httpx.ErrorJSON(w, "MongoDB ping error", err)
			return
		}

		collections, err := client.Database(req.Database).ListCollectionNames(ctx, struct{}{})
		if err != nil {
			httpx.ErrorJSON(w, "Listing collections failed", err)
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
			httpx.ErrorJSON(w, "Redis ping error", err)
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
		httpx.ErrorJSONStatus(w, http.StatusBadRequest,
			fmt.Sprintf("%q is not a database type this build can probe", req.DbType), nil)
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
