package test

import (
	"context"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/db/mongodb"
	"github.com/retail-ai-inc/sync/internal/db/mysql"
	"github.com/retail-ai-inc/sync/internal/db/postgresql"
	"github.com/retail-ai-inc/sync/internal/db/redis"
)

func testDBConnections(t *testing.T) {
	t.Run("TestMySQLConnection", func(t *testing.T) {
		testMySQLConnection(t)
	})

	t.Run("TestPostgreSQLConnection", func(t *testing.T) {
		testPostgreSQLConnection(t)
	})

	t.Run("TestMongoDBConnection", func(t *testing.T) {
		testMongoDBConnection(t)
	})

	t.Run("TestRedisConnection", func(t *testing.T) {
		testRedisConnection(t)
	})
}

func testMySQLConnection(t *testing.T) {
	// Test with valid connection string
	dsn := "root:root@tcp(localhost:3306)/source_db?parseTime=true"
	db, err := mysql.GetMySQLDB(dsn)
	if err != nil {
		// In a real environment, this might fail if MySQL is not available
		t.Logf("MySQL connection failed (expected in test environment): %v", err)
	} else {
		defer db.Close()
		t.Log("Successfully connected to MySQL")
	}

	// Test with invalid connection string
	dsn = "invalid_connection_string"
	db, err = mysql.GetMySQLDB(dsn)
	if err == nil {
		defer db.Close()
		t.Error("Expected error with invalid MySQL connection string, but got none")
	}
}

func testPostgreSQLConnection(t *testing.T) {
	// Test with valid connection string
	connStr := "postgres://root:root@localhost:5432/source_db?sslmode=disable"
	db, err := postgresql.GetPostgreSQLDB(connStr)
	if err != nil {
		// In a real environment, this might fail if PostgreSQL is not available
		t.Logf("PostgreSQL connection failed (expected in test environment): %v", err)
	} else {
		defer db.Close()
		t.Log("Successfully connected to PostgreSQL")
	}

	// Test with invalid connection string
	connStr = "invalid_connection_string"
	db, err = postgresql.GetPostgreSQLDB(connStr)
	if err == nil {
		defer db.Close()
		t.Error("Expected error with invalid PostgreSQL connection string, but got none")
	}
}

func testMongoDBConnection(t *testing.T) {
	// Test with valid connection string
	uri := "mongodb://localhost:27017/source_db"
	ctx := context.Background()
	client, err := mongodb.GetMongoClient(ctx, uri)
	if err != nil {
		// In a real environment, this might fail if MongoDB is not available
		t.Logf("MongoDB connection failed (expected in test environment): %v", err)
	} else {
		defer client.Disconnect(context.Background())
		t.Log("Successfully connected to MongoDB")
	}

	// Test with invalid connection string
	uri = "invalid_connection_string"
	ctx = context.Background()
	client, err = mongodb.GetMongoClient(ctx, uri)
	if err == nil {
		defer client.Disconnect(context.Background())
		t.Error("Expected error with invalid MongoDB connection string, but got none")
	}
}

func testRedisConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test with valid connection string
	addr := "redis://localhost:6379/0"
	client, err := redis.GetRedisClient(addr)
	if err != nil {
		// In a real environment, this might fail if Redis is not available
		t.Logf("Redis connection failed (expected in test environment): %v", err)
	} else {
		defer client.Close()

		// Test ping
		err = client.Ping(ctx).Err()
		if err != nil {
			t.Logf("Redis ping failed (expected in test environment): %v", err)
		} else {
			t.Log("Successfully connected to Redis and pinged")
		}
	}

	// Test with invalid connection string
	addr = "invalid_connection_string"
	client, err = redis.GetRedisClient(addr)
	if err == nil {
		defer client.Close()
		t.Error("Expected error with invalid Redis connection string, but got none")
	}
}
