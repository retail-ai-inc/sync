package mongodb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
	"github.com/retail-ai-inc/sync/pkg/db"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// GetMongoClient establishes a MongoDB connection using the provided URI
func GetMongoClient(ctx context.Context, uri string) (*mongo.Client, error) {
	clientOptions := options.Client().ApplyURI(uri)
	clientOptions.SetConnectTimeout(10 * time.Second)
	clientOptions.SetSocketTimeout(30 * time.Second)
	clientOptions.SetServerSelectionTimeout(10 * time.Second)

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Verify the connection
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		client.Disconnect(ctx)
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	return client, nil
}

// ConnectMongoDB connects to a MongoDB instance with the given parameters
func ConnectMongoDB(ctx context.Context, host string, port string, user string, password string, database string, logger *logrus.Logger) (*mongo.Client, string, error) {
	if logger == nil {
		logger = logrus.StandardLogger()
	}

	logger.Infof("[MongoDB] Connecting to %s:%s database: %s", host, port, database)

	// Build the connection URI
	uri := fmt.Sprintf("mongodb://%s:%s@%s:%s/%s?authSource=admin", user, password, host, port, database)

	logger.Infof("[MongoDB] Connection URI: %s", uri)
	client, err := GetMongoClient(ctx, uri)
	if err != nil {
		logger.Errorf("[MongoDB] Connection failed: %v", err)
		return nil, database, err
	}

	logger.Infof("[MongoDB] Connected successfully to %s:%s database: %s", host, port, database)
	return client, database, nil
}

// ConnectMongoDBFromTaskID connects to MongoDB using the configuration from a sync task ID
func ConnectMongoDBFromTaskID(ctx context.Context, taskID string, logger *logrus.Logger) (*mongo.Client, string, error) {
	if logger == nil {
		logger = logrus.StandardLogger()
	}

	logger.Infof("[MongoDB] Connecting to MongoDB for task ID: %s", taskID)

	// Open the local SQLite database
	db, err := openLocalDB()
	if err != nil {
		logger.Errorf("[MongoDB] Failed to open local DB: %v", err)
		return nil, "", fmt.Errorf("failed to open local DB: %w", err)
	}
	defer db.Close()

	// Get the configuration for the task
	var configJSON string
	err = db.QueryRow("SELECT config_json FROM sync_tasks WHERE id = ?", taskID).Scan(&configJSON)
	if err != nil {
		logger.Errorf("[MongoDB] Failed to get task configuration: %v", err)
		return nil, "", fmt.Errorf("failed to get task configuration: %w", err)
	}

	// Parse the JSON configuration
	var config struct {
		Type       string            `json:"type"`
		SourceConn map[string]string `json:"sourceConn"`
		TargetConn map[string]string `json:"targetConn"`
	}

	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		logger.Errorf("[MongoDB] Failed to parse config JSON: %v", err)
		return nil, "", fmt.Errorf("failed to parse config JSON: %w", err)
	}

	// Verify it's a MongoDB task
	if !strings.EqualFold(config.Type, "mongodb") {
		logger.Errorf("[MongoDB] Task is not MongoDB type: %s", config.Type)
		return nil, "", fmt.Errorf("task is not MongoDB type: %s", config.Type)
	}

	// Extract connection parameters
	host := config.TargetConn["host"]
	port := config.TargetConn["port"]
	username := config.TargetConn["user"]
	password := config.TargetConn["password"]
	database := config.TargetConn["database"]

	// Connect to MongoDB
	return ConnectMongoDB(
		ctx,
		host,
		port,
		username,
		password,
		database,
		logger,
	)
}

// Helper function to open the local SQLite database
func openLocalDB() (*sql.DB, error) {
	return db.OpenSQLiteDB()
}
