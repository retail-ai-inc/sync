package utils

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// IsConnectionError determines if an error is a connection error
func IsConnectionError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	return strings.Contains(errStr, "connection") ||
		strings.Contains(errStr, "network") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "EOF") ||
		strings.Contains(errStr, "reset by peer") ||
		strings.Contains(errStr, "i/o timeout") ||
		strings.Contains(errStr, "connection refused")
}

// RetryDBOperation executes database operations and automatically retries connection errors
func RetryDBOperation(ctx context.Context, logger logrus.FieldLogger, operation string, fn func() error) error {
	maxRetries := 3
	retryDelay := time.Second

	var err error
	for i := 0; i < maxRetries; i++ {
		err = fn()
		if err == nil {
			return nil
		}

		// Only retry connection errors
		if !IsConnectionError(err) {
			logger.Errorf("[DB] Operation '%s' failed with non-connection error: %v", operation, err)
			return err
		}

		logger.Warnf("[DB] Operation '%s' failed with connection error (attempt %d/%d): %v, retrying...",
			operation, i+1, maxRetries, err)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryDelay):
			retryDelay *= 2 // Exponential backoff
		}
	}

	logger.Errorf("[DB] Operation '%s' failed after %d attempts: %v", operation, maxRetries, err)
	return err
}

// RetryMongoOperation is a retry function for MongoDB operations, based on the same pattern as RetryDBOperation
func RetryMongoOperation(ctx context.Context, logger logrus.FieldLogger, operation string, fn func() error) error {
	return RetryDBOperation(ctx, logger, operation, fn)
}

// CheckMongoConnection checks MongoDB connection
func CheckMongoConnection(ctx context.Context, client *mongo.Client) error {
	return client.Ping(ctx, nil)
}

// ReopenMongoConnection reopens MongoDB connection
func ReopenMongoConnection(ctx context.Context, logger logrus.FieldLogger, connURI string) (*mongo.Client, error) {
	var client *mongo.Client
	var err error

	err = Retry(5, 2*time.Second, 2.0, func() error {
		var connErr error
		client, connErr = mongo.Connect(ctx, options.Client().ApplyURI(connURI))
		if connErr != nil {
			return connErr
		}
		return client.Ping(ctx, nil)
	})

	if err != nil {
		logger.Errorf("[MongoDB] Failed to connect after retries: %v", err)
		return nil, err
	}

	logger.Info("[MongoDB] Successfully connected to database")
	return client, nil
}

// SQL database connection check and reopening functions
// CheckSQLConnection checks SQL database connection
func CheckSQLConnection(ctx context.Context, db *sql.DB) error {
	return db.PingContext(ctx)
}

// ReopenSQLConnection reopens SQL database connection
func ReopenSQLConnection(ctx context.Context, logger logrus.FieldLogger, connURI, driverName string) (*sql.DB, error) {
	var db *sql.DB
	var err error

	err = Retry(5, 2*time.Second, 2.0, func() error {
		var connErr error
		db, connErr = sql.Open(driverName, connURI)
		if connErr != nil {
			return connErr
		}

		// Set connection parameters
		db.SetMaxOpenConns(25)
		db.SetMaxIdleConns(5)
		db.SetConnMaxLifetime(5 * time.Minute)

		return db.PingContext(ctx)
	})

	if err != nil {
		logger.Errorf("[%s] Failed to connect after retries: %v", driverName, err)
		return nil, err
	}

	logger.Infof("[%s] Successfully connected to database", driverName)
	return db, nil
}
