package resilience

import (
	"context"
	"database/sql"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// retryAttempts and retryDelay are what a database operation gets before its
// failure is reported. Three attempts one second apart covers an election or a
// connection pool being rebuilt; anything longer belongs to the caller, which
// knows whether the work can wait.
const (
	retryAttempts = 3
	retryDelay    = time.Second
)

// RetryDBOperation runs fn, retrying it while the failure looks transient.
//
// The backoff waits between attempts and not after the last one: the loop is
// finished by then and the same error is returned regardless, so the trailing
// sleep only held the caller's goroutine for four extra seconds during exactly
// the failure it was meant to survive.
func RetryDBOperation(ctx context.Context, logger logrus.FieldLogger, operation string, fn func() error) error {
	delay := retryDelay

	var err error
	for i := 0; i < retryAttempts; i++ {
		if err = fn(); err == nil {
			return nil
		}

		if !IsConnectionError(err) {
			logger.Errorf("[DB] Operation '%s' failed with non-connection error: %v", operation, err)
			return err
		}
		if i == retryAttempts-1 {
			break
		}

		logger.Warnf("[DB] Operation '%s' failed with connection error (attempt %d/%d): %v, retrying...",
			operation, i+1, retryAttempts, err)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			delay *= 2
		}
	}

	logger.Errorf("[DB] Operation '%s' failed after %d attempts: %v", operation, retryAttempts, err)
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

// ReopenMongoConnection reopens MongoDB connection.
//
// A failure that waiting cannot fix — a URI the driver will not parse — is
// reported at once rather than after five attempts and half a minute of
// backoff, and the context stops the waiting when the process is shutting down.
func ReopenMongoConnection(ctx context.Context, logger logrus.FieldLogger, connURI string) (*mongo.Client, error) {
	var client *mongo.Client

	err := Retry(ctx, 5, 2*time.Second, 2.0, func() error {
		var connErr error
		client, connErr = mongo.Connect(ctx, options.Client().ApplyURI(connURI))
		if connErr != nil {
			return permanentUnless(connErr)
		}
		return permanentUnless(client.Ping(ctx, nil))
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

	err := Retry(ctx, 5, 2*time.Second, 2.0, func() error {
		var connErr error
		db, connErr = sql.Open(driverName, connURI)
		if connErr != nil {
			return permanentUnless(connErr)
		}

		// Set connection parameters
		db.SetMaxOpenConns(25)
		db.SetMaxIdleConns(5)
		db.SetConnMaxLifetime(5 * time.Minute)

		return permanentUnless(db.PingContext(ctx))
	})

	if err != nil {
		logger.Errorf("[%s] Failed to connect after retries: %v", driverName, err)
		return nil, err
	}

	logger.Infof("[%s] Successfully connected to database", driverName)
	return db, nil
}

// permanentFailure wraps an error that no further attempt will survive, so Retry
// stops rather than spending its whole backoff on a typo in a connection string.
type permanentFailure struct{ error }

// Unwrap lets callers still match on the underlying error.
func (p permanentFailure) Unwrap() error { return p.error }

// Permanent tells Retry to stop.
func (p permanentFailure) Permanent() bool { return true }

// permanentUnless returns err marked as permanent when retrying it is pointless.
func permanentUnless(err error) error {
	if err == nil || IsConnectionError(err) {
		return err
	}
	return permanentFailure{err}
}
