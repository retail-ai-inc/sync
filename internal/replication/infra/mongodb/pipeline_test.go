package mongodb

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// deadClient returns a client that resolves handles without dialling and fails
// every operation quickly. The driver connects lazily, so this needs no server;
// the short timeouts keep the failure paths fast.
func deadClient(t *testing.T) *mongo.Client {
	t.Helper()

	client, err := mongo.Connect(context.Background(), options.Client().
		ApplyURI("mongodb://127.0.0.1:1").
		SetServerSelectionTimeout(10*time.Millisecond).
		SetConnectTimeout(10*time.Millisecond))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	return client
}

// briefCtx bounds the retry backoff: RetryDBOperation aborts its sleep as soon
// as the context is done, so a failing write reports in milliseconds instead of
// the three seconds the backoff would otherwise take.
func briefCtx(t *testing.T) context.Context {
	t.Helper()

	// Long enough to read a buffer file on a loaded machine — the parser stops
	// when its context is done — and short enough to cut the retry backoff at
	// the first sleep.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	t.Cleanup(cancel)
	return ctx
}

// event encodes one change-stream document the way the disk buffer stores it.
func event(t *testing.T, doc bson.M) bson.Raw {
	t.Helper()

	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return raw
}
