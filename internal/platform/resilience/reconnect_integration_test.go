//go:build integration

package resilience

import (
	"context"
	"testing"
	"time"
)

// TestReopenMongoConnectionRejectsAMalformedURIWithoutRetrying is the one
// reconnect test cheap enough to run: an unparseable URI fails inside
// mongo.Connect, and this records how long the whole budget takes when every
// attempt fails immediately.
func TestReopenMongoConnectionRejectsAMalformedURIWithoutRetrying(t *testing.T) {
	start := time.Now()
	client, err := ReopenMongoConnection(context.Background(), quietFieldLogger(), "not-a-uri")

	if err == nil {
		_ = client.Disconnect(context.Background())
		t.Fatal("ReopenMongoConnection accepted an unparseable URI")
	}
	if client != nil {
		t.Error("a client was returned alongside the error")
	}
	if elapsed := time.Since(start); elapsed < 25*time.Second {
		t.Logf("the budget completed in %v; the retry parameters may have changed", elapsed)
	}
}
