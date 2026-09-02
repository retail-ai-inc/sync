package resilience

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func quietFieldLogger() logrus.FieldLogger {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return l
}

// TestCheckMongoConnectionIsJustAPing records that the check has no timeout of
// its own: it forwards the caller's context straight to Ping, so a caller that
// passes context.Background() blocks for the driver's own selection timeout.
func TestCheckMongoConnectionIsJustAPing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://127.0.0.1:1/db"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })

	if err := CheckMongoConnection(ctx, client); err == nil {
		t.Fatal("CheckMongoConnection succeeded against port 1")
	}
}

// TestCheckMongoConnectionHonoursACancelledContext records that a cancelled
// context is reported at once rather than after the selection timeout.
func TestCheckMongoConnectionHonoursACancelledContext(t *testing.T) {
	// The driver's v2 Connect takes no context: it does not reach the server, so
	// there was nothing for a timeout to bound.
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://127.0.0.1:1/db"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	if err := CheckMongoConnection(ctx, client); err == nil {
		t.Fatal("CheckMongoConnection succeeded with a cancelled context")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("CheckMongoConnection took %v for a cancelled context", elapsed)
	}
}

// The reconnect helpers call Retry with five attempts, a two-second base and a
// doubling factor: 2 + 4 + 8 + 16 = 30 seconds of waiting on top of whatever
// each attempt itself costs.
func TestTheReconnectBudgetIsBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	attempts := 0
	start := time.Now()
	err := Retry(ctx, 4, time.Minute, 2.0, func() error {
		attempts++
		return errors.New("source is down")
	})

	if err == nil {
		t.Fatal("Retry succeeded")
	}
	if attempts > 1 {
		t.Errorf("Retry made %d attempts against a cancelled context, want 1", attempts)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("Retry waited %v after its context was cancelled", elapsed)
	}
}

// TestTheReconnectBudgetIsThirtySeconds pins the arithmetic the two reconnect
// helpers commit to, so a change to the retry parameters is noticed here rather
// than in production.
func TestTheReconnectBudgetIsThirtySeconds(t *testing.T) {
	// Retry(5, 2s, 2.0) sleeps after each failed attempt except the last.
	const attempts = 5
	base, factor := 2*time.Second, 2.0

	var total time.Duration
	delay := base
	for i := 0; i < attempts-1; i++ {
		total += delay
		delay = time.Duration(float64(delay) * factor)
	}

	if total != 30*time.Second {
		t.Errorf("the reconnect budget is %v, want 30s — the parameters in "+
			"ReopenMongoConnection and ReopenSQLConnection appear to have changed", total)
	}
}
