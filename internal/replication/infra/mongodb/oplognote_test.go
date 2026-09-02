package mongodb

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// TestUnauthorizedTellsRefusalFromFailure keeps the two apart.
func TestUnauthorizedTellsRefusalFromFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "credentials may not run the command",
			err:  mongo.CommandError{Code: 13, Message: "not authorized on admin"},
			want: true,
		},
		{
			name: "server does not know the command",
			err:  mongo.CommandError{Code: 59, Message: "no such command: appendOplogNote"},
			want: true,
		},
		{
			name: "wrapped refusal is still a refusal",
			err:  fmt.Errorf("nudge: %w", mongo.CommandError{Code: 13}),
			want: true,
		},
		{
			name: "cluster could not answer",
			err:  errors.New("server selection error: context deadline exceeded"),
			want: false,
		},
		{
			name: "a command error that is not a refusal",
			err:  mongo.CommandError{Code: 11602, Message: "interrupted due to replica set state change"},
			want: false,
		},
		{
			name: "no error at all",
			err:  nil,
			want: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := unauthorized(tc.err); got != tc.want {
				t.Errorf("unauthorized(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestStopIsSafeOnANilNudger covers the replica-set path: startNudging returns
// nil there, and Close must not panic on it.
func TestStopIsSafeOnANilNudger(t *testing.T) {
	var n *nudger
	n.Stop()
}

// TestStopEndsTheLoop makes sure Stop waits rather than leaving a goroutine
// writing to the source after the reader has closed.
func TestStopEndsTheLoop(t *testing.T) {
	n := &nudger{
		interval: time.Millisecond,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go func() {
		defer close(n.done)
		<-n.stop
	}()

	done := make(chan struct{})
	go func() { n.Stop(); close(done) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return, so a nudger outlives the reader that owns it")
	}

	// Twice, because Close may run more than once.
	n.Stop()
}
