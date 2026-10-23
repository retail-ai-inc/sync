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

// The timeout is not the interval. Tied to it, a nudge that took longer than
// one tick to reach every shard would be abandoned every time -- which is the
// case the nudger exists for.
func TestTheNudgeTimeoutIsNotTheInterval(t *testing.T) {
	if nudgeTimeout <= nudgeInterval {
		t.Errorf("nudgeTimeout (%v) is not longer than nudgeInterval (%v), so a "+
			"nudge slower than one tick would never finish", nudgeTimeout, nudgeInterval)
	}
}

// The await window is the delay; the heartbeat interval is liveness. They were
// one constant, so shortening the delay would have multiplied the position
// writes and the source clock reads by the same factor.
func TestTheAwaitWindowIsShorterThanTheHeartbeat(t *testing.T) {
	if streamAwait >= idleHeartbeat {
		t.Errorf("streamAwait (%v) is not shorter than idleHeartbeat (%v), so the "+
			"await window is doing nothing for latency", streamAwait, idleHeartbeat)
	}
	// mongos returns when the window ends rather than when the event arrives, so
	// this is a floor under every change's latency on a sharded source.
	if streamAwait > 500*time.Millisecond {
		t.Errorf("streamAwait is %v, which is that much under every event", streamAwait)
	}
}
