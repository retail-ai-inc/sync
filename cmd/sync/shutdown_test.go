package main

import (
	"testing"
	"time"
)

// TestEveryStuckTaskSeesTheShutdownDeadline covers the shape that hung
// shutdown: the deadline was a time.After channel, which carries exactly one
// value. The first task to time out consumed it, and every task after that
// waited on a channel that would never fire again -- so two stuck tasks left
// stopAll blocked for ever, and the clean-up after the loop, which clears the
// running set and cancels the monitors, was never reached.
func TestEveryStuckTaskSeesTheShutdownDeadline(t *testing.T) {
	restore := drainTimeout
	drainTimeout = 200 * time.Millisecond
	t.Cleanup(func() { drainTimeout = restore })

	s := &supervisor{
		log:     quietLogger(),
		running: map[int]*runningTask{},
	}
	// Two tasks that never finish: their done channels are never closed.
	for _, id := range []int{1, 2, 3} {
		s.running[id] = &runningTask{cancel: func() {}, done: make(chan struct{})}
	}

	finished := make(chan struct{})
	go func() { s.stopAll(); close(finished) }()

	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("stopAll did not return once the deadline passed, so shutdown " +
			"never reached the clean-up after the wait loop")
	}

	if len(s.running) != 0 {
		t.Errorf("the running set still holds %d tasks", len(s.running))
	}
}
