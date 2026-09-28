package app

import "sync"

// The four monitors below all run as goroutines started from a Start… call and
// stopped by cancelling the context handed to it. Cancelling, though, only
// asks: each one may still be part-way through a sweep, and every one writes to
// the control database, so the process could return from shutdown while a
// goroutine was still writing to SQLite. watchers is what makes "stopped" mean
// stopped.
var watchers sync.WaitGroup

// watch runs fn as one of the monitors, recorded so WaitForWatchers can wait for
// it.
func watch(fn func()) {
	watchers.Add(1)
	go func() {
		defer watchers.Done()
		fn()
	}()
}

// WaitForWatchers blocks until every monitor started so far has returned. The
// caller cancels their context first; this waits for them to notice.
func WaitForWatchers() { watchers.Wait() }
