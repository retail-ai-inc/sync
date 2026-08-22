package app

import "sync"

// The four monitors below all run as goroutines started from a Start… call and
// stopped by cancelling the context handed to it. Cancelling, though, only asks:
// each one may still be part-way through a sweep, and every one of them writes
// to the control database.
//
// So the process could return from shutdown while a goroutine was still opening
// SQLite and writing to it, which leaves a -wal file beside a database nobody is
// using any more — and in a test, files appearing under a directory the harness
// is trying to remove.
//
// watchers is what makes "stopped" mean stopped.
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
