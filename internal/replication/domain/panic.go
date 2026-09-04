package domain

import (
	"fmt"
	"runtime/debug"
)

// A panic on one task's goroutine used to end the process, and with it the other
// tasks: four replication links stopped because one of them dereferenced
// something. Every goroutine a task owns turns its panic into an error instead,
// which the supervisor treats as the task stopping -- and the pipeline resumes
// from its stored position, so a restart begins from a point the target agrees
// with rather than from whatever the panic interrupted.

// Recovered turns a recovered panic into an error carrying its stack.
//
// The stack matters more here than in most errors: a panic has no message of
// its own worth reading, and the line it happened on is the whole content.
func Recovered(recovered interface{}) error {
	if recovered == nil {
		return nil
	}
	if err, ok := recovered.(error); ok {
		return fmt.Errorf("a task panicked: %w\n%s", err, debug.Stack())
	}
	return fmt.Errorf("a task panicked: %v\n%s", recovered, debug.Stack())
}

// Guard runs fn and returns its panic as an error rather than letting it end
// the process. Used at every goroutine boundary a task owns, because a
// goroutine's panic cannot be recovered by whoever started it.
func Guard(fn func() error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = Recovered(recovered)
		}
	}()
	return fn()
}
