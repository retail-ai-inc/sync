package resilience

import (
	"errors"
	"strings"
	"testing"
)

// A panic on one task's goroutine used to end the process, and with it the
// other three replication links. It becomes that task's error instead.
func TestAPanicBecomesAnError(t *testing.T) {
	err := Guard(func() error { panic("a nil map, probably") })
	if err == nil {
		t.Fatal("a panic produced no error, so it would have ended the process")
	}
	if !strings.Contains(err.Error(), "a nil map, probably") {
		t.Errorf("error = %v, want it to carry what the panic said", err)
	}
	// The line it happened on is the whole content of a panic.
	if !strings.Contains(err.Error(), "panic_test.go") {
		t.Errorf("error = %v, want it to carry the stack", err)
	}
}

// A panic of an error keeps that error wrappable, so a caller can still ask
// whether it was unrecoverable.
func TestAPanickedErrorStaysWrapped(t *testing.T) {
	sentinel := errors.New("the source went away")
	err := Guard(func() error { panic(sentinel) })
	if !errors.Is(err, sentinel) {
		t.Errorf("error = %v, want the panicked error to remain wrapped", err)
	}
}

// Guard must not change what a function that does not panic returns.
func TestGuardPassesThrough(t *testing.T) {
	if err := Guard(func() error { return nil }); err != nil {
		t.Errorf("nil became %v", err)
	}
	sentinel := errors.New("an ordinary failure")
	if err := Guard(func() error { return sentinel }); !errors.Is(err, sentinel) {
		t.Errorf("error = %v, want it passed through unchanged", err)
	}
}
