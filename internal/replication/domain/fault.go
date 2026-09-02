package domain

import (
	"errors"
	"fmt"
)

// ErrUnrecoverable marks a stop that retrying cannot fix. The distinction
// matters because a task is restarted when it exits.
var ErrUnrecoverable = errors.New("replication cannot continue without intervention")

func Unrecoverable(format string, args ...interface{}) error {
	return fmt.Errorf("%w: %s", ErrUnrecoverable, fmt.Sprintf(format, args...))
}

func IsUnrecoverable(err error) bool {
	return errors.Is(err, ErrUnrecoverable)
}
