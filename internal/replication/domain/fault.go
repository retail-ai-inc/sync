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

// ErrPositionUnusable marks the narrower case where what stops the task is the
// stored position itself: the source cannot continue from it and cannot say
// what happened after it. Redis is where this arises -- its replication
// history lives in memory, so a restart of the source ends the history a
// stored offset belongs to.
//
// It is unrecoverable as well: without being asked to do otherwise, a task
// that cannot resume stops rather than deciding on its own to rebuild the
// target. What the setting adds is the option of asking.
var ErrPositionUnusable = errors.New("the stored position can no longer be used")

func PositionUnusable(format string, args ...interface{}) error {
	return fmt.Errorf("%w: %w: %s", ErrUnrecoverable, ErrPositionUnusable,
		fmt.Sprintf(format, args...))
}

func IsPositionUnusable(err error) bool {
	return errors.Is(err, ErrPositionUnusable)
}
