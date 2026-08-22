package domain

import (
	"errors"
	"fmt"
)

// ErrUnrecoverable marks a stop that retrying cannot fix.
//
// The distinction matters because a task is restarted when it exits. A source
// that is briefly unreachable, a target that was restarting, a connection reset
// across the region boundary — those come back, and a task that gave up on them
// permanently would mean replication silently stopping the first time a network
// hiccuped.
//
// A purged binlog or a rolled-over oplog is the opposite: the position the task
// would resume from no longer exists, so every attempt will fail the same way
// for as long as anybody lets it. Restarting is not merely useless there, it
// hides the one thing an operator needs to be told — that a fresh copy is
// required and until it is made the replica is falling further behind.
var ErrUnrecoverable = errors.New("replication cannot continue without intervention")

// Unrecoverable wraps a reason as a stop that must not be retried.
func Unrecoverable(format string, args ...interface{}) error {
	return fmt.Errorf("%w: %s", ErrUnrecoverable, fmt.Sprintf(format, args...))
}

// IsUnrecoverable reports whether an error says retrying is pointless.
func IsUnrecoverable(err error) bool {
	return errors.Is(err, ErrUnrecoverable)
}
