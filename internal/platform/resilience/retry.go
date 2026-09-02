package resilience

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"
)

// Retry runs operation until it succeeds, up to attempts times, waiting between
// attempts and multiplying the wait by factor each time.
//
// The wait happens between attempts and not after the last one. The loop is
// over by then and the same error goes back to the caller either way, so with
// the settings the syncers use — five attempts starting at two seconds — the
// trailing sleep delayed the report of an unreachable source by thirty-two
// seconds for nothing.
//
// The context cancels both the waiting and the loop. Without one a syncer asked
// to stop while its source was down went on sleeping for the best part of a
// minute, and the supervisor gives a task ten seconds before it gives up on it.
func Retry(ctx context.Context, attempts int, initialDelay time.Duration, factor float64, operation func() error) error {
	if attempts <= 0 {
		// Reporting success for work that was never attempted is worse than
		// reporting the mistake.
		return fmt.Errorf("retry: asked for %d attempts, so the operation never ran", attempts)
	}

	delay := initialDelay
	var err error
	for i := 1; i <= attempts; i++ {
		if ctxErr := ctx.Err(); ctxErr != nil {
			if err != nil {
				return err
			}
			return ctxErr
		}

		if err = operation(); err == nil {
			return nil
		}
		if i == attempts || isPermanent(err) {
			break
		}

		log.Printf("[Retry] Attempt %d/%d failed: %v. Retrying in %s...", i, attempts, err, delay)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return err
		case <-timer.C:
		}
		delay = time.Duration(float64(delay) * factor)
	}
	return err
}

// Permanent is implemented by errors that no further attempt will survive. Retry
// stops on one instead of spending its whole backoff on, say, a connection
// string the driver will never parse.
type Permanent interface {
	error
	Permanent() bool
}

func isPermanent(err error) bool {
	var p Permanent
	return errors.As(err, &p) && p.Permanent()
}
