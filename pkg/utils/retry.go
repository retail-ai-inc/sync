package utils

import (
	"log"
	"time"
)

func Retry(attempts int, initialDelay time.Duration, factor float64, operation func() error) error {
	delay := initialDelay
	var err error
	for i := 1; i <= attempts; i++ {
		err = operation()
		if err == nil {
			return nil
		}
		log.Printf("[Retry] Attempt %d/%d failed: %v. Retrying in %s...", i, attempts, err, delay)
		time.Sleep(delay)
		delay = time.Duration(float64(delay) * factor)
	}
	return err
}
