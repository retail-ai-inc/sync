package test

import (
	"errors"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/pkg/utils"
)

func testTC13Utils(t *testing.T) {
	now := utils.GetCurrentTime()
	t.Logf("Utils.GetCurrentTime => %s", now)

	utils.UnzipDistFile("", "")

	testRetry(t)
}

// Test the Retry function
func testRetry(t *testing.T) {
	// Test successful retry
	successCount := 0
	err := utils.Retry(3, 10*time.Millisecond, 1.5, func() error {
		successCount++
		if successCount < 2 {
			return errors.New("temporary error")
		}
		return nil
	})

	if err != nil {
		t.Errorf("Expected successful retry, but got error: %v", err)
	}

	if successCount != 2 {
		t.Errorf("Expected 2 attempts for successful retry, got %d", successCount)
	}

	// Test failure after all retries
	failureCount := 0
	err = utils.Retry(3, 10*time.Millisecond, 1.5, func() error {
		failureCount++
		return errors.New("persistent error")
	})

	if err == nil {
		t.Error("Expected error after all retries, but got nil")
	}

	if failureCount != 3 {
		t.Errorf("Expected 3 attempts for failed retry, got %d", failureCount)
	}
}
