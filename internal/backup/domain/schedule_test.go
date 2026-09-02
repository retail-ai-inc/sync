package domain

import (
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// TestNextBackupTimeReadsTheCronExpression covers a figure shown to operators.
func TestNextBackupTimeReadsTheCronExpression(t *testing.T) {
	now := time.Now().UTC()

	for expr, within := range map[string]time.Duration{
		"*/5 * * * *": 6 * time.Minute,
		"0 * * * *":   61 * time.Minute,
		"0 3 * * *":   25 * time.Hour,
		"0 0 1 * *":   32 * 24 * time.Hour,
	} {
		t.Run(expr, func(t *testing.T) {
			got := NextBackupTime(expr)
			parsed, err := time.Parse("2006-01-02 15:04:05", got)
			if err != nil {
				t.Fatalf("NextBackupTime(%q) = %q: %v", expr, got, err)
			}
			if !parsed.After(now) {
				t.Errorf("NextBackupTime(%q) = %q, which is not in the future", expr, got)
			}
			if parsed.Sub(now) > within {
				t.Errorf("NextBackupTime(%q) = %q, more than %v away", expr, got, within)
			}
		})
	}
}

// TestNextBackupTimeOfSomethingThatIsNotASchedule covers the other half: an
// expression that cannot be read answers nothing, so the column is empty rather
// than carrying a confident wrong answer.
func TestNextBackupTimeOfSomethingThatIsNotASchedule(t *testing.T) {
	for _, expr := range []string{"", "not a cron", "0 3 * *", "0 99 * * *"} {
		if got := NextBackupTime(expr); got != "" {
			t.Errorf("NextBackupTime(%q) = %q, want nothing", expr, got)
		}
	}
}
