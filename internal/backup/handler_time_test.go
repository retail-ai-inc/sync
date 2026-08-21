package backup

import (
	"testing"
	"time"
)

// calculateNextBackupTime takes a cron expression and never reads it. Every
// backup task reports the same next run — twenty-four hours from now —
// regardless of its actual schedule, so the value shown in the UI is unrelated
// to when the job will fire.
func TestNextBackupTimeIgnoresTheCronExpression(t *testing.T) {
	schedules := []string{
		"*/5 * * * *", // every five minutes
		"0 3 * * *",   // 03:00 daily
		"0 0 1 * *",   // monthly
		"",            // not a schedule at all
		"not a cron",  // malformed
	}

	want := time.Now().UTC().Add(24 * time.Hour).Format("2006-01-02 15:04:05")
	for _, expr := range schedules {
		got := calculateNextBackupTime(expr)
		parsed, err := time.Parse("2006-01-02 15:04:05", got)
		if err != nil {
			t.Fatalf("calculateNextBackupTime(%q) = %q, not a SQL datetime: %v", expr, got, err)
		}
		if delta := parsed.Sub(time.Now().UTC().Add(24 * time.Hour)); delta < -2*time.Second || delta > 2*time.Second {
			t.Fatalf("calculateNextBackupTime(%q) = %q, no longer now+24h (want ~%s) — the expression appears to be parsed now; assert the real next run instead", expr, got, want)
		}
	}
}
