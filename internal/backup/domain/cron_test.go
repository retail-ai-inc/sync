package domain

import (
	"testing"
	"time"
)

func at(y int, m time.Month, d, hh, mm int) time.Time {
	return time.Date(y, m, d, hh, mm, 0, 0, time.UTC)
}

func TestTheNextRunFollowsTheExpression(t *testing.T) {
	// A Saturday.
	from := at(2026, time.August, 22, 10, 7)

	for name, tc := range map[string]struct {
		expr string
		want time.Time
	}{
		"every minute":        {"* * * * *", at(2026, time.August, 22, 10, 8)},
		"every five minutes":  {"*/5 * * * *", at(2026, time.August, 22, 10, 10)},
		"on the hour":         {"0 * * * *", at(2026, time.August, 22, 11, 0)},
		"daily at three":      {"0 3 * * *", at(2026, time.August, 23, 3, 0)},
		"first of the month":  {"0 0 1 * *", at(2026, time.September, 1, 0, 0)},
		"a list of minutes":   {"0,30 * * * *", at(2026, time.August, 22, 10, 30)},
		"a range of hours":    {"0 9-17 * * *", at(2026, time.August, 22, 11, 0)},
		"a step from a value": {"7/20 * * * *", at(2026, time.August, 22, 10, 27)},
		"sundays":             {"0 0 * * 0", at(2026, time.August, 23, 0, 0)},
		"sunday written as 7": {"0 0 * * 7", at(2026, time.August, 23, 0, 0)},
		"one month a year":    {"0 0 1 1 *", at(2027, time.January, 1, 0, 0)},
	} {
		t.Run(name, func(t *testing.T) {
			schedule, err := ParseSchedule(tc.expr)
			if err != nil {
				t.Fatalf("ParseSchedule(%q): %v", tc.expr, err)
			}
			if got := schedule.Next(from); !got.Equal(tc.want) {
				t.Errorf("Next = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestTheTwoDayFieldsAreAlternatives covers the one place cron's semantics are
// not "every field must match": when both the day of the month and the day of
// the week name something in particular, either one matching is enough.
func TestTheTwoDayFieldsAreAlternatives(t *testing.T) {
	schedule, err := ParseSchedule("0 0 1 * 0") // the 1st, or any Sunday
	if err != nil {
		t.Fatalf("ParseSchedule: %v", err)
	}

	// From Wednesday 26 August 2026: the next Sunday is the 30th, before the 1st.
	if got := schedule.Next(at(2026, time.August, 26, 12, 0)); !got.Equal(at(2026, time.August, 30, 0, 0)) {
		t.Errorf("Next = %v, want the Sunday", got)
	}
}

func TestAnExpressionThatIsNotOneIsRefused(t *testing.T) {
	for name, expr := range map[string]string{
		"empty":                "",
		"four fields":          "0 3 * *",
		"six fields":           "0 3 * * * *",
		"prose":                "every five minutes",
		"minute out of range":  "60 * * * *",
		"hour out of range":    "0 24 * * *",
		"day out of range":     "0 0 32 * *",
		"month out of range":   "0 0 1 13 *",
		"weekday out of range": "0 0 * * 8",
		"backwards range":      "0 17-9 * * *",
		"a step of zero":       "*/0 * * * *",
		"an empty term":        "0,,30 * * * *",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseSchedule(expr); err == nil {
				t.Errorf("ParseSchedule(%q) was accepted", expr)
			}
		})
	}
}

// TestADateThatNeverComesAnswersNothing covers an expression that is well formed
// and names no instant, so the search has to give up rather than run forever.
func TestADateThatNeverComesAnswersNothing(t *testing.T) {
	schedule, err := ParseSchedule("0 0 30 2 *") // 30 February
	if err != nil {
		t.Fatalf("ParseSchedule: %v", err)
	}

	if got := schedule.Next(at(2026, time.August, 22, 10, 0)); !got.IsZero() {
		t.Errorf("Next = %v, want the zero time", got)
	}
}

// TestTheNextRunIsStrictlyLater covers the boundary: asked at exactly a time the
// schedule names, the answer is the one after it, not the same instant.
func TestTheNextRunIsStrictlyLater(t *testing.T) {
	schedule, err := ParseSchedule("0 3 * * *")
	if err != nil {
		t.Fatalf("ParseSchedule: %v", err)
	}

	three := at(2026, time.August, 22, 3, 0)
	if got := schedule.Next(three); !got.Equal(at(2026, time.August, 23, 3, 0)) {
		t.Errorf("Next = %v, want the following day", got)
	}
}
