package monitoringhttp

import (
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/retail-ai-inc/sync/internal/platform/httpx"
)

func TestConvertToJST(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"rfc3339 utc", "2026-08-21T00:30:00Z", "2026-08-21T09:30+09:00"},
		{"rfc3339 with an offset", "2026-08-21T00:30:00+02:00", "2026-08-21T07:30+09:00"},
		{"seconds are dropped", "2026-08-21T00:30:45Z", "2026-08-21T09:30+09:00"},
		{"an empty string is passed through", "", ""},
		{"a sql timestamp is not understood", "2026-08-21 00:30:00", "2026-08-21 00:30:00"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := convertToJST(tc.input); got != tc.want {
				t.Errorf("convertToJST(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

// The package carries two JST converters that disagree on both the accepted
// input and the emitted layout: convertToJST (monitor_handler.go) takes
// RFC3339 and drops seconds, ConvertTimeToJST (sync_handler.go) takes a SQL
// timestamp and keeps them.
func TestTheTwoJSTConvertersDisagree(t *testing.T) {
	const rfc = "2026-08-21T00:30:00Z"
	const sqlTS = "2026-08-21 00:30:00"

	if convertToJST(rfc) == httpx.ConvertTimeToJST(rfc) {
		t.Fatal("the two converters now agree on RFC3339 — they appear to have been unified")
	}
	if convertToJST(sqlTS) != sqlTS {
		t.Fatal("convertToJST now understands SQL timestamps — the converters appear to have been unified")
	}
	if httpx.ConvertTimeToJST(rfc) == rfc {
		t.Fatal("ConvertTimeToJST no longer understands RFC3339 — the converters appear to have been unified")
	}
}

func TestParseRangeToSince(t *testing.T) {
	tests := []struct {
		input string
		back  time.Duration
	}{
		{"1h", time.Hour},
		{"2h", 2 * time.Hour},
		{"3h", 3 * time.Hour},
		{"6h", 6 * time.Hour},
		{"12h", 12 * time.Hour},
		{"1d", 24 * time.Hour},
		{"2d", 48 * time.Hour},
		{"7d", 7 * 24 * time.Hour},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got, err := parseRangeToSince(tc.input)
			if err != nil {
				t.Fatalf("parseRangeToSince(%q): %v", tc.input, err)
			}
			want := time.Now().UTC().Add(-tc.back)
			if delta := got.Sub(want); delta < -2*time.Second || delta > 2*time.Second {
				t.Errorf("parseRangeToSince(%q) = %v, want ~%v (off by %v)", tc.input, got, want, delta)
			}
		})
	}
}

func TestParseRangeToSinceIsCaseInsensitive(t *testing.T) {
	lower, _ := parseRangeToSince("12h")
	upper, _ := parseRangeToSince("12H")

	if delta := upper.Sub(lower); delta < -2*time.Second || delta > 2*time.Second {
		t.Errorf("parseRangeToSince(\"12H\") = %v, parseRangeToSince(\"12h\") = %v", upper, lower)
	}
}

func TestParseRangeToSinceEmptyIsTheZeroTime(t *testing.T) {
	got, err := parseRangeToSince("")
	if err != nil {
		t.Fatalf("parseRangeToSince(\"\"): %v", err)
	}
	if !got.IsZero() {
		t.Errorf("parseRangeToSince(\"\") = %v, want the zero time", got)
	}
}

// The switch listed eight spellings and answered anything else with ten hours
// — a value that appears nowhere in the set it documents — so "30m" and "24h",
// both perfectly reasonable, silently returned ten hours of data.
func TestAnyDurationIsARange(t *testing.T) {
	for input, back := range map[string]time.Duration{
		"30m":   30 * time.Minute,
		"24h":   24 * time.Hour,
		"4h":    4 * time.Hour,
		"90s":   90 * time.Second,
		"30d":   30 * 24 * time.Hour,
		"1h30m": 90 * time.Minute,
	} {
		t.Run(input, func(t *testing.T) {
			got, err := parseRangeToSince(input)
			if err != nil {
				t.Fatalf("parseRangeToSince(%q): %v", input, err)
			}
			want := time.Now().UTC().Add(-back)
			if delta := got.Sub(want); delta < -2*time.Second || delta > 2*time.Second {
				t.Errorf("parseRangeToSince(%q) = %v, want ~%v", input, got, want)
			}
		})
	}
}

// TestARangeThatIsNotOneIsReported is the other half: something that cannot be
// read as a window is an error, not ten hours.
func TestARangeThatIsNotOneIsReported(t *testing.T) {
	for _, input := range []string{"banana", "-1h", "0", "1 hour", "d", "-3d"} {
		if got, err := parseRangeToSince(input); err == nil {
			t.Errorf("parseRangeToSince(%q) = %v, want a refusal", input, got)
		}
	}
}
