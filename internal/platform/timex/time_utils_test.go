package timex

import (
	"strings"
	"testing"
	"time"
)

// fixedDate is a Friday in August, chosen so that the month (08) and day (21)
// are distinguishable from each other and from the year.
var fixedDate = time.Date(2026, 8, 21, 13, 45, 30, 0, time.UTC)

func TestReplaceDatePlaceholdersWithDate(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		want    string
	}{
		{"braced upper", "{YYYY}-{MM}-{DD}", "2026-08-21"},
		{"braced lower", "{yyyy}/{mm}/{dd}", "2026/08/21"},
		{"bare upper", "YYYYMMDD", "20260821"},
		{"bare lower", "yyyymmdd", "20260821"},
		{"mixed with prefix", "backup_{YYYY}{MM}{DD}.json", "backup_20260821.json"},
		{"no placeholder", "orders.json", "orders.json"},
		{"empty", "", ""},
		{"month is zero padded", "{MM}", "08"},
		{"day is zero padded", "{DD}", "21"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ReplaceDatePlaceholdersWithDate(tt.pattern, fixedDate); got != tt.want {
				t.Errorf("ReplaceDatePlaceholdersWithDate(%q) = %q, want %q", tt.pattern, got, tt.want)
			}
		})
	}
}

// The bare forms — YYYY, MM, DD and their lower-case spellings — used to be
// replaced as plain substrings, and "mm" and "dd" occur in ordinary English,
// so any wording in a file-name pattern came back written in digits.
func TestOrdinaryWordsAreLeftAlone(t *testing.T) {
	for _, word := range []string{
		"summary", "address", "middleware", "comment", "recommended", "orders", "payments",
	} {
		t.Run(word, func(t *testing.T) {
			if got := ReplaceDatePlaceholdersWithDate(word, fixedDate); got != word {
				t.Errorf("ReplaceDatePlaceholdersWithDate(%q) = %q, want it untouched", word, got)
			}
		})
	}
}

// TestABareDateIsStillADate covers the patterns written before the braces
// existed: a word that is nothing but date tokens is still substituted.
func TestABareDateIsStillADate(t *testing.T) {
	tests := []struct{ pattern, want string }{
		{"YYYYMMDD", "20260821"},
		{"yyyymmdd", "20260821"},
		{"summary_YYYYMM.json", "summary_202608.json"},
		{"orders_YYYY-MM-DD.sql", "orders_2026-08-21.sql"},
		{"backup_YYYYMMDD_summary.json", "backup_20260821_summary.json"},
	}

	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			if got := ReplaceDatePlaceholdersWithDate(tt.pattern, fixedDate); got != tt.want {
				t.Errorf("ReplaceDatePlaceholdersWithDate(%q) = %q, want %q", tt.pattern, got, tt.want)
			}
		})
	}
}

// TestTheDatePartIsReplacedAndTheRestIsNot puts both behaviours in one
// realistic pattern: the date suffix resolves and the words around it survive.
func TestTheDatePartIsReplacedAndTheRestIsNot(t *testing.T) {
	const pattern = "order_summary_YYYYMM.json"

	got := ReplaceDatePlaceholdersWithDate(pattern, fixedDate)

	if want := "order_summary_202608.json"; got != want {
		t.Errorf("ReplaceDatePlaceholdersWithDate(%q) = %q, want %q", pattern, got, want)
	}
}

// An offset that was not a JSON number used to fall back to the default with
// nothing said, so a task asking for the last week backed up yesterday.
func TestDailyOffsetsAcceptsEverySpellingOfANumber(t *testing.T) {
	for name, spec := range map[string]map[string]interface{}{
		"json numbers": {"startOffset": float64(-7), "endOffset": float64(0)},
		"go ints":      {"startOffset": -7, "endOffset": 0},
		"strings":      {"startOffset": "-7", "endOffset": "0"},
		"int64":        {"startOffset": int64(-7), "endOffset": int64(0)},
	} {
		t.Run(name, func(t *testing.T) {
			start, end, err := DailyOffsets(spec)
			if err != nil {
				t.Fatalf("DailyOffsets: %v", err)
			}
			if start != -7 || end != 0 {
				t.Errorf("offsets = %d..%d, want -7..0", start, end)
			}
		})
	}
}

// TestAnOffsetThatIsNotANumberIsReported is the other half: something that
// cannot be read as a number is said out loud rather than replaced by a default
// that quietly backs up the wrong days.
func TestAnOffsetThatIsNotANumberIsReported(t *testing.T) {
	for name, spec := range map[string]map[string]interface{}{
		"words":   {"startOffset": "last week", "endOffset": float64(0)},
		"a list":  {"startOffset": []interface{}{-1}, "endOffset": float64(0)},
		"a float": {"startOffset": "-1.5", "endOffset": float64(0)},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := DailyOffsets(spec); err == nil {
				t.Error("DailyOffsets accepted an offset that is not a number of days")
			}
		})
	}
}

// TestAnEmptyWindowIsReported covers the intuitive spelling of "just today".
// endOffset is exclusive, so 0 to 0 names no time at all, and an export built
// from it writes an empty file and reports a successful backup.
func TestAnEmptyWindowIsReported(t *testing.T) {
	for name, spec := range map[string]map[string]interface{}{
		"same day":    {"startOffset": float64(0), "endOffset": float64(0)},
		"end earlier": {"startOffset": float64(0), "endOffset": float64(-1)},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := DailyOffsets(spec); err == nil {
				t.Error("DailyOffsets accepted a window nothing can fall in")
			}
		})
	}
}

func TestTheDefaultWindowIsYesterday(t *testing.T) {
	start, end, err := DailyOffsets(map[string]interface{}{"type": "daily"})
	if err != nil {
		t.Fatalf("DailyOffsets: %v", err)
	}
	if start != -1 || end != 0 {
		t.Errorf("offsets = %d..%d, want -1..0", start, end)
	}
}

func TestParseDatabaseTimestamp(t *testing.T) {
	got, err := ParseDatabaseTimestamp("2026-08-21 13:45:30")
	if err != nil {
		t.Fatalf("ParseDatabaseTimestamp returned %v", err)
	}
	if want := time.Date(2026, 8, 21, 13, 45, 30, 0, time.UTC); !got.Equal(want) {
		t.Errorf("ParseDatabaseTimestamp = %v, want %v", got, want)
	}

	for _, bad := range []string{"2026-08-21", "21/08/2026 13:45:30", "", "not a time"} {
		if _, err := ParseDatabaseTimestamp(bad); err == nil {
			t.Errorf("ParseDatabaseTimestamp(%q) succeeded, want an error", bad)
		}
	}
}

func TestGetJSTTimeRange(t *testing.T) {
	start, end, err := GetJSTTimeRange(-7, 0)
	if err != nil {
		t.Fatalf("GetJSTTimeRange returned %v", err)
	}

	// Both bounds sit at midnight in Asia/Tokyo.
	for name, ts := range map[string]time.Time{"start": start, "end": end} {
		if ts.Hour() != 0 || ts.Minute() != 0 || ts.Second() != 0 || ts.Nanosecond() != 0 {
			t.Errorf("%s = %v, want midnight", name, ts)
		}
		if zone, offset := ts.Zone(); offset != 9*60*60 {
			t.Errorf("%s zone = %s (%d s), want JST (+32400 s)", name, zone, offset)
		}
	}
	if got := end.Sub(start); got != 7*24*time.Hour {
		t.Errorf("range spans %v, want 168h for offsets -7..0", got)
	}
}

func TestGetUTCTimeRangeMatchesJST(t *testing.T) {
	startJST, endJST, err := GetJSTTimeRange(-1, 0)
	if err != nil {
		t.Fatalf("GetJSTTimeRange returned %v", err)
	}
	startUTC, endUTC, err := GetUTCTimeRange(-1, 0)
	if err != nil {
		t.Fatalf("GetUTCTimeRange returned %v", err)
	}

	if !startUTC.Equal(startJST) || !endUTC.Equal(endJST) {
		t.Errorf("UTC range %v..%v is not the same instant as JST %v..%v",
			startUTC, endUTC, startJST, endJST)
	}
	// JST midnight is 15:00 UTC on the previous day.
	if startUTC.UTC().Hour() != 15 {
		t.Errorf("start in UTC = %v, want 15:00", startUTC.UTC())
	}
}

// Two formats are in the control database: what this writes, and RFC 3339 from
// an earlier version. A reader that knew only the first reported every one of
// the older rows as never having happened.
func TestBothStoredTimestampFormatsAreRead(t *testing.T) {
	want := time.Date(2026, 9, 4, 15, 20, 34, 0, time.UTC)

	for name, stored := range map[string]string{
		"what this writes":            "2026-09-04 15:20:34",
		"what an earlier version did": "2026-09-04T15:20:34Z",
	} {
		t.Run(name, func(t *testing.T) {
			got, err := ParseDatabaseTimestamp(stored)
			if err != nil {
				t.Fatalf("ParseDatabaseTimestamp(%q): %v", stored, err)
			}
			if !got.Equal(want) {
				t.Errorf("read %v, want %v", got, want)
			}
			if got.Location() != time.UTC {
				t.Errorf("read in %v, want UTC", got.Location())
			}
		})
	}

	// And something that is neither is still an error, naming the format a new
	// row will be in.
	_, err := ParseDatabaseTimestamp("last Tuesday")
	if err == nil {
		t.Fatal("\"last Tuesday\" read as a time")
	}
	if !strings.Contains(err.Error(), "2006-01-02 15:04:05") {
		t.Errorf("the error does not name the format this writes: %v", err)
	}
}
