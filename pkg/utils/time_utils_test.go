package utils

import (
	"reflect"
	"regexp"
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

// TestReplaceDatePlaceholdersCorruptsOrdinaryText records a defect with a wide
// blast radius. After handling the braced forms, the function also replaces the
// bare substrings YYYY, MM, DD, yyyy, mm and dd "for backward compatibility".
// Those are two-letter sequences that occur in ordinary English words, so any
// table or file name containing them is silently rewritten with digits.
//
// The one caller is processFileNamePattern in pkg/backup/executor.go, which
// applies this to the user-supplied file name pattern. The table name itself is
// appended afterwards and so survives, but any descriptive wording in the
// pattern does not: `summary_YYYYMM.json` yields `su08ary_202608.json`, and
// that is the name the backup is stored under in GCS.
func TestReplaceDatePlaceholdersCorruptsOrdinaryText(t *testing.T) {
	tests := []struct {
		input string
		want  string
		why   string
	}{
		{"summary", "su08ary", "mm -> 08"},
		{"address", "a21ress", "dd -> 21"},
		{"middleware", "mi21leware", "dd -> 21"},
		{"comment", "co08ent", "mm -> 08"},
		{"recommended", "reco08ended", "mm -> 08"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := ReplaceDatePlaceholdersWithDate(tt.input, fixedDate)

			if got == tt.input {
				t.Fatalf("ReplaceDatePlaceholdersWithDate(%q) is now left alone; the bare "+
					"substring replacement may have been removed, so assert that instead", tt.input)
			}
			if got != tt.want {
				t.Errorf("ReplaceDatePlaceholdersWithDate(%q) = %q, want %q (%s)",
					tt.input, got, tt.want, tt.why)
			}
		})
	}
}

// TestReplaceDatePlaceholdersMixesSuccessAndCorruption shows both behaviours in
// one realistic pattern: the intended date suffix resolves correctly while the
// table name in the same string is mangled.
func TestReplaceDatePlaceholdersMixesSuccessAndCorruption(t *testing.T) {
	const pattern = "order_summary_YYYYMM.json"

	got := ReplaceDatePlaceholdersWithDate(pattern, fixedDate)

	if want := "order_su08ary_202608.json"; got != want {
		t.Errorf("ReplaceDatePlaceholdersWithDate(%q) = %q, want %q", pattern, got, want)
	}
}

func TestReplaceDatePlaceholdersUsesCurrentDate(t *testing.T) {
	// Only the shape is asserted, since the real clock is involved.
	got := ReplaceDatePlaceholders("{YYYY}-{MM}-{DD}")

	if !regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`).MatchString(got) {
		t.Errorf("ReplaceDatePlaceholders = %q, want a YYYY-MM-DD shaped string", got)
	}
}

func TestGetTodayDateString(t *testing.T) {
	got := GetTodayDateString()

	if !regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`).MatchString(got) {
		t.Errorf("GetTodayDateString = %q, want a YYYY-MM-DD shaped string", got)
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

func TestProcessTimeRangeQueryConvertsDaily(t *testing.T) {
	query := map[string]interface{}{
		"created_at": map[string]interface{}{
			"type":        "daily",
			"startOffset": float64(-1),
			"endOffset":   float64(0),
		},
	}

	got, err := ProcessTimeRangeQuery(query)
	if err != nil {
		t.Fatalf("ProcessTimeRangeQuery returned %v", err)
	}

	converted, ok := got["created_at"].(map[string]interface{})
	if !ok {
		t.Fatalf("created_at is not a map: %#v", got["created_at"])
	}
	gte, ok := converted["$gte"].(map[string]interface{})
	if !ok {
		t.Fatalf("$gte is missing or not a map: %#v", converted)
	}
	lt, ok := converted["$lt"].(map[string]interface{})
	if !ok {
		t.Fatalf("$lt is missing or not a map: %#v", converted)
	}

	// Both bounds are extended JSON dates at UTC midnight-equivalents of JST
	// day boundaries, which is 15:00:00 UTC on the preceding day.
	for name, bound := range map[string]map[string]interface{}{"$gte": gte, "$lt": lt} {
		s, ok := bound["$date"].(string)
		if !ok {
			t.Fatalf("%s has no $date string: %#v", name, bound)
		}
		parsed, err := time.Parse("2006-01-02T15:04:05.000Z", s)
		if err != nil {
			t.Fatalf("%s date %q does not parse: %v", name, s, err)
		}
		if parsed.Hour() != 15 || parsed.Minute() != 0 || parsed.Second() != 0 {
			t.Errorf("%s = %q, want 15:00:00 UTC (JST midnight)", name, s)
		}
	}

	startStr := gte["$date"].(string)
	endStr := lt["$date"].(string)
	start, _ := time.Parse("2006-01-02T15:04:05.000Z", startStr)
	end, _ := time.Parse("2006-01-02T15:04:05.000Z", endStr)
	if !start.Before(end) {
		t.Errorf("start %q is not before end %q", startStr, endStr)
	}
	if got := end.Sub(start); got != 24*time.Hour {
		t.Errorf("range spans %v, want 24h for offsets -1..0", got)
	}
}

func TestProcessTimeRangeQueryPassesThroughNonRanges(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
	}{
		{"scalar", "active"},
		{"map without type", map[string]interface{}{"$gt": 5}},
		{"non-string type", map[string]interface{}{"type": 42}},
		// The comparison against "daily" is case-sensitive, so a capitalised
		// type is forwarded verbatim and reaches MongoDB as a literal filter.
		{"capitalised daily", map[string]interface{}{"type": "Daily", "startOffset": float64(-1), "endOffset": float64(0)}},
		{"unknown type", map[string]interface{}{"type": "weekly", "startOffset": float64(-7), "endOffset": float64(0)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ProcessTimeRangeQuery(map[string]interface{}{"field": tt.value})
			if err != nil {
				t.Fatalf("ProcessTimeRangeQuery returned %v", err)
			}
			if !reflect.DeepEqual(got["field"], tt.value) {
				t.Errorf("field = %#v, want it passed through unchanged (%#v)", got["field"], tt.value)
			}
		})
	}
}

// TestProcessTimeRangeQueryKeepsMalformedRangeVerbatim records a defect that
// turns a configuration mistake into an empty backup. When a daily range fails
// to convert — a missing offset, or offsets supplied as anything other than a
// JSON number — the error is logged and the *original object* is kept as the
// query value. MongoDB then receives `{type: "daily", startOffset: ...}` as a
// literal equality filter, matches nothing, and the backup completes with an
// empty file and no error anywhere.
func TestProcessTimeRangeQueryKeepsMalformedRangeVerbatim(t *testing.T) {
	malformed := []struct {
		name  string
		value map[string]interface{}
	}{
		{"missing endOffset", map[string]interface{}{"type": "daily", "startOffset": float64(-1)}},
		{"missing both offsets", map[string]interface{}{"type": "daily"}},
		{"offsets as strings", map[string]interface{}{"type": "daily", "startOffset": "-1", "endOffset": "0"}},
		{"offsets as ints", map[string]interface{}{"type": "daily", "startOffset": -1, "endOffset": 0}},
	}

	for _, tt := range malformed {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ProcessTimeRangeQuery(map[string]interface{}{"created_at": tt.value})
			if err != nil {
				t.Fatalf("ProcessTimeRangeQuery returned %v", err)
			}

			kept, ok := got["created_at"].(map[string]interface{})
			if !ok {
				t.Fatalf("created_at is not a map: %#v", got["created_at"])
			}
			if _, converted := kept["$gte"]; converted {
				t.Fatalf("the malformed range was converted; error handling may have " +
					"changed, so assert the new behaviour instead")
			}
			if kept["type"] != "daily" {
				t.Errorf("created_at = %#v, want the original object preserved verbatim", kept)
			}
		})
	}
}

// TestProcessTimeRangeQueryNeverReturnsError records that the error return is
// always nil: conversion failures are logged and swallowed inside the loop.
// Callers cannot distinguish a converted query from a silently skipped one.
func TestProcessTimeRangeQueryNeverReturnsError(t *testing.T) {
	inputs := []map[string]interface{}{
		nil,
		{},
		{"created_at": map[string]interface{}{"type": "daily"}},
		{"created_at": map[string]interface{}{"type": "daily", "startOffset": "bad", "endOffset": "bad"}},
	}

	for _, in := range inputs {
		if _, err := ProcessTimeRangeQuery(in); err != nil {
			t.Errorf("ProcessTimeRangeQuery(%#v) returned %v; if it now reports "+
				"failures, update this test and the callers", in, err)
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
