package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWriteJSONSetsContentTypeAndBody(t *testing.T) {
	rec := httptest.NewRecorder()

	writeJSON(rec, map[string]interface{}{"success": true, "count": 3})

	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v (%q)", err, rec.Body.String())
	}
	if body["success"] != true {
		t.Errorf("success = %v, want true", body["success"])
	}
	if body["count"] != float64(3) {
		t.Errorf("count = %v, want 3", body["count"])
	}
}

func TestWriteJSONEncodesNil(t *testing.T) {
	rec := httptest.NewRecorder()

	writeJSON(rec, nil)

	if got := rec.Body.String(); got != "null\n" {
		t.Errorf("body = %q, want %q", got, "null\n")
	}
}

func TestErrorJSONShape(t *testing.T) {
	rec := httptest.NewRecorder()

	errorJSON(rec, "failed to open the task", errors.New("no such file"))

	var body map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v (%q)", err, rec.Body.String())
	}
	if body["success"] != false {
		t.Errorf("success = %v, want false", body["success"])
	}
	if body["error"] != "failed to open the task" {
		t.Errorf("error = %v", body["error"])
	}
	if body["detail"] != "no such file" {
		t.Errorf("detail = %v", body["detail"])
	}
}

// errorJSON reports failure in the body but leaves the status line untouched,
// so every error this helper produces is served as 200 OK. A client that
// branches on the status code — a load balancer, a probe, a generated SDK —
// sees a successful request. If this ever starts returning a 4xx/5xx the
// helper has been fixed; assert the intended status instead.
func TestErrorJSONStillReturnsHTTP200(t *testing.T) {
	rec := httptest.NewRecorder()

	errorJSON(rec, "database is unreachable", errors.New("connection refused"))

	if rec.Code != http.StatusOK {
		t.Fatalf("errorJSON now returns %d — it appears to be fixed; assert the intended status instead", rec.Code)
	}
}

// errorJSON dereferences its error argument unconditionally, so a caller that
// reports a failure without one takes down the request goroutine.
func TestErrorJSONPanicsOnNilError(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("errorJSON no longer panics on a nil error — it appears to be fixed; assert the emitted detail instead")
		}
	}()

	errorJSON(httptest.NewRecorder(), "something went wrong", nil)
}

func TestTimeNowStrIsUTCSQLFormat(t *testing.T) {
	got := timeNowStr()

	parsed, err := time.Parse("2006-01-02 15:04:05", got)
	if err != nil {
		t.Fatalf("timeNowStr() = %q, not a SQL datetime: %v", got, err)
	}
	if delta := time.Since(parsed.UTC()); delta < -2*time.Second || delta > 2*time.Second {
		t.Errorf("timeNowStr() = %q, %v away from now — it is probably not UTC", got, delta)
	}
}

func TestConvertTimeToJST(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"sql format shifts by nine hours", "2026-08-21 00:30:00", "2026-08-21 09:30:00"},
		{"sql format crosses the date line", "2026-08-21 20:00:00", "2026-08-22 05:00:00"},
		{"rfc3339 utc is normalised to sql format", "2026-08-21T00:30:00Z", "2026-08-21 09:30:00"},
		{"rfc3339 with an offset is respected", "2026-08-21T00:30:00+02:00", "2026-08-21 07:30:00"},
		{"an empty string stays empty", "", ""},
		{"an unparseable string is passed through", "not a time", "not a time"},
		{"a date without a clock is passed through", "2026-08-21", "2026-08-21"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := convertTimeToJST(tc.input); got != tc.want {
				t.Errorf("convertTimeToJST(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

// convertTimeToJST treats a naive SQL timestamp as UTC. Any caller that stores
// local time in that column gets a second, silent nine-hour shift.
func TestConvertTimeToJSTAssumesTheInputIsUTC(t *testing.T) {
	const alreadyJST = "2026-08-21 09:30:00"

	if got := convertTimeToJST(alreadyJST); got != "2026-08-21 18:30:00" {
		t.Fatalf("convertTimeToJST(%q) = %q — the UTC assumption appears to have changed", alreadyJST, got)
	}
}

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
// RFC3339 and drops seconds, convertTimeToJST (sync_handler.go) takes a SQL
// timestamp and keeps them. Each silently passes through what the other
// handles, so the format a field arrives in depends on which handler served it.
func TestTheTwoJSTConvertersDisagree(t *testing.T) {
	const rfc = "2026-08-21T00:30:00Z"
	const sqlTS = "2026-08-21 00:30:00"

	if convertToJST(rfc) == convertTimeToJST(rfc) {
		t.Fatal("the two converters now agree on RFC3339 — they appear to have been unified")
	}
	if convertToJST(sqlTS) != sqlTS {
		t.Fatal("convertToJST now understands SQL timestamps — the converters appear to have been unified")
	}
	if convertTimeToJST(rfc) == rfc {
		t.Fatal("convertTimeToJST no longer understands RFC3339 — the converters appear to have been unified")
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
			got := parseRangeToSince(tc.input)
			want := time.Now().UTC().Add(-tc.back)
			if delta := got.Sub(want); delta < -2*time.Second || delta > 2*time.Second {
				t.Errorf("parseRangeToSince(%q) = %v, want ~%v (off by %v)", tc.input, got, want, delta)
			}
		})
	}
}

func TestParseRangeToSinceIsCaseInsensitive(t *testing.T) {
	lower := parseRangeToSince("12h")
	upper := parseRangeToSince("12H")

	if delta := upper.Sub(lower); delta < -2*time.Second || delta > 2*time.Second {
		t.Errorf("parseRangeToSince(\"12H\") = %v, parseRangeToSince(\"12h\") = %v", upper, lower)
	}
}

func TestParseRangeToSinceEmptyIsTheZeroTime(t *testing.T) {
	if got := parseRangeToSince(""); !got.IsZero() {
		t.Errorf("parseRangeToSince(\"\") = %v, want the zero time", got)
	}
}

// An unrecognised range is not rejected and does not fall back to a documented
// default — it silently becomes a ten-hour window, a value that appears
// nowhere in the accepted set. A caller asking for "30m" or "24h" (neither is
// in the switch) gets ten hours of data and no indication anything was wrong.
func TestUnknownRangesSilentlyBecomeTenHours(t *testing.T) {
	for _, input := range []string{"30m", "24h", "4h", "banana", "-1h", "0"} {
		got := parseRangeToSince(input)
		want := time.Now().UTC().Add(-10 * time.Hour)
		if delta := got.Sub(want); delta < -2*time.Second || delta > 2*time.Second {
			t.Fatalf("parseRangeToSince(%q) = %v, no longer the undocumented 10h default — it appears to be fixed; assert the new behaviour instead", input, got)
		}
	}
}

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
