package httpx

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

	WriteJSON(rec, map[string]interface{}{"success": true, "count": 3})

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

	WriteJSON(rec, nil)

	if got := rec.Body.String(); got != "null\n" {
		t.Errorf("body = %q, want %q", got, "null\n")
	}
}

func TestErrorJSONShape(t *testing.T) {
	rec := httptest.NewRecorder()

	ErrorJSON(rec, "failed to open the task", errors.New("no such file"))

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

// The helper reported the failure in the body and left the status line
// untouched, so anything that branches on the status — a load balancer, a
// health probe, a generated client — read it as a successful request.
func TestErrorJSONSetsAStatus(t *testing.T) {
	rec := httptest.NewRecorder()

	ErrorJSON(rec, "database is unreachable", errors.New("connection refused"))

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("ErrorJSON answered %d, want 500", rec.Code)
	}
}

// TestErrorJSONStatusCarriesTheGivenStatus covers the failures that are the
// caller's rather than this program's.
func TestErrorJSONStatusCarriesTheGivenStatus(t *testing.T) {
	rec := httptest.NewRecorder()

	ErrorJSONStatus(rec, http.StatusBadRequest, "that is not a range", errors.New("30 fortnights"))

	if rec.Code != http.StatusBadRequest {
		t.Errorf("ErrorJSONStatus answered %d, want 400", rec.Code)
	}
}

// TestErrorJSONWithoutAnError covers a caller reporting a failure with nothing
// to attach. The error used to be dereferenced unconditionally, which took down
// the request goroutine.
func TestErrorJSONWithoutAnError(t *testing.T) {
	rec := httptest.NewRecorder()

	ErrorJSON(rec, "something went wrong", nil)

	var body map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["error"] != "something went wrong" {
		t.Errorf("error = %v", body["error"])
	}
	if body["detail"] != "" {
		t.Errorf("detail = %v, want empty", body["detail"])
	}
}

func TestTimeNowStrIsUTCSQLFormat(t *testing.T) {
	got := TimeNowStr()

	parsed, err := time.Parse("2006-01-02 15:04:05", got)
	if err != nil {
		t.Fatalf("TimeNowStr() = %q, not a SQL datetime: %v", got, err)
	}
	if delta := time.Since(parsed.UTC()); delta < -2*time.Second || delta > 2*time.Second {
		t.Errorf("TimeNowStr() = %q, %v away from now — it is probably not UTC", got, delta)
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
			if got := ConvertTimeToJST(tc.input); got != tc.want {
				t.Errorf("ConvertTimeToJST(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

// ConvertTimeToJST treats a naive SQL timestamp as UTC. Any caller that stores
// local time in that column gets a second, silent nine-hour shift.
func TestConvertTimeToJSTAssumesTheInputIsUTC(t *testing.T) {
	const alreadyJST = "2026-08-21 09:30:00"

	if got := ConvertTimeToJST(alreadyJST); got != "2026-08-21 18:30:00" {
		t.Fatalf("ConvertTimeToJST(%q) = %q — the UTC assumption appears to have changed", alreadyJST, got)
	}
}
