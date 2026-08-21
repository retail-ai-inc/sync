package timex

import (
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func TestGetJSTTimeRangeReturnsMidnightBoundaries(t *testing.T) {
	start, end, err := GetJSTTimeRange(-1, 0)
	if err != nil {
		t.Fatalf("GetJSTTimeRange: %v", err)
	}

	for _, ts := range []time.Time{start, end} {
		if ts.Hour() != 0 || ts.Minute() != 0 || ts.Second() != 0 || ts.Nanosecond() != 0 {
			t.Errorf("%v is not midnight", ts)
		}
		if name, offset := ts.Zone(); offset != 9*60*60 {
			t.Errorf("%v is in zone %s (offset %d), want JST (+32400)", ts, name, offset)
		}
	}
	if got := end.Sub(start); got != 24*time.Hour {
		t.Errorf("end - start = %v, want 24h", got)
	}
}

func TestGetJSTTimeRangeSpansMultipleDays(t *testing.T) {
	start, end, err := GetJSTTimeRange(-7, 0)
	if err != nil {
		t.Fatalf("GetJSTTimeRange: %v", err)
	}
	if got := end.Sub(start); got != 7*24*time.Hour {
		t.Errorf("end - start = %v, want 168h", got)
	}
}

func TestGetUTCTimeRangeIsTheJSTRangeShifted(t *testing.T) {
	startJST, endJST, err := GetJSTTimeRange(-1, 0)
	if err != nil {
		t.Fatalf("GetJSTTimeRange: %v", err)
	}
	startUTC, endUTC, err := GetUTCTimeRange(-1, 0)
	if err != nil {
		t.Fatalf("GetUTCTimeRange: %v", err)
	}

	if !startUTC.Equal(startJST) || !endUTC.Equal(endJST) {
		t.Errorf("UTC range (%v, %v) is not the same instant as the JST range (%v, %v)",
			startUTC, endUTC, startJST, endJST)
	}
	// JST midnight is 15:00 the previous day in UTC.
	if startUTC.UTC().Hour() != 15 {
		t.Errorf("start in UTC is %v, want 15:00 the previous day", startUTC.UTC())
	}
	if name, offset := startUTC.Zone(); offset != 0 {
		t.Errorf("start zone = %s (offset %d), want UTC", name, offset)
	}
}
