package utils

import (
	"database/sql"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
)

func TestUnzipDistFileDependsOnAnExternalBinary(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PATH", dir) // an empty PATH: no unzip anywhere

	err := UnzipDistFile(filepath.Join(dir, "x.zip"), filepath.Join(dir, "out"))
	if err == nil {
		t.Fatal("UnzipDistFile() = nil without unzip on PATH — it appears to use archive/zip now")
	}
	if !strings.Contains(err.Error(), "system unzip command") {
		t.Fatalf("err = %v — the external dependency appears to be gone", err)
	}
}

func TestInitDBPathRespectsAnExistingEnvironmentVariable(t *testing.T) {
	t.Setenv("SYNC_DB_PATH", "/somewhere/else/sync.db")

	initDBPath()

	if got := os.Getenv("SYNC_DB_PATH"); got != "/somewhere/else/sync.db" {
		t.Errorf("SYNC_DB_PATH = %q, want the pre-set value to be left alone", got)
	}
}

// When SYNC_DB_PATH is unset, the fallback is derived from runtime.Caller,
// which yields the path of this source file **on the machine that compiled
// the binary**. In a container built elsewhere that directory does not exist,
// and OpenSQLiteDB then MkdirAll's it and creates an empty database there — so
// a deployment that forgets SYNC_DB_PATH starts with no tasks rather than
// failing loudly.
func TestTheDBPathFallbackIsABuildTimeSourcePath(t *testing.T) {
	t.Setenv("SYNC_DB_PATH", "")

	initDBPath()

	got := os.Getenv("SYNC_DB_PATH")
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Skip("runtime.Caller is unavailable")
	}
	want := filepath.Join(filepath.Dir(thisFile), "..", "..", "sync.db")

	if got != want {
		t.Fatalf("SYNC_DB_PATH = %q, want the build-time source path %q — the fallback appears to have changed; assert the new one instead", got, want)
	}
	if !filepath.IsAbs(got) {
		t.Errorf("the fallback %q is not absolute", got)
	}
}

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

func TestNewQueryCounterSuppliesADefaultLogger(t *testing.T) {
	if qc := NewQueryCounter(nil); qc.logger == nil {
		t.Error("NewQueryCounter(nil) left the logger nil")
	}

	given := logrus.New()
	if qc := NewQueryCounter(given); qc.logger != given {
		t.Error("NewQueryCounter replaced the supplied logger")
	}
}
func TestCheckSQLConnection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "probe.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if err := CheckSQLConnection(t.Context(), db); err != nil {
		t.Errorf("CheckSQLConnection on an open database = %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := CheckSQLConnection(t.Context(), db); err == nil {
		t.Error("CheckSQLConnection on a closed database = nil, want an error")
	}
}

func TestReopenSQLConnection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reopen.db")

	db, err := ReopenSQLConnection(t.Context(), quietLogger(), path, "sqlite3")
	if err != nil {
		t.Fatalf("ReopenSQLConnection: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.PingContext(t.Context()); err != nil {
		t.Errorf("the returned handle does not ping: %v", err)
	}
}

// ReopenSQLConnection routes every failure through Retry(5, 2s, 2.0) with no
// error classification, so a permanent misconfiguration — an unregistered
// driver, a malformed DSN — is retried five times over 62 seconds (2+4+8+16+32)
// before the error surfaces. This test only proves the classification is
// absent; the timing is covered by TestRetrySleepsAfterTheFinalFailure.
func TestReopenSQLConnectionDoesNotClassifyErrors(t *testing.T) {
	attempts := 0
	err := Retry(2, time.Millisecond, 1.0, func() error {
		attempts++
		_, openErr := sql.Open("no-such-driver", "whatever")
		return openErr
	})

	if err == nil {
		t.Fatal("an unregistered driver did not produce an error")
	}
	if attempts != 2 {
		t.Fatalf("a permanent error was attempted %d times, want 2 — Retry appears to classify errors now", attempts)
	}
}

// The existing formatFilterCondition table does not reach $gt, or the
// non-time branches of $gte and $lte. These pin them.
func TestFormatFilterConditionRemainingOperators(t *testing.T) {
	qc := NewQueryCounter(nil)

	tests := []struct {
		name  string
		value interface{}
		want  string
	}{
		{"$gt", bson.M{"$gt": 10}, "age: {$gt: 10}"},
		{"$gte with a plain value", bson.M{"$gte": 1}, "age: {$gte: 1}"},
		{"$lte with a plain value", bson.M{"$lte": 1}, "age: {$lte: 1}"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := qc.formatFilterCondition("age", tc.value); got != tc.want {
				t.Errorf("formatFilterCondition(age, %#v) = %q, want %q", tc.value, got, tc.want)
			}
		})
	}
}
