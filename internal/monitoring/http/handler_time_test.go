package monitoringhttp

import (
	"testing"

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
