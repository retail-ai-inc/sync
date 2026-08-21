package backup

import (
	"regexp"
	"strings"
	"testing"
	"time"
)

// whereRE extracts the two timestamps from a generated time-range condition.
var whereRE = regexp.MustCompile(`>= '([^']+)' AND \w+ < '([^']+)'`)

func parseWhereBounds(t *testing.T, where string) (time.Time, time.Time) {
	t.Helper()

	m := whereRE.FindStringSubmatch(where)
	if m == nil {
		t.Fatalf("condition %q does not look like a time range", where)
	}
	start, err := time.Parse("2006-01-02 15:04:05", m[1])
	if err != nil {
		t.Fatalf("start %q does not parse: %v", m[1], err)
	}
	end, err := time.Parse("2006-01-02 15:04:05", m[2])
	if err != nil {
		t.Fatalf("end %q does not parse: %v", m[2], err)
	}
	return start, end
}

func dailyQuery(start, end interface{}) map[string]interface{} {
	q := map[string]interface{}{"type": "daily"}
	if start != nil {
		q["startOffset"] = start
	}
	if end != nil {
		q["endOffset"] = end
	}
	return q
}

func TestConvertTimeRangeQueryForMySQLDailyRange(t *testing.T) {
	where := newExecutor().convertTimeRangeQueryForMySQL(
		map[string]interface{}{"created_at": dailyQuery(float64(-1), float64(0))})

	start, end := parseWhereBounds(t, where)

	// Both bounds are JST midnight expressed in UTC, i.e. 15:00 the day before.
	for name, ts := range map[string]time.Time{"start": start, "end": end} {
		if ts.Hour() != 15 || ts.Minute() != 0 || ts.Second() != 0 {
			t.Errorf("%s = %v, want 15:00:00 (JST midnight in UTC)", name, ts)
		}
	}
	// endOffset is exclusive here, so -1..0 covers exactly one day.
	if got := end.Sub(start); got != 24*time.Hour {
		t.Errorf("range spans %v, want 24h", got)
	}
}

// TestConvertTimeRangeQueryForMySQLDisagreesWithTableSelection pins the
// divergence recorded as T-022. Two implementations run inside a single backup
// job and compute different windows from the same offsets:
//
//   - extractTimeRange, which decides *which tables* to export, adds one to
//     endOffset and truncates against UTC rather than JST
//   - convertTimeRangeQueryForMySQL, which builds the WHERE clause deciding
//     *which rows* to export, uses time.Date in JST and treats endOffset as
//     exclusive
//
// The selection window is the wider of the two, so it is over-inclusive rather
// than lossy, but the two can never be reasoned about together.
func TestConvertTimeRangeQueryForMySQLDisagreesWithTableSelection(t *testing.T) {
	query := map[string]interface{}{"created_at": dailyQuery(float64(-1), float64(0))}

	rowWindow := newExecutor().convertTimeRangeQueryForMySQL(query)
	rowStart, rowEnd := parseWhereBounds(t, rowWindow)

	tableWindow := newExecutor().extractTimeRange(query)
	if tableWindow == nil {
		t.Fatal("extractTimeRange returned nil")
	}

	rowSpan := rowEnd.Sub(rowStart)
	tableSpan := tableWindow.End.Sub(tableWindow.Start)

	if rowSpan == tableSpan {
		t.Fatalf("both implementations now span %v; if they were reconciled, "+
			"replace this test with one asserting the agreed window", rowSpan)
	}
	if rowSpan != 24*time.Hour || tableSpan != 48*time.Hour {
		t.Errorf("row window spans %v and table window spans %v, want 24h and 48h",
			rowSpan, tableSpan)
	}
}

// TestConvertTimeRangeQueryForMySQLEqualOffsetsMatchNothing records a defect:
// endOffset is exclusive, so the intuitive "just today" spelling of 0..0
// produces `col >= X AND col < X`, which no row can satisfy. The export
// succeeds and writes an empty file.
func TestConvertTimeRangeQueryForMySQLEqualOffsetsMatchNothing(t *testing.T) {
	where := newExecutor().convertTimeRangeQueryForMySQL(
		map[string]interface{}{"created_at": dailyQuery(float64(0), float64(0))})

	start, end := parseWhereBounds(t, where)

	if !start.Equal(end) {
		t.Errorf("bounds are %v..%v; equal offsets no longer collapse the range, "+
			"so assert the new behaviour instead", start, end)
	}
}

func TestConvertTimeRangeQueryForMySQLEqualityConditions(t *testing.T) {
	tests := []struct {
		name  string
		query map[string]interface{}
		want  string
	}{
		{"string", map[string]interface{}{"status": "active"}, "status = 'active'"},
		{"float", map[string]interface{}{"score": float64(1.5)}, "score = 1.5"},
		{"int", map[string]interface{}{"count": 3}, "count = 3"},
		{"quote is doubled", map[string]interface{}{"name": "O'Brien"}, "name = 'O''Brien'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newExecutor().convertTimeRangeQueryForMySQL(tt.query); got != tt.want {
				t.Errorf("convertTimeRangeQueryForMySQL = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestConvertTimeRangeQueryForMySQLDropsUnsupportedInput(t *testing.T) {
	tests := []struct {
		name  string
		query map[string]interface{}
	}{
		// A non-daily range object is logged and dropped, so the condition
		// silently disappears and the export covers the whole table.
		{"non-daily range", map[string]interface{}{"created_at": map[string]interface{}{"type": "weekly"}}},
		{"capitalised daily", map[string]interface{}{"created_at": map[string]interface{}{"type": "Daily"}}},
		// Value types outside string/float64/int are dropped the same way.
		{"bool value", map[string]interface{}{"active": true}},
		{"nil value", map[string]interface{}{"deleted_at": nil}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newExecutor().convertTimeRangeQueryForMySQL(tt.query); got != "" {
				t.Errorf("convertTimeRangeQueryForMySQL = %q, want an empty clause; "+
					"unsupported input is no longer dropped, so assert that instead", got)
			}
		})
	}
}

// TestConvertTimeRangeQueryForMySQLInterpolatesKeysVerbatim records an
// injection path. Column names are pasted into the SQL with no escaping or
// validation, and values are escaped only by doubling single quotes — which
// MySQL's default backslash handling defeats. The query map comes from a backup
// task's config_json, and POST /api/backup requires no authentication, so
// anyone who can reach the API can place arbitrary SQL into a WHERE clause that
// the mysql CLI then executes.
func TestConvertTimeRangeQueryForMySQLInterpolatesKeysVerbatim(t *testing.T) {
	t.Run("column name is not escaped", func(t *testing.T) {
		got := newExecutor().convertTimeRangeQueryForMySQL(
			map[string]interface{}{"1=1 OR id": "x"})

		if !strings.Contains(got, "1=1 OR id = 'x'") {
			t.Errorf("clause = %q; column names may now be validated or quoted, "+
				"so assert that instead", got)
		}
	})

	t.Run("backslash is left intact", func(t *testing.T) {
		got := newExecutor().convertTimeRangeQueryForMySQL(
			map[string]interface{}{"name": `back\slash`})

		if !strings.Contains(got, `back\slash`) {
			t.Errorf("clause = %q; backslashes may now be escaped, so assert that instead", got)
		}
	})
}

func TestBuildMySQLSelectQuery(t *testing.T) {
	base := func() ExecutorBackupConfig {
		var c ExecutorBackupConfig
		c.Database.Fields = map[string][]string{}
		c.Query = map[string]map[string]interface{}{}
		return c
	}

	t.Run("all columns by default", func(t *testing.T) {
		got := newExecutor().buildMySQLSelectQuery("orders", base())
		if want := "SELECT * FROM orders"; got != want {
			t.Errorf("query = %q, want %q", got, want)
		}
	})

	t.Run("explicit field list", func(t *testing.T) {
		c := base()
		c.Database.Fields["orders"] = []string{"id", "created_at"}
		got := newExecutor().buildMySQLSelectQuery("orders", c)
		if want := "SELECT id, created_at FROM orders"; got != want {
			t.Errorf("query = %q, want %q", got, want)
		}
	})

	t.Run("the all sentinel means every column", func(t *testing.T) {
		c := base()
		c.Database.Fields["orders"] = []string{"all"}
		got := newExecutor().buildMySQLSelectQuery("orders", c)
		if want := "SELECT * FROM orders"; got != want {
			t.Errorf("query = %q, want %q", got, want)
		}
	})

	t.Run("query conditions become a WHERE clause", func(t *testing.T) {
		c := base()
		c.Query["orders"] = map[string]interface{}{"status": "active"}
		got := newExecutor().buildMySQLSelectQuery("orders", c)
		if want := "SELECT * FROM orders WHERE status = 'active'"; got != want {
			t.Errorf("query = %q, want %q", got, want)
		}
	})

	t.Run("a dropped condition leaves no WHERE clause", func(t *testing.T) {
		// Combined with the dropping behaviour above, an unsupported condition
		// turns a filtered export into a full-table export.
		c := base()
		c.Query["orders"] = map[string]interface{}{"active": true}
		got := newExecutor().buildMySQLSelectQuery("orders", c)
		if want := "SELECT * FROM orders"; got != want {
			t.Errorf("query = %q, want %q", got, want)
		}
	})
}

func TestParseMySQLConnectionURL(t *testing.T) {
	tests := []struct {
		name       string
		url        string
		host, port string
	}{
		{"host and port", "db.example.com:3307", "db.example.com", "3307"},
		{"host only", "db.example.com", "db.example.com", "3306"},
		{"empty falls back to defaults", "", "localhost", "3306"},
		{"empty port keeps the default", "db.example.com:", "db.example.com", "3306"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, port, user, pass := parseMySQLConnectionURL(tt.url)
			if host != tt.host || port != tt.port {
				t.Errorf("parseMySQLConnectionURL(%q) = %q/%q, want %q/%q",
					tt.url, host, port, tt.host, tt.port)
			}
			// The signature promises credentials but the body always returns
			// empty strings for them.
			if user != "" || pass != "" {
				t.Errorf("credentials = %q/%q; the function now parses them, "+
					"so assert the parsed values instead", user, pass)
			}
		})
	}
}

// TestParseMySQLConnectionURLMisreadsFullDSN records that the parser assumes a
// bare host:port and splits on every colon, so a full DSN silently becomes a
// nonsense host and port instead of being rejected.
func TestParseMySQLConnectionURLMisreadsFullDSN(t *testing.T) {
	host, port, _, _ := parseMySQLConnectionURL("root:secret@tcp(db:3306)/orders")

	if host == "db" {
		t.Fatalf("the parser now understands full DSNs; assert the parsed host instead")
	}
	if host != "root" || port != "secret@tcp(db" {
		t.Errorf("host/port = %q/%q, want %q/%q", host, port, "root", "secret@tcp(db")
	}
}

func TestBuildMySQLConnectionString(t *testing.T) {
	host, port, user, pass := buildMySQLConnectionString("db.example.com:3307", "root", "secret")

	if host != "db.example.com" || port != "3307" {
		t.Errorf("host/port = %q/%q", host, port)
	}
	// Credentials are passed through, not parsed out of the URL.
	if user != "root" || pass != "secret" {
		t.Errorf("credentials = %q/%q, want root/secret", user, pass)
	}
}

func TestIsCompressionDisabled(t *testing.T) {
	for _, in := range []string{"none", "NONE", "None", "  none  "} {
		if !isCompressionDisabled(in) {
			t.Errorf("isCompressionDisabled(%q) = false, want true", in)
		}
	}
	for _, in := range []string{"", "gzip", "zip", "no", "none.", "nonetheless"} {
		if isCompressionDisabled(in) {
			t.Errorf("isCompressionDisabled(%q) = true, want false", in)
		}
	}
}

func TestMaskMySQLPassword(t *testing.T) {
	// The masker has to match the argument form the callers actually build,
	// which is "-p" concatenated with the password.
	got := newExecutor().maskMySQLPassword([]string{
		"mysqldump", "-h", "db", "-P", "3306", "-u", "root", "-psecret", "orders",
	})

	if strings.Contains(got, "secret") {
		t.Errorf("masked command still contains the password: %q", got)
	}
	if !strings.Contains(got, "-p***") {
		t.Errorf("masked command = %q, want it to contain -p***", got)
	}
	// Non-password arguments survive untouched, including the uppercase port flag.
	for _, want := range []string{"mysqldump", "-h db", "-P 3306", "-u root", "orders"} {
		if !strings.Contains(got, want) {
			t.Errorf("masked command = %q, want it to contain %q", got, want)
		}
	}
}

func TestMaskMySQLPasswordLeavesInputUnchanged(t *testing.T) {
	args := []string{"mysqldump", "-psecret"}

	newExecutor().maskMySQLPassword(args)

	if args[1] != "-psecret" {
		t.Errorf("the input slice was mutated: %v", args)
	}
}
