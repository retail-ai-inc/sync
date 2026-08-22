package export

import (
	"regexp"
	"strings"
	"testing"
	"time"
)

// whereRE extracts the two timestamps from a generated time-range condition.
var whereRE = regexp.MustCompile(`>= '([^']+)' AND ` + "`?" + `\w+` + "`?" + ` < '([^']+)'`)

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

// mustClause builds a WHERE clause and fails the test if it cannot.
func mustClause(t *testing.T, query map[string]interface{}) string {
	t.Helper()

	where, err := newExecutor().convertTimeRangeQueryForMySQL(query)
	if err != nil {
		t.Fatalf("convertTimeRangeQueryForMySQL(%v): %v", query, err)
	}
	return where
}

func TestConvertTimeRangeQueryForMySQLDailyRange(t *testing.T) {
	where := mustClause(t, map[string]interface{}{"created_at": dailyQuery(float64(-1), float64(0))})

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

// TestTheRowWindowAndTheTableWindowAgree covers what used to be two
// implementations of the same window inside one backup job. Table selection
// added one to endOffset and truncated against UTC rather than JST — forty-eight
// hours, aligned to the wrong day for the nine hours each day when the UTC and
// JST dates differ, which is when the backup cron usually runs. The WHERE clause
// resolved the same offsets to twenty-four JST hours. Both now come from timex.
func TestTheRowWindowAndTheTableWindowAgree(t *testing.T) {
	query := map[string]interface{}{"created_at": dailyQuery(float64(-1), float64(0))}

	rowStart, rowEnd := parseWhereBounds(t, mustClause(t, query))

	tableWindow := newExecutor().extractTimeRange(query)
	if tableWindow == nil {
		t.Fatal("extractTimeRange returned nil")
	}

	if !rowStart.Equal(tableWindow.Start.UTC()) || !rowEnd.Equal(tableWindow.End.UTC()) {
		t.Errorf("rows are filtered on %v..%v but tables are selected on %v..%v",
			rowStart, rowEnd, tableWindow.Start.UTC(), tableWindow.End.UTC())
	}
	if got := rowEnd.Sub(rowStart); got != 24*time.Hour {
		t.Errorf("the window spans %v, want 24h", got)
	}
}

// TestAnEmptyWindowIsRefused covers the intuitive spelling of "just today".
// endOffset is exclusive, so 0..0 produced `col >= X AND col < X`, which no row
// can satisfy: the export succeeded, wrote an empty file, and reported a
// successful backup.
func TestAnEmptyWindowIsRefused(t *testing.T) {
	_, err := newExecutor().convertTimeRangeQueryForMySQL(
		map[string]interface{}{"created_at": dailyQuery(float64(0), float64(0))})

	if err == nil {
		t.Error("convertTimeRangeQueryForMySQL accepted a window no row can fall in")
	}
}

// TestOffsetsNeedNotBeJSONNumbers covers a configuration written by hand or
// through a client that quotes its numbers. Anything other than a JSON number
// used to fall back to the default -1..0 with no error and no log line, so a
// task asking for the last week quietly backed up yesterday.
func TestOffsetsNeedNotBeJSONNumbers(t *testing.T) {
	fromNumbers := mustClause(t, map[string]interface{}{
		"created_at": dailyQuery(float64(-7), float64(0))})
	fromStrings := mustClause(t, map[string]interface{}{
		"created_at": dailyQuery("-7", "0")})

	if fromNumbers != fromStrings {
		t.Errorf("quoted offsets produced %q, want the same as %q", fromStrings, fromNumbers)
	}
	start, end := parseWhereBounds(t, fromStrings)
	if got := end.Sub(start); got != 7*24*time.Hour {
		t.Errorf("the window spans %v, want 168h", got)
	}
}

func TestConvertTimeRangeQueryForMySQLEqualityConditions(t *testing.T) {
	tests := []struct {
		name  string
		query map[string]interface{}
		want  string
	}{
		{"string", map[string]interface{}{"status": "active"}, "`status` = 'active'"},
		{"float", map[string]interface{}{"score": float64(1.5)}, "`score` = 1.5"},
		{"int", map[string]interface{}{"count": 3}, "`count` = 3"},
		{"bool", map[string]interface{}{"active": true}, "`active` = true"},
		{"quote is doubled", map[string]interface{}{"name": "O'Brien"}, "`name` = 'O''Brien'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mustClause(t, tt.query); got != tt.want {
				t.Errorf("convertTimeRangeQueryForMySQL = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestAConditionThatCannotBeRenderedIsAnError covers the difference between a
// filtered export and a full-table one. An unrecognised condition used to be
// logged and dropped, which leaves the WHERE clause empty — so the archive was
// far larger than intended, held rows it was not meant to, and the job still
// reported success.
func TestAConditionThatCannotBeRenderedIsAnError(t *testing.T) {
	tests := []struct {
		name  string
		query map[string]interface{}
	}{
		{"non-daily range", map[string]interface{}{"created_at": map[string]interface{}{"type": "weekly"}}},
		{"capitalised daily", map[string]interface{}{"created_at": map[string]interface{}{"type": "Daily"}}},
		{"nil value", map[string]interface{}{"deleted_at": nil}},
		{"nested object", map[string]interface{}{"meta": map[string]interface{}{"a": 1}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := newExecutor().convertTimeRangeQueryForMySQL(tt.query); err == nil {
				t.Errorf("convertTimeRangeQueryForMySQL = %q, want a refusal", got)
			}
		})
	}
}

// TestTheClauseCannotBeUsedToInjectSQL covers the WHERE clause an operator's
// backup configuration ends up controlling. Column names were pasted into the
// SQL with no escaping and no validation, and values were escaped only by
// doubling the single quote — which MySQL's default backslash handling defeats.
// The clause is then handed to the mysql client to execute.
func TestTheClauseCannotBeUsedToInjectSQL(t *testing.T) {
	t.Run("a column name that is not one is refused", func(t *testing.T) {
		for _, name := range []string{"1=1 OR id", "id`", "id; DROP TABLE orders", "", "id name"} {
			if got, err := newExecutor().convertTimeRangeQueryForMySQL(
				map[string]interface{}{name: "x"}); err == nil {
				t.Errorf("the column name %q was accepted: %q", name, got)
			}
		}
	})

	t.Run("a backslash cannot escape the closing quote", func(t *testing.T) {
		got := mustClause(t, map[string]interface{}{"name": `back\slash`})

		if strings.Contains(got, `back\slash'`) {
			t.Errorf("clause = %q; the backslash still reaches the statement intact", got)
		}
		if !strings.Contains(got, `back\\slash`) {
			t.Errorf("clause = %q, want the backslash doubled", got)
		}
	})

	t.Run("a value ending in a backslash cannot open the statement", func(t *testing.T) {
		got := mustClause(t, map[string]interface{}{"name": `x\`})

		// Unescaped this would read as ... = 'x\' AND ..., with the closing
		// quote consumed and the rest of the clause inside the literal.
		if !strings.HasSuffix(got, `= 'x\\'`) {
			t.Errorf("clause = %q, want the trailing backslash doubled", got)
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

	mustQuery := func(t *testing.T, table string, c ExecutorBackupConfig) string {
		t.Helper()
		q, err := newExecutor().buildMySQLSelectQuery(table, c)
		if err != nil {
			t.Fatalf("buildMySQLSelectQuery: %v", err)
		}
		return q
	}

	t.Run("all columns by default", func(t *testing.T) {
		got := mustQuery(t, "orders", base())
		if want := "SELECT * FROM orders"; got != want {
			t.Errorf("query = %q, want %q", got, want)
		}
	})

	t.Run("explicit field list", func(t *testing.T) {
		c := base()
		c.Database.Fields["orders"] = []string{"id", "created_at"}
		got := mustQuery(t, "orders", c)
		if want := "SELECT id, created_at FROM orders"; got != want {
			t.Errorf("query = %q, want %q", got, want)
		}
	})

	t.Run("the all sentinel means every column", func(t *testing.T) {
		c := base()
		c.Database.Fields["orders"] = []string{"all"}
		got := mustQuery(t, "orders", c)
		if want := "SELECT * FROM orders"; got != want {
			t.Errorf("query = %q, want %q", got, want)
		}
	})

	t.Run("query conditions become a WHERE clause", func(t *testing.T) {
		c := base()
		c.Query["orders"] = map[string]interface{}{"status": "active"}
		got := mustQuery(t, "orders", c)
		if want := "SELECT * FROM orders WHERE `status` = 'active'"; got != want {
			t.Errorf("query = %q, want %q", got, want)
		}
	})

	t.Run("a condition that cannot be rendered stops the export", func(t *testing.T) {
		// Dropping it would turn a filtered export into a full-table one, which
		// is a much larger archive holding rows the job was not asked for.
		c := base()
		c.Query["orders"] = map[string]interface{}{"created_at": map[string]interface{}{"type": "weekly"}}
		if got, err := newExecutor().buildMySQLSelectQuery("orders", c); err == nil {
			t.Errorf("query = %q, want a refusal", got)
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
