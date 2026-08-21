package export

import (
	"reflect"
	"regexp"
	"testing"
	"time"
)

// newExecutor returns an executor usable for the pure helpers, which never touch the
// database handle.
func newExecutor() *BackupExecutor { return &BackupExecutor{} }

func TestExtractTablePrefix(t *testing.T) {
	tests := []struct {
		name  string
		table string
		want  string
	}{
		{"monthly with underscore", "orders_202608", "orders"},
		{"daily with underscore", "orders_20260821", "orders"},
		{"yearly with underscore", "orders_2026", "orders"},
		{"monthly without underscore", "orders202608", "orders"},
		{"numeric suffix", "users1", "users"},
		{"no suffix", "users", "users"},
		{"underscore before number keeps it", "table_1", "table_"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newExecutor().extractTablePrefix(tt.table); got != tt.want {
				t.Errorf("extractTablePrefix(%q) = %q, want %q", tt.table, got, tt.want)
			}
		})
	}
}

// TestExtractTablePrefixPatternOrderLeavesStrayDigits records a defect in the
// pattern list: `\d{6}$` is tried before `\d{8}$`, so an eight-digit date with
// no underscore separator has only its last six digits stripped, leaving two
// stray digits on the prefix.
func TestExtractTablePrefixPatternOrderLeavesStrayDigits(t *testing.T) {
	tests := []struct{ table, want string }{
		{"orders20260821", "orders20"}, // want "orders"
		{"20260821", "20"},             // an all-numeric name loses all but two digits
	}

	for _, tt := range tests {
		t.Run(tt.table, func(t *testing.T) {
			got := newExecutor().extractTablePrefix(tt.table)

			if got == "orders" || got == "" {
				t.Fatalf("extractTablePrefix(%q) = %q; the pattern order may have been "+
					"fixed, so assert the correct prefix instead", tt.table, got)
			}
			if got != tt.want {
				t.Errorf("extractTablePrefix(%q) = %q, want %q", tt.table, got, tt.want)
			}
		})
	}
}

func TestGroupTablesByPrefix(t *testing.T) {
	got := newExecutor().groupTablesByPrefix([]string{
		"orders_202606", "orders_202607", "orders_202608",
		"users", "users1", "users2",
		"events_20260821",
	})

	want := map[string][]string{
		"orders": {"orders_202606", "orders_202607", "orders_202608"},
		"users":  {"users", "users1", "users2"},
		"events": {"events_20260821"},
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("groupTablesByPrefix =\n  %v\nwant\n  %v", got, want)
	}
}

func TestGroupTablesByPrefixEmpty(t *testing.T) {
	if got := newExecutor().groupTablesByPrefix(nil); len(got) != 0 {
		t.Errorf("groupTablesByPrefix(nil) = %v, want an empty map", got)
	}
}

func TestParseYearMonth(t *testing.T) {
	year, month, err := parseYearMonth("202608")
	if err != nil {
		t.Fatalf("parseYearMonth returned %v", err)
	}
	if year != 2026 || month != time.August {
		t.Errorf("parseYearMonth(\"202608\") = %d/%v, want 2026/August", year, month)
	}

	for _, bad := range []string{"", "2026", "2026081", "20260a", "abcdef", "202600", "202613"} {
		if _, _, err := parseYearMonth(bad); err == nil {
			t.Errorf("parseYearMonth(%q) succeeded, want an error", bad)
		}
	}
}

func TestParseYear(t *testing.T) {
	year, err := parseYear("2026")
	if err != nil {
		t.Fatalf("parseYear returned %v", err)
	}
	if year != 2026 {
		t.Errorf("parseYear(\"2026\") = %d, want 2026", year)
	}

	for _, bad := range []string{"", "202", "20266", "20a6"} {
		if _, err := parseYear(bad); err == nil {
			t.Errorf("parseYear(%q) succeeded, want an error", bad)
		}
	}
}

func TestExtractTableTimePattern(t *testing.T) {
	tests := []struct {
		name  string
		table string
		start time.Time
		end   time.Time
	}{
		{
			"monthly", "orders_202608",
			time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			"daily", "orders_20260821",
			time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC),
		},
		{
			"yearly", "orders_2026",
			time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newExecutor().extractTableTimePattern(tt.table)
			if got == nil {
				t.Fatalf("extractTableTimePattern(%q) = nil", tt.table)
			}
			if !got.Start.Equal(tt.start) || !got.End.Equal(tt.end) {
				t.Errorf("extractTableTimePattern(%q) = %v..%v, want %v..%v",
					tt.table, got.Start, got.End, tt.start, tt.end)
			}
		})
	}

	// Names carrying no date pattern yield nil, which callers treat as
	// "include the table to be safe".
	for _, table := range []string{"users", "orders_bak", "orders_2026081"} {
		if got := newExecutor().extractTableTimePattern(table); got != nil {
			t.Errorf("extractTableTimePattern(%q) = %v, want nil", table, got)
		}
	}
}

func TestIsTableRelevantForTimeRange(t *testing.T) {
	august := &TimeRange{
		Start: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		End:   time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
	}

	tests := []struct {
		table string
		want  bool
	}{
		{"orders_202608", true},   // exactly the range
		{"orders_20260815", true}, // a day inside it
		{"orders_2026", true},     // the year overlaps
		{"orders_202606", false},  // two months before: genuinely outside
		{"orders_202610", false},  // two months after: genuinely outside
		{"users", true},           // no pattern, included to be safe
	}

	for _, tt := range tests {
		t.Run(tt.table, func(t *testing.T) {
			if got := newExecutor().isTableRelevantForTimeRange(tt.table, august); got != tt.want {
				t.Errorf("isTableRelevantForTimeRange(%q) = %v, want %v", tt.table, got, tt.want)
			}
		})
	}
}

// TestIsTableRelevantForTimeRangeIncludesTouchingIntervals records an
// off-by-one. Both TimeRange values are half-open — extractTableTimePattern
// builds End with AddDate, so a monthly table ends on the first of the next
// month — but the overlap test uses Before and After rather than their strict
// complements. An interval whose End equals the range Start therefore counts as
// overlapping, so the months either side of the window are always pulled in: a
// one-month backup exports three months of tables.
func TestIsTableRelevantForTimeRangeIncludesTouchingIntervals(t *testing.T) {
	august := &TimeRange{
		Start: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		End:   time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
	}

	for _, table := range []string{
		"orders_202607",   // ends exactly at the range start
		"orders_20260731", // ditto, one day granularity
		"orders_202609",   // starts exactly at the range end
	} {
		t.Run(table, func(t *testing.T) {
			if !newExecutor().isTableRelevantForTimeRange(table, august) {
				t.Errorf("isTableRelevantForTimeRange(%q) = false; the boundary "+
					"comparison may have been tightened, so assert that instead", table)
			}
		})
	}
}

func TestFilterRelevantTables(t *testing.T) {
	tables := []string{"orders_202607", "orders_202608", "orders_202609"}

	t.Run("no conditions returns everything", func(t *testing.T) {
		got := newExecutor().filterRelevantTables(tables, nil, "orders")
		if !reflect.DeepEqual(got, tables) {
			t.Errorf("got %v, want %v", got, tables)
		}
	})

	t.Run("conditions without a time range return everything", func(t *testing.T) {
		conditions := map[string]map[string]interface{}{
			"orders": {"status": "active"},
		}
		got := newExecutor().filterRelevantTables(tables, conditions, "orders")
		if !reflect.DeepEqual(got, tables) {
			t.Errorf("got %v, want %v", got, tables)
		}
	})
}

// TestFilterRelevantTablesFallsBackToFirstTable records a defect: when a time
// range excludes every table, the function backs up `tables[0]` instead of
// reporting that nothing matched. The job then succeeds while archiving a table
// outside the requested window, which looks like a successful backup of the
// wrong data.
func TestFilterRelevantTablesFallsBackToFirstTable(t *testing.T) {
	// A daily range around today cannot overlap tables from 2020.
	tables := []string{"orders_202001", "orders_202002"}
	conditions := map[string]map[string]interface{}{
		"orders": {"created_at": map[string]interface{}{
			"type":        "daily",
			"startOffset": float64(-1),
			"endOffset":   float64(0),
		}},
	}

	got := newExecutor().filterRelevantTables(tables, conditions, "orders")

	if len(got) != 1 || got[0] != "orders_202001" {
		t.Errorf("filterRelevantTables = %v; the fallback may have been replaced "+
			"with an error or an empty result, so assert that instead", got)
	}
}

// TestFilterRelevantTablesPanicsOnEmptyInput records that the same fallback
// indexes tables[0] without checking the slice, so an empty table list with a
// time-range condition panics instead of returning nothing.
func TestFilterRelevantTablesPanicsOnEmptyInput(t *testing.T) {
	conditions := map[string]map[string]interface{}{
		"orders": {"created_at": map[string]interface{}{
			"type":        "daily",
			"startOffset": float64(-1),
			"endOffset":   float64(0),
		}},
	}

	defer func() {
		if recover() == nil {
			t.Error("filterRelevantTables with no tables no longer panics; a guard " +
				"may have been added, so assert the returned value instead")
		}
	}()

	newExecutor().filterRelevantTables(nil, conditions, "orders")
}

func TestExtractTimeRange(t *testing.T) {
	t.Run("nil without a daily entry", func(t *testing.T) {
		for _, query := range []map[string]interface{}{
			nil,
			{"status": "active"},
			{"created_at": map[string]interface{}{"type": "weekly"}},
			{"created_at": map[string]interface{}{"$gt": 1}},
		} {
			if got := newExecutor().extractTimeRange(query); got != nil {
				t.Errorf("extractTimeRange(%v) = %v, want nil", query, got)
			}
		}
	})

	t.Run("spans one day per offset step", func(t *testing.T) {
		query := map[string]interface{}{"created_at": map[string]interface{}{
			"type":        "daily",
			"startOffset": float64(-3),
			"endOffset":   float64(0),
		}}

		got := newExecutor().extractTimeRange(query)
		if got == nil {
			t.Fatal("extractTimeRange returned nil")
		}
		// The end bound is endOffset+1, so -3..0 covers four days.
		if want := 4 * 24 * time.Hour; got.End.Sub(got.Start) != want {
			t.Errorf("range spans %v, want %v", got.End.Sub(got.Start), want)
		}
	})

	// The -1..0 default spans two days, not one: the end bound is computed as
	// endOffset+1, so "yesterday" also takes in today, whose data is still
	// being written. utils.convertToMongoDBTimeRange, the other implementation
	// of the same configuration, omits the +1 and spans a single day — the same
	// task description therefore means different windows depending on which
	// code path handles it.
	t.Run("default offsets span two days", func(t *testing.T) {
		query := map[string]interface{}{"created_at": map[string]interface{}{"type": "daily"}}

		got := newExecutor().extractTimeRange(query)
		if got == nil {
			t.Fatal("extractTimeRange returned nil")
		}
		if want := 48 * time.Hour; got.End.Sub(got.Start) != want {
			t.Errorf("range spans %v, want %v for the -1..0 default", got.End.Sub(got.Start), want)
		}
	})

	t.Run("offsets must be JSON numbers", func(t *testing.T) {
		// Strings and Go ints fail the float64 assertion and are silently
		// replaced by the -1..0 default rather than reported.
		query := map[string]interface{}{"created_at": map[string]interface{}{
			"type":        "daily",
			"startOffset": "-3",
			"endOffset":   0,
		}}

		got := newExecutor().extractTimeRange(query)
		if got == nil {
			t.Fatal("extractTimeRange returned nil")
		}
		if want := 48 * time.Hour; got.End.Sub(got.Start) != want {
			t.Errorf("range spans %v; non-float offsets are no longer silently "+
				"replaced by the default, so assert the new behaviour instead",
				got.End.Sub(got.Start))
		}
	})
}

func TestProcessFileNamePattern(t *testing.T) {
	yesterday := time.Now().AddDate(0, 0, -1)

	t.Run("empty pattern uses table and yesterday", func(t *testing.T) {
		want := "orders_" + yesterday.Format("2006-01-02")
		if got := processFileNamePattern("", "orders"); got != want {
			t.Errorf("processFileNamePattern = %q, want %q", got, want)
		}
	})

	t.Run("table placeholder is substituted", func(t *testing.T) {
		got := processFileNamePattern("{table}_{YYYY}{MM}{DD}.json", "orders")
		want := "orders_" + yesterday.Format("20060102") + ".json"
		if got != want {
			t.Errorf("processFileNamePattern = %q, want %q", got, want)
		}
	})

	t.Run("uppercase table placeholder", func(t *testing.T) {
		if got := processFileNamePattern("{TABLE}.json", "orders"); got != "ORDERS.json" {
			t.Errorf("processFileNamePattern = %q, want %q", got, "ORDERS.json")
		}
	})

	t.Run("table name is prepended before the extension when absent", func(t *testing.T) {
		got := processFileNamePattern("dump.json", "orders")
		if got != "orders_dump.json" {
			t.Errorf("processFileNamePattern = %q, want %q", got, "orders_dump.json")
		}
	})

	t.Run("regex anchors are stripped", func(t *testing.T) {
		got := processFileNamePattern("^{table}$", "orders")
		if got != "orders" {
			t.Errorf("processFileNamePattern = %q, want %q", got, "orders")
		}
	})
}

// TestProcessFileNamePatternInheritsPlaceholderCorruption shows T-017 reaching
// the backup file name: descriptive wording in the pattern is rewritten with
// digits before the table name is attached.
func TestProcessFileNamePatternInheritsPlaceholderCorruption(t *testing.T) {
	yesterday := time.Now().AddDate(0, 0, -1)

	got := processFileNamePattern("summary_YYYYMM.json", "orders")

	want := "orders_su" + yesterday.Format("01") + "ary_" + yesterday.Format("200601") + ".json"
	if got != want {
		t.Errorf("processFileNamePattern = %q, want %q", got, want)
	}
}

func TestBuildMongoDBConnectionString(t *testing.T) {
	tests := []struct {
		name            string
		url, user, pass string
		want            string
	}{
		{
			"with credentials", "localhost:27017", "root", "root",
			"mongodb://root:root@localhost:27017/?authSource=admin&directConnection=true",
		},
		{
			"without credentials", "localhost:27017", "", "",
			"mongodb://localhost:27017/?directConnection=true",
		},
		{
			// Same asymmetry as the syncer DSN builder: a user without a
			// password is dropped and the connection becomes anonymous.
			"user without password", "localhost:27017", "root", "",
			"mongodb://localhost:27017/?directConnection=true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := buildMongoDBConnectionString(tt.url, tt.user, tt.pass); got != tt.want {
				t.Errorf("buildMongoDBConnectionString =\n  %q\nwant\n  %q", got, tt.want)
			}
		})
	}
}

// TestBuildMongoDBConnectionStringForcesDirectConnection mirrors T-007 for the
// backup path: every connection string pins the driver to a single node, so a
// backup taken against a replica set stops working after an election.
func TestBuildMongoDBConnectionStringForcesDirectConnection(t *testing.T) {
	for _, conn := range []string{
		buildMongoDBConnectionString("localhost:27017", "root", "root"),
		buildMongoDBConnectionString("localhost:27017", "", ""),
	} {
		if !regexp.MustCompile(`directConnection=true`).MatchString(conn) {
			t.Errorf("connection string %q no longer forces directConnection; "+
				"assert the new behaviour instead", conn)
		}
	}
}
