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

// TestAnEightDigitDateIsStrippedWhole covers the pattern list's order. The
// six-digit form used to be tried first, so an eight-digit date with no
// underscore had only its last six digits removed and two stray digits stayed on
// the prefix: orders20260821 grouped as "orders20", which is a different group
// from every other day of that month.
func TestAnEightDigitDateIsStrippedWhole(t *testing.T) {
	tests := []struct{ table, want string }{
		{"orders20260821", "orders"},
		{"orders20260822", "orders"},
		{"20260821", ""},
	}

	for _, tt := range tests {
		t.Run(tt.table, func(t *testing.T) {
			if got := newExecutor().extractTablePrefix(tt.table); got != tt.want {
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

// TestATouchingIntervalDoesNotOverlap covers an off-by-one at the boundary. Both
// intervals are half-open — a monthly table ends on the first of the next month
// — but the overlap test used Before and After rather than their strict
// complements, so an interval ending exactly where the window starts counted as
// overlapping and a one-month backup exported three months of tables.
func TestATouchingIntervalDoesNotOverlap(t *testing.T) {
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
			if newExecutor().isTableRelevantForTimeRange(table, august) {
				t.Errorf("isTableRelevantForTimeRange(%q) = true for an interval that "+
					"only touches the window", table)
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

// TestNoMatchingTableSelectsNothing covers what a job did when its window
// excluded every table: it backed up tables[0] and reported success, so the
// archive held a table from outside the requested window and looked like a fresh
// backup of the right thing. Selecting nothing is now what happens, and Execute
// reports it.
func TestNoMatchingTableSelectsNothing(t *testing.T) {
	// A daily range around today cannot overlap tables from 2020.
	tables := []string{"orders_202001", "orders_202002"}
	conditions := map[string]map[string]interface{}{
		"orders": {"created_at": map[string]interface{}{
			"type":        "daily",
			"startOffset": float64(-1),
			"endOffset":   float64(0),
		}},
	}

	if got := newExecutor().filterRelevantTables(tables, conditions, "orders"); len(got) != 0 {
		t.Errorf("filterRelevantTables = %v, want nothing", got)
	}
}

// TestAnEmptyTableListIsNotAPanic covers the same path with nothing to choose
// from: the fallback indexed tables[0] without checking the slice.
func TestAnEmptyTableListIsNotAPanic(t *testing.T) {
	conditions := map[string]map[string]interface{}{
		"orders": {"created_at": map[string]interface{}{
			"type":        "daily",
			"startOffset": float64(-1),
			"endOffset":   float64(0),
		}},
	}

	if got := newExecutor().filterRelevantTables(nil, conditions, "orders"); len(got) != 0 {
		t.Errorf("filterRelevantTables = %v, want nothing", got)
	}
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
		// endOffset is exclusive, the same as it is for the row filters, so
		// -3..0 covers three days.
		if want := 3 * 24 * time.Hour; got.End.Sub(got.Start) != want {
			t.Errorf("range spans %v, want %v", got.End.Sub(got.Start), want)
		}
	})

	// The -1..0 default is yesterday and only yesterday. It used to add a day to
	// the end bound, so it also took in today — whose rows are still being
	// written — while the row filters resolved the same offsets to one day.
	t.Run("the default is yesterday alone", func(t *testing.T) {
		query := map[string]interface{}{"created_at": map[string]interface{}{"type": "daily"}}

		got := newExecutor().extractTimeRange(query)
		if got == nil {
			t.Fatal("extractTimeRange returned nil")
		}
		if want := 24 * time.Hour; got.End.Sub(got.Start) != want {
			t.Errorf("range spans %v, want %v for the -1..0 default", got.End.Sub(got.Start), want)
		}
	})

	t.Run("offsets need not be JSON numbers", func(t *testing.T) {
		// A string or a Go int used to fail the float64 assertion and be
		// replaced by the -1..0 default with nothing said.
		query := map[string]interface{}{"created_at": map[string]interface{}{
			"type":        "daily",
			"startOffset": "-3",
			"endOffset":   0,
		}}

		got := newExecutor().extractTimeRange(query)
		if got == nil {
			t.Fatal("extractTimeRange returned nil")
		}
		if want := 3 * 24 * time.Hour; got.End.Sub(got.Start) != want {
			t.Errorf("range spans %v, want %v", got.End.Sub(got.Start), want)
		}
	})

	// A window nothing can fall in is not a window. extractTimeRange cannot
	// report it, so it declines to filter and every table is considered; the
	// export itself refuses the same configuration outright.
	t.Run("an empty window filters nothing", func(t *testing.T) {
		query := map[string]interface{}{"created_at": map[string]interface{}{
			"type":        "daily",
			"startOffset": float64(0),
			"endOffset":   float64(0),
		}}

		if got := newExecutor().extractTimeRange(query); got != nil {
			t.Errorf("extractTimeRange = %v, want nil", got)
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

// TestTheWordingInAPatternSurvives covers the name an archive is stored under in
// GCS. The bare date placeholders used to be replaced as plain substrings, and
// "mm" and "dd" occur in ordinary English, so summary_YYYYMM.json was stored as
// su08ary_202608.json.
func TestTheWordingInAPatternSurvives(t *testing.T) {
	yesterday := time.Now().AddDate(0, 0, -1)

	got := processFileNamePattern("summary_YYYYMM.json", "orders")

	want := "orders_summary_" + yesterday.Format("200601") + ".json"
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
			"mongodb://root:root@localhost:27017/?authSource=admin&journal=true&w=majority",
		},
		{
			"without credentials", "localhost:27017", "", "",
			"mongodb://localhost:27017/?journal=true&w=majority",
		},
		{
			// A user with no password is kept now: dropping it made the
			// connection anonymous and the failure surfaced later as an
			// authentication error that did not mention the discarded user.
			"user without password", "localhost:27017", "root", "",
			"mongodb://root@localhost:27017/?authSource=admin&journal=true&w=majority",
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

// TestTheBackupNoLongerPinsOneNode is the counterpart to T-007 for the backup
// path. Pinning the driver to a single node meant a backup taken against a
// replica set read from whichever node it happened to reach, and failed
// outright once that node stopped serving.
func TestTheBackupNoLongerPinsOneNode(t *testing.T) {
	for _, conn := range []string{
		buildMongoDBConnectionString("localhost:27017", "root", "root"),
		buildMongoDBConnectionString("localhost:27017", "", ""),
		buildMongoDBConnectionString("a:27017,b:27017", "", ""),
	} {
		if regexp.MustCompile(`directConnection`).MatchString(conn) {
			t.Errorf("connection string %q still pins one node", conn)
		}
	}
}

// TestTheBackupAcceptsASeedList covers the shape a replica set is named with,
// which the hand-built string happened to pass through and which now goes
// through the shared builder.
func TestTheBackupAcceptsASeedList(t *testing.T) {
	got := buildMongoDBConnectionString("a:27017,b:27017,c:27017", "root", "root")

	if !regexp.MustCompile(`a:27017,b:27017,c:27017`).MatchString(got) {
		t.Errorf("connection string = %q, want the whole seed list", got)
	}
}
