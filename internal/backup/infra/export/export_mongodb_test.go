package export

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestMaskSensitiveArgs(t *testing.T) {
	// The masker has to match the argument form the callers build, which is
	// "--uri" followed by the connection string as a separate argument.
	got := newExecutor().maskSensitiveArgs([]string{
		"mongoexport", "--uri", "mongodb://root:secret@db:27017/?authSource=admin",
		"--collection", "orders",
	})

	if strings.Contains(got, "secret") || strings.Contains(got, "root") {
		t.Errorf("masked command still contains credentials: %q", got)
	}
	if !strings.Contains(got, "mongodb://***:***@db:27017/?authSource=admin") {
		t.Errorf("masked command = %q, want the host part preserved", got)
	}
	for _, want := range []string{"mongoexport", "--collection orders"} {
		if !strings.Contains(got, want) {
			t.Errorf("masked command = %q, want it to contain %q", got, want)
		}
	}
}

func TestMaskSensitiveArgsLeavesOtherFormsAlone(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		// No credentials to hide.
		{"uri without credentials", []string{"--uri", "mongodb://db:27017/"}},
		// A username with no password has no colon in the credential part, so
		// the branch does not fire; only the username is exposed.
		{"username only", []string{"--uri", "mongodb://root@db:27017/"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newExecutor().maskSensitiveArgs(tt.args)
			if got != strings.Join(tt.args, " ") {
				t.Errorf("maskSensitiveArgs = %q, want it unchanged", got)
			}
		})
	}
}

func TestMaskSensitiveArgsLeavesInputUnchanged(t *testing.T) {
	args := []string{"--uri", "mongodb://root:secret@db:27017/"}

	newExecutor().maskSensitiveArgs(args)

	if args[1] != "mongodb://root:secret@db:27017/" {
		t.Errorf("the input slice was mutated: %v", args)
	}
}

func mongoBounds(t *testing.T, converted map[string]interface{}, field string) (time.Time, time.Time) {
	t.Helper()

	q, ok := converted[field].(map[string]interface{})
	if !ok {
		t.Fatalf("%s was not converted: %#v", field, converted[field])
	}
	read := func(op string) time.Time {
		bound, ok := q[op].(map[string]interface{})
		if !ok {
			t.Fatalf("%s has no %s object: %#v", field, op, q)
		}
		s, ok := bound["$date"].(string)
		if !ok {
			t.Fatalf("%s %s has no $date string: %#v", field, op, bound)
		}
		ts, err := time.Parse("2006-01-02T15:04:05.000Z", s)
		if err != nil {
			t.Fatalf("%s %s date %q does not parse: %v", field, op, s, err)
		}
		return ts
	}
	return read("$gte"), read("$lt")
}

func TestConvertTimeRangeQuery(t *testing.T) {
	got := newExecutor().convertTimeRangeQuery(
		map[string]interface{}{"created_at": dailyQuery(float64(-1), float64(0))})

	start, end := mongoBounds(t, got, "created_at")

	// JST midnight rendered in UTC is 15:00 on the preceding day.
	for name, ts := range map[string]time.Time{"$gte": start, "$lt": end} {
		if ts.Hour() != 15 || ts.Minute() != 0 || ts.Second() != 0 {
			t.Errorf("%s = %v, want 15:00:00", name, ts)
		}
	}
	if span := end.Sub(start); span != 24*time.Hour {
		t.Errorf("range spans %v, want 24h", span)
	}
}

// TestMongoAndMySQLTimeRangesAgree pins the other half of T-022. The MongoDB
// and MySQL query builders are separate copies of the same logic, and they do
// agree with each other — both use time.Date in JST and treat endOffset as
// exclusive. Only the table-selection window in extractTimeRange is the
// outlier, which is what makes it worth reconciling rather than the other two.
func TestMongoAndMySQLTimeRangesAgree(t *testing.T) {
	query := map[string]interface{}{"created_at": dailyQuery(float64(-3), float64(-1))}

	mongoStart, mongoEnd := mongoBounds(t, newExecutor().convertTimeRangeQuery(query), "created_at")
	mysqlStart, mysqlEnd := parseWhereBounds(t, newExecutor().convertTimeRangeQueryForMySQL(query))

	if !mongoStart.Equal(mysqlStart) || !mongoEnd.Equal(mysqlEnd) {
		t.Errorf("mongo window %v..%v differs from mysql window %v..%v",
			mongoStart, mongoEnd, mysqlStart, mysqlEnd)
	}
	if span := mongoEnd.Sub(mongoStart); span != 48*time.Hour {
		t.Errorf("range spans %v, want 48h for offsets -3..-1", span)
	}
}

func TestConvertTimeRangeQueryPassesThroughOtherEntries(t *testing.T) {
	input := map[string]interface{}{
		"status":     "active",
		"score":      float64(3),
		"weekly":     map[string]interface{}{"type": "weekly"},
		"plain":      map[string]interface{}{"$gt": float64(1)},
		"created_at": dailyQuery(float64(-1), float64(0)),
	}

	got := newExecutor().convertTimeRangeQuery(input)

	for _, key := range []string{"status", "score", "weekly", "plain"} {
		if !reflect.DeepEqual(got[key], input[key]) {
			t.Errorf("%s = %#v, want it passed through unchanged", key, got[key])
		}
	}
	if _, converted := got["created_at"].(map[string]interface{})["$gte"]; !converted {
		t.Error("created_at was not converted")
	}
}

// TestConvertTimeRangeQueryEqualOffsetsMatchNothing mirrors the MySQL variant:
// endOffset is exclusive, so 0..0 yields $gte and $lt at the same instant and
// the export writes an empty file without reporting anything.
func TestConvertTimeRangeQueryEqualOffsetsMatchNothing(t *testing.T) {
	got := newExecutor().convertTimeRangeQuery(
		map[string]interface{}{"created_at": dailyQuery(float64(0), float64(0))})

	start, end := mongoBounds(t, got, "created_at")

	if !start.Equal(end) {
		t.Errorf("bounds are %v..%v; equal offsets no longer collapse the range, "+
			"so assert the new behaviour instead", start, end)
	}
}

func TestCleanQueryStringValues(t *testing.T) {
	tests := []struct {
		name  string
		input map[string]interface{}
		want  map[string]interface{}
	}{
		{
			"double quotes are stripped",
			map[string]interface{}{"status": `"active"`},
			map[string]interface{}{"status": "active"},
		},
		{
			"single quotes are stripped",
			map[string]interface{}{"status": `'active'`},
			map[string]interface{}{"status": "active"},
		},
		{
			"both layers are stripped in one pass",
			map[string]interface{}{"status": `"'active'"`},
			map[string]interface{}{"status": "active"},
		},
		{
			"unquoted values are untouched",
			map[string]interface{}{"status": "active"},
			map[string]interface{}{"status": "active"},
		},
		{
			"non-strings are untouched",
			map[string]interface{}{"score": float64(1), "ok": true, "none": nil},
			map[string]interface{}{"score": float64(1), "ok": true, "none": nil},
		},
		{
			"nested objects are cleaned recursively",
			map[string]interface{}{"user": map[string]interface{}{"name": `"jack"`}},
			map[string]interface{}{"user": map[string]interface{}{"name": "jack"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cleanQueryStringValues(tt.input); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("cleanQueryStringValues = %#v, want %#v", got, tt.want)
			}
		})
	}
}

// TestCleanQueryStringValuesCorruptsLegitimateQuotes records a defect: the
// function only checks that a value starts and ends with a quote, without
// verifying that they are a matching pair. A value that legitimately opens and
// closes with quoted words loses its outer characters, and a value consisting
// of a single quote character is emptied entirely.
func TestCleanQueryStringValuesCorruptsLegitimateQuotes(t *testing.T) {
	tests := []struct {
		name     string
		in, want string
	}{
		{"quoted words at both ends", `"hello" and "world"`, `hello" and "world`},
		{"a lone double quote becomes empty", `"`, ""},
		{"a lone single quote becomes empty", `'`, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cleanQueryStringValues(map[string]interface{}{"v": tt.in})["v"]

			if got == tt.in {
				t.Fatalf("value %q is now left alone; the quote handling may have "+
					"been fixed, so assert that instead", tt.in)
			}
			if got != tt.want {
				t.Errorf("cleanQueryStringValues(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
