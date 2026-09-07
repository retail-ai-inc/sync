package timex

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ReplaceDatePlaceholdersWithDate replaces date placeholders in a pattern with
// the specified date. The braced forms — {YYYY}, {MM}, {DD} and their lower-
// case spellings — are unambiguous.
func ReplaceDatePlaceholdersWithDate(pattern string, targetDate time.Time) string {
	result := pattern

	result = strings.ReplaceAll(result, "{YYYY}", targetDate.Format("2006"))
	result = strings.ReplaceAll(result, "{MM}", targetDate.Format("01"))
	result = strings.ReplaceAll(result, "{DD}", targetDate.Format("02"))
	result = strings.ReplaceAll(result, "{yyyy}", targetDate.Format("2006"))
	result = strings.ReplaceAll(result, "{mm}", targetDate.Format("01"))
	result = strings.ReplaceAll(result, "{dd}", targetDate.Format("02"))

	return replaceBareDateWords(result, targetDate)
}

// letterRun matches a maximal run of ASCII letters, which is the unit the bare
// replacement works on: a word is either entirely a date or it is a word.
var letterRun = regexp.MustCompile(`[A-Za-z]+`)

// replaceBareDateWords substitutes runs of letters that are made of nothing but
// date tokens. "YYYYMMDD" is a date and becomes one; "summary" is a word and is
// left as it is.
func replaceBareDateWords(pattern string, targetDate time.Time) string {
	return letterRun.ReplaceAllStringFunc(pattern, func(word string) string {
		var built strings.Builder
		for rest := word; rest != ""; {
			switch {
			case strings.HasPrefix(rest, "YYYY"), strings.HasPrefix(rest, "yyyy"):
				built.WriteString(targetDate.Format("2006"))
				rest = rest[4:]
			case strings.HasPrefix(rest, "MM"), strings.HasPrefix(rest, "mm"):
				built.WriteString(targetDate.Format("01"))
				rest = rest[2:]
			case strings.HasPrefix(rest, "DD"), strings.HasPrefix(rest, "dd"):
				built.WriteString(targetDate.Format("02"))
				rest = rest[2:]
			default:
				// Not a date all the way through, so it is a word.
				return word
			}
		}
		return built.String()
	})
}

// ParseDatabaseTimestamp reads a time out of the control database.
//
// Two formats are in there. The Go code writes "2006-01-02 15:04:05" in UTC
// and always has; rows written by an earlier version carry RFC 3339, and a
// reader that knew only the first called them "not a timestamp" and treated
// what they recorded as never having happened -- twenty-one warnings for every
// pass over seven backup jobs, and a last-run time the interface could not
// show. Both are read, and both come back as UTC.
func ParseDatabaseTimestamp(timestamp string) (time.Time, error) {
	stored, err := time.Parse("2006-01-02 15:04:05", timestamp)
	if err == nil {
		return stored, nil
	}
	if rfc, rfcErr := time.Parse(time.RFC3339, timestamp); rfcErr == nil {
		return rfc.UTC(), nil
	}
	// The first error is the one worth reporting: it names the format this
	// writes, which is the format a new row will be in.
	return time.Time{}, err
}

// jst is the zone every backup window is expressed in. The offsets in a task's
// configuration name calendar days in Tokyo, not 24-hour blocks from now.
var jst = time.FixedZone("JST", 9*3600)

// GetJSTTimeRange returns the half-open window [start, end) that the offsets
// name, as JST midnights. An offset of -1 is yesterday, 0 is today, so the
// common "yesterday" window is -1 to 0.
func GetJSTTimeRange(startOffset, endOffset int) (time.Time, time.Time, error) {
	now := time.Now().In(jst)

	start := time.Date(now.Year(), now.Month(), now.Day()+startOffset, 0, 0, 0, 0, jst)
	end := time.Date(now.Year(), now.Month(), now.Day()+endOffset, 0, 0, 0, 0, jst)

	return start, end, nil
}

// GetUTCTimeRange returns the same window in UTC, which is what the databases
// are queried in.
func GetUTCTimeRange(startOffset, endOffset int) (time.Time, time.Time, error) {
	startJST, endJST, err := GetJSTTimeRange(startOffset, endOffset)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	return startJST.UTC(), endJST.UTC(), nil
}

// DailyOffsets reads the offsets out of a {"type":"daily", ...} condition.
//
// The offsets used to have to be JSON numbers: anything else — a string, a Go
// int from a hand-built map — fell back to the default -1..0 with no error and
// no log line, so a task configured with "startOffset": "-7" quietly backed up
// yesterday instead of the last week.
func DailyOffsets(spec map[string]interface{}) (int, int, error) {
	start, err := offsetValue(spec, "startOffset", -1)
	if err != nil {
		return 0, 0, err
	}
	end, err := offsetValue(spec, "endOffset", 0)
	if err != nil {
		return 0, 0, err
	}
	if start >= end {
		// endOffset is exclusive, so "0 to 0" — the intuitive spelling of "just
		// today" — names an empty window, and the export then writes an empty
		// file and reports success.
		return 0, 0, fmt.Errorf("startOffset %d and endOffset %d name an empty "+
			"window: endOffset is exclusive, so today alone is 0 to 1", start, end)
	}
	return start, end, nil
}

// offsetValue reads one offset, accepting every spelling a configuration
// document can carry it in.
func offsetValue(spec map[string]interface{}, key string, fallback int) (int, error) {
	raw, present := spec[key]
	if !present || raw == nil {
		return fallback, nil
	}

	switch v := raw.(type) {
	case float64:
		return int(v), nil
	case float32:
		return int(v), nil
	case int:
		return v, nil
	case int32:
		return int(v), nil
	case int64:
		return int(v), nil
	case json.Number:
		n, err := v.Int64()
		if err != nil {
			return 0, fmt.Errorf("%s is %q, which is not a whole number of days", key, v)
		}
		return int(n), nil
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return 0, fmt.Errorf("%s is %q, which is not a whole number of days", key, v)
		}
		return n, nil
	default:
		return 0, fmt.Errorf("%s is a %T, which is not a number of days", key, raw)
	}
}
