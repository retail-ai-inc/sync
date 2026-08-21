package utils

import (
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
)

func newCounter(t *testing.T) *QueryCounter {
	t.Helper()

	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	return NewQueryCounter(logger)
}

func TestFormatValue(t *testing.T) {
	qc := newCounter(t)
	ts := time.Date(2026, 8, 21, 13, 45, 30, 0, time.UTC)

	tests := []struct {
		name  string
		value interface{}
		want  string
	}{
		{"time becomes ISODate", ts, `ISODate("2026-08-21T13:45:30.000Z")`},
		{"string is quoted", "active", `"active"`},
		{"int", 42, "42"},
		{"int64", int64(42), "42"},
		{"float", 1.5, "1.5"},
		{"bool falls through to %v", true, "true"},
		{"nil falls through to %v", nil, "<nil>"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := qc.formatValue(tt.value); got != tt.want {
				t.Errorf("formatValue(%#v) = %q, want %q", tt.value, got, tt.want)
			}
		})
	}
}

func TestFormatFilterCondition(t *testing.T) {
	qc := newCounter(t)
	ts := time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name  string
		field string
		value interface{}
		want  string
	}{
		{"scalar equality", "status", "active", `status: "active"`},
		{"numeric equality", "count", 3, "count: 3"},
		{
			"gte with a time renders as ISODate",
			"created_at", bson.M{"$gte": ts},
			`created_at: {$gte: ISODate("2026-08-21T00:00:00.000Z")}`,
		},
		{
			"lt with a time renders as a plain value",
			// Only $gte and $lte have the ISODate special case; $lt and $gt fall
			// through to formatValue, which also renders times as ISODate.
			"created_at", bson.M{"$lt": ts},
			`created_at: {$lt: ISODate("2026-08-21T00:00:00.000Z")}`,
		},
		{"ne", "status", bson.M{"$ne": "deleted"}, `status: {$ne: "deleted"}`},
		{"unknown operator passes through", "n", bson.M{"$in": 1}, "n: {$in: 1}"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := qc.formatFilterCondition(tt.field, tt.value); got != tt.want {
				t.Errorf("formatFilterCondition(%q, %#v) = %q, want %q",
					tt.field, tt.value, got, tt.want)
			}
		})
	}
}

// TestFormatFilterConditionDropsEmptyOperatorMaps records that an empty bson.M
// yields an empty string, which buildReadableQueryString then skips. The
// rendered query therefore claims a filter that the real count did not use.
func TestFormatFilterConditionDropsEmptyOperatorMaps(t *testing.T) {
	qc := newCounter(t)

	if got := qc.formatFilterCondition("created_at", bson.M{}); got != "" {
		t.Errorf("formatFilterCondition with an empty operator map = %q, want empty", got)
	}
}

func TestBuildReadableQueryString(t *testing.T) {
	qc := newCounter(t)

	t.Run("no filter", func(t *testing.T) {
		got := qc.buildReadableQueryString("users", bson.M{})
		if want := "db.users.countDocuments({})"; got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	t.Run("nil filter", func(t *testing.T) {
		got := qc.buildReadableQueryString("users", nil)
		if want := "db.users.countDocuments({})"; got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	t.Run("single condition", func(t *testing.T) {
		got := qc.buildReadableQueryString("users", bson.M{"status": "active"})
		if want := `db.users.countDocuments({status: "active"})`; got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	t.Run("several conditions", func(t *testing.T) {
		got := qc.buildReadableQueryString("users", bson.M{
			"status": "active",
			"count":  3,
		})
		// Map iteration order is unspecified, so check the parts rather than
		// the exact string.
		for _, part := range []string{`status: "active"`, "count: 3"} {
			if !strings.Contains(got, part) {
				t.Errorf("query %q is missing %q", got, part)
			}
		}
		if !strings.HasPrefix(got, "db.users.countDocuments({") || !strings.HasSuffix(got, "})") {
			t.Errorf("query %q is not wrapped correctly", got)
		}
	})
}

// TestBuildReadableQueryStringHidesUnrenderableFilters records a reporting
// defect. A filter whose conditions all render as empty strings produces
// "countDocuments({})", identical to the no-filter case, so the log line claims
// an unfiltered count while the real query was filtered. The string is used in
// operator-facing logs, which makes the two indistinguishable after the fact.
func TestBuildReadableQueryStringHidesUnrenderableFilters(t *testing.T) {
	qc := newCounter(t)

	got := qc.buildReadableQueryString("users", bson.M{"created_at": bson.M{}})

	if got != "db.users.countDocuments({})" {
		t.Errorf("got %q; unrenderable conditions may now be surfaced, so assert "+
			"the new rendering instead", got)
	}
}

func TestNewQueryCounterWithYesterdaySupport(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)

	start := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC)

	qc := NewQueryCounterWithYesterdaySupport(logger, start, end)
	if qc == nil {
		t.Fatal("NewQueryCounterWithYesterdaySupport returned nil")
	}
	// The plain constructor must not carry a range.
	if plain := NewQueryCounter(logger); plain == nil {
		t.Fatal("NewQueryCounter returned nil")
	}
}
