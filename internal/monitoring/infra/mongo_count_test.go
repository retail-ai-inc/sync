package infra

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/monitoring/domain"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// deadMongo returns a client that resolves collection handles without dialling
// and fails every operation quickly. The counter builds its filter before it
// runs the count, so the filter can be inspected without a server.
func deadMongo(t *testing.T) *mongo.Client {
	t.Helper()

	client, err := mongo.Connect(context.Background(), options.Client().
		ApplyURI("mongodb://127.0.0.1:1").
		SetServerSelectionTimeout(10*time.Millisecond).
		SetConnectTimeout(10*time.Millisecond))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	return client
}

// countedFilter runs a count against an unreachable server and returns the
// query the counter logged on its way there. That log line is the only seam the
// filter builder offers.
func countedFilter(t *testing.T, qc *QueryCounter, out *bytes.Buffer,
	collection string, query *domain.CountQuery) string {
	t.Helper()

	out.Reset()
	count, err := qc.CountMongoDBDocuments(context.Background(), deadMongo(t),
		"shop", collection, query)
	if err == nil {
		t.Fatalf("the count against an unreachable server succeeded with %d", count)
	}
	if count != -1 {
		t.Errorf("count = %d on failure, want -1", count)
	}

	for _, line := range strings.Split(out.String(), "\n") {
		i := strings.Index(line, "db."+collection+".countDocuments(")
		if i < 0 {
			continue
		}
		// logrus quotes the message and escapes the quotes inside it.
		query := strings.TrimSuffix(line[i:], `"`)
		return strings.ReplaceAll(query, `\"`, `"`)
	}
	t.Fatalf("no query was logged; output was:\n%s", out.String())
	return ""
}

// loggingCounter returns a counter whose debug output is captured.
func loggingCounter(t *testing.T) (*QueryCounter, *bytes.Buffer) {
	t.Helper()

	var out bytes.Buffer
	logger := logrus.New()
	logger.SetOutput(&out)
	logger.SetLevel(logrus.DebugLevel)
	return NewQueryCounter(logger), &out
}

func TestAnEmptyQueryUsesTheEstimatedCount(t *testing.T) {
	qc, out := loggingCounter(t)

	for _, query := range []*domain.CountQuery{nil, {}} {
		out.Reset()
		count, err := qc.CountMongoDBDocuments(context.Background(), deadMongo(t),
			"shop", "orders", query)
		if err == nil {
			t.Fatal("the count against an unreachable server succeeded")
		}
		if count != -1 {
			t.Errorf("count = %d, want -1", count)
		}
		if !strings.Contains(out.String(), "estimatedDocumentCount") {
			t.Errorf("output = %q, want the estimated count path", out.String())
		}
		if !strings.Contains(err.Error(), "estimated document count failed") {
			t.Errorf("err = %v", err)
		}
	}
}

// TestAConditionForAnotherTableIsIgnored records the filter for the table at
// hand only: conditions naming a different table are skipped, so a shared query
// document can carry per-table rules.
func TestAConditionForAnotherTableIsIgnored(t *testing.T) {
	qc, out := loggingCounter(t)

	got := countedFilter(t, qc, out, "orders", &domain.CountQuery{
		Conditions: []domain.CountCondition{
			{Table: "customers", Field: "status", Operator: "=", Value: "active"},
		},
	})

	if got != "db.orders.countDocuments({})" {
		t.Errorf("query = %q, want an empty filter", got)
	}
	if !strings.Contains(out.String(), "No relevant conditions") {
		t.Error("the empty selection was not warned about")
	}
}

func TestTheEqualityOperatorPicksATypeForTheValue(t *testing.T) {
	qc, out := loggingCounter(t)

	tests := []struct {
		value string
		want  string
	}{
		{"42", "status: 42"},
		{"4.5", "status: 4.5"},
		{"active", `status: "active"`},
		{"-1", "status: -1"},
		{"0042", "status: 42"}, // a zero-padded id loses its padding
	}

	for _, tc := range tests {
		t.Run(tc.value, func(t *testing.T) {
			got := countedFilter(t, qc, out, "orders", &domain.CountQuery{
				Conditions: []domain.CountCondition{
					{Table: "orders", Field: "status", Operator: "=", Value: tc.value},
				},
			})
			if !strings.Contains(got, tc.want) {
				t.Errorf("query = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestANumericStringIdIsQueriedAsANumber records a real mismatch risk: any value
// that parses as a number is sent to MongoDB as a number, and MongoDB does not
// match a numeric filter against a string field. An id column stored as a string
// therefore counts zero rows.
func TestANumericStringIdIsQueriedAsANumber(t *testing.T) {
	qc, out := loggingCounter(t)

	got := countedFilter(t, qc, out, "orders", &domain.CountQuery{
		Conditions: []domain.CountCondition{
			{Table: "orders", Field: "order_id", Operator: "=", Value: "1001"},
		},
	})
	if strings.Contains(got, `"1001"`) {
		t.Fatalf("query = %q; the value appears to be kept as a string now, so "+
			"assert that instead", got)
	}
}

func TestTheComparisonOperators(t *testing.T) {
	qc, out := loggingCounter(t)

	tests := []struct {
		operator string
		want     string
	}{
		{">", "$gt: 10"},
		{">=", "$gte: 10"},
		{"<", "$lt: 10"},
		{"<=", "$lte: 10"},
		{"!=", "$ne: 10"},
		{"<>", "$ne: 10"},
	}

	for _, tc := range tests {
		t.Run(tc.operator, func(t *testing.T) {
			got := countedFilter(t, qc, out, "orders", &domain.CountQuery{
				Conditions: []domain.CountCondition{
					{Table: "orders", Field: "total", Operator: tc.operator, Value: "10"},
				},
			})
			if !strings.Contains(got, tc.want) {
				t.Errorf("query = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTheComparisonOperatorsAcceptFloatsAndStrings(t *testing.T) {
	qc, out := loggingCounter(t)

	for _, tc := range []struct{ value, want string }{
		{"10.5", "$gt: 10.5"},
		{"abc", `$gt: "abc"`},
	} {
		t.Run(tc.value, func(t *testing.T) {
			got := countedFilter(t, qc, out, "orders", &domain.CountQuery{
				Conditions: []domain.CountCondition{
					{Table: "orders", Field: "total", Operator: ">", Value: tc.value},
				},
			})
			if !strings.Contains(got, tc.want) {
				t.Errorf("query = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestAnUnknownOperatorIsDropped records that an operator the builder does not
// recognise contributes nothing to the filter, so the count silently covers the
// whole collection instead of the intended slice.
func TestAnUnknownOperatorIsDropped(t *testing.T) {
	qc, out := loggingCounter(t)

	got := countedFilter(t, qc, out, "orders", &domain.CountQuery{
		Conditions: []domain.CountCondition{
			{Table: "orders", Field: "name", Operator: "LIKE", Value: "A%"},
		},
	})
	if got != "db.orders.countDocuments({})" {
		t.Fatalf("query = %q; LIKE appears to be handled now, so assert that "+
			"instead", got)
	}
}

// TestAConditionWithNoValueIsDropped records the same silent widening for a
// condition whose value is empty — the UI can produce one by leaving the field
// blank.
func TestAConditionWithNoValueIsDropped(t *testing.T) {
	qc, out := loggingCounter(t)

	got := countedFilter(t, qc, out, "orders", &domain.CountQuery{
		Conditions: []domain.CountCondition{
			{Table: "orders", Field: "status", Operator: "=", Value: ""},
		},
	})
	if got != "db.orders.countDocuments({})" {
		t.Errorf("query = %q, want an empty filter", got)
	}
}

// TestTheDateRangesAreBuiltInJST records the timezone the ranges are anchored
// to: the day boundaries are Japanese local time converted to UTC, so a "daily"
// count covers 15:00–15:00 UTC rather than midnight to midnight.
func TestTheDateRangesAreBuiltInJST(t *testing.T) {
	qc, out := loggingCounter(t)
	jst := time.FixedZone("JST", 9*3600)

	for _, value := range []string{"daily", "today", "DAILY"} {
		got := countedFilter(t, qc, out, "orders", &domain.CountQuery{
			Conditions: []domain.CountCondition{
				{Table: "orders", Field: "created_at", Operator: "dateRange", Value: value},
			},
		})

		now := time.Now().In(jst)
		wantStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, jst).
			UTC().Format("2006-01-02T15:04:05Z")
		if !strings.Contains(got, wantStart) {
			t.Errorf("query for %q = %q, want the JST midnight %s", value, got, wantStart)
		}
		if !strings.Contains(got, "$gte") || !strings.Contains(got, "$lte") {
			t.Errorf("query = %q, want a bounded range", got)
		}
	}
}

func TestTheWeeklyAndMonthlyRanges(t *testing.T) {
	qc, out := loggingCounter(t)
	jst := time.FixedZone("JST", 9*3600)
	now := time.Now().In(jst)

	weekly := countedFilter(t, qc, out, "orders", &domain.CountQuery{
		Conditions: []domain.CountCondition{
			{Table: "orders", Field: "created_at", Operator: "dateRange", Value: "weekly"},
		},
	})
	startOfWeek := now.AddDate(0, 0, -int(now.Weekday()))
	wantWeek := time.Date(startOfWeek.Year(), startOfWeek.Month(), startOfWeek.Day(),
		0, 0, 0, 0, jst).UTC().Format("2006-01-02T15:04:05Z")
	if !strings.Contains(weekly, wantWeek) {
		t.Errorf("weekly = %q, want the week starting %s", weekly, wantWeek)
	}

	monthly := countedFilter(t, qc, out, "orders", &domain.CountQuery{
		Conditions: []domain.CountCondition{
			{Table: "orders", Field: "created_at", Operator: "dateRange", Value: "monthly"},
		},
	})
	wantMonth := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, jst).
		UTC().Format("2006-01-02T15:04:05Z")
	if !strings.Contains(monthly, wantMonth) {
		t.Errorf("monthly = %q, want the month starting %s", monthly, wantMonth)
	}
}

// TestTheWeeklyRangeEndsTodayNotAtTheWeekEnd records that "weekly" means
// week-to-date: the upper bound is the end of today, not the end of the week.
// The same holds for "monthly". So the figure grows through the period rather
// than being a complete-period total.
func TestTheWeeklyRangeEndsTodayNotAtTheWeekEnd(t *testing.T) {
	qc, out := loggingCounter(t)
	jst := time.FixedZone("JST", 9*3600)
	now := time.Now().In(jst)

	got := countedFilter(t, qc, out, "orders", &domain.CountQuery{
		Conditions: []domain.CountCondition{
			{Table: "orders", Field: "created_at", Operator: "dateRange", Value: "weekly"},
		},
	})
	wantEnd := time.Date(now.Year(), now.Month(), now.Day(), 23, 59, 59, 999999999, jst).
		UTC().Format("2006-01-02T15:04:05Z")
	if !strings.Contains(got, wantEnd) {
		t.Errorf("weekly = %q, want it to end today at %s", got, wantEnd)
	}
}

// TestTheYesterdayRangeUsesTheInjectedWindow records the seam the daily summary
// relies on: a counter built with an explicit window uses it verbatim instead of
// recomputing yesterday, so every table in one summary run shares the same
// boundaries even if the run straddles midnight.
func TestTheYesterdayRangeUsesTheInjectedWindow(t *testing.T) {
	var out bytes.Buffer
	logger := logrus.New()
	logger.SetOutput(&out)
	logger.SetLevel(logrus.DebugLevel)

	start := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 8, 20, 23, 59, 59, 0, time.UTC)
	qc := NewQueryCounterWithYesterdaySupport(logger, start, end)

	got := countedFilter(t, qc, &out, "orders", &domain.CountQuery{
		Conditions: []domain.CountCondition{
			{Table: "orders", Field: "created_at", Operator: "dateRange", Value: "yesterday"},
		},
	})
	if !strings.Contains(got, "2026-08-20T00:00:00Z") {
		t.Errorf("query = %q, want the injected window", got)
	}
}

// TestTheYesterdayRangeFallsBackToJST records what a plain counter does with a
// "yesterday" condition — it computes the window itself, in JST.
func TestTheYesterdayRangeFallsBackToJST(t *testing.T) {
	qc, out := loggingCounter(t)
	jst := time.FixedZone("JST", 9*3600)
	yesterday := time.Now().In(jst).AddDate(0, 0, -1)

	got := countedFilter(t, qc, out, "orders", &domain.CountQuery{
		Conditions: []domain.CountCondition{
			{Table: "orders", Field: "created_at", Operator: "dateRange", Value: "yesterday"},
		},
	})
	want := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, jst).
		UTC().Format("2006-01-02T15:04:05Z")
	if !strings.Contains(got, want) {
		t.Errorf("query = %q, want the JST yesterday %s", got, want)
	}
}

// TestAnUnknownDateRangeIsDropped records the widening again: a range name the
// builder does not know contributes nothing, so the count covers everything.
func TestAnUnknownDateRangeIsDropped(t *testing.T) {
	qc, out := loggingCounter(t)

	got := countedFilter(t, qc, out, "orders", &domain.CountQuery{
		Conditions: []domain.CountCondition{
			{Table: "orders", Field: "created_at", Operator: "dateRange", Value: "quarterly"},
		},
	})
	if got != "db.orders.countDocuments({})" {
		t.Errorf("query = %q, want an empty filter", got)
	}
	if !strings.Contains(out.String(), "Unknown date range type") {
		t.Error("the unknown range was not warned about")
	}
}

// TestADateRangeWithNoFieldFallsThroughToTheComparisonBranch records an
// unexpected interaction: the dateRange branch requires a field, and without one
// the condition falls through to the operator switch, where "dateRange" is not a
// known operator either. The result is an empty filter and a debug line, not the
// warning the unknown-range case gets.
func TestADateRangeWithNoFieldFallsThroughToTheComparisonBranch(t *testing.T) {
	qc, out := loggingCounter(t)

	got := countedFilter(t, qc, out, "orders", &domain.CountQuery{
		Conditions: []domain.CountCondition{
			{Table: "orders", Operator: "dateRange", Value: "daily"},
		},
	})
	if got != "db.orders.countDocuments({})" {
		t.Errorf("query = %q, want an empty filter", got)
	}
	if strings.Contains(out.String(), "Unknown date range type") {
		t.Error("the fieldless range took the date branch")
	}
}

// TestTwoConditionsOnOneFieldKeepOnlyTheLast records that the filter is a plain
// map keyed by field name, so a range and a comparison on the same field cannot
// coexist — the second condition overwrites the first without a word.
func TestTwoConditionsOnOneFieldKeepOnlyTheLast(t *testing.T) {
	qc, out := loggingCounter(t)

	got := countedFilter(t, qc, out, "orders", &domain.CountQuery{
		Conditions: []domain.CountCondition{
			{Table: "orders", Field: "total", Operator: ">", Value: "10"},
			{Table: "orders", Field: "total", Operator: "<", Value: "100"},
		},
	})
	if strings.Contains(got, "$gt") {
		t.Fatalf("query = %q; both bounds appear to be kept now, so assert that "+
			"instead", got)
	}
	if !strings.Contains(got, "$lt: 100") {
		t.Errorf("query = %q, want the last condition", got)
	}
}

func TestTwoConditionsOnDifferentFieldsAreBothApplied(t *testing.T) {
	qc, out := loggingCounter(t)

	got := countedFilter(t, qc, out, "orders", &domain.CountQuery{
		Conditions: []domain.CountCondition{
			{Table: "orders", Field: "total", Operator: ">", Value: "10"},
			{Table: "orders", Field: "status", Operator: "=", Value: "paid"},
		},
	})
	if !strings.Contains(got, "$gt: 10") || !strings.Contains(got, `status: "paid"`) {
		t.Errorf("query = %q, want both conditions", got)
	}
}
