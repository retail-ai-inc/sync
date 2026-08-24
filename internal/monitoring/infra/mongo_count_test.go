package infra

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/monitoring/domain"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// deadMongo returns a client that resolves collection handles without dialling
// and fails every operation quickly. The counter builds its filter before it
// runs the count, so the filter can be inspected without a server.
func deadMongo(t *testing.T) *mongo.Client {
	t.Helper()

	client, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://127.0.0.1:1").
		SetServerSelectionTimeout(10 * time.Millisecond).
		SetConnectTimeout(10 * time.Millisecond))
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

// TestTheEqualityOperatorMatchesEitherRepresentation covers a value that looks
// like a number. It used to be converted and the string dropped, and MongoDB
// does not match a numeric filter against a text field — so an order id stored
// as text counted zero rows, which reads as a replica that has lost everything.
// Nothing here knows which way a given collection stores it, so both are matched.
func TestTheEqualityOperatorMatchesEitherRepresentation(t *testing.T) {
	qc, out := loggingCounter(t)

	tests := []struct {
		value string
		want  string
	}{
		{"42", "status: {$in: [42 42]}"},
		{"4.5", "status: {$in: [4.5 4.5]}"},
		{"active", `status: "active"`},
		{"-1", "status: {$in: [-1 -1]}"},
		// A zero-padded id keeps its padding in the string half of the match.
		{"0042", "status: {$in: [42 0042]}"},
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

// TestANumericStringIdIsStillMatched is the case that mattered: an id column
// stored as text. The value was sent as a number and MongoDB matched nothing.
func TestANumericStringIdIsStillMatched(t *testing.T) {
	qc, out := loggingCounter(t)

	got := countedFilter(t, qc, out, "orders", &domain.CountQuery{
		Conditions: []domain.CountCondition{
			{Table: "orders", Field: "order_id", Operator: "=", Value: "1001"},
		},
	})
	if !strings.Contains(got, "1001 1001") {
		t.Errorf("query = %q, want both the number and the string matched", got)
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

// TestAConditionThatCannotBeExpressedIsReported covers a count that silently
// became a count of something else. A condition the builder did not recognise
// contributed nothing to the filter, so the number reported as "orders matching
// X" was the size of the whole collection — and nothing said so.
func TestAConditionThatCannotBeExpressedIsReported(t *testing.T) {
	for name, condition := range map[string]domain.CountCondition{
		"an unknown operator":        {Table: "orders", Field: "name", Operator: "LIKE", Value: "A%"},
		"no value":                   {Table: "orders", Field: "status", Operator: "=", Value: ""},
		"an unknown date range":      {Table: "orders", Field: "created_at", Operator: "dateRange", Value: "quarterly"},
		"a date range with no field": {Table: "orders", Operator: "dateRange", Value: "daily"},
	} {
		t.Run(name, func(t *testing.T) {
			qc, _ := loggingCounter(t)

			count, err := qc.CountMongoDBDocuments(context.Background(), deadMongo(t),
				"shop", "orders", &domain.CountQuery{Conditions: []domain.CountCondition{condition}})
			if err == nil {
				t.Fatalf("the condition was dropped and %d was reported", count)
			}
			if !strings.Contains(err.Error(), "cannot be expressed") {
				t.Errorf("err = %v, want it to say the condition could not be used", err)
			}
		})
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

// TestTwoConditionsOnOneFieldAreReported covers a range — "total > 10 and
// total < 100", the obvious thing to want. The filter is a map keyed by field
// name, so the second condition replaced the first and the count was taken with
// only half the range, without a word.
func TestTwoConditionsOnOneFieldAreReported(t *testing.T) {
	qc, _ := loggingCounter(t)

	count, err := qc.CountMongoDBDocuments(context.Background(), deadMongo(t),
		"shop", "orders", &domain.CountQuery{
			Conditions: []domain.CountCondition{
				{Table: "orders", Field: "total", Operator: ">", Value: "10"},
				{Table: "orders", Field: "total", Operator: "<", Value: "100"},
			},
		})
	if err == nil {
		t.Fatalf("only half the range was applied and %d was reported", count)
	}
	if !strings.Contains(err.Error(), "second condition") {
		t.Errorf("err = %v, want it to name the condition it could not combine", err)
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
