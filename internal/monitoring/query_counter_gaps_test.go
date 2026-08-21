package monitoring

import (
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
)

func TestNewQueryCounterSuppliesADefaultLogger(t *testing.T) {
	if qc := NewQueryCounter(nil); qc.logger == nil {
		t.Error("NewQueryCounter(nil) left the logger nil")
	}

	given := logrus.New()
	if qc := NewQueryCounter(given); qc.logger != given {
		t.Error("NewQueryCounter replaced the supplied logger")
	}
}

// The existing formatFilterCondition table does not reach $gt, or the
// non-time branches of $gte and $lte. These pin them.
func TestFormatFilterConditionRemainingOperators(t *testing.T) {
	qc := NewQueryCounter(nil)

	tests := []struct {
		name  string
		value interface{}
		want  string
	}{
		{"$gt", bson.M{"$gt": 10}, "age: {$gt: 10}"},
		{"$gte with a plain value", bson.M{"$gte": 1}, "age: {$gte: 1}"},
		{"$lte with a plain value", bson.M{"$lte": 1}, "age: {$lte: 1}"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := qc.formatFilterCondition("age", tc.value); got != tc.want {
				t.Errorf("formatFilterCondition(age, %#v) = %q, want %q", tc.value, got, tc.want)
			}
		})
	}
}
