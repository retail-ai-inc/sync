package mysql

import (
	"strings"
	"testing"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/sirupsen/logrus"
)

// newSyncer builds a syncer that writes SQLite, which is what the hermetic
// suite has to drive it against.
func newSyncer(t *testing.T) *MySQLSyncer {
	t.Helper()

	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	// Type is set because the endpoint and database-name helpers dispatch on it.
	return &MySQLSyncer{
		cfg:     config.SyncConfig{Type: "mysql"},
		logger:  logger,
		dialect: dialectSQLite,
	}
}

func TestMakeQuestionMarks(t *testing.T) {
	tests := []struct {
		n    int
		want string
	}{
		{0, ""},
		{1, "?"},
		{3, "?,?,?"},
	}

	for _, tt := range tests {
		got := makeQuestionMarks(tt.n)
		if len(got) != tt.n {
			t.Errorf("makeQuestionMarks(%d) has %d elements, want %d", tt.n, len(got), tt.n)
		}
		if joined := strings.Join(got, ","); joined != tt.want {
			t.Errorf("makeQuestionMarks(%d) = %q, want %q", tt.n, joined, tt.want)
		}
	}
}
