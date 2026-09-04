package mysql

import (
	"strings"
	"testing"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
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

// TestTheSnapshotSharesTheSeriesThePipelineWritesTo: these labels used to carry
// the endpoints, which put the snapshot this syncer records on a different
// series from the one the pipeline starts, so every snapshot metric came out
// twice per task and the pipeline's copy stayed at the zero it opens with.
func TestTheSnapshotSharesTheSeriesThePipelineWritesTo(t *testing.T) {
	s := newSyncer(t)
	s.cfg.ID = 41
	s.cfg.SourceConnection = "root:pw@tcp(tokyo:3306)/shop"
	s.cfg.TargetConnection = "root:pw@tcp(osaka:3306)/shop_bk"

	got := s.metricLabels()
	want := metrics.Labels{"task": "41", "engine": "mysql"}

	if got.Key() != want.Key() {
		t.Errorf("labels = %v, want %v", got, want)
	}
}
