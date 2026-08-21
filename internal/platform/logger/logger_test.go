package logger

import (
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func TestGetLogLevel(t *testing.T) {
	tests := []struct {
		in   string
		want logrus.Level
	}{
		{"debug", logrus.DebugLevel},
		{"info", logrus.InfoLevel},
		{"warn", logrus.WarnLevel},
		{"warning", logrus.WarnLevel},
		{"error", logrus.ErrorLevel},
		{"fatal", logrus.FatalLevel},
		{"panic", logrus.PanicLevel},
		{"DEBUG", logrus.DebugLevel},
		{"Warning", logrus.WarnLevel},
		// Anything unrecognised silently becomes info rather than being
		// rejected, so a typo in config_global downgrades logging without a word.
		{"verbose", logrus.InfoLevel},
		{"trace", logrus.InfoLevel},
		{"", logrus.InfoLevel},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			if got := getLogLevel(tt.in); got != tt.want {
				t.Errorf("getLogLevel(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func formatEntry(t *testing.T, entry *logrus.Entry) string {
	t.Helper()

	out, err := (&CustomTextFormatter{}).Format(entry)
	if err != nil {
		t.Fatalf("Format: %v", err)
	}
	return string(out)
}

func TestCustomTextFormatter(t *testing.T) {
	ts := time.Date(2026, 8, 21, 13, 45, 30, 0, time.UTC)
	line := formatEntry(t, &logrus.Entry{
		Time:    ts,
		Level:   logrus.InfoLevel,
		Message: "sync started",
	})

	if want := "[2026/08/21 13:45:30] [INFO] sync started\n"; line != want {
		t.Errorf("Format produced %q, want %q", line, want)
	}
}

func TestCustomTextFormatterIncludesFields(t *testing.T) {
	line := formatEntry(t, &logrus.Entry{
		Time:    time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC),
		Level:   logrus.ErrorLevel,
		Message: "failed",
		Data:    logrus.Fields{"sync_task_id": 7},
	})

	if !strings.Contains(line, "sync_task_id=7") {
		t.Errorf("Format produced %q, want it to carry the field", line)
	}
	if !strings.Contains(line, "[ERROR]") {
		t.Errorf("Format produced %q, want the level in upper case", line)
	}
}

// TestCustomTextFormatterRunsFieldsTogether records a formatting defect: the
// field parts are joined with an empty separator, so two or more fields are
// concatenated with nothing between them. `sync_task_id=7` and `table=users`
// come out as `sync_task_id=7table=users`, which neither reads nor parses.
func TestCustomTextFormatterRunsFieldsTogether(t *testing.T) {
	line := formatEntry(t, &logrus.Entry{
		Time:    time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC),
		Level:   logrus.InfoLevel,
		Message: "counted",
		Data:    logrus.Fields{"sync_task_id": 7, "table": "users"},
	})

	if strings.Contains(line, "7 table=users") {
		t.Fatalf("fields are now separated (%q); the join may have been fixed, "+
			"so assert the readable form instead", line)
	}
	if !strings.Contains(line, "sync_task_id=7table=users") {
		t.Errorf("Format produced %q, want the fields run together", line)
	}
}

func TestCustomTextFormatterSortsFields(t *testing.T) {
	line := formatEntry(t, &logrus.Entry{
		Time:    time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC),
		Level:   logrus.InfoLevel,
		Message: "m",
		Data:    logrus.Fields{"zebra": 1, "alpha": 2},
	})

	// Sorting makes the output stable across runs despite map iteration order.
	alpha := strings.Index(line, "alpha=")
	zebra := strings.Index(line, "zebra=")
	if alpha == -1 || zebra == -1 || alpha > zebra {
		t.Errorf("Format produced %q, want fields in sorted order", line)
	}
}

func TestInitLoggerAppliesLevel(t *testing.T) {
	// SQLite logging stays off unless the environment opts in, so this does not
	// touch the database.
	t.Setenv("ENABLE_SQLITE_LOGGING", "")

	log := InitLogger("error")
	if log == nil {
		t.Fatal("InitLogger returned nil")
	}
	if got := log.GetLevel(); got != logrus.ErrorLevel {
		t.Errorf("level = %v, want error", got)
	}
	if len(log.Hooks) != 0 {
		t.Errorf("hooks were installed with SQLite logging disabled: %v", log.Hooks)
	}
	// The package-level logger is repointed as a side effect.
	if Log != log {
		t.Error("InitLogger did not update the package-level Log")
	}
}

func TestSQLiteHookLevels(t *testing.T) {
	// The hook subscribes to every level, so enabling it writes a database row
	// for each debug line as well.
	if got, want := len(NewSQLiteHook().Levels()), len(logrus.AllLevels); got != want {
		t.Errorf("the hook covers %d levels, want %d", got, want)
	}
}
