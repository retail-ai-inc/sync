package export

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
)

// captureWarnings collects what the package logs for the duration of a test.
func captureWarnings(t *testing.T) *bytes.Buffer {
	t.Helper()

	var out bytes.Buffer
	previous := logrus.StandardLogger().Out
	level := logrus.GetLevel()
	logrus.SetOutput(&out)
	logrus.SetLevel(logrus.WarnLevel)
	t.Cleanup(func() {
		logrus.SetOutput(previous)
		logrus.SetLevel(level)
	})
	return &out
}

// Jobs 10 and 14 on staging have uploaded empty files since 8-31 and every run
// reported success, because an empty day and a broken query look identical from
// outside: a 222-byte zip in a bucket.
func TestAnEmptyExportIsReported(t *testing.T) {
	out := captureWarnings(t)

	reportIfEmpty("MongoDB", "RetailerCouponUsages", 0,
		`{"CreateAt":{"$gte":{"$date":"2026-08-31T15:00:00Z"}}}`)

	logged := out.String()
	for _, want := range []string{"0 records", "RetailerCouponUsages", "CreateAt"} {
		if !strings.Contains(logged, want) {
			t.Errorf("the warning does not mention %q, so it does not say what to "+
				"look at: %s", want, logged)
		}
	}
}

// A backup that carried rows must not warn, or the warning stops meaning
// anything.
func TestAnExportWithRowsIsNotReported(t *testing.T) {
	out := captureWarnings(t)

	reportIfEmpty("MySQL", "RetailerStores", 412, "SELECT * FROM RetailerStores")

	if logged := out.String(); logged != "" {
		t.Errorf("a backup of 412 rows warned: %s", logged)
	}
}

func TestCountCSVDataRowsDoesNotCountTheHeader(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want int64
	}{
		{"header and two rows", "id,name\n1,Ada\n2,Grace\n", 2},
		{"header only", "id,name\n", 0},
		{"empty file", "", 0},
		{"no trailing newline", "id,name\n1,Ada", 1},
		{"blank lines are not rows", "id,name\n1,Ada\n\n\n", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "export.csv")
			if err := os.WriteFile(path, []byte(tc.body), 0o600); err != nil {
				t.Fatalf("write: %v", err)
			}

			got, err := countCSVDataRows(path)
			if err != nil {
				t.Fatalf("countCSVDataRows: %v", err)
			}
			if got != tc.want {
				t.Errorf("rows = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestCountCSVDataRowsOnAMissingFile(t *testing.T) {
	if _, err := countCSVDataRows(filepath.Join(t.TempDir(), "absent.csv")); err == nil {
		t.Error("a missing file was counted as zero rows rather than reported")
	}
}
