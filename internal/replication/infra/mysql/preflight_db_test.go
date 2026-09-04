package mysql

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
)

func discardLogger() logrus.FieldLogger {
	log := logrus.New()
	log.SetOutput(io.Discard)
	return log
}

// The source checks.

func TestPreflightRefusesAPartialRowImage(t *testing.T) {
	fake := &fakeDB{replies: []reply{variable("binlog_row_image", "MINIMAL")}}

	err := preflight(context.Background(), fake.open(t), "mysql", discardLogger())
	if err == nil {
		t.Fatal("a source logging partial rows was accepted")
	}
	if !strings.Contains(err.Error(), "MINIMAL") {
		t.Errorf("the refusal does not say what the source is set to: %v", err)
	}
}

func TestPreflightAcceptsAFullRowImage(t *testing.T) {
	fake := &fakeDB{replies: []reply{
		variable("binlog_row_image", "FULL"),
		variable("gtid_mode", "ON"),
	}}

	if err := preflight(context.Background(), fake.open(t), "mysql", discardLogger()); err != nil {
		t.Fatalf("a correctly configured source was refused: %v", err)
	}
}

// TestPreflightDoesNotAskMariaDBAboutRowImages: MariaDB has no such setting and
// always logs whole rows, so asking gets an empty answer that would read as a
// partial image and refuse every MariaDB task.
func TestPreflightDoesNotAskMariaDBAboutRowImages(t *testing.T) {
	fake := &fakeDB{replies: []reply{noSuchVariable("gtid_mode")}}

	if err := preflight(context.Background(), fake.open(t), "mariadb", discardLogger()); err != nil {
		t.Fatalf("a MariaDB source was refused: %v", err)
	}
	if fake.wasAsked("binlog_row_image") {
		t.Error("MariaDB was asked about binlog_row_image, which it does not have")
	}
}

// TestASourceThatWillNotSayItsGTIDModeStillReplicates. A source that refuses
// this cannot be checked, but it can be replicated, and refusing the task over
// a check that failed would be worse than the warning it would have produced.
func TestASourceThatWillNotSayItsGTIDModeStillReplicates(t *testing.T) {
	fake := &fakeDB{replies: []reply{
		variable("binlog_row_image", "FULL"),
		{match: "gtid_mode", err: errors.New("access denied")},
	}}

	if err := preflight(context.Background(), fake.open(t), "mysql", discardLogger()); err != nil {
		t.Fatalf("a source that would not answer was refused: %v", err)
	}
}

// TestASourceWithoutGTIDsIsWarnedAboutAndNotRefused. Replication works either
// way; what does not work is recovering the position after the source fails
// over, which is the ordinary case in the setup this is for.
func TestASourceWithoutGTIDsIsWarnedAboutAndNotRefused(t *testing.T) {
	fake := &fakeDB{replies: []reply{
		variable("binlog_row_image", "FULL"),
		variable("gtid_mode", "OFF"),
	}}
	log := &recordingFieldLogger{}

	if err := preflight(context.Background(), fake.open(t), "mysql", log.logger()); err != nil {
		t.Fatalf("a source without GTIDs was refused: %v", err)
	}
	if !log.saw("gtid_mode=OFF") {
		t.Error("a source without GTIDs produced no warning")
	}
}

func TestAnUnreadableRowImageIsReported(t *testing.T) {
	fake := &fakeDB{replies: []reply{
		{match: "binlog_row_image", err: errors.New("the server went away")},
	}}

	if err := preflight(context.Background(), fake.open(t), "mysql", discardLogger()); err == nil {
		t.Fatal("a source that would not answer about row images was accepted")
	}
}

// The target checks.

func TestAReadOnlyTargetIsRefused(t *testing.T) {
	for _, setting := range []string{"read_only", "super_read_only"} {
		t.Run(setting, func(t *testing.T) {
			replies := []reply{
				variable("read_only", "OFF"),
				variable("super_read_only", "OFF"),
			}
			for i := range replies {
				if strings.Contains(replies[i].match, setting) {
					replies[i] = variable(setting, "ON")
				}
			}
			// super_read_only's match is a substring of nothing else, but
			// read_only matches super_read_only's statement too, so the
			// specific one has to come first.
			if setting == "super_read_only" {
				replies = []reply{variable("super_read_only", "ON"), variable("read_only", "OFF")}
			}
			fake := &fakeDB{replies: replies}

			err := targetPreflight(context.Background(), fake.open(t), "", nil, discardLogger())
			if err == nil {
				t.Fatalf("a target with %s=ON was accepted, so every write would fail", setting)
			}
			if !strings.Contains(err.Error(), "failover") {
				t.Errorf("the refusal does not mention what leaves a target this way: %v", err)
			}
		})
	}
}

func TestAWritableTargetIsAccepted(t *testing.T) {
	fake := &fakeDB{replies: []reply{
		variable("super_read_only", "OFF"),
		variable("read_only", "OFF"),
		variable("event_scheduler", "OFF"),
		{match: "information_schema.triggers",
			columns: []string{"trigger_name", "event_object_table"}},
	}}

	if err := targetPreflight(context.Background(), fake.open(t), "tenant_trial_naviee_bk",
		[]string{"orders"}, discardLogger()); err != nil {
		t.Fatalf("a correctly configured target was refused: %v", err)
	}
}

// TestATargetThatWillNotSayIsNotRefused: the write itself reports the problem
// if there is one, and refusing over a check that could not run would stop
// replication to a target that works.
func TestATargetThatWillNotSayIsNotRefused(t *testing.T) {
	fake := &fakeDB{replies: []reply{
		{match: "read_only", err: errors.New("access denied")},
	}}

	if err := targetPreflight(context.Background(), fake.open(t), "", nil,
		discardLogger()); err != nil {
		t.Fatalf("a target that would not answer was refused: %v", err)
	}
}

func TestATargetRunningEventsIsWarnedAbout(t *testing.T) {
	fake := &fakeDB{replies: []reply{
		variable("super_read_only", "OFF"),
		variable("read_only", "OFF"),
		variable("event_scheduler", "ON"),
		{match: "information_schema.triggers",
			columns: []string{"trigger_name", "event_object_table"}},
	}}
	log := &recordingFieldLogger{}

	if err := targetPreflight(context.Background(), fake.open(t), "db", []string{"orders"},
		log.logger()); err != nil {
		t.Fatalf("targetPreflight: %v", err)
	}
	if !log.saw("event_scheduler=ON") {
		t.Error("a target running events produced no warning")
	}
}

func TestTriggersOnTheTargetsTablesAreNamed(t *testing.T) {
	fake := &fakeDB{replies: []reply{
		variable("super_read_only", "OFF"),
		variable("read_only", "OFF"),
		variable("event_scheduler", "OFF"),
		{match: "information_schema.triggers",
			columns: []string{"trigger_name", "event_object_table"},
			rows: [][]driver.Value{
				{"orders_after_insert", "orders"},
				{"payments_after_update", "payments"},
			}},
	}}
	log := &recordingFieldLogger{}

	if err := targetPreflight(context.Background(), fake.open(t), "db",
		[]string{"orders", "payments"}, log.logger()); err != nil {
		t.Fatalf("targetPreflight: %v", err)
	}
	if !log.saw("orders.orders_after_insert") {
		t.Errorf("the warning does not name the trigger: %v", log.lines)
	}
}

// TestTheTriggerCheckBindsTheTableNames. A table name comes from a task's
// configuration, so interpolating it would put operator-supplied text into a
// statement.
func TestTheTriggerCheckBindsTheTableNames(t *testing.T) {
	fake := &fakeDB{replies: []reply{
		{match: "information_schema.triggers",
			columns: []string{"trigger_name", "event_object_table"}},
	}}

	if _, err := triggersOn(context.Background(), fake.open(t), "db",
		[]string{"orders", "payments"}); err != nil {
		t.Fatalf("triggersOn: %v", err)
	}

	statements := fake.statements()
	if len(statements) != 1 {
		t.Fatalf("ran %d statements, want 1", len(statements))
	}
	if strings.Contains(statements[0], "orders") {
		t.Errorf("a table name was interpolated into the statement: %q", statements[0])
	}
	if want := "IN (?,?)"; !strings.Contains(statements[0], want) {
		t.Errorf("the statement does not bind one placeholder per table: %q", statements[0])
	}
}

func TestTheTriggerCheckAsksNothingWithoutASchemaOrTables(t *testing.T) {
	fake := &fakeDB{}
	db := fake.open(t)

	for name, c := range map[string]struct {
		schema string
		tables []string
	}{
		"no schema": {"", []string{"orders"}},
		"no tables": {"db", nil},
		"neither":   {"", nil},
	} {
		t.Run(name, func(t *testing.T) {
			found, err := triggersOn(context.Background(), db, c.schema, c.tables)
			if err != nil {
				t.Fatalf("triggersOn: %v", err)
			}
			if len(found) != 0 {
				t.Errorf("found %v", found)
			}
		})
	}
	if len(fake.statements()) != 0 {
		t.Errorf("statements were run with nothing to check: %v", fake.statements())
	}
}

// TestASettingNameThatIsNotOneIsRefused covers the guard on the one place a
// value is interpolated: SHOW GLOBAL VARIABLES takes no placeholder, so the
// name is checked to be a name.
func TestASettingNameThatIsNotOneIsRefused(t *testing.T) {
	fake := &fakeDB{}
	db := fake.open(t)

	for _, name := range []string{"", "read_only'; DROP TABLE x; --", "read only", "read-only", "x1"} {
		if _, err := globalVariable(context.Background(), db, name); err == nil {
			t.Errorf("globalVariable(%q) was accepted", name)
		}
	}
	if len(fake.statements()) != 0 {
		t.Errorf("a statement was sent for a name that is not one: %v", fake.statements())
	}
}

// recordingFieldLogger captures warnings, which is the whole behaviour of the
// paths that do not refuse.
type recordingFieldLogger struct {
	lines []string
}

func (r *recordingFieldLogger) logger() logrus.FieldLogger {
	log := logrus.New()
	log.SetOutput(r)
	log.SetLevel(logrus.DebugLevel)
	return log
}

func (r *recordingFieldLogger) Write(p []byte) (int, error) {
	r.lines = append(r.lines, string(p))
	return len(p), nil
}

func (r *recordingFieldLogger) saw(substring string) bool {
	for _, line := range r.lines {
		if strings.Contains(line, substring) {
			return true
		}
	}
	return false
}
