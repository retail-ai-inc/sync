package app

import (
	"context"
	"strconv"
	"strings"
	"testing"
)

// TaskProgress reaches both databases, so what can be covered without them is
// the dispatch and the refusals -- and the refusals are the part that matters:
// each one has to fail loudly rather than answer with a zero-valued report,
// because an empty report read as "caught up" is what would let somebody
// promote a region that holds nothing.

func TestTaskProgressRefusesANonNumericID(t *testing.T) {
	useTempTaskDB(t)

	for _, id := range []string{"", "abc", "1.5", "42abc", " 42"} {
		progress, err := TaskProgress(context.Background(), id)
		if err == nil {
			t.Errorf("TaskProgress(%q) was accepted", id)
		}
		if progress.CaughtUp() {
			t.Errorf("TaskProgress(%q) reported caught up", id)
		}
	}
}

func TestTaskProgressOfATaskThatIsNotThere(t *testing.T) {
	useTempTaskDB(t)

	progress, err := TaskProgress(context.Background(), "9999")
	if err == nil {
		t.Fatal("a task that does not exist was reported on successfully")
	}
	if progress.CaughtUp() {
		t.Error("a task that does not exist reported itself caught up")
	}
}

// TestTaskProgressRefusesAnEngineThatRecordsNoPosition covers PostgreSQL, which
// does not go through the shared pipeline and records nothing this can read. It
// says so by name rather than reporting an empty result, which would read as a
// task with nothing outstanding.
func TestTaskProgressRefusesAnEngineThatRecordsNoPosition(t *testing.T) {
	db := useTempTaskDB(t)
	id := insertTask(t, db, 1, `{"type":"postgresql","taskName":"pg"}`)

	progress, err := TaskProgress(context.Background(), strconv.FormatInt(id, 10))
	if err == nil {
		t.Fatal("a PostgreSQL task was reported on as though it recorded a position")
	}
	if !strings.Contains(err.Error(), "postgresql") {
		t.Errorf("the refusal does not name the engine: %v", err)
	}
	if progress.CaughtUp() {
		t.Error("a task with no position reported itself caught up")
	}
}

func TestTaskProgressRefusesAnUnknownEngine(t *testing.T) {
	db := useTempTaskDB(t)
	id := insertTask(t, db, 1, `{"type":"cassandra","taskName":"nope"}`)

	if _, err := TaskProgress(context.Background(), strconv.FormatInt(id, 10)); err == nil {
		t.Fatal("an engine this build does not replicate was reported on")
	}
}

// StoredEndpointPassword resolves the mask an edit form carries. Without it,
// opening a task to look at it asks the operator to retype a password, and
// saving the untouched field would store "********" as the credential.

func TestStoredEndpointPasswordResolvesEachEnd(t *testing.T) {
	db := useTempTaskDB(t)
	id := insertTask(t, db, 1, `{"type":"mysql","taskName":"t",
		"sourceConn":{"host":"10.118.192.8","password":"source-pw"},
		"targetConn":{"host":"10.118.192.9","password":"target-pw"}}`)
	taskID := strconv.FormatInt(id, 10)

	for role, want := range map[string]string{"source": "source-pw", "target": "target-pw"} {
		got, ok := StoredEndpointPassword(taskID, role)
		if !ok {
			t.Errorf("%s password was not resolved", role)
			continue
		}
		if got != want {
			t.Errorf("%s password = %q, want %q", role, got, want)
		}
	}
}

// TestAnyRoleButTargetIsTheSource pins the branch: the check is for "target",
// so anything else reads the source rather than failing. A typo in the role
// must not silently hand out the wrong end's credential.
func TestAnyRoleButTargetIsTheSource(t *testing.T) {
	db := useTempTaskDB(t)
	id := insertTask(t, db, 1, `{"type":"mysql","taskName":"t",
		"sourceConn":{"password":"source-pw"},
		"targetConn":{"password":"target-pw"}}`)

	got, ok := StoredEndpointPassword(strconv.FormatInt(id, 10), "somethingelse")
	if !ok || got != "source-pw" {
		t.Errorf("an unrecognised role gave %q (%v), want the source's", got, ok)
	}
}

// TestAnEmptyStoredPasswordIsReportedAsAbsent: the caller's next move is to
// refuse the probe and say the mask could not be resolved, which is right --
// connecting with an empty password would either fail or, worse, succeed.
func TestAnEmptyStoredPasswordIsReportedAsAbsent(t *testing.T) {
	db := useTempTaskDB(t)
	id := insertTask(t, db, 1, `{"type":"mysql","taskName":"t",
		"sourceConn":{"host":"10.118.192.8"},"targetConn":{}}`)
	taskID := strconv.FormatInt(id, 10)

	for _, role := range []string{"source", "target"} {
		if got, ok := StoredEndpointPassword(taskID, role); ok {
			t.Errorf("%s reported a password %q where none is stored", role, got)
		}
	}
}

// TestStoredEndpointPasswordReportsFalseForEveryFailure covers the reason it
// returns a bool rather than an error: the caller's next move is the same in
// each case.
func TestStoredEndpointPasswordReportsFalseForEveryFailure(t *testing.T) {
	db := useTempTaskDB(t)
	insertTask(t, db, 1, `{not json`)

	for name, id := range map[string]string{
		"no such task":             "9999",
		"not a number":             "abc",
		"unreadable stored config": "1",
	} {
		t.Run(name, func(t *testing.T) {
			if got, ok := StoredEndpointPassword(id, "source"); ok {
				t.Errorf("resolved %q from a task that cannot be read", got)
			}
		})
	}
}
