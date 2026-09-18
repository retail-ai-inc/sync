package app

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/retail-ai-inc/sync/internal/platform/config"
)

// A failover leaves nothing behind in the databases, so somebody has to record
// it. These cover the dispatch and the refusals; the engines' stores are
// exercised against real servers in promote_integration_test.go.

func TestPromotingSomethingThatIsNotATaskIsRefused(t *testing.T) {
	useTempTaskDB(t)
	ctx := context.Background()

	for _, id := range []string{"", "abc", "1.5", "-"} {
		if _, err := PromoteTarget(ctx, id, "operator"); err == nil {
			t.Errorf("PromoteTarget(%q) was accepted", id)
		} else if !strings.Contains(err.Error(), "not a task id") {
			t.Errorf("PromoteTarget(%q) = %v, want it to say what was wrong", id, err)
		}
		if err := DemoteTarget(ctx, id); err == nil {
			t.Errorf("DemoteTarget(%q) was accepted", id)
		}
	}
}

func TestPromotingATaskThatDoesNotExistIsRefused(t *testing.T) {
	useTempTaskDB(t)

	if _, err := PromoteTarget(context.Background(), "404", "operator"); err == nil {
		t.Error("a task that does not exist was promoted")
	}
	if _, _, err := TargetPromotion(context.Background(), "404"); err == nil {
		t.Error("a task that does not exist reported a promotion")
	}
}

// An engine with no direction claim says so rather than reporting success and
// leaving the target unprotected.
func TestAnEngineWithNoDirectionClaimSaysSo(t *testing.T) {
	db := useTempTaskDB(t)
	id := insertTask(t, db, 1, `{"type":"cassandra","sourceConn":{"host":"a"},"targetConn":{"host":"b"}}`)

	_, err := PromoteTarget(context.Background(), itoa(id), "operator")
	if err == nil {
		t.Fatal("a cassandra task reported its target promoted")
	}
	if !strings.Contains(err.Error(), "cassandra") {
		t.Errorf("error = %v, want it to name the engine", err)
	}
}

// A target that cannot be reached is an error, not a silent no-op: believing a
// promotion was recorded when it was not is how the old task comes back and
// overwrites Osaka.
func TestAPromotionThatCouldNotBeWrittenIsReported(t *testing.T) {
	db := useTempTaskDB(t)
	id := insertTask(t, db, 1, `{"type":"redis",
		"sourceConn":{"host":"127.0.0.1","port":"1","database":"0"},
		"targetConn":{"host":"127.0.0.1","port":"1","database":"0"}}`)

	if _, err := PromoteTarget(context.Background(), itoa(id), "operator"); err == nil {
		t.Error("promoting a target that could not be reached reported success")
	}
}

// The tasks stopped with a promotion are the ones writing into that database:
// matched on the endpoint without credentials, so a different user or a
// mariadb label does not hide one, and not the tasks reading out of it or the
// ones already stopped.
func TestOnlyTheEnabledTasksWritingIntoThePromotedTargetAreStopped(t *testing.T) {
	osaka := "tcp(osaka:3306)/pay"
	tasks := []config.SyncConfig{
		{ID: 1, Enable: true, Type: "mysql", SourceConnection: "sync:x@tcp(tokyo:3306)/pay", TargetConnection: "sync:x@" + osaka},
		{ID: 2, Enable: true, Type: "mariadb", SourceConnection: "sync:x@tcp(nagoya:3306)/pay", TargetConnection: "other:y@" + osaka + "?charset=utf8"},
		{ID: 3, Enable: false, Type: "mysql", SourceConnection: "sync:x@tcp(tokyo:3306)/pay", TargetConnection: "sync:x@" + osaka},
		{ID: 4, Enable: true, Type: "mysql", SourceConnection: "sync:x@tcp(tokyo:3306)/pay", TargetConnection: "sync:x@tcp(osaka:3306)/other"},
		{ID: 5, Enable: true, Type: "mysql", SourceConnection: "sync:x@" + osaka, TargetConnection: "sync:x@tcp(tokyo:3306)/pay"},
	}

	got := tasksWritingInto(tasks, tasks[0])
	if want := []int{1, 2}; !reflect.DeepEqual(got, want) {
		t.Errorf("tasksWritingInto = %v, want %v", got, want)
	}
}

// A task that could not be stopped is reported as such, not as a failed
// promotion: the marker is already on the target, and "try again" is the wrong
// advice when what is needed is to stop that task by hand.
func TestATaskThatCouldNotBeStoppedNamesItself(t *testing.T) {
	err := error(&StopAfterPromotion{TaskID: 7, Err: errors.New("database is locked")})
	var halfDone *StopAfterPromotion
	if !errors.As(err, &halfDone) || halfDone.TaskID != 7 {
		t.Fatalf("errors.As did not find the task: %v", err)
	}
	for _, want := range []string{"marked as promoted", "task 7", "still be writing", "database is locked"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not say %q", err, want)
		}
	}
}
