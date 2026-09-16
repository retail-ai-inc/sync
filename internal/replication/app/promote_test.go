package app

import (
	"context"
	"strings"
	"testing"
)

// A failover leaves nothing behind in the databases, so somebody has to record
// it. These cover the dispatch and the refusals; the engines' stores are
// exercised against real servers in promote_integration_test.go.

func TestPromotingSomethingThatIsNotATaskIsRefused(t *testing.T) {
	useTempTaskDB(t)
	ctx := context.Background()

	for _, id := range []string{"", "abc", "1.5", "-"} {
		if err := PromoteTarget(ctx, id, "operator"); err == nil {
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

	if err := PromoteTarget(context.Background(), "404", "operator"); err == nil {
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

	err := PromoteTarget(context.Background(), itoa(id), "operator")
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

	if err := PromoteTarget(context.Background(), itoa(id), "operator"); err == nil {
		t.Error("promoting a target that could not be reached reported success")
	}
}
