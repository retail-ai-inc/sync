package redis

import (
	"strings"
	"testing"
)

// TestAFailoverIsReportedAsAMovedMaster covers the harmless change: the slots
// stayed where they were, so the keys did too, and the position still applies.
func TestAFailoverIsReportedAsAMovedMaster(t *testing.T) {
	before := map[string]string{"0-5460": "a:6379", "5461-10922": "b:6379"}
	after := map[string]string{"0-5460": "c:6379", "5461-10922": "b:6379"}

	got := describe(before, after)
	if !strings.Contains(got, "0-5460 moved from a:6379 to c:6379") {
		t.Errorf("describe = %q, want the moved master named", got)
	}
	if strings.Contains(got, "5461-10922") {
		t.Errorf("describe = %q, want the untouched shard left out", got)
	}
}

// TestAReshardIsReportedAsNewAndDepartedShards covers the dangerous change.
//
// Slots moving between shards is what deletes keys from one master and restores
// them on another, down two connections with no ordering between them. It has to
// be noticed, which means being described rather than silently tolerated.
func TestAReshardIsReportedAsNewAndDepartedShards(t *testing.T) {
	before := map[string]string{"0-8191": "a:6379", "8192-16383": "b:6379"}
	after := map[string]string{
		"0-5460": "a:6379", "5461-10922": "b:6379", "10923-16383": "c:6379",
	}

	got := describe(before, after)
	for _, want := range []string{"0-8191", "8192-16383", "0-5460", "10923-16383"} {
		if !strings.Contains(got, want) {
			t.Errorf("describe = %q, want it to mention %s", got, want)
		}
	}
}

// TestNoChangeIsReportedAsNothing keeps the watcher from crying wolf every time
// it looks.
func TestNoChangeIsReportedAsNothing(t *testing.T) {
	shape := map[string]string{"0-5460": "a:6379", "5461-16383": "b:6379"}
	if got := describe(shape, shape); got != "" {
		t.Errorf("describe of an unchanged shape = %q, want nothing", got)
	}
}
