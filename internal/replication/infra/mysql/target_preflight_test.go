package mysql

import (
	"strings"
	"testing"
)

func TestIsOn(t *testing.T) {
	for _, on := range []string{"ON", "on", "On", "1"} {
		if !isOn(on) {
			t.Errorf("isOn(%q) = false", on)
		}
	}
	for _, off := range []string{"OFF", "off", "0", ""} {
		if isOn(off) {
			t.Errorf("isOn(%q) = true", off)
		}
	}
}

// TestDescribeEventSchedulerSaysWhyItDiverges covers a target that changes rows
// of its own accord: the two sides drift apart while the task reports that it
// applied everything, because it did.
func TestDescribeEventSchedulerSaysWhyItDiverges(t *testing.T) {
	if got := describeEventScheduler("ON"); got == "" {
		t.Fatal("a target running events raised nothing")
	}
	for _, off := range []string{"OFF", "0", ""} {
		if got := describeEventScheduler(off); got != "" {
			t.Errorf("describeEventScheduler(%q) = %q, want nothing", off, got)
		}
	}
}

func TestDescribeTriggersNamesThem(t *testing.T) {
	got := describeTriggers([]string{"orders.orders_ai", "payments.payments_au"})
	if got == "" {
		t.Fatal("triggers on the target raised nothing")
	}
	for _, name := range []string{"orders.orders_ai", "payments.payments_au"} {
		if !strings.Contains(got, name) {
			t.Errorf("the warning does not name %q: %q", name, got)
		}
	}
	if describeTriggers(nil) != "" {
		t.Error("a target with no triggers raised a warning")
	}
}
