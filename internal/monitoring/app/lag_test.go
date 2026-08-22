package app

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/platform/slack"
	"github.com/sirupsen/logrus"
)

// recordingNotifier stands in for Slack.
type recordingNotifier struct {
	configured bool
	messages   []string
	err        error
}

func (n *recordingNotifier) IsConfigured() bool { return n.configured }

func (n *recordingNotifier) SendNotification(_ context.Context, message string, _ *slack.SlackNotificationOptions) error {
	n.messages = append(n.messages, message)
	return n.err
}

func quiet() *logrus.Logger {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return l
}

// lagFor records a lag reading for a task and removes it afterwards, so the
// tests do not see each other's numbers through the shared registry.
func lagFor(t *testing.T, task string, seconds float64) metrics.Labels {
	t.Helper()

	labels := metrics.Labels{
		"task": task, "engine": "mysql",
		"source": "tokyo:3306/shop", "target": "osaka:3306/shop",
	}
	metrics.SetLag(labels, seconds)
	t.Cleanup(func() { metrics.Default.Forget(labels) })
	return labels
}

func TestATaskWithinTheThresholdIsNotReported(t *testing.T) {
	lagFor(t, "lag-within", 5)
	n := &recordingNotifier{configured: true}

	checkLag(context.Background(), n, quiet(), map[string]time.Time{}, time.Now())

	for _, m := range n.messages {
		if strings.Contains(m, "lag-within") {
			t.Errorf("a task 5s behind was alerted on: %s", m)
		}
	}
}

// TestATaskTooFarBehindIsReported is the number a disaster-recovery setup is
// judged on: how much would be lost if the source went away right now. Nothing
// watched it before.
func TestATaskTooFarBehindIsReported(t *testing.T) {
	lagFor(t, "lag-behind", defaultLagAlertSeconds+60)
	n := &recordingNotifier{configured: true}

	alerted := checkLag(context.Background(), n, quiet(), map[string]time.Time{}, time.Now())

	if len(alerted) == 0 {
		t.Fatal("a task far behind was not alerted on")
	}
	found := false
	for _, m := range n.messages {
		if strings.Contains(m, "lag-behind") {
			found = true
			if !strings.Contains(m, "tokyo:3306/shop") || !strings.Contains(m, "osaka:3306/shop") {
				t.Errorf("the alert does not name both endpoints: %s", m)
			}
			if !strings.Contains(m, "would be lost") {
				t.Errorf("the alert does not say what is at stake: %s", m)
			}
		}
	}
	if !found {
		t.Errorf("no message named the task: %v", n.messages)
	}
}

// TestTheSameTaskIsNotReportedRepeatedly pins the cool-down: a task that stays
// behind would otherwise alert every minute for as long as it took to fix.
func TestTheSameTaskIsNotReportedRepeatedly(t *testing.T) {
	lagFor(t, "lag-repeat", defaultLagAlertSeconds+60)
	n := &recordingNotifier{configured: true}
	seen := map[string]time.Time{}
	now := time.Now()

	first := checkLag(context.Background(), n, quiet(), seen, now)
	second := checkLag(context.Background(), n, quiet(), seen, now.Add(time.Minute))

	if len(first) == 0 {
		t.Fatal("the first check did not alert")
	}
	if len(second) != 0 {
		t.Errorf("the second check alerted again inside the cool-down: %v", second)
	}

	third := checkLag(context.Background(), n, quiet(), seen, now.Add(lagAlertCooldown+time.Minute))
	if len(third) == 0 {
		t.Error("the task was never reported again after the cool-down")
	}
}

func TestTheThresholdComesFromTheEnvironment(t *testing.T) {
	if got := lagThreshold(); got != defaultLagAlertSeconds {
		t.Errorf("threshold = %v, want the default", got)
	}

	t.Setenv("SYNC_LAG_ALERT_SECONDS", "30")
	if got := lagThreshold(); got != 30 {
		t.Errorf("threshold = %v, want 30", got)
	}

	for _, bad := range []string{"not a number", "0", "-5"} {
		t.Setenv("SYNC_LAG_ALERT_SECONDS", bad)
		if got := lagThreshold(); got != defaultLagAlertSeconds {
			t.Errorf("threshold = %v for %q, want the default", got, bad)
		}
	}
}

// TestAnAlertIsLoggedEvenWithNoSlack covers the deployment that has no webhook
// configured: the alert still has to reach the log, which is what an operator
// has left.
func TestAnAlertIsLoggedEvenWithNoSlack(t *testing.T) {
	lagFor(t, "lag-nolog", defaultLagAlertSeconds+60)

	if got := checkLag(context.Background(), nil, quiet(), map[string]time.Time{}, time.Now()); len(got) == 0 {
		t.Error("nothing was reported with no notifier configured")
	}

	unconfigured := &recordingNotifier{configured: false}
	if got := checkLag(context.Background(), unconfigured, quiet(), map[string]time.Time{}, time.Now()); len(got) == 0 {
		t.Error("nothing was reported with an unconfigured notifier")
	}
	if len(unconfigured.messages) != 0 {
		t.Error("a message was sent through an unconfigured notifier")
	}
}

// TestASlackFailureDoesNotStopTheCheck covers the webhook being down, which
// must not stop the remaining tasks from being examined.
func TestASlackFailureDoesNotStopTheCheck(t *testing.T) {
	lagFor(t, "lag-slackfail-a", defaultLagAlertSeconds+60)
	lagFor(t, "lag-slackfail-b", defaultLagAlertSeconds+60)
	n := &recordingNotifier{configured: true, err: errors.New("webhook is down")}

	alerted := checkLag(context.Background(), n, quiet(), map[string]time.Time{}, time.Now())

	if len(alerted) < 2 {
		t.Errorf("only %d tasks were examined after a send failure: %v", len(alerted), alerted)
	}
}

func TestStartLagAlertingStopsWithItsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	StartLagAlerting(ctx, nil, quiet())
	cancel()
	// Nothing to assert beyond it not panicking or blocking: the goroutine
	// returns on the cancelled context.
}

// deadLettersFor records dead-lettered operations for a task and removes them
// afterwards, so the tests do not see each other's numbers.
func deadLettersFor(t *testing.T, task string, count float64) metrics.Labels {
	t.Helper()

	labels := metrics.Labels{
		"task": task, "engine": "mongodb", "collection": "orders",
		"source": "tokyo:27017/shop", "target": "osaka:27017/shop",
	}
	metrics.SetDeadLettered(labels, count)
	t.Cleanup(func() { metrics.Default.Forget(labels) })
	return labels
}

// TestADeadLetteredChangeIsReported covers the alert on the hole a dead letter
// leaves. The operation is on the source and not on the target, and it lived in
// a file on the syncer's own disk with a counter nobody watched — so the copy
// that exists to be switched to could be missing rows and the first anybody
// heard of it was after the switch.
func TestADeadLetteredChangeIsReported(t *testing.T) {
	deadLettersFor(t, "dl-1", 4)
	n := &recordingNotifier{configured: true}

	alerted := checkDeadLetters(context.Background(), n, quiet(),
		map[string]time.Time{}, time.Now())

	if len(alerted) != 1 {
		t.Fatalf("%d alerts, want one: %v", len(alerted), alerted)
	}
	if len(n.messages) != 1 {
		t.Fatalf("%d notifications, want one", len(n.messages))
	}
	message := n.messages[0]
	for _, want := range []string{"4 change(s)", "dl-1", "orders", "tokyo:27017/shop", "osaka:27017/shop"} {
		if !strings.Contains(message, want) {
			t.Errorf("message = %q, want it to carry %q", message, want)
		}
	}
}

// TestATaskWithNoDeadLettersIsNotReported records that a counter sitting at zero
// is silence. Alerting on it would train everybody to ignore the alert.
func TestATaskWithNoDeadLettersIsNotReported(t *testing.T) {
	deadLettersFor(t, "dl-2", 0)
	n := &recordingNotifier{configured: true}

	if alerted := checkDeadLetters(context.Background(), n, quiet(),
		map[string]time.Time{}, time.Now()); len(alerted) != 0 {
		t.Errorf("alerts = %v, want none for a task with nothing dead-lettered", alerted)
	}
	if len(n.messages) != 0 {
		t.Errorf("a notification was sent for a task with nothing dead-lettered: %v", n.messages)
	}
}

// TestTheSameDeadLetterIsNotReportedRepeatedly records the cooldown. The counter
// stays raised until the retry succeeds, so without one every tick would send
// the same alert.
func TestTheSameDeadLetterIsNotReportedRepeatedly(t *testing.T) {
	deadLettersFor(t, "dl-3", 2)
	n := &recordingNotifier{configured: true}
	lastAlert := map[string]time.Time{}
	now := time.Now()

	first := checkDeadLetters(context.Background(), n, quiet(), lastAlert, now)
	if len(first) != 1 {
		t.Fatalf("the first check reported %v, want one alert", first)
	}

	again := checkDeadLetters(context.Background(), n, quiet(), lastAlert, now.Add(time.Minute))
	if len(again) != 0 {
		t.Errorf("the same dead letter was reported again within the cooldown: %v", again)
	}

	later := checkDeadLetters(context.Background(), n, quiet(), lastAlert, now.Add(2*lagAlertCooldown))
	if len(later) != 1 {
		t.Errorf("nothing was reported after the cooldown had passed: %v", later)
	}
}

// TestADeadLetterIsLoggedEvenWithNoSlack records that the alert reaches the log
// whether or not a webhook is configured. A deployment with no Slack must still
// leave a trace of a replica that is missing rows.
func TestADeadLetterIsLoggedEvenWithNoSlack(t *testing.T) {
	deadLettersFor(t, "dl-4", 7)

	log := logrus.New()
	var out strings.Builder
	log.SetOutput(&out)

	alerted := checkDeadLetters(context.Background(), nil, log, map[string]time.Time{}, time.Now())
	if len(alerted) != 1 {
		t.Fatalf("%d alerts, want one: %v", len(alerted), alerted)
	}
	if !strings.Contains(out.String(), "7 change(s)") {
		t.Errorf("output = %q, want the count in it", out.String())
	}
}

// TestASlackFailureDoesNotStopTheDeadLetterCheck records that one task whose
// notification fails does not silence the rest.
func TestASlackFailureDoesNotStopTheDeadLetterCheck(t *testing.T) {
	deadLettersFor(t, "dl-5", 1)
	n := &recordingNotifier{configured: true, err: errors.New("slack is down")}

	if alerted := checkDeadLetters(context.Background(), n, quiet(),
		map[string]time.Time{}, time.Now()); len(alerted) != 1 {
		t.Errorf("alerts = %v, want the check to have carried on", alerted)
	}
}
