package app

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/platform/slack"
	"github.com/sirupsen/logrus"
)

const (
	// defaultLagAlertSeconds is how far behind a task may fall before it is
	// worth waking somebody. Five minutes is a recovery point objective a
	// payment system can state; the number is what makes the objective
	// measurable rather than aspirational.
	defaultLagAlertSeconds = 300
	// lagCheckInterval is how often the recorded lag is compared to the
	// threshold.
	lagCheckInterval = time.Minute
	// lagAlertCooldown is how long the same task stays quiet after alerting, so
	// a task that is far behind reports once rather than every minute.
	lagAlertCooldown = 15 * time.Minute
)

// lagThreshold reports the number of seconds a task may fall behind before it
// is alerted on.
//
// The stored setting is the value, and zero there means the built-in default.
// SYNC_LAG_ALERT_SECONDS still overrules it: that is how this is deployed
// today, and an upgrade that ignored it would move the threshold without
// anyone asking.
func lagThreshold() float64 {
	raw := os.Getenv("SYNC_LAG_ALERT_SECONDS")
	if raw == "" {
		if stored, err := config.LoadSettings(); err == nil && stored.LagAlertSeconds > 0 {
			return stored.LagAlertSeconds
		}
		return defaultLagAlertSeconds
	}
	seconds, err := strconv.ParseFloat(raw, 64)
	if err != nil || seconds <= 0 {
		return defaultLagAlertSeconds
	}
	return seconds
}

// StartLagAlerting watches the recorded replication lag and reports a task
// that has fallen too far behind. Nothing watched it before: a task could be
// an hour behind, or stalled entirely, and the only sign was a graph nobody
// had because there were no metrics either.
func StartLagAlerting(ctx context.Context, cfg *config.Config, log *logrus.Logger) {
	watch(func() {
		var n notifier
		if cfg != nil {
			n = slack.NewSlackNotifierFromConfig(cfg, log)
		}
		lastAlert := map[string]time.Time{}
		ticker := time.NewTicker(lagCheckInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				checkLag(ctx, n, log, lastAlert, time.Now())
				checkDeadLetters(ctx, n, log, lastAlert, time.Now())
			}
		}
	})
}

// notifier is the part of the Slack client this needs, so the check can be
// exercised without one.
type notifier interface {
	IsConfigured() bool
	SendNotification(ctx context.Context, message string, opts *slack.SlackNotificationOptions) error
}

// checkDeadLetters reports the tasks holding changes the target never
// accepted. A dead-lettered operation is a hole in the replica: the source has
// it and the target does not, and the only thing that will ever close it is
// the retry loop succeeding.
func checkDeadLetters(ctx context.Context, n notifier, log *logrus.Logger, lastAlert map[string]time.Time, now time.Time) []string {
	var alerted []string
	for _, sample := range metrics.Default.Snapshot(metrics.DeadLettered) {
		if sample.Value < 1 {
			continue
		}
		key := "dead-letter:" + sample.Labels.Key()
		if at, seen := lastAlert[key]; seen && now.Sub(at) < lagAlertCooldown {
			continue
		}
		lastAlert[key] = now
		alerted = append(alerted, key)

		message := fmt.Sprintf(
			"\u26a0\ufe0f %.0f change(s) could not be applied to the target\n\nTask: %s\nEngine: %s\nCollection: %s\nSource: %s\nTarget: %s\n\nThe source has them and the target does not. They are held for retry; until that succeeds the replica is missing them.",
			sample.Value, sample.Labels["task"], sample.Labels["engine"],
			sample.Labels["collection"], sample.Labels["source"], sample.Labels["target"])

		log.Error(message)
		if n == nil || !n.IsConfigured() {
			continue
		}
		if err := n.SendNotification(ctx, message, &slack.SlackNotificationOptions{
			AlertType: slack.SlackAlertDanger,
			Trigger:   "dead-lettered-changes",
		}); err != nil {
			log.Warnf("[Monitor] Could not send the dead letter alert: %v", err)
		}
	}
	sort.Strings(alerted)
	return alerted
}

// checkLag reports the tasks that are further behind than the threshold, and
// returns what it alerted on so a test can see it.
func checkLag(ctx context.Context, n notifier, log *logrus.Logger, lastAlert map[string]time.Time, now time.Time) []string {
	threshold := lagThreshold()

	var alerted []string
	for _, sample := range metrics.Default.Snapshot(metrics.LagSeconds) {
		if sample.Value < threshold {
			continue
		}
		key := sample.Labels.Key()
		if at, seen := lastAlert[key]; seen && now.Sub(at) < lagAlertCooldown {
			continue
		}
		lastAlert[key] = now
		alerted = append(alerted, key)

		message := fmt.Sprintf(
			"⚠️ Replication is %.0fs behind\n\nTask: %s\nEngine: %s\nSource: %s\nTarget: %s\n\n"+
				"Everything written at the source in the last %.0f seconds would be lost "+
				"if it became unavailable now.",
			sample.Value, sample.Labels["task"], sample.Labels["engine"],
			sample.Labels["source"], sample.Labels["target"], sample.Value)

		log.Warn(message)
		if n == nil || !n.IsConfigured() {
			continue
		}
		if err := n.SendNotification(ctx, message, &slack.SlackNotificationOptions{
			AlertType: slack.SlackAlertWarning,
			Trigger:   "replication-lag",
		}); err != nil {
			log.Warnf("[Monitor] Could not send the replication lag alert: %v", err)
		}
	}
	sort.Strings(alerted)
	return alerted
}
