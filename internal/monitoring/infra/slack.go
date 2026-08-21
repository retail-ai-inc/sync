package infra

import (
	"fmt"
	"time"

	// "github.com/sirupsen/logrus"
	"context"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/slack"
	"github.com/sirupsen/logrus"
)

// SendTableComparisonSlackNotification sends Slack notification for a single table comparison
func SendTableComparisonSlackNotification(ctx context.Context, sc config.SyncConfig, srcDB, srcTable, tgtDB, tgtTable string,
	srcCount, tgtCount int64, yesterdayStart time.Time, log *logrus.Logger) {

	// Skip if counts are invalid (error occurred during counting)
	if srcCount < 0 || tgtCount < 0 {
		return
	}

	// Get global config to access Slack settings
	cfg := config.NewConfig()
	slackNotifier := slack.NewSlackNotifierFromConfig(cfg, log)
	if !slackNotifier.IsConfigured() {
		return
	}

	// Calculate difference and determine status
	difference := srcCount - tgtCount
	status := "✅"
	alertType := slack.SlackAlertGood

	if difference != 0 {
		status = "⚠️"
		alertType = slack.SlackAlertWarning
	}

	// Format message
	yesterday := yesterdayStart.Format("2006-01-02")
	message := fmt.Sprintf("%s Daily Data Comparison\n\nTask ID: %d\nDate: %s (JST)\nType: MONGODB\n\n%s %s.%s → %s.%s\nSource: %d\nTarget: %d\nDifference: %d",
		status, sc.ID, yesterday, status, srcDB, srcTable, tgtDB, tgtTable, srcCount, tgtCount, difference)

	// Send notification asynchronously
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		err := slackNotifier.SendNotification(ctx, message, &slack.SlackNotificationOptions{
			AlertType: alertType,
			Trigger:   "daily-data-comparison",
		})
		if err != nil {
			log.Warnf("[Monitor] Failed to send Slack notification for task %d, table %s.%s: %v", sc.ID, srcDB, srcTable, err)
		} else {
			log.Infof("[Monitor] Slack notification sent for task %d, table %s.%s (diff: %d)", sc.ID, srcDB, srcTable, difference)
		}
	}()
}
