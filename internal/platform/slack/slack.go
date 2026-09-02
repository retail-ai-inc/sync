package slack

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

type SlackNotifier struct {
	webhookURL string
	channel    string
	username   string
	scriptPath string
	logger     *logrus.Logger
}

type SlackAlertType string

const (
	SlackAlertGood    SlackAlertType = "good"    // Green color for success
	SlackAlertWarning SlackAlertType = "warning" // Yellow color for warnings
	SlackAlertDanger  SlackAlertType = "danger"  // Red color for errors
)

type SlackNotificationOptions struct {
	AlertType   SlackAlertType
	BranchName  string
	Trigger     string
	CommitURL   string
	ExtraFields map[string]string
}

func NewSlackNotifier(webhookURL, channel string, logger *logrus.Logger) *SlackNotifier {
	return &SlackNotifier{
		webhookURL: webhookURL,
		channel:    channel,
		username:   "sync-service",
		scriptPath: findCloudBuildScript(),
		logger:     logger,
	}
}

func NewSlackNotifierFromConfig(cfg ConfigProvider, logger *logrus.Logger) *SlackNotifier {
	return NewSlackNotifier(cfg.GetSlackWebhookURL(), cfg.GetSlackChannel(), logger)
}

// NewSlackNotifierFromConfigWithFieldLogger creates a SlackNotifier from config interface with FieldLogger
func NewSlackNotifierFromConfigWithFieldLogger(cfg ConfigProvider, logger logrus.FieldLogger) *SlackNotifier {
	// Create a new logger instance and copy the level if possible
	newLogger := logrus.New()
	if stdLogger, ok := logger.(*logrus.Logger); ok {
		newLogger.SetLevel(stdLogger.GetLevel())
		newLogger.SetFormatter(stdLogger.Formatter)
	}
	return NewSlackNotifier(cfg.GetSlackWebhookURL(), cfg.GetSlackChannel(), newLogger)
}

type ConfigProvider interface {
	GetSlackWebhookURL() string
	GetSlackChannel() string
}

func findCloudBuildScript() string {
	possiblePaths := []string{
		"/app/cloudbuild.sh",
		"./cloudbuild.sh",
		"../cloudbuild.sh",
	}

	for _, path := range possiblePaths {
		if _, err := os.Stat(path); err == nil {
			absPath, _ := filepath.Abs(path)
			return absPath
		}
	}

	return ""
}

// IsConfigured checks if Slack notification is properly configured. It no
// longer requires cloudbuild.sh.
func (s *SlackNotifier) IsConfigured() bool {
	return s.webhookURL != ""
}

// SendNotification sends a notification to Slack.
//
// It posts to the webhook directly. The script is still used when one is
// present, because a deployment may rely on what it adds, but it is no longer
// required — and an alert that could not be sent is now an error rather than a
// debug line and a nil.
func (s *SlackNotifier) SendNotification(ctx context.Context, message string, opts *SlackNotificationOptions) error {
	if !s.IsConfigured() {
		// Not an error: a deployment with no webhook has asked for no alerts.
		// One that has a webhook and cannot reach it is a different thing, and
		// that one is reported.
		s.logger.Debugf("[Slack] Notification skipped - no webhook configured: %s", message)
		return nil
	}

	if s.scriptPath == "" {
		return s.postToWebhook(ctx, message, opts)
	}

	if opts == nil {
		opts = &SlackNotificationOptions{
			AlertType: SlackAlertGood,
		}
	}

	args := []string{
		"-w", s.webhookURL,
		"-c", s.channel,
		"-u", s.username,
		"-m", message,
	}

	if opts.AlertType != "" {
		args = append(args, "-a", string(opts.AlertType))
	}

	if opts.BranchName != "" {
		args = append(args, "-b", opts.BranchName)
	}
	if opts.Trigger != "" {
		args = append(args, "-t", opts.Trigger)
	}
	if opts.CommitURL != "" {
		args = append(args, "-U", opts.CommitURL)
	}

	cmdArgs := append([]string{s.scriptPath}, args...)
	fullCommand := strings.Join(cmdArgs, " ")

	s.logger.Infof("[Slack] Sending notification: %s", message)
	s.logger.Debugf("[Slack] Executing command: %s", fullCommand)

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Use exec.Command with separate arguments instead of bash -c
	cmd := exec.CommandContext(ctx, s.scriptPath, args...)
	cmd.Stderr = os.Stderr

	output, err := cmd.Output()
	if err != nil {
		s.logger.Errorf("[Slack] Failed to send notification: %v, output: %s", err, string(output))
		return fmt.Errorf("slack notification failed: %w", err)
	}

	s.logger.Infof("[Slack] Notification sent successfully")
	s.logger.Debugf("[Slack] Command output: %s", string(output))

	return nil
}

// postToWebhook sends the message to Slack over HTTP.
func (s *SlackNotifier) postToWebhook(ctx context.Context, message string, opts *SlackNotificationOptions) error {
	if opts == nil {
		opts = &SlackNotificationOptions{AlertType: SlackAlertGood}
	}

	attachment := map[string]interface{}{
		"color": string(opts.AlertType),
		"text":  message,
	}
	var fields []map[string]interface{}
	for name, value := range opts.ExtraFields {
		fields = append(fields, map[string]interface{}{"title": name, "value": value, "short": true})
	}
	if opts.Trigger != "" {
		fields = append(fields, map[string]interface{}{"title": "Trigger", "value": opts.Trigger, "short": true})
	}
	if opts.BranchName != "" {
		fields = append(fields, map[string]interface{}{"title": "Branch", "value": opts.BranchName, "short": true})
	}
	if opts.CommitURL != "" {
		fields = append(fields, map[string]interface{}{"title": "Commit", "value": opts.CommitURL, "short": false})
	}
	if len(fields) > 0 {
		// A stable order, so the same alert always reads the same way.
		sort.Slice(fields, func(i, j int) bool {
			return fields[i]["title"].(string) < fields[j]["title"].(string)
		})
		attachment["fields"] = fields
	}

	payload := map[string]interface{}{
		"username":    s.username,
		"text":        message,
		"attachments": []interface{}{attachment},
	}
	if s.channel != "" {
		payload["channel"] = s.channel
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("render the Slack message: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.webhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build the Slack request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.logger.Errorf("[Slack] Failed to send notification: %v", err)
		return fmt.Errorf("send the Slack notification: %w", err)
	}
	defer resp.Body.Close()

	answer, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	if resp.StatusCode != http.StatusOK {
		s.logger.Errorf("[Slack] Failed to send notification: %s: %s", resp.Status, answer)
		return fmt.Errorf("send the Slack notification: %s: %s", resp.Status, answer)
	}

	s.logger.Infof("[Slack] Notification sent successfully")
	return nil
}

func (s *SlackNotifier) SendSuccess(ctx context.Context, operation, details string) error {
	message := fmt.Sprintf("✅ %s completed successfully", operation)
	if details != "" {
		message += fmt.Sprintf("\n%s", details)
	}

	return s.SendNotification(ctx, message, &SlackNotificationOptions{
		AlertType: SlackAlertGood,
		Trigger:   operation,
	})
}

func (s *SlackNotifier) SendWarning(ctx context.Context, operation, warning string) error {
	message := fmt.Sprintf("⚠️ %s completed with warnings", operation)
	if warning != "" {
		message += fmt.Sprintf("\n%s", warning)
	}

	return s.SendNotification(ctx, message, &SlackNotificationOptions{
		AlertType: SlackAlertWarning,
		Trigger:   operation,
	})
}

func (s *SlackNotifier) SendError(ctx context.Context, operation, errorMsg string) error {
	message := fmt.Sprintf("❌ %s failed", operation)
	if errorMsg != "" {
		message += fmt.Sprintf("\nError: %s", errorMsg)
	}

	return s.SendNotification(ctx, message, &SlackNotificationOptions{
		AlertType: SlackAlertDanger,
		Trigger:   operation,
	})
}

func (s *SlackNotifier) SendSyncStatus(ctx context.Context, syncType, sourceName, targetName string, recordCount int, duration time.Duration) error {
	message := fmt.Sprintf("🔄 %s Sync Completed", syncType)
	details := fmt.Sprintf("Source: %s → Target: %s\nRecords: %d\nDuration: %v",
		sourceName, targetName, recordCount, duration)

	return s.SendNotification(ctx, message+"\n"+details, &SlackNotificationOptions{
		AlertType: SlackAlertGood,
		Trigger:   fmt.Sprintf("%s-sync", strings.ToLower(syncType)),
	})
}

func (s *SlackNotifier) SendBackupStatus(ctx context.Context, backupType, tableName, fileName string, fileSize int64) error {
	message := fmt.Sprintf("💾 %s Backup Completed", backupType)
	details := fmt.Sprintf("Table: %s\nFile: %s\nSize: %s",
		tableName, fileName, formatFileSize(fileSize))

	return s.SendNotification(ctx, message+"\n"+details, &SlackNotificationOptions{
		AlertType: SlackAlertGood,
		Trigger:   fmt.Sprintf("%s-backup", strings.ToLower(backupType)),
	})
}

func formatFileSize(size int64) string {
	const unit = 1024
	if size < unit {
		return fmt.Sprintf("%d B", size)
	}
	div, exp := int64(unit), 0
	for n := size / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(size)/float64(div), "KMGTPE"[exp])
}
