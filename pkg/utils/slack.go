package utils

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

// SlackNotifier manages Slack notifications using cloudbuild.sh script
type SlackNotifier struct {
	webhookURL   string
	channel      string
	username     string
	scriptPath   string
	logger       *logrus.Logger
}

// SlackAlertType defines the type of alert for Slack message color
type SlackAlertType string

const (
	SlackAlertGood    SlackAlertType = "good"    // Green color for success
	SlackAlertWarning SlackAlertType = "warning" // Yellow color for warnings
	SlackAlertDanger  SlackAlertType = "danger"  // Red color for errors
)

// SlackNotificationOptions contains optional parameters for notifications
type SlackNotificationOptions struct {
	AlertType   SlackAlertType
	BranchName  string
	Trigger     string
	CommitURL   string
	ExtraFields map[string]string
}

// NewSlackNotifier creates a new Slack notifier instance
func NewSlackNotifier(webhookURL, channel string, logger *logrus.Logger) *SlackNotifier {
	return &SlackNotifier{
		webhookURL: webhookURL,
		channel:    channel,
		username:   "sync-service",
		scriptPath: findCloudBuildScript(),
		logger:     logger,
	}
}

// NewSlackNotifierFromConfig creates a SlackNotifier from config interface
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

// ConfigProvider interface for getting Slack configuration
type ConfigProvider interface {
	GetSlackWebhookURL() string
	GetSlackChannel() string
}

// findCloudBuildScript finds the cloudbuild.sh script in possible locations
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

// IsConfigured checks if Slack notification is properly configured
func (s *SlackNotifier) IsConfigured() bool {
	if s.webhookURL == "" || s.channel == "" {
		return false
	}
	
	if s.scriptPath == "" {
		s.logger.Warn("[Slack] cloudbuild.sh script not found in any location")
		return false
	}
	
	return true
}

// SendNotification sends a notification to Slack using the cloudbuild.sh script
func (s *SlackNotifier) SendNotification(ctx context.Context, message string, opts *SlackNotificationOptions) error {
	if !s.IsConfigured() {
		s.logger.Debugf("[Slack] Notification skipped - not configured: %s", message)
		return nil
	}

	// Set default options if not provided
	if opts == nil {
		opts = &SlackNotificationOptions{
			AlertType: SlackAlertGood,
		}
	}

	// Build command arguments
	args := []string{
		"-w", s.webhookURL,
		"-c", s.channel,
		"-u", s.username,
		"-m", message,
	}

	// Add alert type if specified
	if opts.AlertType != "" {
		args = append(args, "-a", string(opts.AlertType))
	}

	// Add optional parameters
	if opts.BranchName != "" {
		args = append(args, "-b", opts.BranchName)
	}
	if opts.Trigger != "" {
		args = append(args, "-t", opts.Trigger)
	}
	if opts.CommitURL != "" {
		args = append(args, "-U", opts.CommitURL)
	}

	// Create the command with proper argument handling
	cmdArgs := append([]string{s.scriptPath}, args...)
	fullCommand := strings.Join(cmdArgs, " ")
	
	s.logger.Infof("[Slack] Sending notification: %s", message)
	s.logger.Debugf("[Slack] Executing command: %s", fullCommand)

	// Execute with timeout context
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

// SendSuccess sends a success notification with good alert type
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

// SendWarning sends a warning notification with warning alert type
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

// SendError sends an error notification with danger alert type
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

// SendSyncStatus sends synchronization status notification
func (s *SlackNotifier) SendSyncStatus(ctx context.Context, syncType, sourceName, targetName string, recordCount int, duration time.Duration) error {
	message := fmt.Sprintf("🔄 %s Sync Completed", syncType)
	details := fmt.Sprintf("Source: %s → Target: %s\nRecords: %d\nDuration: %v", 
		sourceName, targetName, recordCount, duration)

	return s.SendNotification(ctx, message+"\n"+details, &SlackNotificationOptions{
		AlertType: SlackAlertGood,
		Trigger:   fmt.Sprintf("%s-sync", strings.ToLower(syncType)),
	})
}

// SendBackupStatus sends backup completion notification
func (s *SlackNotifier) SendBackupStatus(ctx context.Context, backupType, tableName, fileName string, fileSize int64) error {
	message := fmt.Sprintf("💾 %s Backup Completed", backupType)
	details := fmt.Sprintf("Table: %s\nFile: %s\nSize: %s", 
		tableName, fileName, formatFileSize(fileSize))

	return s.SendNotification(ctx, message+"\n"+details, &SlackNotificationOptions{
		AlertType: SlackAlertGood,
		Trigger:   fmt.Sprintf("%s-backup", strings.ToLower(backupType)),
	})
}

// formatFileSize formats file size in human readable format
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