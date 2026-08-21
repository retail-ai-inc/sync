package notify

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// fakeConfig satisfies ConfigProvider.
type fakeConfig struct {
	webhook string
	channel string
}

func (f fakeConfig) GetSlackWebhookURL() string { return f.webhook }
func (f fakeConfig) GetSlackChannel() string    { return f.channel }

// installScript writes an executable stub at ./cloudbuild.sh in a temporary
// working directory, which is one of the three paths findCloudBuildScript
// searches.
func installScript(t *testing.T, body string) string {
	t.Helper()

	dir := t.TempDir()
	t.Chdir(dir)

	path := filepath.Join(dir, "cloudbuild.sh")
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}
	return path
}

// chdirWithoutScript moves to an empty temporary directory so none of the
// three search paths resolve.
func chdirWithoutScript(t *testing.T) {
	t.Helper()

	t.Chdir(t.TempDir())
}

func TestFormatFileSize(t *testing.T) {
	tests := []struct {
		size int64
		want string
	}{
		{0, "0 B"},
		{1, "1 B"},
		{1023, "1023 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1024 * 1024, "1.0 MB"},
		{1024 * 1024 * 1024, "1.0 GB"},
		{1024 * 1024 * 1024 * 1024, "1.0 TB"},
		{5 * 1024 * 1024 * 1024 * 1024 * 1024, "5.0 PB"},
	}

	for _, tc := range tests {
		if got := formatFileSize(tc.size); got != tc.want {
			t.Errorf("formatFileSize(%d) = %q, want %q", tc.size, got, tc.want)
		}
	}
}

// A negative size is smaller than the unit, so it is reported in bytes rather
// than rejected. No caller can produce one today (os.FileInfo.Size never
// returns negative), so this pins the behaviour rather than reporting a bug.
func TestFormatFileSizeOnANegativeSize(t *testing.T) {
	if got := formatFileSize(-1); got != "-1 B" {
		t.Errorf("formatFileSize(-1) = %q, want %q", got, "-1 B")
	}
}

func TestFindCloudBuildScriptReturnsAnAbsolutePath(t *testing.T) {
	want := installScript(t, "#!/bin/sh\nexit 0\n")

	got := findCloudBuildScript()
	if !filepath.IsAbs(got) {
		t.Errorf("findCloudBuildScript() = %q, want an absolute path", got)
	}
	gotResolved, _ := filepath.EvalSymlinks(got)
	wantResolved, _ := filepath.EvalSymlinks(want)
	if gotResolved != wantResolved {
		t.Errorf("findCloudBuildScript() = %q, want %q", gotResolved, wantResolved)
	}
}

func TestFindCloudBuildScriptReturnsEmptyWhenAbsent(t *testing.T) {
	chdirWithoutScript(t)

	if got := findCloudBuildScript(); got != "" {
		t.Errorf("findCloudBuildScript() = %q, want an empty string", got)
	}
}

func TestNewSlackNotifierFromConfig(t *testing.T) {
	installScript(t, "#!/bin/sh\nexit 0\n")

	n := NewSlackNotifierFromConfig(fakeConfig{webhook: "https://hooks.example/x", channel: "#ops"}, quietLogger())

	if n.webhookURL != "https://hooks.example/x" || n.channel != "#ops" {
		t.Errorf("webhookURL = %q, channel = %q", n.webhookURL, n.channel)
	}
	if n.username != "sync-service" {
		t.Errorf("username = %q, want sync-service", n.username)
	}
	if n.scriptPath == "" {
		t.Error("scriptPath is empty despite the script being present")
	}
}

func TestNewSlackNotifierFromConfigWithFieldLogger(t *testing.T) {
	installScript(t, "#!/bin/sh\nexit 0\n")

	src := logrus.New()
	src.SetOutput(io.Discard)
	src.SetLevel(logrus.WarnLevel)

	n := NewSlackNotifierFromConfigWithFieldLogger(fakeConfig{webhook: "w", channel: "c"}, src)

	if n.logger == nil {
		t.Fatal("logger is nil")
	}
	if n.logger.GetLevel() != logrus.WarnLevel {
		t.Errorf("level = %v, want %v", n.logger.GetLevel(), logrus.WarnLevel)
	}
}

func TestIsConfigured(t *testing.T) {
	installScript(t, "#!/bin/sh\nexit 0\n")

	tests := []struct {
		name    string
		webhook string
		channel string
		want    bool
	}{
		{"both set", "https://hooks.example/x", "#ops", true},
		{"no webhook", "", "#ops", false},
		{"no channel", "https://hooks.example/x", "", false},
		{"neither", "", "", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			n := NewSlackNotifier(tc.webhook, tc.channel, quietLogger())
			if got := n.IsConfigured(); got != tc.want {
				t.Errorf("IsConfigured() = %v, want %v", got, tc.want)
			}
		})
	}
}

// Notifications are delivered by shelling out to cloudbuild.sh, which is
// looked for at three hard-coded paths. In any deployment that does not ship
// the script at one of them, IsConfigured returns false, SendNotification
// returns nil, and the only trace is a Debug-level line. Every alert — sync
// failure, backup failure — is silently dropped while the caller is told it
// succeeded.
func TestAMissingScriptSilentlyDropsEveryNotification(t *testing.T) {
	chdirWithoutScript(t)

	n := NewSlackNotifier("https://hooks.example/x", "#ops", quietLogger())

	if n.IsConfigured() {
		t.Fatal("IsConfigured() = true without the script — the transport appears to have changed")
	}
	if err := n.SendNotification(context.Background(), "the tokyo cluster is down", nil); err != nil {
		t.Fatalf("SendNotification() = %v — a dropped notification appears to be reported now; assert the error instead", err)
	}
	if err := n.SendError(context.Background(), "replication", "target diverged"); err != nil {
		t.Fatalf("SendError() = %v — a dropped notification appears to be reported now", err)
	}
}

func TestSendNotificationInvokesTheScript(t *testing.T) {
	out := filepath.Join(t.TempDir(), "args.txt")
	installScript(t, "#!/bin/sh\nprintf '%s\\n' \"$@\" > "+out+"\nexit 0\n")

	n := NewSlackNotifier("https://hooks.example/x", "#ops", quietLogger())
	err := n.SendNotification(context.Background(), "hello", &SlackNotificationOptions{
		AlertType:  SlackAlertDanger,
		BranchName: "main",
		Trigger:    "sync",
		CommitURL:  "https://example/commit",
	})
	if err != nil {
		t.Fatalf("SendNotification() = %v", err)
	}

	data, readErr := os.ReadFile(out)
	if readErr != nil {
		t.Fatalf("the script did not run: %v", readErr)
	}
	args := string(data)
	for _, want := range []string{"-w", "https://hooks.example/x", "-c", "#ops", "-u", "sync-service",
		"-m", "hello", "-a", "danger", "-b", "main", "-t", "sync", "-U", "https://example/commit"} {
		if !strings.Contains(args, want) {
			t.Errorf("argument %q was not passed (got: %q)", want, args)
		}
	}
}

func TestSendNotificationReportsAFailingScript(t *testing.T) {
	installScript(t, "#!/bin/sh\necho boom >&2\nexit 3\n")

	n := NewSlackNotifier("https://hooks.example/x", "#ops", quietLogger())

	if err := n.SendNotification(context.Background(), "hello", nil); err == nil {
		t.Error("SendNotification() = nil, want the script's non-zero exit")
	}
}

func TestSendHelpersFormatTheirMessages(t *testing.T) {
	out := filepath.Join(t.TempDir(), "args.txt")
	installScript(t, "#!/bin/sh\nprintf '%s\\n' \"$@\" > "+out+"\nexit 0\n")

	n := NewSlackNotifier("w", "c", quietLogger())

	cases := []struct {
		name string
		call func() error
		want string
	}{
		{"success", func() error { return n.SendSuccess(context.Background(), "backup", "12 tables") }, "✅ backup completed successfully"},
		{"warning", func() error { return n.SendWarning(context.Background(), "replication", "lag 40s") }, "⚠️ replication"},
		{"error", func() error { return n.SendError(context.Background(), "replication", "diverged") }, "❌ replication"},
		{"sync status", func() error {
			return n.SendSyncStatus(context.Background(), "MySQL", "tokyo", "osaka", 1200, 3*time.Second)
		}, "🔄 MySQL Sync Completed"},
		{"backup status", func() error {
			return n.SendBackupStatus(context.Background(), "MongoDB", "orders", "orders.json", 3*1024*1024)
		}, "Size: 3.0 MB"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); err != nil {
				t.Fatalf("call: %v", err)
			}
			data, err := os.ReadFile(out)
			if err != nil {
				t.Fatalf("the script did not run: %v", err)
			}
			if !strings.Contains(string(data), tc.want) {
				t.Errorf("message does not contain %q (got: %q)", tc.want, string(data))
			}
		})
	}
}
