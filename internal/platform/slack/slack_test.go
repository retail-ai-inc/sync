package slack

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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
		// A webhook carries its own default channel, so one is enough. This used
		// to require a channel and a shell script as well.
		{"no channel", "https://hooks.example/x", "", true},
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

// TestWithoutTheScriptTheWebhookIsUsed covers every deployment that does not
// ship cloudbuild.sh at one of three hardcoded paths. IsConfigured returned
// false, SendNotification returned nil, and the only trace was a debug line — so
// every alert, including "replication has stopped", was dropped while the caller
// was told it had been sent.
func TestWithoutTheScriptTheWebhookIsUsed(t *testing.T) {
	chdirWithoutScript(t)

	var got map[string]interface{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	n := NewSlackNotifier(server.URL, "#ops", quietLogger())
	if !n.IsConfigured() {
		t.Fatal("IsConfigured() = false with a webhook and no script")
	}

	if err := n.SendError(context.Background(), "replication", "target diverged"); err != nil {
		t.Fatalf("SendError: %v", err)
	}
	if got == nil {
		t.Fatal("nothing was posted to the webhook")
	}
	if text, _ := got["text"].(string); !strings.Contains(text, "replication") {
		t.Errorf("posted %#v, want the alert", got)
	}
	if got["channel"] != "#ops" {
		t.Errorf("channel = %v", got["channel"])
	}
}

// TestAnAlertThatCouldNotBeSentIsReported is the other half: an alert nobody
// received must not read as one that was sent.
func TestAnAlertThatCouldNotBeSentIsReported(t *testing.T) {
	chdirWithoutScript(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("invalid_token"))
	}))
	t.Cleanup(server.Close)

	n := NewSlackNotifier(server.URL, "#ops", quietLogger())

	err := n.SendNotification(context.Background(), "the tokyo cluster is down", nil)
	if err == nil {
		t.Fatal("SendNotification = nil for an alert Slack refused")
	}
	if !strings.Contains(err.Error(), "invalid_token") {
		t.Errorf("err = %v, want it to carry what Slack said", err)
	}
}

// TestNoWebhookIsNotAFailure covers a deployment that has asked for no alerts.
func TestNoWebhookIsNotAFailure(t *testing.T) {
	chdirWithoutScript(t)

	n := NewSlackNotifier("", "", quietLogger())
	if err := n.SendNotification(context.Background(), "anything", nil); err != nil {
		t.Errorf("SendNotification = %v with no webhook configured", err)
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
