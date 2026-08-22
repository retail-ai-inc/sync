package infra

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/sirupsen/logrus"
)

// syncBuffer collects log output that is written from another goroutine.
//
// The notification is sent in the background, so the test and the sender touch
// the buffer at the same time — captureLog's plain bytes.Buffer is a data race
// here, and the race detector is part of the default suite.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureBackgroundLog is captureLog for a path that logs from a goroutine.
func captureBackgroundLog() (*logrus.Logger, *syncBuffer) {
	out := &syncBuffer{}
	logger := logrus.New()
	logger.SetOutput(out)
	logger.SetLevel(logrus.DebugLevel)
	return logger, out
}

// awaitLog waits for a line to appear, since the sender writes it after the
// test has already returned from the call.
func awaitLog(t *testing.T, out *syncBuffer, want string) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for {
		if strings.Contains(out.String(), want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("output = %q, want it to carry %q", out.String(), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// webhook stands in for Slack. It answers 200 and hands each posted body to the
// test through a channel, because the notification is sent from a goroutine the
// caller does not wait for.
func webhook(t *testing.T) (url string, posted <-chan map[string]interface{}) {
	t.Helper()

	bodies := make(chan map[string]interface{}, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var payload map[string]interface{}
		if err := json.Unmarshal(raw, &payload); err != nil {
			t.Errorf("the posted body is not JSON: %v (%s)", err, raw)
		}
		select {
		case bodies <- payload:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	return server.URL, bodies
}

// awaitPost waits for the notification goroutine to reach the webhook.
func awaitPost(t *testing.T, posted <-chan map[string]interface{}) map[string]interface{} {
	t.Helper()

	select {
	case payload := <-posted:
		return payload
	case <-time.After(10 * time.Second):
		t.Fatal("no notification reached the webhook")
		return nil
	}
}

// colour reports the alert colour of the first attachment, which is what makes a
// difference look different from an agreement in the channel.
func colour(t *testing.T, payload map[string]interface{}) string {
	t.Helper()

	attachments, ok := payload["attachments"].([]interface{})
	if !ok || len(attachments) == 0 {
		t.Fatalf("payload has no attachments: %+v", payload)
	}
	first, ok := attachments[0].(map[string]interface{})
	if !ok {
		t.Fatalf("attachment is not an object: %+v", attachments[0])
	}
	value, _ := first["color"].(string)
	return value
}

// TestADifferenceIsReportedToSlack covers the path that exists for one reason:
// telling somebody the copy that will be switched to is missing rows. It used to
// be reachable only through a shell script the process looked for in three fixed
// paths, so a deployment without it dropped every alert while reporting success.
func TestADifferenceIsReportedToSlack(t *testing.T) {
	logger, out := captureBackgroundLog()
	url, posted := webhook(t)
	seedGlobalConfig(t, url, "#dr")

	SendTableComparisonSlackNotification(context.Background(), config.SyncConfig{ID: 7},
		"shop", "orders", "shop", "orders", 100, 90,
		time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC), logger)

	payload := awaitPost(t, posted)

	text, _ := payload["text"].(string)
	for _, want := range []string{"Task ID: 7", "shop.orders", "Source: 100", "Target: 90", "Difference: 10"} {
		if !strings.Contains(text, want) {
			t.Errorf("message = %q, want it to carry %q", text, want)
		}
	}
	if !strings.Contains(text, "2026-08-21") {
		t.Errorf("message = %q, want the day it compares named", text)
	}
	if got := colour(t, payload); got != "warning" {
		t.Errorf("colour = %q, want warning for a difference", got)
	}
	if channel, _ := payload["channel"].(string); channel != "#dr" {
		t.Errorf("channel = %q, want the configured one", channel)
	}

	awaitLog(t, out, "notification sent")
}

// TestAnAgreementIsReportedAsGood records that the daily comparison reports both
// outcomes. Silence for a match and silence for a broken notifier would look the
// same, which is the thing this whole path is here to avoid.
func TestAnAgreementIsReportedAsGood(t *testing.T) {
	logger, _ := captureBackgroundLog()
	url, posted := webhook(t)
	seedGlobalConfig(t, url, "")

	SendTableComparisonSlackNotification(context.Background(), config.SyncConfig{ID: 7},
		"shop", "orders", "shop", "orders", 100, 100, time.Now(), logger)

	payload := awaitPost(t, posted)
	if got := colour(t, payload); got != "good" {
		t.Errorf("colour = %q, want good when the two sides agree", got)
	}
	if text, _ := payload["text"].(string); !strings.Contains(text, "Difference: 0") {
		t.Errorf("message = %q, want a difference of zero", text)
	}
	if _, present := payload["channel"]; present {
		t.Error("a channel was sent although none is configured, which overrides the webhook's own")
	}
}

// TestAWebhookThatRefusesIsReported records that a notifier which cannot deliver
// says so. An alert nobody receives and no line in the log is indistinguishable
// from no alert being needed.
func TestAWebhookThatRefusesIsReported(t *testing.T) {
	logger, out := captureBackgroundLog()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no such hook", http.StatusNotFound)
	}))
	t.Cleanup(server.Close)
	seedGlobalConfig(t, server.URL, "#dr")

	SendTableComparisonSlackNotification(context.Background(), config.SyncConfig{ID: 7},
		"shop", "orders", "shop", "orders", 100, 90, time.Now(), logger)

	awaitLog(t, out, "Failed to send Slack notification")
}
