package test

import (
	"context"
	"fmt"
	"github.com/retail-ai-inc/sync/pkg/config"
	"testing"
	"time"
)

func testTC24MonitoringAndAlerts(t *testing.T, syncConfigs []config.SyncConfig) {
	// Sync latency monitoring test
	t.Run("SyncLatency", func(t *testing.T) {
		syncConfig := syncConfigs[0]
		// Wait for syncer to start
		time.Sleep(2 * time.Second)

		// Write test data
		writeTestData(t, syncConfig, 10)

		// Check latency metrics
		latency := measureSyncLatency(t, syncConfig)
		if latency > 10*time.Second {
			t.Errorf("Sync latency too high: %v", latency)
		}
	})

	// Error alert test
	t.Run("ErrorAlerts", func(t *testing.T) {
		syncConfig := syncConfigs[0]

		// Inject error condition
		injectError(t, syncConfig)

		// Validate alert triggering
		alerts := checkAlerts(t, syncConfig.ID)
		validateAlerts(t, alerts)
	})

	// Resource usage monitoring test
	t.Run("ResourceUsage", func(t *testing.T) {
		syncConfig := syncConfigs[0]
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		syncer := createSyncer(syncConfig)
		go syncer.Start(ctx)

		// Monitor resource usage
		metrics := collectResourceMetrics(t, time.Minute)
		validateResourceUsage(t, metrics)
	})
}

func measureSyncLatency(t *testing.T, syncConfig config.SyncConfig) time.Duration {
	start := time.Now()
	marker := fmt.Sprintf("test_marker_%d", time.Now().UnixNano())

	// Write marker data to source
	writeMarker(t, syncConfig, marker)

	// Wait for marker data to sync to target
	for {
		if checkMarkerExists(t, syncConfig, marker) {
			break
		}
		if time.Since(start) > time.Minute {
			t.Fatal("Sync timeout")
		}
		time.Sleep(100 * time.Millisecond)
	}

	return time.Since(start)
}

func collectResourceMetrics(t *testing.T, duration time.Duration) map[string]float64 {
	metrics := make(map[string]float64)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	deadline := time.Now().Add(duration)

	for time.Now().Before(deadline) {
		select {
		case <-ticker.C:
			// Collect CPU usage
			metrics["cpu"] += getCPUUsage()
			// Collect memory usage
			metrics["memory"] += getMemoryUsage()
			// Collect disk IO
			metrics["disk_io"] += getDiskIO()
		}
	}

	// Calculate average
	sampleCount := float64(duration / time.Second)
	for k, v := range metrics {
		metrics[k] = v / sampleCount
	}

	return metrics
}
