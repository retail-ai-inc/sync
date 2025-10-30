package utils

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/sirupsen/logrus"
)

// UploadToGCS uploads a local file to Google Cloud Storage
func UploadToGCS(ctx context.Context, localFile, gcsPath string) error {
	// Parse filename for more friendly log message
	fileName := filepath.Base(localFile)
	destPath := fmt.Sprintf("%s/%s", strings.TrimSuffix(gcsPath, "/"), fileName)

	// Check if local file exists and get its size
	fileInfo, err := os.Stat(localFile)
	if err != nil {
		logrus.Errorf("[GCS] Failed to stat local file %s: %v", localFile, err)
		return fmt.Errorf("failed to stat local file: %w", err)
	}

	logrus.Infof("[GCS] 🌤️ Starting upload of %s (size: %.2f MB) to %s",
		fileName, float64(fileInfo.Size())/1024/1024, destPath)

	// 🔍 Memory before GCS upload
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	logrus.Infof("[GCS] 🌤️ Memory BEFORE GCS upload: Alloc=%.2fMB, Sys=%.2fMB, Free=%.2fMB",
		float64(memStats.Alloc)/1024/1024,
		float64(memStats.Sys)/1024/1024,
		float64(memStats.Sys-memStats.Alloc)/1024/1024)

	// Use full path to avoid command parsing issues
	gsutilPath, err := exec.LookPath("gsutil")
	if err != nil {
		gsutilPath = "gsutil" // If not found, use default command name
		logrus.Warnf("[GCS] gsutil command not found in PATH, using default name")
	}

	cmd := exec.CommandContext(ctx, gsutilPath, "cp", localFile, destPath)

	// Execute command with streaming output to avoid memory buffering
	logrus.Infof("[GCS] Executing command: %s %s", filepath.Base(cmd.Path), strings.Join(cmd.Args[1:], " "))

	// Use streaming output to avoid buffering large command output
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start gsutil command: %w", err)
	}

	// Process output streams without buffering full content
	var lastProgressLine string
	var errorLines []string

	// Monitor stdout for progress updates
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			// Only keep progress updates to reduce memory usage
			if strings.Contains(line, "Copying") || strings.Contains(line, "Uploading") ||
				strings.Contains(line, "Operation completed") {
				lastProgressLine = line
			}
		}
	}()

	// Collect error output (errors need to be preserved)
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			errorLines = append(errorLines, scanner.Text())
		}
	}()

	// Wait for command completion
	err = cmd.Wait()

	// Log progress information if available
	if lastProgressLine != "" {
		logrus.Infof("[GCS] %s", lastProgressLine)
	}

	if err != nil {
		logrus.Errorf("[GCS] Upload failed for %s: %v", fileName, err)
		if len(errorLines) > 0 {
			errorOutput := strings.Join(errorLines, "\n")
			logrus.Errorf("[GCS] Error output: %s", errorOutput)
			return fmt.Errorf("gsutil command failed: %w, errors: %s", err, errorOutput)
		}
		return fmt.Errorf("gsutil command failed: %w", err)
	}

	// 🔍 Memory after GCS upload
	runtime.ReadMemStats(&memStats)
	logrus.Infof("[GCS] 🌤️ Memory AFTER GCS upload: Alloc=%.2fMB, Sys=%.2fMB, Free=%.2fMB",
		float64(memStats.Alloc)/1024/1024,
		float64(memStats.Sys)/1024/1024,
		float64(memStats.Sys-memStats.Alloc)/1024/1024)

	logrus.Infof("[GCS] Successfully uploaded %s to %s", fileName, destPath)
	return nil
}
