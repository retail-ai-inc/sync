package utils

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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

	logrus.Infof("[GCS] Starting upload of %s (size: %.2f MB) to %s",
		fileName, float64(fileInfo.Size())/1024/1024, destPath)

	// Use full path to avoid command parsing issues
	gsutilPath, err := exec.LookPath("gsutil")
	if err != nil {
		gsutilPath = "gsutil" // If not found, use default command name
		logrus.Warnf("[GCS] gsutil command not found in PATH, using default name")
	}

	cmd := exec.CommandContext(ctx, gsutilPath, "cp", localFile, destPath)

	// Execute command with detailed logging
	logrus.Infof("[GCS] Executing command: %s %s", filepath.Base(cmd.Path), strings.Join(cmd.Args[1:], " "))
	output, err := cmd.CombinedOutput()

	// Log output for debugging
	if len(output) > 0 {
		logrus.Debugf("[GCS] Command output: %s", string(output))
	}

	if err != nil {
		logrus.Errorf("[GCS] Upload failed for %s: %v", fileName, err)
		logrus.Errorf("[GCS] Command output: %s", string(output))
		return fmt.Errorf("gsutil command failed: %w, output: %s", err, string(output))
	}

	logrus.Infof("[GCS] Successfully uploaded %s to %s", fileName, destPath)
	return nil
}
