package utils

import (
	"context"
	"fmt"
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

	// Use full path to avoid command parsing issues
	gsutilPath, err := exec.LookPath("gsutil")
	if err != nil {
		gsutilPath = "gsutil" // If not found, use default command name
		logrus.Warnf("[GCS] gsutil command not found in PATH, using default name")
	}

	cmd := exec.CommandContext(ctx, gsutilPath, "cp", localFile, destPath)

	// Execute command with detailed logging
	logrus.Debugf("[GCS] Executing command: %s %s", filepath.Base(cmd.Path), strings.Join(cmd.Args[1:], " "))
	output, err := cmd.CombinedOutput()

	// Log output for debugging
	if len(output) > 0 {
		logrus.Debugf("[GCS] Command output: %s", string(output))
	}

	if err != nil {
		return fmt.Errorf("gsutil command failed: %w, output: %s", err, string(output))
	}

	logrus.Debugf("[GCS] Successfully uploaded %s to %s", fileName, destPath)
	return nil
}
