package backup

import (
	"context"
	"fmt"
	"os"
	"os/exec"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
)

// executeExternalZip executes external zip command
func (e *BackupExecutor) executeExternalZip(ctx context.Context, workDir, inputFile, outputFile string) error {
	// Use system zip command
	cmd := exec.CommandContext(ctx, "zip", "-j", outputFile, inputFile)
	cmd.Dir = workDir

	logrus.Infof("[BackupExecutor] Executing: zip -j %s %s", outputFile, inputFile)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("zip failed: %w, output: %s", err, string(output))
	}

	// Check output file
	if _, err := os.Stat(outputFile); err != nil {
		return fmt.Errorf("zip output file not created: %w", err)
	}

	// Log compression results
	if stat, err := os.Stat(outputFile); err == nil {
		logrus.Infof("[BackupExecutor] ✅ Zip completed: %.2f MB", float64(stat.Size())/1024/1024)
	}

	return nil
}

// executeExternalGCSUpload executes external gsutil upload
func (e *BackupExecutor) executeExternalGCSUpload(ctx context.Context, localFile, gcsPath string) error {
	cmd := exec.CommandContext(ctx, "gsutil", "cp", localFile, gcsPath)

	logrus.Infof("[BackupExecutor] Executing: gsutil cp %s %s", localFile, gcsPath)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("gsutil upload failed: %w, output: %s", err, string(output))
	}

	logrus.Infof("[BackupExecutor] ✅ GCS upload completed: %s", gcsPath)
	return nil
}
