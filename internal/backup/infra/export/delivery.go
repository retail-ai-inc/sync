package export

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/retail-ai-inc/sync/internal/backup/infra/transfer"
	"github.com/sirupsen/logrus"
)

// removeTemp deletes a file an export made and no longer needs.
//
// Failing to remove it is a warning rather than an error: the backup itself has
// already been taken and uploaded, and the directory goes away with the job.
// A file that was never created is not a failure at all — the zip is absent
// whenever the job asked for no compression.
func removeTemp(kind, path string) {
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return
		}
		logrus.Warnf("[BackupExecutor] Failed to remove %s %s: %v", kind, path, err)
		return
	}
	logrus.Debugf("[BackupExecutor] 🗑️  Cleaned up %s: %s", kind, path)
}

// compressAndUpload finishes an export: it compresses the file when asked to,
// and uploads the result when the job names somewhere to upload it. It returns
// the path that was uploaded so the caller can clean up.
func (e *BackupExecutor) compressAndUpload(
	ctx context.Context, tempDir, filePath, zipPath string, compress bool, config ExecutorBackupConfig,
) (uploadPath string, err error) {
	uploadPath = filePath
	if !compress {
		logrus.Infof("[BackupExecutor] ⏭️  Step 2: Compression disabled, uploading %s as-is",
			filepath.Base(filePath))
	} else {
		logrus.Infof("[BackupExecutor] 🗜️ Step 2: External zip compression")
		if err := transfer.Zip(ctx, tempDir, filePath, zipPath); err != nil {
			return "", fmt.Errorf("external zip failed: %w", err)
		}
		e.logMemoryUsage("AFTER_ZIP")
		uploadPath = zipPath
	}

	if config.Destination.GCSPath == "" {
		logrus.Infof("[BackupExecutor] ⏭️  Step 3: No destination configured, keeping %s locally",
			filepath.Base(uploadPath))
		e.countUpload(uploadPath)
		return uploadPath, nil
	}

	gcsPath := fmt.Sprintf("%s/%s", config.Destination.GCSPath, filepath.Base(uploadPath))
	logrus.Infof("[BackupExecutor] ☁️ Step 3: External GCS upload")
	if err := transfer.UploadGCS(ctx, uploadPath, gcsPath); err != nil {
		return "", fmt.Errorf("external GCS upload failed: %w", err)
	}
	// Measured before the temporary directory goes, which is the last moment
	// the file exists anywhere this can see.
	e.countUpload(uploadPath)
	return uploadPath, nil
}
