package transfer

import (
	"context"
	"fmt"
	"os/exec"

	"github.com/sirupsen/logrus"
)

// UploadGCS copies a local file to a GCS path with the system gsutil command.
//
// The exit code of gsutil is the only signal: nothing reads the object back to
// check its size or checksum, so a truncated or empty archive is recorded as a
// successful upload (T-120). That is unchanged here.
func UploadGCS(ctx context.Context, localFile, gcsPath string) error {
	cmd := exec.CommandContext(ctx, "gsutil", "cp", localFile, gcsPath)

	logrus.Infof("[BackupExecutor] Executing: gsutil cp %s %s", localFile, gcsPath)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("gsutil upload failed: %w, output: %s", err, string(output))
	}

	logrus.Infof("[BackupExecutor] ✅ GCS upload completed: %s", gcsPath)
	return nil
}
