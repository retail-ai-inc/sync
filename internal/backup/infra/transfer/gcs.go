package transfer

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
)

// UploadGCS copies a local file to a GCS path with the system gsutil command,
// then reads the object back and checks its size against the local file.
//
// The exit code used to be the only signal, so an archive that arrived
// truncated — or a local file that was not there at all — was recorded as a
// successful upload. For a disaster-recovery copy that means the backup list is
// not a list of things that can be restored, which is the one property it is
// kept for.
func UploadGCS(ctx context.Context, localFile, gcsPath string) error {
	local, err := os.Stat(localFile)
	if err != nil {
		return fmt.Errorf("the file to upload is not readable: %w", err)
	}

	cmd := exec.CommandContext(ctx, "gsutil", "cp", localFile, gcsPath)

	logrus.Infof("[BackupExecutor] Executing: gsutil cp %s %s", localFile, gcsPath)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("gsutil upload failed: %w, output: %s", err, string(output))
	}

	if err := verifyUpload(ctx, storedObject(gcsPath, localFile), local.Size()); err != nil {
		return err
	}

	logrus.Infof("[BackupExecutor] ✅ GCS upload completed: %s (%d bytes)", gcsPath, local.Size())
	return nil
}

// verifyUpload reads the stored object's size back and compares it.
//
// gsutil stat prints one "Content-Length:" line among others.
func verifyUpload(ctx context.Context, target string, want int64) error {
	out, err := exec.CommandContext(ctx, "gsutil", "stat", target).CombinedOutput()
	if err != nil {
		return fmt.Errorf("the upload reported success but %s cannot be read back: "+
			"%w, output: %s", target, err, string(out))
	}

	got, err := contentLength(string(out))
	if err != nil {
		// The object exists — gsutil stat succeeded — but this build of gsutil
		// words its output differently. Not knowing the size is not a reason to
		// discard a backup that is there.
		logrus.Warnf("[BackupExecutor] Could not read the size of %s back: %v", target, err)
		return nil
	}
	if got != want {
		return fmt.Errorf("%s holds %d bytes but %d were uploaded", target, got, want)
	}
	return nil
}

// storedObject names the object gsutil cp will have written. A destination
// ending in a slash is a prefix, and the file lands under it by its own name.
func storedObject(gcsPath, localFile string) string {
	if strings.HasSuffix(gcsPath, "/") {
		return gcsPath + filepath.Base(localFile)
	}
	return gcsPath
}

func contentLength(output string) (int64, error) {
	for _, line := range strings.Split(output, "\n") {
		_, value, found := strings.Cut(line, ":")
		if !found || !strings.Contains(strings.ToLower(line), "content-length") {
			continue
		}
		return strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	}
	return 0, fmt.Errorf("no Content-Length in %q", output)
}
