// Package transfer moves a finished export off the machine: it compresses the
// file and hands it to object storage. Both steps shell out to the system
// binary rather than using a library, which is why they live in infrastructure.
package transfer

import (
	"context"
	"fmt"
	"os"
	"os/exec"

	"github.com/sirupsen/logrus"
)

// Zip compresses inputFile into outputFile with the system zip command.
func Zip(ctx context.Context, workDir, inputFile, outputFile string) error {
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
