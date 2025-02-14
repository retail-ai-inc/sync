package utils

import (
	"fmt"
	"os"
	"os/exec"
	"time"
)

// GetCurrentTime returns the current time
func GetCurrentTime() time.Time {
	return time.Now()
}

func UnzipDistFile(zipPath, destDir string) error {
	if _, err := os.Stat(destDir); os.IsNotExist(err) {
		if err := os.MkdirAll(destDir, os.ModePerm); err != nil {
			return fmt.Errorf("failed to create destination directory: %w", err)
		}
	}

	cmd := exec.Command("unzip", "-o", zipPath, "-d", destDir)
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to unzip file using system unzip command: %w", err)
	}

	return nil
}
