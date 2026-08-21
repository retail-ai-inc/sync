package export

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/retail-ai-inc/sync/internal/platform/timex"
	"github.com/sirupsen/logrus"
)

// copyFile Copy file from source to destination
func (e *BackupExecutor) copyFile(src, dst string) error {
	// Log memory before file copy
	var memStatsBefore runtime.MemStats
	runtime.ReadMemStats(&memStatsBefore)
	logrus.Infof("[BackupExecutor] 📋 Memory BEFORE copyFile: Alloc=%.2fMB, Sys=%.2fMB",
		float64(memStatsBefore.Alloc)/1024/1024, float64(memStatsBefore.Sys)/1024/1024)

	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	// Create destination directory if it doesn't exist
	dstDir := filepath.Dir(dst)
	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return err
	}

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	// Use buffered copy with controlled buffer size to avoid memory spike
	bufSize := 64 * 1024 // 64KB buffer instead of default 32KB
	buf := make([]byte, bufSize)

	logrus.Infof("[BackupExecutor] 📋 Starting file copy with %dKB buffer...", bufSize/1024)

	for {
		n, readErr := sourceFile.Read(buf)
		if n > 0 {
			_, writeErr := destFile.Write(buf[:n])
			if writeErr != nil {
				return writeErr
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}

	// Log memory after file copy
	var memStatsAfter runtime.MemStats
	runtime.ReadMemStats(&memStatsAfter)
	logrus.Infof("[BackupExecutor] 📋 Memory AFTER copyFile: Alloc=%.2fMB, Sys=%.2fMB (Delta: +%.2fMB)",
		float64(memStatsAfter.Alloc)/1024/1024,
		float64(memStatsAfter.Sys)/1024/1024,
		float64(memStatsAfter.Alloc-memStatsBefore.Alloc)/1024/1024)

	return nil
}

// processFileNamePattern Process file name pattern and replace date placeholders
func processFileNamePattern(pattern, tableName string) string {
	if pattern == "" {
		// Fallback to default pattern if not specified, use yesterday's date
		dateStr := time.Now().AddDate(0, 0, -1).Format("2006-01-02")
		return fmt.Sprintf("%s_%s", tableName, dateStr)
	}

	// Remove regex anchors if present
	cleanPattern := strings.TrimPrefix(pattern, "^")
	cleanPattern = strings.TrimSuffix(cleanPattern, "$")

	// Replace date placeholders using utils function with yesterday's date
	yesterdayDate := time.Now().AddDate(0, 0, -1)
	result := timex.ReplaceDatePlaceholdersWithDate(cleanPattern, yesterdayDate)

	// If the pattern contains table placeholder, replace it
	if strings.Contains(result, "{table}") || strings.Contains(result, "{TABLE}") {
		result = strings.ReplaceAll(result, "{table}", tableName)
		result = strings.ReplaceAll(result, "{TABLE}", strings.ToUpper(tableName))
	} else {
		// If no table placeholder, prepend table name
		ext := filepath.Ext(result)
		if ext != "" {
			nameWithoutExt := strings.TrimSuffix(result, ext)
			result = fmt.Sprintf("%s_%s%s", tableName, nameWithoutExt, ext)
		} else {
			result = fmt.Sprintf("%s_%s", tableName, result)
		}
	}

	logrus.Debugf("[BackupExecutor] Processed file name pattern '%s' for table '%s' -> '%s'", pattern, tableName, result)
	return result
}

// readJSONFile Read JSON documents from exported file
func (e *BackupExecutor) readJSONFile(tempDir, tableName, dateStr string) ([]interface{}, error) {
	fileName := fmt.Sprintf("%s_%s.json", tableName, dateStr)
	filePath := filepath.Join(tempDir, fileName)

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", filePath, err)
	}

	// MongoDB mongoexport outputs each document on a separate line (JSONL format)
	// We need to parse each line as a separate JSON object
	var documents []interface{}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")

	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue // Skip empty lines
		}

		var doc interface{}
		if err := json.Unmarshal([]byte(line), &doc); err != nil {
			logrus.Warnf("[BackupExecutor] Failed to parse JSON line %d in %s: %v", i+1, filePath, err)
			continue
		}
		documents = append(documents, doc)
	}

	return documents, nil
}

// writeJSONFile Write JSON documents to file in JSONL format (one JSON object per line)
func (e *BackupExecutor) writeJSONFile(filePath string, documents []interface{}) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filePath, err)
	}
	defer file.Close()

	// Write each document as a separate line (JSONL format)
	for _, doc := range documents {
		docBytes, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("failed to marshal JSON document: %w", err)
		}

		// Write document followed by newline
		if _, err := file.Write(docBytes); err != nil {
			return fmt.Errorf("failed to write document to file: %w", err)
		}
		if _, err := file.WriteString("\n"); err != nil {
			return fmt.Errorf("failed to write newline to file: %w", err)
		}
	}

	return nil
}
