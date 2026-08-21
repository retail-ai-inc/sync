package mongodb

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"time"
)

// Smart Batch Controller Implementation
//
// This implementation replaces the fixed file count processing (maxFilesPerRun = 5)
// with an intelligent batch controller that limits memory usage to a target size (default: 256MB).
//
// Key Features:
// - Dynamic batch size based on actual file sizes
// - Memory usage control to prevent OOM
// - Configurable target batch size (targetBatchSizeBytes)
// - Automatic optimization and monitoring
// - Detailed logging for performance analysis
//
// Performance Benefits:
// - Processes large numbers of small files efficiently
// - Handles large files safely without OOM
// - Maximizes memory utilization up to the target limit
// - Provides throughput metrics for monitoring
//
// Configuration:
// - targetBatchSizeBytes: 256MB (default) - maximum memory per batch
// - maxFilesPerBatch: 1000 (default) - safety limit for file count
// - minFilesPerBatch: 5 (default) - minimum files to process
//
// Usage Example:
// To adjust batch size at runtime:
//   syncer.updateBatchSizeConfig(512*1024*1024, 2000, 10) // 512MB, max 2000 files, min 10 files

// generateBatchID generates a unique batch ID for tracking performance
func generateBatchID() string {
	bytes := make([]byte, 8)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// BatchMetrics holds performance metrics for a batch processing operation
type BatchMetrics struct {
	BatchID           string
	StartTime         time.Time
	FileSelectionTime time.Duration
	FileParsingTime   time.Duration
	DatabaseWriteTime time.Duration
	FileDeletionTime  time.Duration
	TotalTime         time.Duration
	FileCount         int
	TotalSizeBytes    int64
	WriteModelCount   int
	RemainingFiles    int
}

// buildSmartBatch builds a batch of files that doesn't exceed the target memory size
func (s *MongoDBSyncer) buildSmartBatch(bufferPath string) ([]string, int64) {
	files, err := os.ReadDir(bufferPath)
	if err != nil {
		s.logger.Errorf("[MongoDB] Failed to read buffer directory %s: %v", bufferPath, err)
		return nil, 0
	}

	if len(files) == 0 {
		return nil, 0
	}

	var selectedFiles []string
	currentBatchSize := int64(0)

	s.logger.Debugf("[MongoDB] Building smart batch from %d files, target size: %.2f MB",
		len(files), float64(s.targetBatchSizeBytes)/(1024*1024))

	for _, file := range files {
		filePath := filepath.Join(bufferPath, file.Name())
		info, err := os.Stat(filePath)
		if err != nil {
			s.logger.Warnf("[MongoDB] Failed to stat file %s: %v", filePath, err)
			continue
		}

		fileSize := info.Size()

		// Check if adding this file would exceed the target size
		if currentBatchSize+fileSize > s.targetBatchSizeBytes && len(selectedFiles) >= s.minFilesPerBatch {
			s.logger.Debugf("[MongoDB] Stopping batch construction: adding file would exceed target size (current: %.2f MB + file: %.2f MB > target: %.2f MB)",
				float64(currentBatchSize)/(1024*1024), float64(fileSize)/(1024*1024), float64(s.targetBatchSizeBytes)/(1024*1024))
			break
		}

		// Check if we've reached the maximum file count limit
		if len(selectedFiles) >= s.maxFilesPerBatch {
			s.logger.Debugf("[MongoDB] Stopping batch construction: reached max files per batch (%d)", s.maxFilesPerBatch)
			break
		}

		selectedFiles = append(selectedFiles, filePath)
		currentBatchSize += fileSize

		s.logger.Debugf("[MongoDB] Added file %s (%.2f KB) to batch, total: %d files, %.2f MB",
			file.Name(), float64(fileSize)/1024, len(selectedFiles), float64(currentBatchSize)/(1024*1024))
	}

	if len(selectedFiles) > 0 {
		s.logger.Debugf("[MongoDB] Built smart batch: %d files, %.2f MB (%.1f%% of target)",
			len(selectedFiles), float64(currentBatchSize)/(1024*1024),
			float64(currentBatchSize)/float64(s.targetBatchSizeBytes)*100)
	} else {
		s.logger.Debugf("[MongoDB] No files selected for batch")
	}

	return selectedFiles, currentBatchSize
}

// calculateAverageFileSize calculates the average file size in the buffer directory
func (s *MongoDBSyncer) calculateAverageFileSize(bufferPath string) int64 {
	files, err := os.ReadDir(bufferPath)
	if err != nil || len(files) == 0 {
		return 0
	}

	// Intelligent sampling: take first 20 files or all files (if less than 20)
	sampleSize := 20
	if sampleSize > len(files) {
		sampleSize = len(files)
	}

	totalSize := int64(0)
	validFiles := 0

	for i := 0; i < sampleSize; i++ {
		filePath := filepath.Join(bufferPath, files[i].Name())
		if info, err := os.Stat(filePath); err == nil {
			totalSize += info.Size()
			validFiles++
		}
	}

	if validFiles == 0 {
		return 0
	}

	avgSize := totalSize / int64(validFiles)
	s.logger.Debugf("[MongoDB] Calculated average file size: %.2f KB (sampled %d files)",
		float64(avgSize)/1024, validFiles)

	return avgSize
}

// updateBatchSizeConfig allows dynamic adjustment of batch size parameters
func (s *MongoDBSyncer) updateBatchSizeConfig(targetSizeBytes int64, maxFiles, minFiles int) {
	if targetSizeBytes > 0 {
		s.targetBatchSizeBytes = targetSizeBytes
	}
	if maxFiles > 0 {
		s.maxFilesPerBatch = maxFiles
	}
	if minFiles > 0 {
		s.minFilesPerBatch = minFiles
	}

	s.logger.Infof("[MongoDB] Updated batch size config: target=%.2f MB, maxFiles=%d, minFiles=%d",
		float64(s.targetBatchSizeBytes)/(1024*1024), s.maxFilesPerBatch, s.minFilesPerBatch)
}

// getBatchSizeConfig returns current batch size configuration
func (s *MongoDBSyncer) getBatchSizeConfig() (int64, int, int) {
	return s.targetBatchSizeBytes, s.maxFilesPerBatch, s.minFilesPerBatch
}

// estimateOptimalBatchSize estimates optimal batch size based on buffer directory statistics
func (s *MongoDBSyncer) estimateOptimalBatchSize(bufferPath string) {
	avgFileSize := s.calculateAverageFileSize(bufferPath)
	if avgFileSize == 0 {
		return
	}

	// Estimate how many files would fit in target batch size
	estimatedFileCount := s.targetBatchSizeBytes / avgFileSize

	s.logger.Debugf("[MongoDB] Batch size estimation: avgFileSize=%.2f KB, estimatedFileCount=%d, target=%.2f MB",
		float64(avgFileSize)/1024, estimatedFileCount, float64(s.targetBatchSizeBytes)/(1024*1024))

	// Adjust max files per batch if estimation is reasonable
	if estimatedFileCount > int64(s.maxFilesPerBatch) {
		s.logger.Infof("[MongoDB] Current maxFilesPerBatch (%d) might be limiting, estimated optimal: %d",
			s.maxFilesPerBatch, estimatedFileCount)
	} else if estimatedFileCount < int64(s.minFilesPerBatch) {
		s.logger.Warnf("[MongoDB] Files are very large (avg %.2f MB), might need to reduce target batch size",
			float64(avgFileSize)/(1024*1024))
	}
}
