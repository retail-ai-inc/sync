package mongodb

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// bsonRawSeparator is a unique byte sequence used to separate BSON documents in a buffer file.
var bsonRawSeparator = []byte{0xDE, 0xAD, 0xBE, 0xEF, 0xDE, 0xAD, 0xBE, 0xEF}

// persistedChange represents a change that will be saved to disk
type persistedChange struct {
	ID         string    `bson:"id"`
	Token      bson.Raw  `bson:"token"`
	RawData    bson.Raw  `bson:"raw_data"` // Store the raw BSON event directly
	SourceDB   string    `bson:"source_db"`
	SourceColl string    `bson:"source_coll"`
	Timestamp  time.Time `bson:"timestamp"`
}

// FileParseJob represents a file parsing job for parallel processing
type FileParseJob struct {
	FilePath       string
	FileIndex      int
	TotalFiles     int
	SourceDB       string
	CollectionName string
}

// FileParseResult represents the result of parsing a file
type FileParseResult struct {
	FilePath    string
	FileIndex   int
	WriteModels []mongo.WriteModel
	ParseTime   time.Duration
	Error       error
}

func (s *MongoDBSyncer) diskWriter(ctx context.Context, eventChannel <-chan streamEvent, sourceDB, collectionName string) {
	s.logger.Infof("[MongoDB] Starting disk writer for %s.%s", sourceDB, collectionName)
	defer s.logger.Infof("[MongoDB] Stopping disk writer for %s.%s", sourceDB, collectionName)

	var buffer []streamEvent
	const batchSize = 100
	flushInterval := 2 * time.Second
	timer := time.NewTimer(flushInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			if len(buffer) > 0 {
				s.flushBufferToDisk(ctx, &buffer, sourceDB, collectionName)
			}
			return
		case event, ok := <-eventChannel:
			if !ok {
				if len(buffer) > 0 {
					s.flushBufferToDisk(ctx, &buffer, sourceDB, collectionName)
				}
				return
			}
			buffer = append(buffer, event)
			if len(buffer) >= batchSize {
				s.flushBufferToDisk(ctx, &buffer, sourceDB, collectionName)
				timer.Reset(flushInterval)
			}
		case <-timer.C:
			if len(buffer) > 0 {
				s.flushBufferToDisk(ctx, &buffer, sourceDB, collectionName)
			}
			timer.Reset(flushInterval)
		}
	}
}

func (s *MongoDBSyncer) flushBufferToDisk(ctx context.Context, buffer *[]streamEvent, sourceDB, collectionName string) {
	if len(*buffer) == 0 {
		return
	}

	bufferPath := s.getBufferPath(sourceDB, collectionName)
	if err := os.MkdirAll(bufferPath, os.ModePerm); err != nil {
		s.logger.Errorf("[MongoDB] Failed to create buffer directory %s: %v", bufferPath, err)
		return
	}

	timestamp := time.Now().UnixNano()
	fileName := fmt.Sprintf("batch_%d.bsonstream", timestamp)
	filePath := filepath.Join(bufferPath, fileName)

	file, err := os.Create(filePath)
	if err != nil {
		s.logger.Errorf("[MongoDB] Failed to create buffer file %s: %v", filePath, err)
		return
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	var lastToken bson.Raw
	for _, event := range *buffer {
		if _, err := writer.Write(event.RawData); err != nil {
			s.logger.Errorf("[MongoDB] Failed to write event to buffer file %s: %v", filePath, err)
			return // Stop on first error
		}
		if _, err := writer.Write(bsonRawSeparator); err != nil {
			s.logger.Errorf("[MongoDB] Failed to write separator to buffer file %s: %v", filePath, err)
			return // Stop on first error
		}
		lastToken = event.ResumeToken
	}

	if err := writer.Flush(); err != nil {
		s.logger.Errorf("[MongoDB] Failed to flush buffer file %s: %v", filePath, err)
		return
	}

	// If we successfully wrote the entire buffer to a file, we can save the last token.
	if lastToken != nil {
		s.saveMongoDBResumeToken(sourceDB, collectionName, lastToken)
	}

	// Clear the buffer now that it's been persisted.
	*buffer = (*buffer)[:0]
}

func (s *MongoDBSyncer) processPersistentBuffer(ctx context.Context, sourceDB, collectionName, targetDBName, targetCollectionName string) {
	ticker := time.NewTicker(100 * time.Millisecond) // Check for files every 100ms for better responsiveness
	defer ticker.Stop()

	// Add optimization ticker to periodically analyze and optimize batch size
	optimizationTicker := time.NewTicker(5 * time.Minute) // Optimize every 5 minutes
	defer optimizationTicker.Stop()

	// Add dead letter queue retry ticker
	deadLetterTicker := time.NewTicker(s.retryInterval) // Retry dead letter queue
	defer deadLetterTicker.Stop()

	s.logger.Infof("[MongoDB] Started persistent buffer processor for %s.%s with smart batch control (target: %.2f MB)",
		sourceDB, collectionName, float64(s.targetBatchSizeBytes)/(1024*1024))

	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("[MongoDB] Stopping persistent buffer processor for %s.%s", sourceDB, collectionName)
			return
		case <-ticker.C:
			s.processBufferedChanges(ctx, sourceDB, collectionName, targetDBName, targetCollectionName)
		case <-optimizationTicker.C:
			// Periodically analyze buffer and optimize batch size
			bufferPath := s.getBufferPath(sourceDB, collectionName)
			s.estimateOptimalBatchSize(bufferPath)
		case <-deadLetterTicker.C:
			// Periodically retry dead letter queue
			if s.enableDeadLetterQueue {
				s.processDeadLetterQueue(ctx, sourceDB, collectionName, targetDBName, targetCollectionName)
			}
		}
	}
}

func (s *MongoDBSyncer) processBufferedChanges(ctx context.Context, sourceDB, collectionName, targetDBName, targetCollectionName string) {
	// Generate unique batch ID for tracking
	batchID := generateBatchID()
	batchStartTime := time.Now()

	targetColl := s.targetClient.Database(targetDBName).Collection(targetCollectionName)
	bufferPath := s.getBufferPath(sourceDB, collectionName)

	if _, err := os.Stat(bufferPath); os.IsNotExist(err) {
		return
	}

	// === STEP 1: File Selection ===
	step1StartTime := time.Now()
	s.logger.Debugf("[MongoDB] [BatchID:%s] Starting file selection for %s.%s",
		batchID, sourceDB, collectionName)

	selectedFiles, batchSize := s.buildSmartBatch(bufferPath)
	if len(selectedFiles) == 0 {
		return
	}

	step1Duration := time.Since(step1StartTime)
	s.logger.Debugf("[MongoDB] [BatchID:%s] File selection completed - selected %d files (%.2f MB) in %v",
		batchID, len(selectedFiles), float64(batchSize)/(1024*1024), step1Duration)

	var processedFiles []string
	var allWriteModels []mongo.WriteModel
	var writeSuccess = true

	// === STEP 2: File Parsing (Parallel) ===
	step2StartTime := time.Now()
	s.logger.Debugf("[MongoDB] [BatchID:%s] Starting parallel file parsing - %d files to process",
		batchID, len(selectedFiles))

	// Use parallel parsing for better performance
	allWriteModels, processedFiles, writeSuccess = s.parseFilesParallel(ctx, selectedFiles, sourceDB, collectionName, batchID)

	step2Duration := time.Since(step2StartTime)
	avgParseTime := time.Duration(0)
	if len(processedFiles) > 0 {
		avgParseTime = time.Duration(int64(step2Duration) / int64(len(processedFiles)))
	}
	s.logger.Debugf("[MongoDB] [BatchID:%s] Parallel file parsing completed - processed %d files, collected %d write models in %v (avg: %v/file)",
		batchID, len(processedFiles), len(allWriteModels), step2Duration, avgParseTime)

	// === STEP 3: Memory Accumulation ===
	estimatedMemoryMB := float64(len(allWriteModels)*1024) / (1024 * 1024) // Assume ~1KB per WriteModel
	s.logger.Debugf("[MongoDB] [BatchID:%s] Memory accumulation completed - %d WriteModels (estimated %.2f MB in memory)",
		batchID, len(allWriteModels), estimatedMemoryMB)

	// === STEP 4: Database Write ===
	var step4Duration time.Duration
	if writeSuccess && len(allWriteModels) > 0 {
		step4StartTime := time.Now()
		s.logger.Debugf("[MongoDB] [BatchID:%s] Starting database bulk write - %d operations to %s.%s",
			batchID, len(allWriteModels), targetDBName, targetCollectionName)

		err := s.flushWriteModels(ctx, targetColl, allWriteModels, sourceDB, collectionName)
		if err != nil {
			s.logger.Errorf("[MongoDB] [BatchID:%s] Database bulk write failed: %v", batchID, err)
			writeSuccess = false
		} else {
			step4Duration = time.Since(step4StartTime)
			opsPerSecond := float64(len(allWriteModels)) / step4Duration.Seconds()
			s.logger.Debugf("[MongoDB] [BatchID:%s] Database bulk write completed - %d operations in %v (%.2f ops/sec)",
				batchID, len(allWriteModels), step4Duration, opsPerSecond)
		}
	}

	// === STEP 5: File Cleanup ===
	var step5Duration time.Duration
	if writeSuccess && len(processedFiles) > 0 {
		step5StartTime := time.Now()
		s.logger.Debugf("[MongoDB] [BatchID:%s] Starting file cleanup - %d files to delete",
			batchID, len(processedFiles))

		deletedCount := 0
		failedDeletes := 0
		for _, filePath := range processedFiles {
			if err := os.Remove(filePath); err != nil {
				s.logger.Warnf("[MongoDB] [BatchID:%s] Failed to delete file %s: %v",
					batchID, filepath.Base(filePath), err)
				failedDeletes++
			} else {
				deletedCount++
			}
		}

		step5Duration = time.Since(step5StartTime)
		s.logger.Debugf("[MongoDB] [BatchID:%s] File cleanup completed - deleted %d files, failed %d in %v",
			batchID, deletedCount, failedDeletes, step5Duration)
	}

	// === BATCH SUMMARY ===
	totalDuration := time.Since(batchStartTime)

	// Calculate remaining files
	remainingFiles := 0
	if remainingFileList, err := os.ReadDir(bufferPath); err == nil {
		remainingFiles = len(remainingFileList)
	}

	// Performance metrics
	throughputMBps := float64(batchSize) / totalDuration.Seconds() / (1024 * 1024)
	operationsPerSecond := float64(len(allWriteModels)) / totalDuration.Seconds()

	if writeSuccess {
		s.logger.Debugf("[MongoDB] [BatchID:%s] Batch completed successfully - processed %d files, %d operations in %v (%.2f MB/s, %.2f ops/sec)",
			batchID, len(processedFiles), len(allWriteModels), totalDuration, throughputMBps, operationsPerSecond)
		s.logger.Debugf("[MongoDB] [BatchID:%s] Remaining files: %d", batchID, remainingFiles)
	} else {
		s.logger.Errorf("[MongoDB] [BatchID:%s] Batch failed - keeping %d files for retry (total time: %v)",
			batchID, len(selectedFiles), totalDuration)
	}

}

// parseFilesParallel parses multiple files in parallel using worker goroutines
func (s *MongoDBSyncer) parseFilesParallel(ctx context.Context, selectedFiles []string, sourceDB, collectionName, batchID string) ([]mongo.WriteModel, []string, bool) {
	// Determine optimal worker count based on CPU cores and file count
	workerCount := runtime.NumCPU()
	if workerCount > 8 {
		workerCount = 8 // Cap at 8 workers to avoid too much contention
	}
	if len(selectedFiles) < workerCount {
		workerCount = len(selectedFiles) // Don't create more workers than files
	}

	s.logger.Debugf("[MongoDB] [BatchID:%s] Using %d parallel workers to parse %d files",
		batchID, workerCount, len(selectedFiles))

	// Create channels for job distribution and result collection
	jobs := make(chan FileParseJob, len(selectedFiles))
	results := make(chan FileParseResult, len(selectedFiles))

	// Start worker goroutines
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			s.fileParseWorker(ctx, workerID, jobs, results, batchID)
		}(i)
	}

	// Send jobs to workers
	for i, filePath := range selectedFiles {
		jobs <- FileParseJob{
			FilePath:       filePath,
			FileIndex:      i,
			TotalFiles:     len(selectedFiles),
			SourceDB:       sourceDB,
			CollectionName: collectionName,
		}
	}
	close(jobs)

	// Wait for all workers to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	fileResults := make([]FileParseResult, len(selectedFiles))
	var allWriteModels []mongo.WriteModel
	var processedFiles []string
	writeSuccess := true
	totalParseTime := time.Duration(0)
	maxParseTime := time.Duration(0)
	minParseTime := time.Duration(1<<63 - 1)

	for result := range results {
		fileResults[result.FileIndex] = result

		if result.Error != nil {
			s.logger.Errorf("[MongoDB] [BatchID:%s] Parallel parsing failed for file %s (index %d): %v",
				batchID, filepath.Base(result.FilePath), result.FileIndex, result.Error)
			writeSuccess = false
		} else {
			allWriteModels = append(allWriteModels, result.WriteModels...)
			processedFiles = append(processedFiles, result.FilePath)
			totalParseTime += result.ParseTime

			if result.ParseTime > maxParseTime {
				maxParseTime = result.ParseTime
			}
			if result.ParseTime < minParseTime {
				minParseTime = result.ParseTime
			}

			// Log individual file parsing time
			s.logger.Debugf("[MongoDB] [BatchID:%s] Parsed file %s (%d/%d) - %d models in %v",
				batchID, filepath.Base(result.FilePath), result.FileIndex+1, len(selectedFiles),
				len(result.WriteModels), result.ParseTime)
		}
	}

	// Calculate statistics
	avgParseTime := time.Duration(0)
	if len(processedFiles) > 0 {
		avgParseTime = totalParseTime / time.Duration(len(processedFiles))
	}

	s.logger.Debugf("[MongoDB] [BatchID:%s] Parallel parsing stats: processed=%d, avg=%v, min=%v, max=%v",
		batchID, len(processedFiles), avgParseTime, minParseTime, maxParseTime)

	return allWriteModels, processedFiles, writeSuccess
}

// fileParseWorker is a worker goroutine that processes file parsing jobs
func (s *MongoDBSyncer) fileParseWorker(ctx context.Context, workerID int, jobs <-chan FileParseJob, results chan<- FileParseResult, batchID string) {
	s.logger.Debugf("[MongoDB] [BatchID:%s] Starting file parse worker %d", batchID, workerID)

	for job := range jobs {
		select {
		case <-ctx.Done():
			// Context cancelled, send error result
			results <- FileParseResult{
				FilePath:  job.FilePath,
				FileIndex: job.FileIndex,
				Error:     ctx.Err(),
			}
			return
		default:
			// Process the file
			startTime := time.Now()
			writeModels, err := s.parseFileToWriteModels(ctx, job.FilePath, job.SourceDB, job.CollectionName)
			parseTime := time.Since(startTime)

			result := FileParseResult{
				FilePath:    job.FilePath,
				FileIndex:   job.FileIndex,
				WriteModels: writeModels,
				ParseTime:   parseTime,
				Error:       err,
			}

			results <- result
		}
	}

	s.logger.Debugf("[MongoDB] [BatchID:%s] File parse worker %d completed", batchID, workerID)
}

// parseFileToWriteModels parses a file and returns all WriteModels without executing them
func (s *MongoDBSyncer) parseFileToWriteModels(ctx context.Context, filePath string, sourceDB, collectionName string) ([]mongo.WriteModel, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// Set buffer size to 100MB to handle large BSON documents
	bufferSize := 100 * 1024 * 1024 // 100MB
	scanner.Buffer(make([]byte, bufferSize), bufferSize)
	// Set the scanner to use our custom split function.
	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}
		if i := bytes.Index(data, bsonRawSeparator); i >= 0 {
			// We have a full event followed by a separator.
			return i + len(bsonRawSeparator), data[0:i], nil
		}
		// If we're at EOF, we have a final, non-terminated event. Return it.
		if atEOF {
			return len(data), data, nil
		}
		// Request more data.
		return 0, nil, nil
	})

	var writeModels []mongo.WriteModel

	for scanner.Scan() {
		eventData := scanner.Bytes()
		if len(eventData) == 0 {
			continue
		}

		model := s.convertRawBSONToWriteModel(eventData, sourceDB, collectionName)
		if model != nil {
			writeModels = append(writeModels, model)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading from file stream: %w", err)
	}

	s.logger.Debugf("[MongoDB] Parsed file %s: extracted %d write models",
		filepath.Base(filePath), len(writeModels))

	return writeModels, nil
}

// Legacy method for backward compatibility - now deprecated
func (s *MongoDBSyncer) processFileAsStream(ctx context.Context, filePath string, targetColl *mongo.Collection, sourceDB, collectionName string) error {
	s.logger.Warnf("[MongoDB] DEPRECATED: processFileAsStream is deprecated, use parseFileToWriteModels + flushWriteModels instead")

	writeModels, err := s.parseFileToWriteModels(ctx, filePath, sourceDB, collectionName)
	if err != nil {
		return err
	}

	if len(writeModels) > 0 {
		return s.flushWriteModels(ctx, targetColl, writeModels, sourceDB, collectionName)
	}

	return nil
}

func (s *MongoDBSyncer) getBufferPath(db, coll string) string {
	return filepath.Join(s.bufferDir, fmt.Sprintf("%s_%s", db, coll))
}
