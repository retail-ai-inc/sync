package mongodb

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
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
	// NewestEvent is when the source made the most recent change in the file.
	// The applied lag is measured from it once the batch lands.
	NewestEvent time.Time
	// LastToken is the resume token of the last event in the file. A change
	// stream event's _id *is* its resume token, so this needs no extra
	// bookkeeping in the file format. It is what the stream resumes from, and
	// it is only recorded once these events have reached the target.
	LastToken bson.Raw
}

// defaultBufferLimitBytes is how much unapplied change data may sit on disk
// before the reader stops taking more.
//
// Without a bound the writer keeps going for as long as the target is
// unreachable, and the first thing to break is the disk — which takes the
// checkpoint with it, because that is written to the same volume when a file
// store is configured. Two gigabytes is a compromise: enough to ride out a
// target restart on a busy collection, small enough to notice on a node.
const defaultBufferLimitBytes = 2 << 30

// bufferFullPause is how long to wait before looking again once the buffer is
// full.
const bufferFullPause = 5 * time.Second

// bufferLimitBytes reports the cap, which SYNC_MONGO_BUFFER_LIMIT_BYTES
// overrides. A value of zero or less turns the cap off, which is a choice an
// operator can make and this one will not make for them.
func bufferLimitBytes() int64 {
	raw := os.Getenv("SYNC_MONGO_BUFFER_LIMIT_BYTES")
	if raw == "" {
		return defaultBufferLimitBytes
	}
	limit, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return defaultBufferLimitBytes
	}
	return limit
}

// bufferBytes reports how much unapplied change data is on disk for one
// collection.
func bufferBytes(bufferPath string) int64 {
	entries, err := os.ReadDir(bufferPath)
	if err != nil {
		return 0
	}
	var total int64
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}
		total += info.Size()
	}
	return total
}

func (s *MongoDBSyncer) diskWriter(ctx context.Context, eventChannel <-chan streamEvent, sourceDB, collectionName string) {
	s.logger.Infof("[MongoDB] Starting disk writer for %s.%s", sourceDB, collectionName)
	defer s.logger.Infof("[MongoDB] Stopping disk writer for %s.%s", sourceDB, collectionName)

	var buffer []streamEvent
	const batchSize = 100
	flushInterval := 2 * time.Second
	timer := time.NewTimer(flushInterval)
	defer timer.Stop()

	limit := bufferLimitBytes()

	for {
		// Stop taking events while the buffer is over its limit. The channel
		// then fills, the change stream reader closes the stream rather than
		// reading further ahead of what can be applied, and the guardian brings
		// it back once the applier has drained some. The alternative is writing
		// until the disk is full, which takes the checkpoint with it.
		if limit > 0 {
			held := bufferBytes(s.getBufferPath(sourceDB, collectionName))
			metrics.Default.SetGauge(metrics.BufferBytes, metrics.HelpBufferBytes,
				s.metricLabels(collectionName), float64(held))
			if held >= limit {
				s.logger.Errorf("[MongoDB] %s.%s has %d bytes of change data waiting to "+
					"be applied, at or over the %d byte limit, so the change stream is "+
					"being held back. The target is not keeping up.",
					sourceDB, collectionName, held, limit)
				select {
				case <-ctx.Done():
					return
				case <-time.After(bufferFullPause):
				}
				continue
			}
		}

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

	for _, event := range *buffer {
		if _, err := writer.Write(event.RawData); err != nil {
			s.logger.Errorf("[MongoDB] Failed to write event to buffer file %s: %v", filePath, err)
			return // Stop on first error
		}
		if _, err := writer.Write(bsonRawSeparator); err != nil {
			s.logger.Errorf("[MongoDB] Failed to write separator to buffer file %s: %v", filePath, err)
			return // Stop on first error
		}
	}

	if err := writer.Flush(); err != nil {
		s.logger.Errorf("[MongoDB] Failed to flush buffer file %s: %v", filePath, err)
		return
	}

	// The resume token is deliberately NOT advanced here.
	//
	// It used to be, and that made the token say "handled" for events that were
	// only sitting on the syncer's own disk. The token itself is recorded on the
	// target database, which survives the region; the buffer is a local
	// directory, which does not. So the token was more durable than the data it
	// pointed past: a rescheduled pod lost the events and the stream resumed
	// after them. It is advanced by processBufferedChanges once the events have
	// actually reached the target.

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
	var newestEvent time.Time
	var lastToken bson.Raw

	// === STEP 2: File Parsing (Parallel) ===
	step2StartTime := time.Now()
	s.logger.Debugf("[MongoDB] [BatchID:%s] Starting parallel file parsing - %d files to process",
		batchID, len(selectedFiles))

	// Use parallel parsing for better performance
	allWriteModels, processedFiles, writeSuccess, newestEvent, lastToken = s.parseFilesParallel(ctx, selectedFiles, sourceDB, collectionName, batchID)

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
			metrics.Failed(s.metricLabels(collectionName), len(allWriteModels))
		} else {
			metrics.Applied(s.metricLabels(collectionName), len(allWriteModels))
			if !newestEvent.IsZero() {
				metrics.SetLag(s.metricLabels(collectionName), time.Since(newestEvent).Seconds())
			}
			// Only now is the stream allowed to move past these events: they
			// are on the target, not merely on this machine's disk.
			if lastToken != nil {
				s.saveMongoDBResumeToken(sourceDB, collectionName, lastToken)
			}
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
func (s *MongoDBSyncer) parseFilesParallel(ctx context.Context, selectedFiles []string, sourceDB, collectionName, batchID string) ([]mongo.WriteModel, []string, bool, time.Time, bson.Raw) {
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
			continue
		}

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

	// Assemble in the order the files were written, not the order the workers
	// happened to finish in. The buffer files are the change stream in sequence,
	// so collecting them as they arrived would reorder one document's changes
	// against another's — and, within a document, its own.
	var newestEvent time.Time
	var lastToken bson.Raw
	for _, result := range fileResults {
		if result.FilePath == "" || result.Error != nil {
			// A file that could not be parsed stops the token here: advancing
			// past it would skip whatever it held.
			break
		}
		allWriteModels = append(allWriteModels, result.WriteModels...)
		processedFiles = append(processedFiles, result.FilePath)
		if result.NewestEvent.After(newestEvent) {
			newestEvent = result.NewestEvent
		}
		if result.LastToken != nil {
			lastToken = result.LastToken
		}
	}

	// Calculate statistics
	avgParseTime := time.Duration(0)
	if len(processedFiles) > 0 {
		avgParseTime = totalParseTime / time.Duration(len(processedFiles))
	}

	s.logger.Debugf("[MongoDB] [BatchID:%s] Parallel parsing stats: processed=%d, avg=%v, min=%v, max=%v",
		batchID, len(processedFiles), avgParseTime, minParseTime, maxParseTime)

	return allWriteModels, processedFiles, writeSuccess, newestEvent, lastToken
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
			writeModels, newest, lastToken, err := s.parseFileWithTiming(ctx, job.FilePath, job.SourceDB, job.CollectionName)
			parseTime := time.Since(startTime)

			result := FileParseResult{
				FilePath:    job.FilePath,
				FileIndex:   job.FileIndex,
				WriteModels: writeModels,
				NewestEvent: newest,
				LastToken:   lastToken,
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
	models, _, _, err := s.parseFileWithTiming(ctx, filePath, sourceDB, collectionName)
	return models, err
}

// parseFileWithTiming reads one buffer file, reporting the changes to apply,
// when the source made the most recent of them, and the resume token the stream
// would continue from once they have landed.
func (s *MongoDBSyncer) parseFileWithTiming(ctx context.Context, filePath string, sourceDB, collectionName string) ([]mongo.WriteModel, time.Time, bson.Raw, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, time.Time{}, nil, fmt.Errorf("failed to open file: %w", err)
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
	var newest time.Time
	var lastToken bson.Raw

	for scanner.Scan() {
		eventData := scanner.Bytes()
		if len(eventData) == 0 {
			continue
		}

		if at, ok := eventClusterTime(bson.Raw(eventData)); ok && at.After(newest) {
			newest = at
		}
		if token, ok := eventResumeToken(bson.Raw(eventData)); ok {
			lastToken = token
		}
		model := s.convertRawBSONToWriteModel(eventData, sourceDB, collectionName)
		if model != nil {
			writeModels = append(writeModels, model)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, time.Time{}, nil, fmt.Errorf("error reading from file stream: %w", err)
	}

	s.logger.Debugf("[MongoDB] Parsed file %s: extracted %d write models",
		filepath.Base(filePath), len(writeModels))

	return writeModels, newest, lastToken, nil
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
