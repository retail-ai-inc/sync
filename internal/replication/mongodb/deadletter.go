package mongodb

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/slack"
	"go.mongodb.org/mongo-driver/mongo"
)

// FailedOperation represents a failed database operation for dead letter queue
type FailedOperation struct {
	ID         string          `json:"id"`
	WriteModel json.RawMessage `json:"write_model"` // Serialized WriteModel
	Error      string          `json:"error"`       // Error message
	ErrorCode  int             `json:"error_code"`  // MongoDB error code
	OpType     string          `json:"op_type"`     // insert, update, replace, delete
	SourceDB   string          `json:"source_db"`
	SourceColl string          `json:"source_coll"`
	Timestamp  time.Time       `json:"timestamp"`
	RetryCount int             `json:"retry_count"`
	BatchIndex int             `json:"batch_index"` // Index in the original batch
}

// DeadLetterBatch represents a batch of failed operations
type DeadLetterBatch struct {
	BatchID       string            `json:"batch_id"`
	FailedOps     []FailedOperation `json:"failed_operations"`
	TotalOps      int               `json:"total_operations"`
	SuccessfulOps int               `json:"successful_operations"`
	Timestamp     time.Time         `json:"timestamp"`
	SourceDB      string            `json:"source_db"`
	SourceColl    string            `json:"source_coll"`
}

// storeToDeadLetterQueue stores failed operations to dead letter queue
func (s *MongoDBSyncer) storeToDeadLetterQueue(failedModels []mongo.WriteModel, failedErrors []mongo.BulkWriteError, totalOps, successfulOps int, sourceDB, collectionName string) error {
	if !s.enableDeadLetterQueue {
		return nil
	}

	// Create dead letter directory for this collection
	collectionDir := filepath.Join(s.deadLetterDir, fmt.Sprintf("%s_%s", sourceDB, collectionName))
	if err := os.MkdirAll(collectionDir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create dead letter collection directory: %w", err)
	}

	// Convert failed operations to serializable format
	var failedOps []FailedOperation
	for i, model := range failedModels {
		// Serialize WriteModel to JSON
		modelBytes, err := s.serializeWriteModel(model)
		if err != nil {
			s.logger.Warnf("[MongoDB] Failed to serialize WriteModel: %v", err)
			continue
		}

		// Get error information
		var errorMsg string
		var errorCode int
		if i < len(failedErrors) {
			errorMsg = failedErrors[i].Message
			errorCode = failedErrors[i].Code
		}

		// Determine operation type
		opType := s.getOperationType(model)

		failedOp := FailedOperation{
			ID:         fmt.Sprintf("%s_%d_%d", sourceDB, time.Now().UnixNano(), i),
			WriteModel: modelBytes,
			Error:      errorMsg,
			ErrorCode:  errorCode,
			OpType:     opType,
			SourceDB:   sourceDB,
			SourceColl: collectionName,
			Timestamp:  time.Now(),
			RetryCount: 0,
			BatchIndex: i,
		}

		failedOps = append(failedOps, failedOp)
	}

	// Create dead letter batch
	batchID := fmt.Sprintf("batch_%s_%s_%d", sourceDB, collectionName, time.Now().UnixNano())
	deadLetterBatch := DeadLetterBatch{
		BatchID:       batchID,
		FailedOps:     failedOps,
		TotalOps:      totalOps,
		SuccessfulOps: successfulOps,
		Timestamp:     time.Now(),
		SourceDB:      sourceDB,
		SourceColl:    collectionName,
	}

	// Save to file
	fileName := fmt.Sprintf("%s.json", batchID)
	filePath := filepath.Join(collectionDir, fileName)

	batchBytes, err := json.MarshalIndent(deadLetterBatch, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal dead letter batch: %w", err)
	}

	if err := os.WriteFile(filePath, batchBytes, 0644); err != nil {
		return fmt.Errorf("failed to write dead letter batch file: %w", err)
	}

	s.logger.Infof("[MongoDB] Stored %d failed operations to dead letter queue: %s", len(failedOps), filePath)

	// Send Slack notification for dead letter queue storage
	if s.globalConfig != nil {
		slackNotifier := slack.NewSlackNotifierFromConfigWithFieldLogger(s.globalConfig, s.logger)
		if slackNotifier.IsConfigured() {
			message := fmt.Sprintf("🚨 MongoDB Operations Failed - Dead Letter Queue\n\nDatabase: %s.%s\nFailed Operations: %d\nTotal Operations: %d\nSuccess Rate: %.1f%%\n\nFile: %s",
				sourceDB, collectionName, len(failedOps), totalOps,
				float64(successfulOps)/float64(totalOps)*100, fileName)

			// Send notification asynchronously to avoid blocking main process
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				err := slackNotifier.SendError(ctx, "MongoDB Dead Letter Queue", message)
				if err != nil {
					s.logger.Warnf("[MongoDB] Failed to send dead letter queue Slack notification: %v", err)
				}
			}()
		}
	}

	return nil
}

// processDeadLetterQueue processes failed operations from dead letter queue
func (s *MongoDBSyncer) processDeadLetterQueue(ctx context.Context, sourceDB, collectionName, targetDBName, targetCollectionName string) {
	collectionDir := filepath.Join(s.deadLetterDir, fmt.Sprintf("%s_%s", sourceDB, collectionName))

	if _, err := os.Stat(collectionDir); os.IsNotExist(err) {
		return // No dead letter queue for this collection
	}

	files, err := os.ReadDir(collectionDir)
	if err != nil {
		s.logger.Errorf("[MongoDB] Failed to read dead letter queue directory %s: %v", collectionDir, err)
		return
	}

	if len(files) == 0 {
		return
	}

	targetColl := s.targetClient.Database(targetDBName).Collection(targetCollectionName)
	processedFiles := 0

	s.logger.Debugf("[MongoDB] Processing dead letter queue for %s.%s: %d files", sourceDB, collectionName, len(files))

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".json") {
			continue
		}

		filePath := filepath.Join(collectionDir, file.Name())
		processed := s.processDeadLetterBatch(ctx, filePath, targetColl, sourceDB, collectionName)
		if processed {
			processedFiles++
		}
	}

	if processedFiles > 0 {
		s.logger.Infof("[MongoDB] Processed %d dead letter batches for %s.%s", processedFiles, sourceDB, collectionName)
	}
}

// processDeadLetterBatch processes a single dead letter batch file
func (s *MongoDBSyncer) processDeadLetterBatch(ctx context.Context, filePath string, targetColl *mongo.Collection, sourceDB, collectionName string) bool {
	// Read and parse the dead letter batch
	batchData, err := os.ReadFile(filePath)
	if err != nil {
		s.logger.Errorf("[MongoDB] Failed to read dead letter batch file %s: %v", filePath, err)
		return false
	}

	var batch DeadLetterBatch
	if err := json.Unmarshal(batchData, &batch); err != nil {
		s.logger.Errorf("[MongoDB] Failed to unmarshal dead letter batch %s: %v", filePath, err)
		return false
	}

	// Check if any operations need retry
	var operationsToRetry []FailedOperation
	for _, op := range batch.FailedOps {
		if op.RetryCount < s.maxRetryAttempts {
			operationsToRetry = append(operationsToRetry, op)
		}
	}

	if len(operationsToRetry) == 0 {
		s.logger.Debugf("[MongoDB] No operations to retry in batch %s (all exceeded max retries)", batch.BatchID)
		return false
	}

	s.logger.Infof("[MongoDB] Retrying %d operations from dead letter batch %s", len(operationsToRetry), batch.BatchID)

	// Convert back to WriteModels and retry
	var retryModels []mongo.WriteModel
	var stillFailedOps []FailedOperation

	for _, op := range operationsToRetry {
		writeModel, err := s.deserializeWriteModel(op.WriteModel, collectionName)
		if err != nil {
			s.logger.Warnf("[MongoDB] Failed to deserialize WriteModel for operation %s: %v", op.ID, err)
			op.RetryCount++
			stillFailedOps = append(stillFailedOps, op)
			continue
		}

		// Skip nil models (e.g., when delete operations are ignored)
		if writeModel != nil {
			retryModels = append(retryModels, writeModel)
		}
	}

	// Execute retry operations
	successCount := 0
	for i, model := range retryModels {
		err := s.executeIndividualOperation(ctx, targetColl, model)
		if err != nil {
			// Increment retry count and keep in failed list
			operationsToRetry[i].RetryCount++
			operationsToRetry[i].Error = err.Error()
			stillFailedOps = append(stillFailedOps, operationsToRetry[i])
			s.logger.Warnf("[MongoDB] Retry failed for operation %s (attempt %d/%d): %v",
				operationsToRetry[i].ID, operationsToRetry[i].RetryCount, s.maxRetryAttempts, err)
		} else {
			successCount++
			s.logger.Debugf("[MongoDB] Retry succeeded for operation %s", operationsToRetry[i].ID)
		}
	}

	// Update the batch with remaining failed operations
	batch.FailedOps = stillFailedOps

	if len(stillFailedOps) == 0 {
		// All operations succeeded, delete the file
		if err := os.Remove(filePath); err != nil {
			s.logger.Warnf("[MongoDB] Failed to delete completed dead letter batch %s: %v", filePath, err)
		} else {
			s.logger.Infof("[MongoDB] Successfully processed and deleted dead letter batch %s", batch.BatchID)
		}
		return true
	} else {
		// Update the file with remaining failed operations
		updatedData, err := json.MarshalIndent(batch, "", "  ")
		if err != nil {
			s.logger.Errorf("[MongoDB] Failed to marshal updated dead letter batch %s: %v", batch.BatchID, err)
			return false
		}

		if err := os.WriteFile(filePath, updatedData, 0644); err != nil {
			s.logger.Errorf("[MongoDB] Failed to update dead letter batch file %s: %v", filePath, err)
			return false
		}

		s.logger.Infof("[MongoDB] Updated dead letter batch %s: %d succeeded, %d still failed",
			batch.BatchID, successCount, len(stillFailedOps))
		return true
	}
}

// getDeadLetterQueueStats returns statistics about the dead letter queue
func (s *MongoDBSyncer) getDeadLetterQueueStats(sourceDB, collectionName string) (int, int, error) {
	collectionDir := filepath.Join(s.deadLetterDir, fmt.Sprintf("%s_%s", sourceDB, collectionName))

	if _, err := os.Stat(collectionDir); os.IsNotExist(err) {
		return 0, 0, nil
	}

	files, err := os.ReadDir(collectionDir)
	if err != nil {
		return 0, 0, err
	}

	totalBatches := 0
	totalFailedOps := 0

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".json") {
			continue
		}

		filePath := filepath.Join(collectionDir, file.Name())
		batchData, err := os.ReadFile(filePath)
		if err != nil {
			continue
		}

		var batch DeadLetterBatch
		if err := json.Unmarshal(batchData, &batch); err != nil {
			continue
		}

		totalBatches++
		totalFailedOps += len(batch.FailedOps)
	}

	return totalBatches, totalFailedOps, nil
}
