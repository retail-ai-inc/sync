package mongodb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// streamEvent represents the data passed from the Change Stream reader to the disk writer.
// It contains the raw BSON data and the corresponding resume token.
type streamEvent struct {
	RawData     bson.Raw
	ResumeToken bson.Raw
}

func (s *MongoDBSyncer) watchChanges(ctx context.Context, sourceColl, targetColl *mongo.Collection, sourceDB, collectionName string) {
	key := fmt.Sprintf("%s.%s", sourceDB, collectionName)

	s.processorMutex.Lock()
	if cancelFunc, exists := s.activeProcessors[key]; exists {
		cancelFunc()
	}
	processorCtx, cancel := context.WithCancel(ctx)
	s.activeProcessors[key] = cancel
	s.processorMutex.Unlock()

	defer func() {
		s.processorMutex.Lock()
		delete(s.activeProcessors, key)
		s.processorMutex.Unlock()
		cancel()
	}()

	eventChannel := make(chan streamEvent, s.channelCapacity)

	go s.diskWriter(processorCtx, eventChannel, sourceDB, collectionName)
	go s.processPersistentBuffer(processorCtx, sourceDB, collectionName, targetColl.Database().Name(), targetColl.Name())

	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "ns.db", Value: sourceDB},
			{Key: "ns.coll", Value: collectionName},
			{Key: "operationType", Value: bson.M{"$in": []string{"insert", "update", "replace", "delete"}}},
		}}},
	}
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	resumeToken := s.loadMongoDBResumeToken(sourceDB, collectionName)
	if resumeToken != nil {
		opts.SetResumeAfter(resumeToken)
	}

	cs, err := sourceColl.Watch(ctx, pipeline, opts)
	if err != nil {
		s.logger.Errorf("[MongoDB] Failed to establish change stream for %s.%s: %v", sourceDB, collectionName, err)
		return
	}
	defer cs.Close(ctx)
	s.logger.Infof("[MongoDB] Watching changes => %s.%s", sourceDB, collectionName)

	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("[MongoDB] Context cancelled, stopping change stream for %s.%s", sourceDB, collectionName)
			return
		default:
			if !cs.Next(ctx) {
				if err := cs.Err(); err != nil {
					s.logger.Errorf("[MongoDB] Change stream error for %s.%s: %v", sourceDB, collectionName, err)

					// Check if this is a recoverable error
					if isRecoverableError(err) {
						s.logger.Warnf("[MongoDB] Recoverable error detected for %s.%s, will be retried by guardian", sourceDB, collectionName)
					} else {
						s.logger.Errorf("[MongoDB] Non-recoverable error for %s.%s, stopping", sourceDB, collectionName)
					}
				} else {
					s.logger.Infof("[MongoDB] Change stream ended normally for %s.%s", sourceDB, collectionName)
				}
				// Stream closed or an error occurred, exit and let the guardian restart.
				return
			}

			event := streamEvent{
				RawData:     cs.Current,
				ResumeToken: cs.ResumeToken(),
			}

			select {
			case eventChannel <- event:
				// Event successfully sent
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
				s.logger.Errorf("[MongoDB] Channel full for 10 seconds, potential deadlock or severe bottleneck for %s.%s. Stopping.", sourceDB, collectionName)
				return
			}
		}
	}
}

// watchChangesWithRetry is a guardian wrapper around watchChanges that provides automatic retry and recovery
func (s *MongoDBSyncer) watchChangesWithRetry(ctx context.Context, sourceColl, targetColl *mongo.Collection, sourceDB, collectionName string) {
	// Get retry settings from configuration
	advancedSettings := s.findTableAdvancedSettings(collectionName)
	maxRetries := advancedSettings.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 10 // Default value
	}

	baseDelay := advancedSettings.BaseRetryDelay
	if baseDelay <= 0 {
		baseDelay = 5 * time.Second // Default value
	}

	maxDelay := advancedSettings.MaxRetryDelay
	if maxDelay <= 0 {
		maxDelay = 5 * time.Minute // Default value
	}

	currentDelay := baseDelay
	retryCount := 0

	s.logger.Infof("[MongoDB] Starting guardian loop for %s.%s", sourceDB, collectionName)

	for {
		select {
		case <-ctx.Done():
			s.logger.Infof("[MongoDB] Guardian loop cancelled for %s.%s", sourceDB, collectionName)
			return
		default:
			s.logger.Infof("[MongoDB] Attempting to start watchChanges for %s.%s (attempt %d)", sourceDB, collectionName, retryCount+1)

			// Start the actual watchChanges in a separate goroutine
			watchCtx, watchCancel := context.WithCancel(ctx)
			watchDone := make(chan struct{})

			go func() {
				defer close(watchDone)
				s.watchChanges(watchCtx, sourceColl, targetColl, sourceDB, collectionName)
			}()

			// Wait for watchChanges to complete or context to be cancelled
			select {
			case <-watchDone:
				// watchChanges has exited, check if it was due to context cancellation
				select {
				case <-ctx.Done():
					s.logger.Infof("[MongoDB] Guardian loop stopping due to context cancellation for %s.%s", sourceDB, collectionName)
					return
				default:
					// watchChanges exited due to error, retry
					retryCount++
					if retryCount > maxRetries {
						s.logger.Errorf("[MongoDB] Max retries (%d) exceeded for %s.%s, stopping guardian loop", maxRetries, sourceDB, collectionName)
						return
					}

					s.logger.Warnf("[MongoDB] watchChanges exited for %s.%s, retrying in %v (attempt %d/%d)",
						sourceDB, collectionName, currentDelay, retryCount, maxRetries)

					// Exponential backoff with jitter
					select {
					case <-ctx.Done():
						return
					case <-time.After(currentDelay):
						// Increase delay for next retry
						currentDelay = time.Duration(float64(currentDelay) * 1.5)
						if currentDelay > maxDelay {
							currentDelay = maxDelay
						}
					}
				}
			case <-ctx.Done():
				watchCancel()
				<-watchDone // Wait for watchChanges to clean up
				return
			}
		}
	}
}

// isRecoverableError determines if a MongoDB error is recoverable and should trigger a retry
func isRecoverableError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()

	// Network-related errors that are typically recoverable
	recoverablePatterns := []string{
		"server selection timeout",
		"connection refused",
		"network timeout",
		"no reachable servers",
		"connection pool exhausted",
		"write concern timeout",
		"read concern timeout",
		"cursor not found",
		"interrupted at shutdown",
		"host unreachable",
		"connection reset",
		"broken pipe",
		"i/o timeout",
	}

	for _, pattern := range recoverablePatterns {
		if strings.Contains(strings.ToLower(errStr), strings.ToLower(pattern)) {
			return true
		}
	}

	// Check for specific MongoDB error codes that are recoverable
	if strings.Contains(errStr, "error code 11600") || // InterruptedAtShutdown
		strings.Contains(errStr, "error code 11602") || // InterruptedDueToReplStateChange
		strings.Contains(errStr, "error code 10107") || // NotMaster
		strings.Contains(errStr, "error code 189") { // PrimarySteppedDown
		return true
	}

	return false
}
