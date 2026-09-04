package mongodb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/internal/platform/resilience"
	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
	"github.com/retail-ai-inc/sync/internal/replication/infra/directionlock"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type MongoDBSyncer struct {
	sourceClient  *mongo.Client
	targetClient  *mongo.Client
	cfg           config.SyncConfig
	logger        logrus.FieldLogger
	resumeTokens  map[string]bson.Raw
	resumeTokensM sync.RWMutex
	// New fields for persistent buffer
	bufferDir     string
	bufferEnabled bool
	// Fields for goroutine lifecycle management
	activeProcessors map[string]context.CancelFunc
	processorMutex   sync.RWMutex
	// New fields for async pipeline
	channelCapacity int
	// Smart batch controller configuration
	targetBatchSizeBytes int64
	maxFilesPerBatch     int
	minFilesPerBatch     int
	// Dead letter queue configuration
	deadLetterDir         string
	maxRetryAttempts      int
	retryInterval         time.Duration
	enableDeadLetterQueue bool
	// Global configuration for accessing Slack settings
	globalConfig *config.Config
	// checkpoints is where the resume tokens and start times are recorded.
	checkpoints checkpoint.Store
	// connectErr is why the constructor could not reach one side, kept so Start
	// can report it rather than the caller having to notice a nil syncer.
	connectErr error
	// faults carries the first reason a watcher gave up, so Start can report it
	// rather than blocking on collections that have all stopped. It holds one
	// value: the first reason is the one worth acting on and the rest follow
	// from it.
	faults chan error
}

// NewMongoDBSyncer builds the syncer for one task. It never returns nil.
func NewMongoDBSyncer(cfg config.SyncConfig, globalConfig *config.Config, logger *logrus.Logger) *MongoDBSyncer {
	ctx := context.Background()

	sourceClient, sourceErr := connectMongo(ctx, cfg.SourceConnection)
	if sourceErr != nil {
		logger.Errorf("[MongoDB] Could not connect to the source: %v", sourceErr)
	}
	targetClient, targetErr := connectMongo(ctx, cfg.TargetConnection)
	if targetErr != nil {
		logger.Errorf("[MongoDB] Could not connect to the target: %v", targetErr)
	}
	connectErr := sourceErr
	if connectErr == nil {
		connectErr = targetErr
	}

	resumeMap := make(map[string]bson.Raw)
	if cfg.MongoDBResumeTokenPath != "" {
		if e2 := os.MkdirAll(cfg.MongoDBResumeTokenPath, os.ModePerm); e2 != nil {
			logger.Warnf("[MongoDB] Failed to create resume token dir %s: %v", cfg.MongoDBResumeTokenPath, e2)
		}
	}

	bufferDir := cfg.MongoDBResumeTokenPath
	if bufferDir == "" {
		bufferDir = "./mongodb_buffer"
	} else {
		bufferDir = filepath.Join(bufferDir, "buffer")
	}

	if err := os.MkdirAll(bufferDir, os.ModePerm); err != nil {
		logger.Warnf("[MongoDB] Failed to create buffer directory %s: %v", bufferDir, err)
	}

	deadLetterDir := cfg.MongoDBResumeTokenPath
	if deadLetterDir == "" {
		deadLetterDir = "./mongodb_dead_letter"
	} else {
		deadLetterDir = filepath.Join(deadLetterDir, "dead_letter")
	}

	if err := os.MkdirAll(deadLetterDir, os.ModePerm); err != nil {
		logger.Warnf("[MongoDB] Failed to create dead letter directory %s: %v", deadLetterDir, err)
	}

	return &MongoDBSyncer{
		sourceClient:          sourceClient,
		targetClient:          targetClient,
		connectErr:            connectErr,
		cfg:                   cfg,
		logger:                logger.WithField("sync_task_id", cfg.ID),
		resumeTokens:          resumeMap,
		bufferDir:             bufferDir,
		bufferEnabled:         true, // Enable persistent buffer by default
		activeProcessors:      make(map[string]context.CancelFunc),
		channelCapacity:       200,               // A smaller, safer default to prevent OOM.
		targetBatchSizeBytes:  256 * 1024 * 1024, // 256MB - Increased batch size for higher throughput
		maxFilesPerBatch:      1000,
		minFilesPerBatch:      5,
		deadLetterDir:         deadLetterDir,
		maxRetryAttempts:      3,
		retryInterval:         time.Second * 5,
		enableDeadLetterQueue: true,
		globalConfig:          globalConfig,
	}
}

// connectMongo dials one side, retrying while the failure looks like something
// a later attempt could survive. A URI the driver rejects is not: it is
// returned immediately so the caller can say so.
func connectMongo(ctx context.Context, uri string) (*mongo.Client, error) {
	var client *mongo.Client

	err := resilience.Retry(ctx, 5, 2*time.Second, 2.0, func() error {
		var connErr error
		// Compression is negotiated, so a server that does not offer it simply
		// goes uncompressed. It earns its place on the copy: every document
		// crosses the wire twice, and for the Tokyo-to-Osaka case that wire is
		// charged for. A URI that names its own compressors keeps them.
		opts := options.Client().ApplyURI(uri)
		if len(opts.Compressors) == 0 {
			opts.SetCompressors([]string{"zstd", "snappy"})
		}
		client, connErr = mongo.Connect(opts)
		if isURIError(connErr) {
			return permanentURI{connErr}
		}
		return connErr
	})
	if err != nil {
		return nil, err
	}
	return client, nil
}

func isURIError(err error) bool {
	if err == nil {
		return false
	}
	text := err.Error()
	return strings.Contains(text, "scheme must be") ||
		strings.Contains(text, "error parsing uri") ||
		strings.Contains(text, "invalid connection string")
}

type permanentURI struct{ error }

func (p permanentURI) Unwrap() error   { return p.error }
func (p permanentURI) Permanent() bool { return true }

func (s *MongoDBSyncer) ensureCollectionExists(ctx context.Context, db *mongo.Database, collName string) error {
	collections, err := db.ListCollectionNames(ctx, bson.M{"name": collName})
	if err != nil {
		s.logger.Errorf("[MongoDB] ListCollectionNames failed => %v", err)
		return err
	}
	if len(collections) == 0 {
		s.logger.Infof("[MongoDB] Creating collection => %s.%s", db.Name(), collName)
		if createErr := db.CreateCollection(ctx, collName); createErr != nil {
			return createErr
		}
	}
	return nil
}

func (s *MongoDBSyncer) copyIndexes(ctx context.Context, sourceColl, targetColl *mongo.Collection) error {
	cursor, err := sourceColl.Indexes().List(ctx)
	if err != nil {
		return fmt.Errorf("list source indexes fail: %w", err)
	}
	defer cursor.Close(ctx)

	var indexDocs []bson.M
	if err2 := cursor.All(ctx, &indexDocs); err2 != nil {
		return fmt.Errorf("read indexes fail: %w", err2)
	}

	targetCursor, err := targetColl.Indexes().List(ctx)
	if err != nil {
		s.logger.Warnf("[MongoDB] Failed to list existing indexes: %v", err)
	}

	existingIndexes := make(map[string]bool)
	if targetCursor != nil {
		var targetIdxDocs []bson.M
		if err := targetCursor.All(ctx, &targetIdxDocs); err == nil {
			for _, idx := range targetIdxDocs {
				if name, ok := idx["name"].(string); ok {
					existingIndexes[name] = true
				}
			}
		}
		targetCursor.Close(ctx)
	}

	indexesCreated := 0
	indexesSkipped := 0
	var failed []string

	for _, idx := range indexDocs {
		if name, ok := idx["name"].(string); ok && name == "_id_" {
			continue
		}

		var name string
		if nameStr, ok := idx["name"].(string); ok {
			name = nameStr
			if existingIndexes[name] {
				s.logger.Debugf("[MongoDB] Index %s already exists, skipping", name)
				indexesSkipped++
				continue
			}
		}

		keyDoc, ok := indexKeyOf(idx["key"])
		if !ok {
			s.logger.Warnf("[MongoDB] Cannot read the key of index %q, so it is not being "+
				"copied: %v", name, idx["key"])
			continue
		}

		indexOptions := options.Index()

		// A text index does not come back the way it was made. The server reports
		// its key as {_fts: "text", _ftsx: 1} and puts the indexed fields in
		// "weights", and creating an index from that key is refused -- "text index
		// option 'weights' must specify fields or the wildcard". The fields are
		// read back out of the weights instead. Without this the copy silently
		// carried every index but the text ones, which are the ones a search
		// depends on.
		if textIndexKey(keyDoc) {
			rebuilt, weights, okText := textIndexFrom(idx["weights"])
			if !okText {
				s.logger.Warnf("[MongoDB] Cannot read the weights of text index %q, so "+
					"it is not being copied: %v", name, idx["weights"])
				failed = append(failed, name)
				continue
			}
			keyDoc = rebuilt
			indexOptions.SetWeights(weights)
			if v, okStr := idx["default_language"].(string); okStr {
				indexOptions.SetDefaultLanguage(v)
			}
			if v, okStr := idx["language_override"].(string); okStr {
				indexOptions.SetLanguageOverride(v)
			}
		}

		if uniqueVal, hasUnique := idx["unique"]; hasUnique {
			if uv, isBool := uniqueVal.(bool); isBool && uv {
				indexOptions.SetUnique(true)
			}
		}

		if nameVal, hasName := idx["name"]; hasName {
			if nameStr, ok := nameVal.(string); ok {
				name = nameStr
				indexOptions.SetName(nameStr)
			}
		}

		indexModel := mongo.IndexModel{
			Keys:    keyDoc,
			Options: indexOptions,
		}

		_, errC := targetColl.Indexes().CreateOne(ctx, indexModel)
		if errC != nil {
			if strings.Contains(errC.Error(), "already exists") {
				indexesSkipped++
			} else {
				s.logger.Warnf("[MongoDB] Create index %s fail: %v", name, errC)
				failed = append(failed, name)
			}
		} else {
			indexesCreated++
		}
	}

	s.logger.Infof("[MongoDB] Index creation summary for %s: created=%d, skipped=%d, failed=%d",
		targetColl.Name(), indexesCreated, indexesSkipped, len(failed))

	// An index that could not be created is reported to the caller rather than
	// left in the log. The caller warns and carries on -- missing an index is
	// slow, missing data is wrong -- but a summary that counts only successes is
	// how a standby ends up short of the indexes its queries need with nothing
	// anywhere saying so.
	if len(failed) > 0 {
		return fmt.Errorf("%d of the source's indexes on %s could not be created: %s",
			len(failed), targetColl.Name(), strings.Join(failed, ", "))
	}
	return nil
}

// textIndexKey reports whether a key is the shape the server hands back for a
// text index rather than the shape one is created from.
func textIndexKey(key bson.D) bool {
	for _, e := range key {
		if e.Key == "_fts" || e.Key == "_ftsx" {
			return true
		}
	}
	return false
}

// textIndexFrom rebuilds a text index's key and weights from the weights
// document, which is where the server keeps the fields the index actually
// covers.
func textIndexFrom(raw interface{}) (key bson.D, weights bson.D, ok bool) {
	weights, ok = indexKeyOf(raw)
	if !ok {
		return nil, nil, false
	}
	key = make(bson.D, 0, len(weights))
	for _, e := range weights {
		key = append(key, bson.E{Key: e.Key, Value: "text"})
	}
	return key, weights, true
}

// indexKeyOf reads an index's key specification, whatever shape the driver
// decoded it into. The shape is not something to rely on.
func indexKeyOf(key interface{}) (bson.D, bool) {
	direction := func(v interface{}) interface{} {
		switch typed := v.(type) {
		case string:
			// "1" and "-1" reach here from a JSON round trip; a named kind —
			// "2dsphere", "text", "hashed" — is passed through as it is.
			switch typed {
			case "1":
				return int32(1)
			case "-1":
				return int32(-1)
			}
			return typed
		case float64:
			return int32(typed)
		case int64:
			return int32(typed)
		case int:
			return int32(typed)
		}
		return v
	}

	switch typed := key.(type) {
	case bson.D:
		out := make(bson.D, 0, len(typed))
		for _, e := range typed {
			out = append(out, bson.E{Key: e.Key, Value: direction(e.Value)})
		}
		return out, len(out) > 0
	case bson.M:
		out := make(bson.D, 0, len(typed))
		for field, v := range typed {
			out = append(out, bson.E{Key: field, Value: direction(v)})
		}
		return out, len(out) > 0
	case bson.Raw:
		elements, err := typed.Elements()
		if err != nil {
			return nil, false
		}
		out := make(bson.D, 0, len(elements))
		for _, e := range elements {
			var v interface{}
			if err := e.Value().Unmarshal(&v); err != nil {
				return nil, false
			}
			out = append(out, bson.E{Key: e.Key(), Value: direction(v)})
		}
		return out, len(out) > 0
	}
	return nil, false
}

func (s *MongoDBSyncer) findTableAdvancedSettings(collName string) config.AdvancedSettings {
	for _, m := range s.cfg.Mappings {
		for _, t := range m.Tables {
			if t.SourceTable == collName {
				return t.AdvancedSettings
			}
		}
	}
	return config.AdvancedSettings{}
}

// claimDirection records which way this task replicates, on both databases, and
// keeps the claims refreshed for as long as it runs.
func (s *MongoDBSyncer) claimDirection(ctx context.Context, sourceDBName, targetDBName string) (func(), error) {
	guard := &directionlock.Guard{
		TaskID: s.cfg.ID,
		Source: &directionlock.MongoStore{
			Database: s.sourceClient.Database(sourceDBName),
			Address:  dsn.Endpoint(s.cfg.Type, s.cfg.SourceConnection),
		},
		Target: &directionlock.MongoStore{
			Database: s.targetClient.Database(targetDBName),
			Address:  dsn.Endpoint(s.cfg.Type, s.cfg.TargetConnection),
		},
	}

	return directionlock.Hold(ctx, guard, s.logger, "MongoDB")
}

// discoveryInterval is how often a task with no configured collections looks
// for ones that have appeared since it started.
const discoveryInterval = time.Minute

// unlistedScanEvery is how often a task that names its collections is compared
// against what the source actually holds.
const unlistedScanEvery = 5 * time.Minute
