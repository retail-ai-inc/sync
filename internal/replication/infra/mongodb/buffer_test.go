package mongodb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/retail-ai-inc/sync/internal/platform/config"
)

// newBufferSyncer builds a syncer with only the fields the disk-side helpers
// use, so the buffer, batching and dead-letter logic can be exercised without
// a MongoDB server.
func newBufferSyncer(t *testing.T) *MongoDBSyncer {
	t.Helper()

	root := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)

	return &MongoDBSyncer{
		logger:                logger,
		bufferDir:             filepath.Join(root, "buffer"),
		deadLetterDir:         filepath.Join(root, "dead_letter"),
		targetBatchSizeBytes:  256 * 1024 * 1024,
		maxFilesPerBatch:      1000,
		minFilesPerBatch:      5,
		enableDeadLetterQueue: true,
		resumeTokens:          map[string]bson.Raw{},
	}
}

func writeBufferFiles(t *testing.T, dir string, sizes ...int) []string {
	t.Helper()

	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create %s: %v", dir, err)
	}
	names := make([]string, 0, len(sizes))
	for i, size := range sizes {
		// The production namer uses batch_<UnixNano>; the fixed width here keeps
		// lexicographic order equal to creation order, which is what ReadDir
		// relies on.
		name := fmt.Sprintf("batch_%019d.bsonstream", time.Now().UnixNano()+int64(i))
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, make([]byte, size), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
		names = append(names, path)
	}
	return names
}

func TestGetBufferAndResumeTokenPaths(t *testing.T) {
	s := newBufferSyncer(t)

	if got, want := s.getBufferPath("source_db", "users"), filepath.Join(s.bufferDir, "source_db_users"); got != want {
		t.Errorf("getBufferPath = %q, want %q", got, want)
	}
	// Collections in different databases must not share a directory.
	if s.getBufferPath("a", "users") == s.getBufferPath("b", "users") {
		t.Error("two databases share one buffer directory")
	}
	if s.getResumeTokenPath("a", "users") == s.getResumeTokenPath("b", "users") {
		t.Error("two databases share one resume token path")
	}
}

func TestBuildSmartBatch(t *testing.T) {
	t.Run("missing directory yields nothing", func(t *testing.T) {
		s := newBufferSyncer(t)
		files, size := s.buildSmartBatch(filepath.Join(s.bufferDir, "nope"))
		if files != nil || size != 0 {
			t.Errorf("got %d files / %d bytes, want nothing", len(files), size)
		}
	})

	t.Run("empty directory yields nothing", func(t *testing.T) {
		s := newBufferSyncer(t)
		dir := s.getBufferPath("db", "coll")
		writeBufferFiles(t, dir)
		files, size := s.buildSmartBatch(dir)
		if files != nil || size != 0 {
			t.Errorf("got %d files / %d bytes, want nothing", len(files), size)
		}
	})

	t.Run("selects every file when well under the target", func(t *testing.T) {
		s := newBufferSyncer(t)
		dir := s.getBufferPath("db", "coll")
		writeBufferFiles(t, dir, 100, 200, 300)

		files, size := s.buildSmartBatch(dir)
		if len(files) != 3 || size != 600 {
			t.Errorf("got %d files / %d bytes, want 3 / 600", len(files), size)
		}
	})

	t.Run("files come back in creation order", func(t *testing.T) {
		s := newBufferSyncer(t)
		dir := s.getBufferPath("db", "coll")
		written := writeBufferFiles(t, dir, 10, 10, 10, 10, 10, 10)

		files, _ := s.buildSmartBatch(dir)
		if len(files) != len(written) {
			t.Fatalf("got %d files, want %d", len(files), len(written))
		}
		for i := range files {
			if files[i] != written[i] {
				t.Errorf("file %d is %q, want %q; ordering is what keeps replay "+
					"chronological", i, filepath.Base(files[i]), filepath.Base(written[i]))
			}
		}
	})

	t.Run("stops at maxFilesPerBatch", func(t *testing.T) {
		s := newBufferSyncer(t)
		s.maxFilesPerBatch = 3
		dir := s.getBufferPath("db", "coll")
		writeBufferFiles(t, dir, 10, 10, 10, 10, 10)

		files, _ := s.buildSmartBatch(dir)
		if len(files) != 3 {
			t.Errorf("got %d files, want 3", len(files))
		}
	})

	t.Run("stops at the target size once the minimum is met", func(t *testing.T) {
		s := newBufferSyncer(t)
		s.targetBatchSizeBytes = 1000
		s.minFilesPerBatch = 2
		dir := s.getBufferPath("db", "coll")
		writeBufferFiles(t, dir, 400, 400, 400, 400)

		files, size := s.buildSmartBatch(dir)
		if len(files) != 2 || size != 800 {
			t.Errorf("got %d files / %d bytes, want 2 / 800", len(files), size)
		}
	})
}

// TestBuildSmartBatchOvershootsTargetBelowMinimum records that the size limit
// is conditional on having already selected minFilesPerBatch files. Until then
// files are added regardless, so a batch of large files can exceed the target
// several times over — the memory ceiling the target is meant to impose does
// not hold for the first few files.
func TestBuildSmartBatchOvershootsTargetBelowMinimum(t *testing.T) {
	s := newBufferSyncer(t)
	s.targetBatchSizeBytes = 1000
	s.minFilesPerBatch = 5
	dir := s.getBufferPath("db", "coll")
	writeBufferFiles(t, dir, 900, 900, 900, 900, 900)

	files, size := s.buildSmartBatch(dir)

	if size <= s.targetBatchSizeBytes {
		t.Fatalf("batch is %d bytes, within the %d target; the minimum-count "+
			"exemption may have been removed, so assert that instead", size, s.targetBatchSizeBytes)
	}
	if len(files) != 5 || size != 4500 {
		t.Errorf("got %d files / %d bytes, want 5 / 4500 (4.5x the target)", len(files), size)
	}
}

func TestCalculateAverageFileSize(t *testing.T) {
	s := newBufferSyncer(t)
	dir := s.getBufferPath("db", "coll")

	if got := s.calculateAverageFileSize(filepath.Join(s.bufferDir, "nope")); got != 0 {
		t.Errorf("average over a missing directory = %d, want 0", got)
	}

	writeBufferFiles(t, dir, 100, 200, 300)
	if got, want := s.calculateAverageFileSize(dir), int64(200); got != want {
		t.Errorf("calculateAverageFileSize = %d, want %d", got, want)
	}
}

// TestEstimateOptimalBatchSizeChangesNothing records that the batch controller
// does not adapt. estimateOptimalBatchSize is called on a five-minute ticker
// and computes how many files would fit the target, but only logs the result —
// it never assigns to any field. updateBatchSizeConfig, which would apply such
// a change, has no callers at all. The batching parameters are therefore fixed
// constants set in the constructor.
func TestEstimateOptimalBatchSizeChangesNothing(t *testing.T) {
	s := newBufferSyncer(t)
	dir := s.getBufferPath("db", "coll")
	// Files far larger than the target, the case that would demand a change.
	s.targetBatchSizeBytes = 1000
	writeBufferFiles(t, dir, 5000, 5000)

	beforeTarget, beforeMax, beforeMin := s.getBatchSizeConfig()
	s.estimateOptimalBatchSize(dir)
	afterTarget, afterMax, afterMin := s.getBatchSizeConfig()

	if beforeTarget != afterTarget || beforeMax != afterMax || beforeMin != afterMin {
		t.Errorf("the configuration changed from %d/%d/%d to %d/%d/%d; adaptive "+
			"batching may have been implemented, so assert the new behaviour instead",
			beforeTarget, beforeMax, beforeMin, afterTarget, afterMax, afterMin)
	}
}

func TestUpdateBatchSizeConfig(t *testing.T) {
	// The setter works; nothing in the codebase calls it.
	s := newBufferSyncer(t)
	s.updateBatchSizeConfig(512*1024*1024, 2000, 10)

	target, max, min := s.getBatchSizeConfig()
	if target != 512*1024*1024 || max != 2000 || min != 10 {
		t.Errorf("config = %d/%d/%d, want 536870912/2000/10", target, max, min)
	}
}

func TestFlushBufferToDisk(t *testing.T) {
	s := newBufferSyncer(t)
	s.cfg = config.SyncConfig{MongoDBResumeTokenPath: t.TempDir()}

	first, err := bson.Marshal(bson.M{"operationType": "insert", "n": 1})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	second, err := bson.Marshal(bson.M{"operationType": "insert", "n": 2})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	token, err := bson.Marshal(bson.M{"_data": "resume-token"})
	if err != nil {
		t.Fatalf("marshal token: %v", err)
	}

	buffer := []streamEvent{
		{RawData: first, ResumeToken: token},
		{RawData: second, ResumeToken: token},
	}
	s.flushBufferToDisk(context.Background(), &buffer, "db", "coll")

	if len(buffer) != 0 {
		t.Errorf("the buffer still holds %d events; it must be cleared once persisted", len(buffer))
	}

	dir := s.getBufferPath("db", "coll")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read buffer dir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("buffer holds %d files, want 1", len(entries))
	}
	if !strings.HasPrefix(entries[0].Name(), "batch_") || !strings.HasSuffix(entries[0].Name(), ".bsonstream") {
		t.Errorf("file is named %q, want batch_<nanos>.bsonstream", entries[0].Name())
	}

	content, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	if err != nil {
		t.Fatalf("read buffer file: %v", err)
	}
	// Both documents plus one separator each.
	wantLen := len(first) + len(second) + 2*len(bsonRawSeparator)
	if len(content) != wantLen {
		t.Errorf("file is %d bytes, want %d", len(content), wantLen)
	}
}

func TestFlushBufferToDiskIgnoresEmptyBuffer(t *testing.T) {
	s := newBufferSyncer(t)
	var buffer []streamEvent

	s.flushBufferToDisk(context.Background(), &buffer, "db", "coll")

	if _, err := os.ReadDir(s.getBufferPath("db", "coll")); !os.IsNotExist(err) {
		t.Errorf("an empty buffer created a directory or file: %v", err)
	}
}

func TestGetOperationType(t *testing.T) {
	s := newBufferSyncer(t)

	tests := []struct {
		model mongo.WriteModel
		want  string
	}{
		{mongo.NewInsertOneModel(), "insert"},
		{mongo.NewUpdateOneModel(), "update"},
		{mongo.NewReplaceOneModel(), "replace"},
		{mongo.NewDeleteOneModel(), "delete"},
		{mongo.NewDeleteManyModel(), "unknown"},
		{nil, "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := s.getOperationType(tt.model); got != tt.want {
				t.Errorf("getOperationType(%T) = %q, want %q", tt.model, got, tt.want)
			}
		})
	}
}

func TestSerializeAndDeserializeWriteModel(t *testing.T) {
	s := newBufferSyncer(t)

	doc := bson.M{"_id": "abc", "name": "jack"}
	original := mongo.NewReplaceOneModel().
		SetFilter(bson.M{"_id": "abc"}).
		SetReplacement(doc).
		SetUpsert(true)

	raw, err := s.serializeWriteModel(original)
	if err != nil {
		t.Fatalf("serializeWriteModel: %v", err)
	}

	restored, err := s.deserializeWriteModel(raw, "coll")
	if err != nil {
		t.Fatalf("deserializeWriteModel: %v", err)
	}
	if got := s.getOperationType(restored); got != "replace" {
		t.Errorf("the restored model is a %q, want replace", got)
	}
}

func TestIsRecoverableError(t *testing.T) {
	recoverable := []string{
		"server selection timeout",
		"connection refused",
		"no reachable servers",
		"i/o timeout",
		"connection reset by peer",
		"broken pipe",
		"cursor not found",
		"SERVER SELECTION TIMEOUT", // matching is case-insensitive
		"error code 11600",         // InterruptedAtShutdown
		"error code 10107",         // NotMaster
		"error code 189",           // PrimarySteppedDown
	}
	for _, msg := range recoverable {
		t.Run("recoverable/"+msg, func(t *testing.T) {
			if !isRecoverableError(errors.New(msg)) {
				t.Errorf("isRecoverableError(%q) = false, want true", msg)
			}
		})
	}

	unrecoverable := []string{
		"duplicate key error",
		"document validation failure",
		"unauthorized",
		"error code 11000",
	}
	for _, msg := range unrecoverable {
		t.Run("unrecoverable/"+msg, func(t *testing.T) {
			if isRecoverableError(errors.New(msg)) {
				t.Errorf("isRecoverableError(%q) = true, want false", msg)
			}
		})
	}

	if isRecoverableError(nil) {
		t.Error("isRecoverableError(nil) = true, want false")
	}
}

// TestIsRecoverableErrorMissesDriverPhrasings records the fragility of matching
// on message text: several errors the driver really emits during a primary
// election or a network blip are not in the list, so the guardian treats them
// as fatal and gives up on the stream instead of retrying.
func TestIsRecoverableErrorMissesDriverPhrasings(t *testing.T) {
	missed := []string{
		"connection() error occurred during connection handshake",
		"socket was unexpectedly closed",
		"client is disconnected",
		"context deadline exceeded",
		"EOF",
	}

	for _, msg := range missed {
		t.Run(msg, func(t *testing.T) {
			if isRecoverableError(errors.New(msg)) {
				t.Skipf("%q is now recognised as recoverable, which is an improvement", msg)
			}
		})
	}
}

func TestFindTableAdvancedSettings(t *testing.T) {
	s := newBufferSyncer(t)
	s.cfg = config.SyncConfig{Mappings: []config.DatabaseMapping{{
		Tables: []config.TableMapping{
			{
				SourceTable:      "users",
				TargetTable:      "users_copy",
				AdvancedSettings: config.AdvancedSettings{SyncIndexes: true, MaxRetries: 7},
			},
		},
	}}}

	t.Run("matches the source name", func(t *testing.T) {
		got := s.findTableAdvancedSettings("users")
		if !got.SyncIndexes || got.MaxRetries != 7 {
			t.Errorf("settings = %+v, want SyncIndexes and MaxRetries 7", got)
		}
	})

	t.Run("unknown collection yields the zero value", func(t *testing.T) {
		got := s.findTableAdvancedSettings("orders")
		if got.SyncIndexes || got.MaxRetries != 0 {
			t.Errorf("settings = %+v, want the zero value", got)
		}
	})
}

func TestGenerateBatchID(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 200; i++ {
		id := generateBatchID()
		if id == "" {
			t.Fatal("generateBatchID returned an empty string")
		}
		if seen[id] {
			t.Fatalf("generateBatchID repeated %q after %d calls", id, i)
		}
		seen[id] = true
	}
}

func TestDeadLetterQueueStatsOnEmptyDirectory(t *testing.T) {
	s := newBufferSyncer(t)

	batches, ops, err := s.getDeadLetterQueueStats("db", "coll")
	if err != nil {
		t.Fatalf("getDeadLetterQueueStats: %v", err)
	}
	if batches != 0 || ops != 0 {
		t.Errorf("stats = %d batches / %d operations, want 0 / 0", batches, ops)
	}
}

// TestBufferPathsCollideOnUnderscore records that the database and collection
// names are joined with an underscore and neither is escaped, so distinct pairs
// can map to one directory and one resume token file. Two tasks sharing a state
// directory — the natural way to configure them — would then interleave their
// buffered events and overwrite each other's resume token.
func TestBufferPathsCollideOnUnderscore(t *testing.T) {
	s := newBufferSyncer(t)
	s.cfg = config.SyncConfig{MongoDBResumeTokenPath: t.TempDir()}

	if a, b := s.getBufferPath("shop_orders", "daily"), s.getBufferPath("shop", "orders_daily"); a != b {
		t.Fatalf("the two pairs now map to %q and %q; the names may have been "+
			"escaped, which would be an improvement", a, b)
	}
	if a, b := s.getResumeTokenPath("shop_orders", "daily"), s.getResumeTokenPath("shop", "orders_daily"); a != b {
		t.Errorf("resume token paths %q and %q no longer collide", a, b)
	}
}
