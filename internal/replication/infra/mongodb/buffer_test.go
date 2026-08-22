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

	if got, want := s.getBufferPath("shop", "users"), filepath.Join(s.bufferDir, "shop_users"); got != want {
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

// TestTheBatchLimitsAreFixedAndSaidToBe covers what used to be called adaptive
// batching. estimateOptimalBatchSize ran on a five-minute ticker and worked out
// how many files would fit the target — and then only logged it. The pair of
// functions that would have applied such a change had no callers at all, so the
// limits were the constants the constructor sets and always had been. The
// reporting is kept, under a name that says what it does; changing the limits
// from there would race with the batch builder, which reads them without a lock.
func TestTheBatchLimitsAreFixedAndSaidToBe(t *testing.T) {
	s := newBufferSyncer(t)
	dir := s.getBufferPath("db", "coll")
	// Files far larger than the target, the case that would demand a change.
	s.targetBatchSizeBytes = 1000
	writeBufferFiles(t, dir, 5000, 5000)

	before := [3]int64{s.targetBatchSizeBytes, int64(s.maxFilesPerBatch), int64(s.minFilesPerBatch)}
	s.reportBatchSizeFit(dir)
	after := [3]int64{s.targetBatchSizeBytes, int64(s.maxFilesPerBatch), int64(s.minFilesPerBatch)}

	if before != after {
		t.Errorf("the limits changed from %v to %v; the report is not meant to "+
			"apply anything", before, after)
	}
}

// TestABatchStopsAtItsSizeLimit covers the limit that exists to bound memory.
// It used to apply only once minFilesPerBatch files had been picked, so the
// first five went in whatever their size — which is exactly the case it is there
// for. A batch of five 256 MB files reached four and a half times the target.
func TestABatchStopsAtItsSizeLimit(t *testing.T) {
	s := newBufferSyncer(t)
	dir := s.getBufferPath("db", "coll")
	s.targetBatchSizeBytes = 1000
	s.minFilesPerBatch = 5
	writeBufferFiles(t, dir, 800, 800, 800, 800, 800)

	files, size := s.buildSmartBatch(dir)

	if len(files) != 1 {
		t.Errorf("batch holds %d files, want the one that fits", len(files))
	}
	if size > s.targetBatchSizeBytes {
		t.Errorf("batch is %d bytes against a target of %d", size, s.targetBatchSizeBytes)
	}
}

// TestASingleOversizedFileIsStillTaken is the other side: refusing a file bigger
// than the whole target would stop the buffer draining at all.
func TestASingleOversizedFileIsStillTaken(t *testing.T) {
	s := newBufferSyncer(t)
	dir := s.getBufferPath("db", "coll")
	s.targetBatchSizeBytes = 1000
	writeBufferFiles(t, dir, 5000)

	if files, _ := s.buildSmartBatch(dir); len(files) != 1 {
		t.Errorf("batch holds %d files, want the oversized one", len(files))
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

// TestWhatTheDriverSaysDuringAnElectionIsRecoverable covers the moments this
// tool exists to survive. These are what the driver really emits while a replica
// set elects a new primary or a managed instance restarts, and none of them was
// in the list — so the guardian read them as fatal and gave up on the change
// stream instead of reconnecting to it.
func TestWhatTheDriverSaysDuringAnElectionIsRecoverable(t *testing.T) {
	for _, msg := range []string{
		"connection() error occurred during connection handshake",
		"socket was unexpectedly closed",
		"client is disconnected",
		"context deadline exceeded",
		"EOF",
		"server selection error: context deadline exceeded",
		"(NotWritablePrimary) not primary",
		"(NotPrimaryOrSecondary) node is recovering",
		"(ShutdownInProgress) shutdown in progress",
	} {
		t.Run(msg, func(t *testing.T) {
			if !isRecoverableError(errors.New(msg)) {
				t.Errorf("isRecoverableError(%q) = false, want true", msg)
			}
		})
	}
}

// TestShuttingDownIsNotAFailureToRecoverFrom is the other side: a cancelled
// context is this process stopping, and retrying through it is how a task asked
// to stop went on reading from a source it had been told to let go of.
func TestShuttingDownIsNotAFailureToRecoverFrom(t *testing.T) {
	if isRecoverableError(context.Canceled) {
		t.Error("isRecoverableError(context.Canceled) = true, want false")
	}
	if isRecoverableError(fmt.Errorf("watch orders: %w", context.Canceled)) {
		t.Error("a wrapped cancellation was read as recoverable")
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

// TestTwoCollectionsCannotShareOneBuffer covers the state directory several
// tasks are naturally pointed at. The database and the collection used to be
// joined with an underscore and neither was escaped, so "daily" of "shop_orders"
// and "orders_daily" of "shop" produced the same name — one buffer directory
// holding both streams' events interleaved, and one resume token file each
// overwriting the other's.
func TestTwoCollectionsCannotShareOneBuffer(t *testing.T) {
	s := newBufferSyncer(t)
	s.cfg = config.SyncConfig{MongoDBResumeTokenPath: t.TempDir()}

	if a, b := s.getBufferPath("shop_orders", "daily"), s.getBufferPath("shop", "orders_daily"); a == b {
		t.Errorf("both pairs map to the buffer directory %q", a)
	}
	if a, b := s.getResumeTokenPath("shop_orders", "daily"), s.getResumeTokenPath("shop", "orders_daily"); a == b {
		t.Errorf("both pairs map to the resume token file %q", a)
	}
	if a, b := s.deadLetterPath("shop_orders", "daily"), s.deadLetterPath("shop", "orders_daily"); a == b {
		t.Errorf("both pairs map to the dead letter directory %q", a)
	}
}

// TestNothingInANameEscapesTheStateDirectory covers what a database or
// collection name is allowed to do to a path.
func TestNothingInANameEscapesTheStateDirectory(t *testing.T) {
	s := newBufferSyncer(t)

	for _, name := range []string{"../escaped", "a/b", "."} {
		got := s.getBufferPath("shop", name)
		if filepath.Dir(got) != s.bufferDir {
			t.Errorf("getBufferPath(%q) = %q, which is outside %q", name, got, s.bufferDir)
		}
	}
}

// TestStateWrittenUnderTheOldNameIsAdopted covers the upgrade. Without it the
// syncer looks under the new name, finds nothing, and whatever was buffered sits
// on disk until somebody notices the volume filling.
func TestStateWrittenUnderTheOldNameIsAdopted(t *testing.T) {
	s := newBufferSyncer(t)

	old := filepath.Join(s.bufferDir, "shop_orders")
	if err := os.MkdirAll(old, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(old, "batch_1.bsonstream"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	current := s.getBufferPath("shop", "orders")
	if _, err := os.Stat(filepath.Join(current, "batch_1.bsonstream")); err != nil {
		t.Errorf("the buffered file did not come across: %v", err)
	}
}

// ------------------------------------------------------------ buffer limit

func TestTheBufferLimitDefaultsAndCanBeOverridden(t *testing.T) {
	if got := bufferLimitBytes(); got != defaultBufferLimitBytes {
		t.Errorf("limit = %d, want the default", got)
	}

	t.Setenv("SYNC_MONGO_BUFFER_LIMIT_BYTES", "1048576")
	if got := bufferLimitBytes(); got != 1<<20 {
		t.Errorf("limit = %d, want 1 MiB", got)
	}

	// An unreadable value falls back rather than turning the cap off by
	// accident, because a mistyped number must not remove the bound.
	t.Setenv("SYNC_MONGO_BUFFER_LIMIT_BYTES", "one gigabyte")
	if got := bufferLimitBytes(); got != defaultBufferLimitBytes {
		t.Errorf("limit = %d for an unreadable value, want the default", got)
	}

	// Turning it off is a choice an operator may make deliberately.
	t.Setenv("SYNC_MONGO_BUFFER_LIMIT_BYTES", "0")
	if got := bufferLimitBytes(); got != 0 {
		t.Errorf("limit = %d, want it turned off", got)
	}
}

func TestTheBufferSizeIsTheSumOfItsFiles(t *testing.T) {
	s := newBufferSyncer(t)
	dir := s.getBufferPath("shop", "orders")
	writeBufferFiles(t, dir, 100, 250, 400)

	if got := bufferBytes(dir); got != 750 {
		t.Errorf("bufferBytes = %d, want 750", got)
	}
}

func TestAnAbsentBufferHoldsNothing(t *testing.T) {
	s := newBufferSyncer(t)

	if got := bufferBytes(s.getBufferPath("shop", "orders")); got != 0 {
		t.Errorf("bufferBytes = %d for a directory that does not exist", got)
	}
}

// TestAFullBufferHoldsTheReaderBack is the backpressure. Without a bound the
// writer keeps going for as long as the target is unreachable, and the first
// thing to break is the disk — which takes the checkpoint with it when a file
// store is configured.
func TestAFullBufferHoldsTheReaderBack(t *testing.T) {
	s := newBufferSyncer(t)
	s.cfg = config.SyncConfig{MongoDBResumeTokenPath: t.TempDir()}
	t.Setenv("SYNC_MONGO_BUFFER_LIMIT_BYTES", "500")
	writeBufferFiles(t, s.getBufferPath("shop", "orders"), 600)

	// A buffered event that must not be taken while the limit is exceeded.
	ch := make(chan streamEvent, 1)
	ch <- streamEvent{RawData: insertEventRaw(t, "1")}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	s.diskWriter(ctx, ch, "shop", "orders")

	if len(ch) != 1 {
		t.Error("the writer took an event while the buffer was over its limit")
	}
	files, err := os.ReadDir(s.getBufferPath("shop", "orders"))
	if err != nil {
		t.Fatalf("read buffer dir: %v", err)
	}
	if len(files) != 1 {
		t.Errorf("%d buffer files, want only the one that was already there", len(files))
	}
}

// TestADrainedBufferLetsTheReaderResume is the other half: the hold is released
// once the applier has caught up.
func TestADrainedBufferLetsTheReaderResume(t *testing.T) {
	s := newBufferSyncer(t)
	s.cfg = config.SyncConfig{MongoDBResumeTokenPath: t.TempDir()}
	t.Setenv("SYNC_MONGO_BUFFER_LIMIT_BYTES", "10000")

	ch := make(chan streamEvent, 1)
	ch <- streamEvent{RawData: insertEventRaw(t, "1")}
	close(ch)

	s.diskWriter(context.Background(), ch, "shop", "orders")

	files, err := os.ReadDir(s.getBufferPath("shop", "orders"))
	if err != nil {
		t.Fatalf("read buffer dir: %v", err)
	}
	if len(files) != 1 {
		t.Errorf("%d buffer files, want the event written", len(files))
	}
}

// insertEventRaw is one change stream document, as the buffer stores them.
func insertEventRaw(t *testing.T, id string) bson.Raw {
	t.Helper()

	raw, err := bson.Marshal(bson.M{
		"_id":           bson.M{"_data": "82" + id},
		"operationType": "insert",
		"documentKey":   bson.M{"_id": id},
		"fullDocument":  bson.M{"_id": id},
	})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return raw
}
