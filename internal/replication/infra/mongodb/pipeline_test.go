package mongodb

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// deadClient returns a client that resolves handles without dialling and fails
// every operation quickly. The driver connects lazily, so this needs no server;
// the short timeouts keep the failure paths fast.
func deadClient(t *testing.T) *mongo.Client {
	t.Helper()

	client, err := mongo.Connect(context.Background(), options.Client().
		ApplyURI("mongodb://127.0.0.1:1").
		SetServerSelectionTimeout(10*time.Millisecond).
		SetConnectTimeout(10*time.Millisecond))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	return client
}

// briefCtx bounds the retry backoff: RetryDBOperation aborts its sleep as soon
// as the context is done, so a failing write reports in milliseconds instead of
// the three seconds the backoff would otherwise take.
func briefCtx(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	t.Cleanup(cancel)
	return ctx
}

// event encodes one change-stream document the way the disk buffer stores it.
func event(t *testing.T, doc bson.M) bson.Raw {
	t.Helper()

	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return raw
}

// writeStream writes events into one buffer file, separated the way the disk
// writer does.
func writeStream(t *testing.T, dir, name string, events ...bson.Raw) string {
	t.Helper()

	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	var body []byte
	for _, e := range events {
		body = append(body, e...)
		body = append(body, bsonRawSeparator...)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	return path
}

func insertEvent(t *testing.T, id string) bson.Raw {
	return event(t, bson.M{
		"operationType": "insert",
		"documentKey":   bson.M{"_id": id},
		"fullDocument":  bson.M{"_id": id, "name": "Ada"},
	})
}

// ---------------------------------------------------- event conversion

func TestConvertRawBSONToWriteModel(t *testing.T) {
	s := newBufferSyncer(t)

	tests := []struct {
		name string
		doc  bson.M
		want interface{}
	}{
		{"insert", bson.M{
			"operationType": "insert",
			"fullDocument":  bson.M{"_id": "1"},
		}, &mongo.InsertOneModel{}},
		{"update", bson.M{
			"operationType": "update",
			"documentKey":   bson.M{"_id": "1"},
			"fullDocument":  bson.M{"_id": "1"},
		}, &mongo.ReplaceOneModel{}},
		{"replace", bson.M{
			"operationType": "replace",
			"documentKey":   bson.M{"_id": "1"},
			"fullDocument":  bson.M{"_id": "1"},
		}, &mongo.ReplaceOneModel{}},
		{"delete", bson.M{
			"operationType": "delete",
			"documentKey":   bson.M{"_id": "1"},
		}, &mongo.DeleteOneModel{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := s.convertRawBSONToWriteModel(event(t, tc.doc), "shop", "orders")
			if got == nil {
				t.Fatalf("conversion returned nil for %v", tc.doc)
			}
			if gotType, wantType := s.getOperationType(got), s.getOperationType(tc.want.(mongo.WriteModel)); gotType != wantType {
				t.Errorf("model type = %s, want %s", gotType, wantType)
			}
		})
	}
}

// TestAnUpdateIsReplicatedAsAWholeDocumentReplacement records the shape of the
// update path: rather than applying the change stream's updateDescription, the
// syncer replaces the whole document with fullDocument and upserts. That needs
// the stream to be watched with fullDocument enabled, and it means a target
// document with extra fields loses them.
func TestAnUpdateIsReplicatedAsAWholeDocumentReplacement(t *testing.T) {
	s := newBufferSyncer(t)

	model := s.convertRawBSONToWriteModel(event(t, bson.M{
		"operationType":     "update",
		"documentKey":       bson.M{"_id": "1"},
		"fullDocument":      bson.M{"_id": "1", "name": "Grace"},
		"updateDescription": bson.M{"updatedFields": bson.M{"name": "Grace"}},
	}), "shop", "orders")

	replace, ok := model.(*mongo.ReplaceOneModel)
	if !ok {
		t.Fatalf("model = %T, want a replacement", model)
	}
	if replace.Upsert == nil || !*replace.Upsert {
		t.Error("the replacement is not an upsert")
	}
}

// TestAnUpdateWithoutFullDocumentIsSilentlyDropped records the consequence of
// the replacement design: if the change stream was not opened with
// fullDocument, or the document was deleted before the lookup, the event
// produces no model at all and the change is lost with a single warning line.
func TestAnUpdateWithoutFullDocumentIsSilentlyDropped(t *testing.T) {
	s := newBufferSyncer(t)

	model := s.convertRawBSONToWriteModel(event(t, bson.M{
		"operationType":     "update",
		"documentKey":       bson.M{"_id": "1"},
		"updateDescription": bson.M{"updatedFields": bson.M{"name": "Grace"}},
	}), "shop", "orders")

	if model != nil {
		t.Fatalf("model = %T; the missing document appears to be handled now, so "+
			"assert that instead", model)
	}
}

// TestAnInsertWithoutFullDocumentIsDropped records the same gap for inserts,
// where fullDocument is always present in a real stream — so this only fires on
// a corrupt buffer file.
func TestAnInsertWithoutFullDocumentIsDropped(t *testing.T) {
	s := newBufferSyncer(t)

	if model := s.convertRawBSONToWriteModel(event(t, bson.M{
		"operationType": "insert",
	}), "shop", "orders"); model != nil {
		t.Errorf("model = %T, want none", model)
	}
}

func TestConversionRejectsAMalformedEvent(t *testing.T) {
	s := newBufferSyncer(t)

	for _, name := range []string{"garbage", "empty", "no operationType", "unknown type"} {
		t.Run(name, func(t *testing.T) {
			var raw bson.Raw
			switch name {
			case "garbage":
				raw = bson.Raw("not bson at all")
			case "empty":
				raw = event(t, bson.M{})
			case "no operationType":
				raw = event(t, bson.M{"fullDocument": bson.M{"_id": "1"}})
			case "unknown type":
				raw = event(t, bson.M{"operationType": "invalidate"})
			}
			if model := s.convertRawBSONToWriteModel(raw, "shop", "orders"); model != nil {
				t.Errorf("model = %T, want none", model)
			}
		})
	}
}

// TestADeleteWithoutADocumentKeyIsDropped covers the guard that keeps an
// unqualified delete out of the target.
func TestADeleteWithoutADocumentKeyIsDropped(t *testing.T) {
	s := newBufferSyncer(t)

	if model := s.convertRawBSONToWriteModel(event(t, bson.M{
		"operationType": "delete",
	}), "shop", "orders"); model != nil {
		t.Errorf("model = %T, want none", model)
	}
}

// TestIgnoreDeleteOpsDropsTheEvent covers the per-table setting that keeps
// deletes out of an archive target.
func TestIgnoreDeleteOpsDropsTheEvent(t *testing.T) {
	s := newBufferSyncer(t)
	s.cfg = config.SyncConfig{Mappings: []config.DatabaseMapping{{
		Tables: []config.TableMapping{{
			SourceTable: "orders",
			AdvancedSettings: config.AdvancedSettings{
				SyncIndexes:     false,
				IgnoreDeleteOps: true,
			},
		}},
	}}}

	deleteEvent := event(t, bson.M{
		"operationType": "delete",
		"documentKey":   bson.M{"_id": "1"},
	})
	if model := s.convertRawBSONToWriteModel(deleteEvent, "shop", "orders"); model != nil {
		t.Errorf("model = %T, want the delete ignored", model)
	}
	// Another collection keeps its deletes.
	if model := s.convertRawBSONToWriteModel(deleteEvent, "shop", "customers"); model == nil {
		t.Error("the setting leaked to a collection it does not name")
	}
}

// ------------------------------------------------------- file parsing

func TestParseFileToWriteModelsReadsEveryEvent(t *testing.T) {
	s := newBufferSyncer(t)
	path := writeStream(t, s.getBufferPath("shop", "orders"), "batch_1.bsonstream",
		insertEvent(t, "1"), insertEvent(t, "2"), insertEvent(t, "3"))

	models, err := s.parseFileToWriteModels(context.Background(), path, "shop", "orders")
	if err != nil {
		t.Fatalf("parseFileToWriteModels: %v", err)
	}
	if len(models) != 3 {
		t.Errorf("models = %d, want 3", len(models))
	}
}

// TestAFinalEventWithNoSeparatorIsStillRead records that a file cut short — the
// process died between writing an event and its separator — is not lost: the
// scanner returns the trailing bytes at EOF.
func TestAFinalEventWithNoSeparatorIsStillRead(t *testing.T) {
	s := newBufferSyncer(t)
	dir := s.getBufferPath("shop", "orders")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	path := filepath.Join(dir, "batch_1.bsonstream")

	body := append([]byte(insertEvent(t, "1")), bsonRawSeparator...)
	body = append(body, insertEvent(t, "2")...) // no trailing separator
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	models, err := s.parseFileToWriteModels(context.Background(), path, "shop", "orders")
	if err != nil {
		t.Fatalf("parseFileToWriteModels: %v", err)
	}
	if len(models) != 2 {
		t.Errorf("models = %d, want both events", len(models))
	}
}

// TestATruncatedEventIsDroppedNotReported records the other half of a torn
// write: a partial BSON document at the end of the file cannot be unmarshalled,
// and the parser logs it and moves on. The file is then treated as fully
// processed and deleted, so the truncated change is lost for good.
func TestATruncatedEventIsDroppedNotReported(t *testing.T) {
	s := newBufferSyncer(t)
	dir := s.getBufferPath("shop", "orders")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	path := filepath.Join(dir, "batch_1.bsonstream")

	good := []byte(insertEvent(t, "1"))
	body := append(append([]byte{}, good...), bsonRawSeparator...)
	body = append(body, good[:len(good)/2]...) // half an event
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	models, err := s.parseFileToWriteModels(context.Background(), path, "shop", "orders")
	if err != nil {
		t.Fatalf("parseFileToWriteModels reported %v; the torn event appears to be "+
			"reported now, so assert that instead", err)
	}
	if len(models) != 1 {
		t.Errorf("models = %d, want only the intact event", len(models))
	}
}

func TestParseFileToWriteModelsOnAnEmptyFile(t *testing.T) {
	s := newBufferSyncer(t)
	path := writeStream(t, s.getBufferPath("shop", "orders"), "batch_1.bsonstream")

	models, err := s.parseFileToWriteModels(context.Background(), path, "shop", "orders")
	if err != nil {
		t.Fatalf("parseFileToWriteModels: %v", err)
	}
	if len(models) != 0 {
		t.Errorf("models = %d, want none", len(models))
	}
}

func TestParseFileToWriteModelsReportsAMissingFile(t *testing.T) {
	s := newBufferSyncer(t)

	if _, err := s.parseFileToWriteModels(context.Background(),
		filepath.Join(t.TempDir(), "absent"), "shop", "orders"); err == nil {
		t.Fatal("parsing a missing file returned no error")
	}
}

// TestParseFileToWriteModelsIgnoresTheContext records that the parser takes a
// context and never consults it, so a cancelled batch keeps reading a 100 MB
// file to the end.
func TestParseFileToWriteModelsIgnoresTheContext(t *testing.T) {
	s := newBufferSyncer(t)
	path := writeStream(t, s.getBufferPath("shop", "orders"), "batch_1.bsonstream",
		insertEvent(t, "1"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	models, err := s.parseFileToWriteModels(ctx, path, "shop", "orders")
	if err != nil {
		t.Fatalf("the parser appears to honour the context now: %v", err)
	}
	if len(models) != 1 {
		t.Errorf("models = %d", len(models))
	}
}

// ----------------------------------------------------- parallel parsing

func TestParseFilesParallelCollectsEveryFile(t *testing.T) {
	s := newBufferSyncer(t)
	dir := s.getBufferPath("shop", "orders")
	var files []string
	for i, name := range []string{"batch_1.bsonstream", "batch_2.bsonstream", "batch_3.bsonstream"} {
		files = append(files, writeStream(t, dir, name,
			insertEvent(t, string(rune('a'+i))), insertEvent(t, string(rune('A'+i)))))
	}

	models, processed, ok := s.parseFilesParallel(context.Background(), files, "shop", "orders", "b1")
	if !ok {
		t.Error("parseFilesParallel reported a failure")
	}
	if len(models) != 6 {
		t.Errorf("models = %d, want 6", len(models))
	}
	if len(processed) != 3 {
		t.Errorf("processed = %d, want 3", len(processed))
	}
}

// TestOneUnreadableFileFailsTheWholeBatch records the all-or-nothing rule: a
// single unreadable file sets writeSuccess to false, which stops the batch from
// being written *and* from being cleaned up. The other files in the batch are
// parsed and then discarded, and the whole batch is retried next tick — so one
// permanently broken file stalls the collection forever.
func TestOneUnreadableFileFailsTheWholeBatch(t *testing.T) {
	s := newBufferSyncer(t)
	dir := s.getBufferPath("shop", "orders")
	good := writeStream(t, dir, "batch_1.bsonstream", insertEvent(t, "1"))
	missing := filepath.Join(dir, "batch_2.bsonstream")

	models, processed, ok := s.parseFilesParallel(context.Background(),
		[]string{good, missing}, "shop", "orders", "b1")

	if ok {
		t.Fatal("the batch was reported as successful")
	}
	if len(models) != 1 || len(processed) != 1 {
		t.Errorf("models = %d, processed = %d; the good file is parsed and then "+
			"thrown away", len(models), len(processed))
	}
}

func TestParseFilesParallelOnAnEmptySelection(t *testing.T) {
	s := newBufferSyncer(t)

	models, processed, ok := s.parseFilesParallel(context.Background(), nil, "shop", "orders", "b1")
	if !ok || len(models) != 0 || len(processed) != 0 {
		t.Errorf("= %d models, %d files, ok=%v", len(models), len(processed), ok)
	}
}

// ------------------------------------------------------- resume tokens

func TestResumeTokensRoundTripThroughTheCache(t *testing.T) {
	s := newBufferSyncer(t)
	s.cfg = config.SyncConfig{MongoDBResumeTokenPath: t.TempDir()}
	token := event(t, bson.M{"_data": "82650000"})

	s.saveMongoDBResumeToken("shop", "orders", token)

	if got := s.loadMongoDBResumeToken("shop", "orders"); len(got) == 0 {
		t.Fatal("the token did not come back")
	}
	// And from disk, with the memory cache cleared.
	s.resumeTokens = map[string]bson.Raw{}
	if got := s.loadMongoDBResumeToken("shop", "orders"); len(got) == 0 {
		t.Error("the token did not come back from disk")
	}
}

// TestASavedTokenIsCachedEvenWithNoConfiguredPath records that the in-memory
// half works regardless, so a task with no configured path resumes correctly
// until the process restarts and then starts from scratch.
func TestASavedTokenIsCachedEvenWithNoConfiguredPath(t *testing.T) {
	s := newBufferSyncer(t)
	token := event(t, bson.M{"_data": "82650000"})

	s.saveMongoDBResumeToken("shop", "orders", token)

	if got := s.loadMongoDBResumeToken("shop", "orders"); len(got) == 0 {
		t.Error("the token was not cached in memory")
	}
	s.resumeTokens = map[string]bson.Raw{}
	if got := s.loadMongoDBResumeToken("shop", "orders"); got != nil {
		t.Error("a token came back with no configured path")
	}
}

func TestANilTokenIsNotStored(t *testing.T) {
	s := newBufferSyncer(t)
	s.cfg = config.SyncConfig{MongoDBResumeTokenPath: t.TempDir()}

	s.saveMongoDBResumeToken("shop", "orders", nil)

	if got := s.loadMongoDBResumeToken("shop", "orders"); got != nil {
		t.Errorf("token = %v, want none", got)
	}
	if entries, _ := os.ReadDir(s.cfg.MongoDBResumeTokenPath); len(entries) != 0 {
		t.Errorf("%d files were written for a nil token", len(entries))
	}
}

// TestACorruptTokenFileIsRemoved records the self-repair: a file that will not
// parse is deleted so the next start watches from now rather than failing
// repeatedly. The changes between the corruption and the restart are lost.
func TestACorruptTokenFileIsRemoved(t *testing.T) {
	s := newBufferSyncer(t)
	dir := t.TempDir()
	s.cfg = config.SyncConfig{MongoDBResumeTokenPath: dir}
	path := s.getResumeTokenPath("shop", "orders")
	if err := os.WriteFile(path, []byte("not json at all"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	if got := s.loadMongoDBResumeToken("shop", "orders"); got != nil {
		t.Errorf("token = %v, want none", got)
	}
	if _, err := os.Stat(path); err == nil {
		t.Error("the corrupt file was left in place")
	}
}

// TestAOneByteTokenFileIsTreatedAsAbsent records the length guard, which exists
// because an empty JSON document round-trips as "" and would otherwise be
// handed to the driver as a resume token.
func TestAOneByteTokenFileIsTreatedAsAbsent(t *testing.T) {
	s := newBufferSyncer(t)
	s.cfg = config.SyncConfig{MongoDBResumeTokenPath: t.TempDir()}
	path := s.getResumeTokenPath("shop", "orders")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	if got := s.loadMongoDBResumeToken("shop", "orders"); got != nil {
		t.Errorf("token = %v, want none", got)
	}
	if _, err := os.Stat(path); err != nil {
		t.Error("the short file was removed; only unparseable files should be")
	}
}

func TestRemoveMongoDBResumeTokenClearsBothCopies(t *testing.T) {
	s := newBufferSyncer(t)
	s.cfg = config.SyncConfig{MongoDBResumeTokenPath: t.TempDir()}
	s.saveMongoDBResumeToken("shop", "orders", event(t, bson.M{"_data": "82"}))

	s.removeMongoDBResumeToken("shop", "orders")

	if got := s.loadMongoDBResumeToken("shop", "orders"); got != nil {
		t.Errorf("token = %v, want none", got)
	}
	if _, err := os.Stat(s.getResumeTokenPath("shop", "orders")); err == nil {
		t.Error("the file was left in place")
	}
}

// ---------------------------------------------------------- disk writer

func TestTheDiskWriterFlushesWhenTheChannelCloses(t *testing.T) {
	s := newBufferSyncer(t)
	s.cfg = config.SyncConfig{MongoDBResumeTokenPath: t.TempDir()}

	ch := make(chan streamEvent, 2)
	ch <- streamEvent{RawData: insertEvent(t, "1"), ResumeToken: event(t, bson.M{"_data": "82"})}
	ch <- streamEvent{RawData: insertEvent(t, "2"), ResumeToken: event(t, bson.M{"_data": "83"})}
	close(ch)

	s.diskWriter(context.Background(), ch, "shop", "orders")

	files, err := os.ReadDir(s.getBufferPath("shop", "orders"))
	if err != nil {
		t.Fatalf("read buffer dir: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("%d buffer files, want 1", len(files))
	}
	// The last event's token was persisted along with the file.
	if got := s.loadMongoDBResumeToken("shop", "orders"); len(got) == 0 {
		t.Error("the resume token was not saved with the flush")
	}
}

// TestTheDiskWriterFlushesOnShutdown records that a cancelled context drains
// what is buffered rather than dropping it.
func TestTheDiskWriterFlushesOnShutdown(t *testing.T) {
	s := newBufferSyncer(t)
	ch := make(chan streamEvent, 1)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		s.diskWriter(ctx, ch, "shop", "orders")
		close(done)
	}()

	ch <- streamEvent{RawData: insertEvent(t, "1")}
	// Give the writer a chance to take the event into its buffer, then stop it.
	for i := 0; i < 100; i++ {
		if len(ch) == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done

	files, err := os.ReadDir(s.getBufferPath("shop", "orders"))
	if err != nil {
		t.Fatalf("read buffer dir: %v", err)
	}
	if len(files) != 1 {
		t.Errorf("%d buffer files, want the buffer drained on shutdown", len(files))
	}
}

// TestTheWrittenFileIsReadBackByTheParser closes the loop between the two disk
// halves, which is what makes the buffer a durable queue rather than two
// independent formats.
func TestTheWrittenFileIsReadBackByTheParser(t *testing.T) {
	s := newBufferSyncer(t)

	ch := make(chan streamEvent, 3)
	for _, id := range []string{"1", "2", "3"} {
		ch <- streamEvent{RawData: insertEvent(t, id)}
	}
	close(ch)
	s.diskWriter(context.Background(), ch, "shop", "orders")

	files, err := os.ReadDir(s.getBufferPath("shop", "orders"))
	if err != nil || len(files) != 1 {
		t.Fatalf("buffer files = %v (%v)", files, err)
	}
	path := filepath.Join(s.getBufferPath("shop", "orders"), files[0].Name())

	models, err := s.parseFileToWriteModels(context.Background(), path, "shop", "orders")
	if err != nil {
		t.Fatalf("parseFileToWriteModels: %v", err)
	}
	if len(models) != 3 {
		t.Errorf("models = %d, want the three events written", len(models))
	}
}

// --------------------------------------------------- buffered batch run

// TestAFailedWriteWithNoDeadLetterQueueLosesTheChanges records the worst path
// through the writer. The bulk write to an unreachable target fails, the
// individual-operation fallback fails too, and with the dead-letter queue
// disabled the operations are dropped — but the fallback still returns nil
// ("always consider successful"), so the batch counts as written and the buffer
// file is deleted. The change is gone from both queues, and the only trace is a
// warning line per operation.
func TestAFailedWriteWithNoDeadLetterQueueLosesTheChanges(t *testing.T) {
	s := newBufferSyncer(t)
	s.targetClient = deadClient(t)
	s.enableDeadLetterQueue = false
	dir := s.getBufferPath("shop", "orders")
	writeStream(t, dir, "batch_1.bsonstream", insertEvent(t, "1"))

	s.processBufferedChanges(briefCtx(t), "shop", "orders", "shop", "orders")

	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read buffer dir: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("%d buffer files left; the failure appears to be reported now, so "+
			"assert that instead", len(files))
	}
	if _, err := os.Stat(s.deadLetterDir); err == nil {
		t.Error("something was parked with the dead letter queue disabled")
	}
}

// TestTheWriterNeverReportsAFailure pins the "always return success" contract
// directly, since that is what makes the loss above possible.
func TestTheWriterNeverReportsAFailure(t *testing.T) {
	s := newBufferSyncer(t)
	s.targetClient = deadClient(t)
	s.enableDeadLetterQueue = false
	coll := s.targetClient.Database("shop").Collection("orders")

	err := s.flushWriteModels(briefCtx(t), coll,
		[]mongo.WriteModel{mongo.NewInsertOneModel().SetDocument(bson.M{"_id": "1"})},
		"shop", "orders")
	if err != nil {
		t.Fatalf("flushWriteModels reported %v; the failure appears to be "+
			"propagated now, so assert that instead", err)
	}
}

func TestTheWriterOnAnEmptyBatchDoesNothing(t *testing.T) {
	s := newBufferSyncer(t)
	s.targetClient = deadClient(t)
	coll := s.targetClient.Database("shop").Collection("orders")

	if err := s.flushWriteModels(context.Background(), coll, nil, "shop", "orders"); err != nil {
		t.Fatalf("flushWriteModels: %v", err)
	}
}

// TestAFileWithNothingToApplyIsDeletedWithoutAWrite records the other side: a
// file whose events all convert to nothing — deletes on a collection that
// ignores them, say — is cleaned up without the target being touched. Note that
// this happens even when the target is unreachable, because the write step is
// skipped when there are no models.
func TestAFileWithNothingToApplyIsDeletedWithoutAWrite(t *testing.T) {
	s := newBufferSyncer(t)
	s.targetClient = deadClient(t)
	dir := s.getBufferPath("shop", "orders")
	writeStream(t, dir, "batch_1.bsonstream",
		event(t, bson.M{"operationType": "invalidate"}))

	s.processBufferedChanges(briefCtx(t), "shop", "orders", "shop", "orders")

	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read buffer dir: %v", err)
	}
	if len(files) != 0 {
		t.Errorf("%d files left, want the empty batch cleaned up", len(files))
	}
}

func TestProcessBufferedChangesWithNoBufferDirectory(t *testing.T) {
	s := newBufferSyncer(t)
	s.targetClient = deadClient(t)

	// Nothing has been written yet, so the directory does not exist.
	s.processBufferedChanges(briefCtx(t), "shop", "orders", "shop", "orders")
}

func TestProcessBufferedChangesWithAnEmptyBufferDirectory(t *testing.T) {
	s := newBufferSyncer(t)
	s.targetClient = deadClient(t)
	if err := os.MkdirAll(s.getBufferPath("shop", "orders"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	s.processBufferedChanges(briefCtx(t), "shop", "orders", "shop", "orders")
}

// TestAFailedBatchReachesTheDeadLetterQueue records the last resort: after the
// bulk write and the individual retries both fail, the operations are written to
// the dead-letter directory and the buffer file is deleted. From that point the
// change lives only in the dead-letter queue, whose retry loop is the only thing
// that can still apply it.
func TestAFailedBatchReachesTheDeadLetterQueue(t *testing.T) {
	s := newBufferSyncer(t)
	s.targetClient = deadClient(t)
	dir := s.getBufferPath("shop", "orders")
	writeStream(t, dir, "batch_1.bsonstream", insertEvent(t, "1"))

	s.processBufferedChanges(briefCtx(t), "shop", "orders", "shop", "orders")

	dlq := filepath.Join(s.deadLetterDir, "shop_orders")
	entries, err := os.ReadDir(dlq)
	if err != nil {
		t.Fatalf("read dead letter dir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("%d dead letter files, want 1", len(entries))
	}

	buffered, _ := os.ReadDir(dir)
	if len(buffered) != 0 {
		t.Errorf("%d buffer files left; the batch appears to be kept for retry now, "+
			"which would queue the change in two places", len(buffered))
	}
}

// ------------------------------------------------------ dead letters

func TestStoreToDeadLetterQueueWritesTheBatch(t *testing.T) {
	s := newBufferSyncer(t)
	models := []mongo.WriteModel{
		mongo.NewInsertOneModel().SetDocument(bson.M{"_id": "1"}),
		mongo.NewDeleteOneModel().SetFilter(bson.M{"_id": "2"}),
	}
	errs := []mongo.BulkWriteError{
		{WriteError: mongo.WriteError{Index: 0, Code: 11000, Message: "duplicate key"}},
	}

	if err := s.storeToDeadLetterQueue(models, errs, 5, 3, "shop", "orders"); err != nil {
		t.Fatalf("storeToDeadLetterQueue: %v", err)
	}

	entries, err := os.ReadDir(filepath.Join(s.deadLetterDir, "shop_orders"))
	if err != nil {
		t.Fatalf("read dead letter dir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("%d files, want 1", len(entries))
	}

	data, err := os.ReadFile(filepath.Join(s.deadLetterDir, "shop_orders", entries[0].Name()))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var batch DeadLetterBatch
	if err := json.Unmarshal(data, &batch); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(batch.FailedOps) != 2 {
		t.Errorf("%d failed operations recorded, want 2", len(batch.FailedOps))
	}
	if batch.TotalOps != 5 || batch.SuccessfulOps != 3 {
		t.Errorf("counts = %d/%d, want 5/3", batch.SuccessfulOps, batch.TotalOps)
	}
	if batch.FailedOps[0].ErrorCode != 11000 {
		t.Errorf("first error code = %d", batch.FailedOps[0].ErrorCode)
	}
	// The second model has no matching error, so its message is left empty.
	if batch.FailedOps[1].Error != "" {
		t.Errorf("second error = %q, want empty", batch.FailedOps[1].Error)
	}
	if batch.FailedOps[1].OpType != "delete" {
		t.Errorf("second op type = %q", batch.FailedOps[1].OpType)
	}
}

// TestTheDeadLetterQueueCanBeTurnedOff records that with the queue disabled the
// call reports success and the operations are dropped, so a failing batch is
// lost rather than parked.
func TestTheDeadLetterQueueCanBeTurnedOff(t *testing.T) {
	s := newBufferSyncer(t)
	s.enableDeadLetterQueue = false

	err := s.storeToDeadLetterQueue(
		[]mongo.WriteModel{mongo.NewInsertOneModel().SetDocument(bson.M{"_id": "1"})},
		nil, 1, 0, "shop", "orders")
	if err != nil {
		t.Fatalf("storeToDeadLetterQueue: %v", err)
	}
	if _, err := os.Stat(s.deadLetterDir); err == nil {
		t.Error("a directory was created with the queue disabled")
	}
}

// TestAnUnserialisableModelIsSkippedNotReported records that a model the
// serialiser does not know is logged and left out of the batch, so the
// operation disappears with the queue reporting success.
func TestAnUnserialisableModelIsSkippedNotReported(t *testing.T) {
	s := newBufferSyncer(t)

	err := s.storeToDeadLetterQueue(
		[]mongo.WriteModel{mongo.NewUpdateManyModel().SetFilter(bson.M{}).SetUpdate(bson.M{})},
		nil, 1, 0, "shop", "orders")
	if err != nil {
		t.Fatalf("storeToDeadLetterQueue: %v", err)
	}

	entries, _ := os.ReadDir(filepath.Join(s.deadLetterDir, "shop_orders"))
	if len(entries) != 1 {
		t.Fatalf("%d files, want the batch still written", len(entries))
	}
	data, _ := os.ReadFile(filepath.Join(s.deadLetterDir, "shop_orders", entries[0].Name()))
	var batch DeadLetterBatch
	if err := json.Unmarshal(data, &batch); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(batch.FailedOps) != 0 {
		t.Errorf("%d operations recorded; the unsupported model appears to be "+
			"handled now, so assert that instead", len(batch.FailedOps))
	}
}

func TestDeadLetterQueueStatsCountTheBatches(t *testing.T) {
	s := newBufferSyncer(t)
	models := []mongo.WriteModel{mongo.NewInsertOneModel().SetDocument(bson.M{"_id": "1"})}
	for i := 0; i < 2; i++ {
		if err := s.storeToDeadLetterQueue(models, nil, 1, 0, "shop", "orders"); err != nil {
			t.Fatalf("storeToDeadLetterQueue: %v", err)
		}
	}

	batches, ops, err := s.getDeadLetterQueueStats("shop", "orders")
	if err != nil {
		t.Fatalf("getDeadLetterQueueStats: %v", err)
	}
	if batches != 2 || ops != 2 {
		t.Errorf("stats = %d batches, %d operations; want 2 and 2", batches, ops)
	}
}

// TestNonJSONFilesAreIgnoredByTheStats records that the directory is filtered by
// extension, so anything else dropped in it is invisible to both the stats and
// the retry loop.
func TestNonJSONFilesAreIgnoredByTheStats(t *testing.T) {
	s := newBufferSyncer(t)
	dir := filepath.Join(s.deadLetterDir, "shop_orders")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	batches, ops, err := s.getDeadLetterQueueStats("shop", "orders")
	if err != nil || batches != 0 || ops != 0 {
		t.Errorf("stats = %d, %d, %v", batches, ops, err)
	}
}

// TestARetryThatFailsAgainIncrementsTheCount records the retry bookkeeping: the
// batch file is rewritten with a higher retry count rather than being dropped or
// left untouched.
func TestARetryThatFailsAgainIncrementsTheCount(t *testing.T) {
	s := newBufferSyncer(t)
	s.targetClient = deadClient(t)
	s.maxRetryAttempts = 3
	models := []mongo.WriteModel{mongo.NewInsertOneModel().SetDocument(bson.M{"_id": "1"})}
	if err := s.storeToDeadLetterQueue(models, nil, 1, 0, "shop", "orders"); err != nil {
		t.Fatalf("storeToDeadLetterQueue: %v", err)
	}

	s.processDeadLetterQueue(briefCtx(t), "shop", "orders", "shop", "orders")

	dir := filepath.Join(s.deadLetterDir, "shop_orders")
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("%d files, want the batch kept", len(entries))
	}
	data, _ := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	var batch DeadLetterBatch
	if err := json.Unmarshal(data, &batch); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(batch.FailedOps) != 1 || batch.FailedOps[0].RetryCount != 1 {
		t.Errorf("failed ops = %+v, want one at retry count 1", batch.FailedOps)
	}
	if !strings.Contains(batch.FailedOps[0].Error, "context") &&
		!strings.Contains(batch.FailedOps[0].Error, "server selection") {
		t.Errorf("recorded error = %q, want the retry failure", batch.FailedOps[0].Error)
	}
}

// TestABatchAtTheRetryLimitIsLeftAlone records that operations past the limit
// stop being retried — and that nothing ever deletes them, so the directory
// grows without bound and no alert is raised.
func TestABatchAtTheRetryLimitIsLeftAlone(t *testing.T) {
	s := newBufferSyncer(t)
	s.targetClient = deadClient(t)
	s.maxRetryAttempts = 1
	models := []mongo.WriteModel{mongo.NewInsertOneModel().SetDocument(bson.M{"_id": "1"})}
	if err := s.storeToDeadLetterQueue(models, nil, 1, 0, "shop", "orders"); err != nil {
		t.Fatalf("storeToDeadLetterQueue: %v", err)
	}

	// One attempt takes it to the limit; the second must leave it untouched.
	s.processDeadLetterQueue(briefCtx(t), "shop", "orders", "shop", "orders")
	dir := filepath.Join(s.deadLetterDir, "shop_orders")
	entries, _ := os.ReadDir(dir)
	before, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	s.processDeadLetterQueue(briefCtx(t), "shop", "orders", "shop", "orders")
	after, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(before) != string(after) {
		t.Error("the exhausted batch was rewritten")
	}
}

func TestProcessDeadLetterQueueWithNoDirectory(t *testing.T) {
	s := newBufferSyncer(t)
	s.targetClient = deadClient(t)

	s.processDeadLetterQueue(briefCtx(t), "shop", "orders", "shop", "orders")
}

// TestAnUnparseableDeadLetterFileIsLeftInPlace records that a corrupt batch file
// is logged and skipped on every pass, so it stays in the directory forever.
func TestAnUnparseableDeadLetterFileIsLeftInPlace(t *testing.T) {
	s := newBufferSyncer(t)
	s.targetClient = deadClient(t)
	dir := filepath.Join(s.deadLetterDir, "shop_orders")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	path := filepath.Join(dir, "batch_broken.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	s.processDeadLetterQueue(briefCtx(t), "shop", "orders", "shop", "orders")

	if _, err := os.Stat(path); err != nil {
		t.Error("the corrupt file was removed")
	}
}

// TestTheRetryLoopMisattributesFailuresAfterASkip records an index bug in the
// retry bookkeeping. The loop walks the models it managed to deserialise but
// indexes the *operation* list with the same counter, so once one operation has
// been skipped the two lists are out of step: the retry count and the error
// message land on the wrong operation, and the operation that actually failed
// keeps its old count — so it is retried forever while an innocent one is
// retired.
func TestTheRetryLoopMisattributesFailuresAfterASkip(t *testing.T) {
	s := newBufferSyncer(t)
	s.targetClient = deadClient(t)
	s.maxRetryAttempts = 5

	// The first operation cannot be deserialised; the second can.
	batch := DeadLetterBatch{
		BatchID:    "batch_shop_orders_1",
		SourceDB:   "shop",
		SourceColl: "orders",
		FailedOps: []FailedOperation{
			{ID: "broken", WriteModel: json.RawMessage(`{"type":"nonsense"}`), OpType: "insert"},
			{ID: "genuine", WriteModel: json.RawMessage(`{"type":"insert","document":{"_id":"1"}}`), OpType: "insert"},
		},
	}
	dir := filepath.Join(s.deadLetterDir, "shop_orders")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	data, _ := json.MarshalIndent(batch, "", "  ")
	path := filepath.Join(dir, "batch_shop_orders_1.json")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	s.processDeadLetterQueue(briefCtx(t), "shop", "orders", "shop", "orders")

	updated, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var after DeadLetterBatch
	if err := json.Unmarshal(updated, &after); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// "broken" is recorded twice — once for the deserialise failure and once
	// wrongly for the retry failure that belongs to "genuine" — and "genuine"
	// never appears at all.
	counts := map[string]int{}
	for _, op := range after.FailedOps {
		counts[op.ID]++
	}
	if counts["genuine"] != 0 || counts["broken"] != 2 {
		t.Fatalf("recorded operations = %v; the indexing appears to be fixed now, "+
			"so assert that instead", counts)
	}
}
