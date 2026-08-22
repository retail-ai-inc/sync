package mongodb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// checkpointSyncer builds a syncer whose checkpoints live in a temporary
// directory, which is what the start-time and resume-token helpers read.
func checkpointSyncer(t *testing.T) *MongoDBSyncer {
	t.Helper()

	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)

	return &MongoDBSyncer{
		logger:       logger,
		resumeTokens: map[string]bson.Raw{},
		cfg:          config.SyncConfig{MongoDBResumeTokenPath: t.TempDir()},
	}
}

// hello encodes the reply the source sends, so the reader can be exercised
// without a server.
func hello(t *testing.T, doc bson.M) bson.Raw {
	t.Helper()

	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return raw
}

// ----------------------------------------------------------- cluster time

func TestTheClusterTimeComesFromTheReply(t *testing.T) {
	want := primitive.Timestamp{T: 1755800000, I: 7}

	got, err := clusterTimeFrom(hello(t, bson.M{
		"ok":           1,
		"$clusterTime": bson.M{"clusterTime": want},
	}))
	if err != nil {
		t.Fatalf("clusterTimeFrom: %v", err)
	}
	if got != want {
		t.Errorf("cluster time = %+v, want %+v", got, want)
	}
}

// TestTheOperationTimeIsTheFallback covers a deployment that answers with only
// the one field.
func TestTheOperationTimeIsTheFallback(t *testing.T) {
	want := primitive.Timestamp{T: 1755800001, I: 2}

	got, err := clusterTimeFrom(hello(t, bson.M{"ok": 1, "operationTime": want}))
	if err != nil {
		t.Fatalf("clusterTimeFrom: %v", err)
	}
	if got != want {
		t.Errorf("cluster time = %+v, want %+v", got, want)
	}
}

// TestAStandaloneIsReported records why this has to be an error rather than a
// zero timestamp: a zero start point would read as "from the beginning of
// time", and the caller would carry on with a copy whose window is unprotected.
func TestAStandaloneIsReported(t *testing.T) {
	_, err := clusterTimeFrom(hello(t, bson.M{"ok": 1, "isWritablePrimary": true}))
	if err == nil {
		t.Fatal("a reply with no cluster time returned no error")
	}
}

func TestATimestampOfTheWrongTypeIsReported(t *testing.T) {
	_, err := clusterTimeFrom(hello(t, bson.M{
		"ok":           1,
		"$clusterTime": bson.M{"clusterTime": "not a timestamp"},
	}))
	if err == nil {
		t.Fatal("a non-timestamp cluster time returned no error")
	}
}

// -------------------------------------------------------- start-time file

func TestTheStartTimeRoundTrips(t *testing.T) {
	s := checkpointSyncer(t)
	want := primitive.Timestamp{T: 1755800000, I: 3}

	s.saveStartTime("shop", "orders", want)

	if got := s.loadStartTime("shop", "orders"); got != want {
		t.Errorf("start time = %+v, want %+v", got, want)
	}
}

func TestAnAbsentStartTimeReadsAsZero(t *testing.T) {
	s := checkpointSyncer(t)

	if got := s.loadStartTime("shop", "orders"); !got.IsZero() {
		t.Errorf("start time = %+v, want the zero value", got)
	}
}

func TestTheStartTimeIsPerCollection(t *testing.T) {
	s := checkpointSyncer(t)
	s.saveStartTime("shop", "orders", primitive.Timestamp{T: 1, I: 1})

	if got := s.loadStartTime("shop", "customers"); !got.IsZero() {
		t.Errorf("customers picked up the orders start time: %+v", got)
	}
}

// TestNoCheckpointPathMeansNoStartTime records that the whole mechanism is off
// when nothing is configured to hold it.
func TestNoCheckpointPathMeansNoStartTime(t *testing.T) {
	s := checkpointSyncer(t)
	s.cfg.MongoDBResumeTokenPath = ""

	s.saveStartTime("shop", "orders", primitive.Timestamp{T: 1, I: 1})
	if got := s.loadStartTime("shop", "orders"); !got.IsZero() {
		t.Errorf("start time = %+v with no path configured", got)
	}
}

func TestACorruptStartTimeReadsAsZero(t *testing.T) {
	s := checkpointSyncer(t)
	path := s.startTimePath("shop", "orders")
	if err := writeFile(t, path, "not json"); err != nil {
		t.Fatalf("write: %v", err)
	}

	if got := s.loadStartTime("shop", "orders"); !got.IsZero() {
		t.Errorf("start time = %+v for an unreadable file", got)
	}
}

// ------------------------------------------------------- snapshot gating

// TestASnapshotWithNoCheckpointHasNotBeenMade is the rule that makes an
// interrupted copy finishable: the copy is redone until the checkpoint that
// follows it exists.
func TestASnapshotWithNoCheckpointHasNotBeenMade(t *testing.T) {
	s := checkpointSyncer(t)

	if s.snapshotDone("shop", "orders") {
		t.Error("the copy was reported done with no checkpoint at all")
	}
}

func TestAStartTimeMeansTheSnapshotWasMade(t *testing.T) {
	s := checkpointSyncer(t)
	s.saveStartTime("shop", "orders", primitive.Timestamp{T: 1755800000, I: 1})

	if !s.snapshotDone("shop", "orders") {
		t.Error("the copy was reported outstanding although its start time is stored")
	}
}

func TestAResumeTokenMeansTheSnapshotWasMade(t *testing.T) {
	s := checkpointSyncer(t)
	token, err := bson.Marshal(bson.M{"_data": "82650000"})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	s.saveMongoDBResumeToken("shop", "orders", token)

	if !s.snapshotDone("shop", "orders") {
		t.Error("the copy was reported outstanding although the stream has resumed")
	}
}

// TestWithNoCheckpointPathTheSnapshotIsNeverRecordedAsDone records the fallback:
// there is nowhere to remember it, so the document count is what the copy has
// to fall back on.
func TestWithNoCheckpointPathTheSnapshotIsNeverRecordedAsDone(t *testing.T) {
	s := checkpointSyncer(t)
	s.cfg.MongoDBResumeTokenPath = ""

	if s.snapshotDone("shop", "orders") {
		t.Error("the copy was reported done with no path to record it in")
	}
}

func writeFile(t *testing.T, path, body string) error {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(body), 0o644)
}
