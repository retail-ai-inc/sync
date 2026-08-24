package mongodb

import (
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

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

// -------------------------------------------------- unrecoverable positions

// TestALostChangeStreamPositionIsRecognised is the distinction the supervisor
// acts on. The oplog is capped: a task stopped for longer than it covers comes
// back to find its resume point gone. Retrying fails identically every time, and
// the tempting repair — dropping the token and watching from now — silently
// skips everything in between.
func TestALostChangeStreamPositionIsRecognised(t *testing.T) {
	for _, text := range []string{
		"(ChangeStreamHistoryLost) Resume of change stream was not possible, as the resume point may no longer be in the oplog.",
		"invalid resume token",
		"Resume of change stream was not possible",
	} {
		if !positionLost(fmt.Errorf("%s", text)) {
			t.Errorf("positionLost(%q) = false", text)
		}
	}
}

// TestATransientChangeStreamFailureIsNotAPositionLoss keeps a task from being
// stopped permanently by something that comes back.
func TestATransientChangeStreamFailureIsNotAPositionLoss(t *testing.T) {
	for _, text := range []string{
		"server selection timeout",
		"connection refused",
		"interrupted at shutdown",
		"cursor not found",
		"",
	} {
		var err error
		if text != "" {
			err = fmt.Errorf("%s", text)
		}
		if positionLost(err) {
			t.Errorf("positionLost(%q) = true", text)
		}
	}
}
