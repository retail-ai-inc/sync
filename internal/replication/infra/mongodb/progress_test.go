package mongodb

import (
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
)

func storedAt(t *testing.T, position streamPosition) string {
	t.Helper()
	payload, err := checkpoint.Encode(position)
	if err != nil {
		t.Fatalf("encode a stored position: %v", err)
	}
	return payload
}

func TestNothingAppliedIsNotCaughtUp(t *testing.T) {
	got := compareClusterTime(time.Unix(1757000000, 0), "")
	if got.CaughtUp || got.Comparable {
		t.Errorf("an empty position reported %+v", got)
	}
	if got.Note == "" {
		t.Error("nothing said why there is no answer")
	}
}

func TestAppliedAtTheSourcesOwnTimeIsCaughtUp(t *testing.T) {
	head := time.Unix(1757000000, 0)
	got := compareClusterTime(head, storedAt(t, streamPosition{
		Token: `{"_data":"82..."}`, At: head.Unix(),
	}))
	if !got.Comparable {
		t.Fatalf("not comparable: %+v", got)
	}
	if !got.CaughtUp {
		t.Errorf("a target at the source's own cluster time reported behind: %+v", got)
	}
}

func TestAppliedBehindTheSource(t *testing.T) {
	head := time.Unix(1757000000, 0)
	got := compareClusterTime(head, storedAt(t, streamPosition{
		Token: `{"_data":"82..."}`, At: head.Add(-90 * time.Second).Unix(),
	}))
	if !got.Comparable {
		t.Fatalf("not comparable: %+v", got)
	}
	if got.CaughtUp {
		t.Error("a target ninety seconds behind reported caught up")
	}
}

// TestAppliedPastTheSourceCountsAsCaughtUp covers an idle source, whose cluster
// time advances on its own with no write involved. The target cannot be asked
// for more than the source's last event.
func TestAppliedPastTheSourceCountsAsCaughtUp(t *testing.T) {
	head := time.Unix(1757000000, 0)
	got := compareClusterTime(head, storedAt(t, streamPosition{
		Token: `{"_data":"82..."}`, At: head.Add(2 * time.Second).Unix(),
	}))
	if !got.CaughtUp {
		t.Errorf("a target at or past the source reported behind: %+v", got)
	}
}

// TestATokenWithNoTimeCannotBeOrdered covers a position stored before the
// cluster time was recorded beside the token. A resume token is opaque, so the
// honest answer is that the two cannot be compared -- not that the target is
// behind, and certainly not that it is caught up.
func TestATokenWithNoTimeCannotBeOrdered(t *testing.T) {
	got := compareClusterTime(time.Unix(1757000000, 0),
		storedAt(t, streamPosition{Token: `{"_data":"82..."}`}))

	if got.Comparable {
		t.Error("an opaque token reported as comparable")
	}
	if got.CaughtUp {
		t.Error("an opaque token reported as caught up")
	}
	if !strings.Contains(got.Note, "opaque") {
		t.Errorf("the note does not say why: %q", got.Note)
	}
}

// TestAPinnedClusterTimeIsUsable covers a task whose snapshot has finished and
// whose stream has not delivered anything yet: the pinned time is what it has
// applied up to.
func TestAPinnedClusterTimeIsUsable(t *testing.T) {
	head := time.Unix(1757000000, 0)
	got := compareClusterTime(head, storedAt(t, streamPosition{
		Cluster: uint32(head.Add(-time.Hour).Unix()), Increment: 1,
	}))
	if !got.Comparable {
		t.Fatalf("a pinned cluster time reported as not comparable: %+v", got)
	}
	if got.CaughtUp {
		t.Error("a position an hour old reported caught up")
	}
}

func TestAnUnreadablePositionIsReported(t *testing.T) {
	got := compareClusterTime(time.Unix(1757000000, 0), "not a stored position")
	if got.Comparable || got.CaughtUp {
		t.Errorf("an unreadable position reported %+v", got)
	}
	if got.Note == "" {
		t.Error("nothing said the position could not be read")
	}
}

// TestTheDistanceIsNeverAByteCount: MongoDB positions are times, so a byte
// figure would be meaningless and a zero would read as caught up.
func TestTheDistanceIsNeverAByteCount(t *testing.T) {
	got := compareClusterTime(time.Unix(1757000000, 0), storedAt(t, streamPosition{
		Token: `{"_data":"82..."}`, At: 1757000000,
	}))
	if got.BehindBytes >= 0 {
		t.Errorf("BehindBytes = %d, want it reported as not a byte count", got.BehindBytes)
	}
}

// TestAnUnreachableSourceStillReportsWhatTheTargetApplied covers the case the
// endpoint exists for: Tokyo is gone, and what Osaka holds is still readable
// because it is written on Osaka.
func TestAnUnreachableSourceStillReportsWhatTheTargetApplied(t *testing.T) {
	stored := streamPosition{At: time.Date(2026, 9, 17, 4, 5, 6, 0, time.UTC).Unix()}
	payload, err := checkpoint.Encode(stored)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// What Progress builds when the source could not be asked.
	applied := compareClusterTime(time.Time{}, payload)
	applied.Source = ""
	applied.Comparable = false
	applied.CaughtUp = false

	if applied.Applied == "" {
		t.Error("the position the target holds was discarded with the source")
	}
	if applied.CaughtUp {
		t.Error("a shard with no source position is reported as caught up")
	}
	if applied.Comparable {
		t.Error("a shard with no source position is reported as comparable")
	}
}

// TestTheSourcesLastWriteIsWhatTheTargetIsComparedWith records why the
// comparison changed.
//
// $clusterTime is a gossiped logical clock: it advances on every operation
// anywhere in the cluster and on the periodic no-op write, so an idle source is
// always a second or two ahead of the last event anybody applied. Compared
// against that, a target that holds everything reads as "not caught up" for
// ever -- and "caught up" is what somebody waits for before promoting Osaka.
func TestTheSourcesLastWriteIsWhatTheTargetIsComparedWith(t *testing.T) {
	// A hello reply from a replica set member: its last write is older than the
	// gossiped cluster time, which is what an idle source looks like.
	reply, err := bson.Marshal(bson.D{
		{Key: "lastWrite", Value: bson.D{
			{Key: "opTime", Value: bson.D{{Key: "ts", Value: bson.Timestamp{T: 1000, I: 1}}}},
			{Key: "majorityOpTime", Value: bson.D{{Key: "ts", Value: bson.Timestamp{T: 1000, I: 1}}}},
		}},
		{Key: "$clusterTime", Value: bson.D{
			{Key: "clusterTime", Value: bson.Timestamp{T: 1010, I: 1}},
		}},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	at, ok := lastWriteFrom(reply)
	if !ok {
		t.Fatal("the hello reply's last write was not read")
	}
	if at.T != 1000 {
		t.Errorf("last write = %d, want the committed write and not the gossiped clock", at.T)
	}

	// A mongos carries no lastWrite, and then the gossiped clock is the only
	// answer -- an upper bound, so the report errs towards "behind".
	mongos, err := bson.Marshal(bson.D{
		{Key: "msg", Value: "isdbgrid"},
		{Key: "$clusterTime", Value: bson.D{
			{Key: "clusterTime", Value: bson.Timestamp{T: 1010, I: 1}},
		}},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, ok := lastWriteFrom(mongos); ok {
		t.Error("a mongos reply reported a last write it does not carry")
	}
	if gossiped, err := clusterTimeFrom(mongos); err != nil || gossiped.T != 1010 {
		t.Errorf("fallback cluster time = %v (%v)", gossiped, err)
	}
}

// And the gap is reported, so "not caught up" says how far.
func TestHowFarBehindIsReported(t *testing.T) {
	stored := streamPosition{At: time.Date(2026, 9, 17, 0, 0, 0, 0, time.UTC).Unix()}
	payload, err := checkpoint.Encode(stored)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	behind := compareClusterTime(time.Date(2026, 9, 17, 0, 0, 30, 0, time.UTC), payload)
	if behind.CaughtUp {
		t.Error("a target thirty seconds behind reads as caught up")
	}
	if !strings.Contains(behind.Note, "30s") {
		t.Errorf("Note = %q, want it to say how far behind", behind.Note)
	}

	level := compareClusterTime(time.Date(2026, 9, 17, 0, 0, 0, 0, time.UTC), payload)
	if !level.CaughtUp {
		t.Error("a target level with the source's last write is not caught up")
	}
	if level.Note != "" {
		t.Errorf("Note = %q for a target that is level", level.Note)
	}
}
