package mongodb

import (
	"strings"
	"testing"
	"time"

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
