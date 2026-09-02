//go:build staging

// The failover case, kept in its own file because it is the only one here that
// disturbs anything outside the two databases this suite owns: asking a
// shard's primary to stand down interrupts writes for every database on that
// shard for the length of an election.
package mongodb

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/retail-ai-inc/sync/internal/replication/infra/verify"
	"github.com/retail-ai-inc/sync/test/harness"
)

// TestAShardFailoverLosesNothing answers the question the whole exercise
// exists for: when a shard elects a new primary, does the replica lose
// changes?  Guarded twice over, because the effect reaches beyond this suite's
// own databases: SYNC_STG_ALLOW_FAILOVER has to be set, and the members have
// to be named rather than discovered.
func TestAShardFailoverLosesNothing(t *testing.T) {
	if os.Getenv("SYNC_STG_ALLOW_FAILOVER") != "true" {
		t.Skip("SYNC_STG_ALLOW_FAILOVER is not set; this test interrupts writes " +
			"for every database on the shard it disturbs")
	}
	members := strings.Split(os.Getenv("SYNC_STG_SHARD_MEMBERS"), ",")
	if len(members) < 3 {
		t.Skip("SYNC_STG_SHARD_MEMBERS needs the three members of one shard")
	}

	name, source, target := stgCollection(t, "payments_failover")
	ctx := context.Background()

	if _, err := source.InsertOne(ctx, payment(0)); err != nil {
		t.Fatalf("seed the source: %v", err)
	}
	stgStart(t, stgTask(t, name))
	harness.Eventually(t, 2*time.Minute, func() error {
		if stgCount(t, target) != 1 {
			return fmt.Errorf("the syncer has not started")
		}
		return nil
	})

	// Write throughout, tolerating the failures an election causes.
	var (
		mu       sync.Mutex
		accepted []int
		refused  int
	)
	writeCtx, stopWriting := context.WithCancel(ctx)
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		ticker := time.NewTicker(100 * time.Millisecond) // 10/s; the cluster is shared
		defer ticker.Stop()
		seq := 1
		for {
			select {
			case <-writeCtx.Done():
				return
			case <-ticker.C:
				_, err := source.InsertOne(writeCtx, payment(seq))
				mu.Lock()
				if err != nil {
					refused++
				} else {
					accepted = append(accepted, seq)
				}
				mu.Unlock()
				seq++
			}
		}
	}()

	// Let the pipeline settle before disturbing it.
	time.Sleep(5 * time.Second)

	before, err := stgWritableMember(t, members)
	if err != nil {
		stopWriting()
		<-writerDone
		t.Fatalf("find the writable member: %v", err)
	}
	t.Logf("asking %s to stand down", before)

	requestedAt := time.Now()
	if err := stgAskToStandDown(t, before); err != nil {
		// The command closes the connection it arrives on, so an error here is
		// the normal outcome rather than a failure.
		t.Logf("the request returned %v, which is expected", err)
	}

	var after string
	harness.Eventually(t, 2*time.Minute, func() error {
		found, err := stgWritableMember(t, members)
		if err != nil {
			return err
		}
		if found == before {
			return fmt.Errorf("%s is still the writable member", before)
		}
		after = found
		return nil
	})
	t.Logf("%s took over %v later", after, time.Since(requestedAt).Round(time.Millisecond))

	// Keep writing through the recovery, then stop and let the replica catch up.
	time.Sleep(20 * time.Second)
	stopWriting()
	<-writerDone

	mu.Lock()
	owed := append([]int(nil), accepted...)
	refusedCount := refused
	mu.Unlock()
	t.Logf("the source accepted %d writes and refused %d while the set was changing hands",
		len(owed), refusedCount)

	harness.Eventually(t, 5*time.Minute, func() error {
		missing, first := 0, 0
		for _, seq := range owed {
			if err := target.FindOne(ctx, bson.M{"seq": seq}).Err(); err != nil {
				if missing == 0 {
					first = seq
				}
				missing++
			}
		}
		if missing > 0 {
			return fmt.Errorf("%d of %d acknowledged writes are not on the target "+
				"(first: %d)", missing, len(owed), first)
		}
		return nil
	})
	t.Logf("every one of the %d acknowledged writes reached the replica", len(owed))

	// And the comparison agrees, which is the check an operator would run before
	// declaring the replica usable again.
	result, err := verify.Compare(ctx,
		&verify.MongoEnd{Coll: source}, &verify.MongoEnd{Coll: target}, 200)
	if err != nil {
		t.Fatalf("Compare: %v", err)
	}
	t.Logf("after the handover: %s", result.Summary())
	if !result.Identical() {
		t.Errorf("the two sides disagree after the handover: %s", result.Summary())
	}
}

func stgWritableMember(t *testing.T, members []string) (string, error) {
	t.Helper()

	for _, member := range members {
		member = strings.TrimSpace(member)
		if member == "" {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		client, err := mongo.Connect(options.Client().ApplyURI(stgMemberURI(member)))
		if err != nil {
			cancel()
			continue
		}
		var hello struct {
			IsWritablePrimary bool `bson:"isWritablePrimary"`
		}
		err = client.Database("admin").
			RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello)
		_ = client.Disconnect(context.Background())
		cancel()
		if err == nil && hello.IsWritablePrimary {
			return member, nil
		}
	}
	return "", fmt.Errorf("none of %v reported itself writable", members)
}

// stgAskToStandDown asks one member to hand over, waiting for a secondary to
// catch up first so the handover is the graceful kind an operator would choose.
func stgAskToStandDown(t *testing.T, member string) error {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(stgMemberURI(member)))
	if err != nil {
		return err
	}
	defer client.Disconnect(context.Background())

	return client.Database("admin").RunCommand(ctx, bson.D{
		{Key: "replSetStepDown", Value: 30},
		{Key: "secondaryCatchUpPeriodSecs", Value: 10},
	}).Err()
}

// stgMemberURI addresses one member directly, which is the one place
// directConnection belongs: the question being asked is about that node itself.
func stgMemberURI(member string) string {
	return fmt.Sprintf("mongodb://%s:%s@%s/admin?directConnection=true",
		stgUser, stgPassword, member)
}
