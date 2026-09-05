//go:build integration

package directionlock

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/retail-ai-inc/sync/test/harness"
)

// The claim is what stops both ends being written at once, so the stores that
// hold it are exercised against the real servers rather than a fake: what is
// being checked is that a claim written on one connection is read back by
// another, which is the whole mechanism.

func mongoStore(t *testing.T) *MongoStore {
	t.Helper()
	host, port := harness.SplitHostPort(t, harness.MongoTarget)
	uri := "mongodb://" + host + ":" + port + "/?directConnection=true"

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("connect to mongo: %v", err)
	}
	database := harness.UniqueName("dirlock")
	t.Cleanup(func() {
		_ = client.Database(database).Drop(context.Background())
		_ = client.Disconnect(context.Background())
	})
	return &MongoStore{Database: client.Database(database), Address: harness.MongoTarget}
}

func redisStore(t *testing.T) *RedisStore {
	t.Helper()
	client := goredis.NewClient(&goredis.Options{Addr: harness.RedisTarget})
	t.Cleanup(func() {
		_ = client.Del(context.Background(), RedisKey).Err()
		_ = client.Close()
	})
	return &RedisStore{Client: client, Address: harness.RedisTarget}
}

// store is what both of them are.
type store interface {
	Endpoint() string
	Claims(context.Context) ([]Claim, error)
	Put(context.Context, Claim) error
	Remove(context.Context, int) error
}

func TestEveryStoreHoldsAClaimTheSameWay(t *testing.T) {
	for name, build := range map[string]func(*testing.T) store{
		"mongo": func(t *testing.T) store { return mongoStore(t) },
		"redis": func(t *testing.T) store { return redisStore(t) },
	} {
		t.Run(name, func(t *testing.T) {
			s := build(t)
			ctx := context.Background()

			if s.Endpoint() == "" {
				t.Error("the store does not say which endpoint it speaks for")
			}
			claims, err := s.Claims(ctx)
			if err != nil {
				t.Fatalf("Claims on an empty store: %v", err)
			}
			if len(claims) != 0 {
				t.Fatalf("an empty store held %d claims", len(claims))
			}

			at := time.Now().UTC().Truncate(time.Second)
			mine := Claim{TaskID: 41, Role: RoleTarget, Peer: "tokyo:3306",
				Owner: "host-a", UpdatedAt: at}
			if err := s.Put(ctx, mine); err != nil {
				t.Fatalf("Put: %v", err)
			}

			claims, err = s.Claims(ctx)
			if err != nil {
				t.Fatalf("Claims: %v", err)
			}
			if len(claims) != 1 {
				t.Fatalf("read back %d claims, want 1", len(claims))
			}
			got := claims[0]
			if got.TaskID != 41 || got.Role != RoleTarget ||
				got.Peer != "tokyo:3306" || got.Owner != "host-a" {
				t.Errorf("the claim read back as %+v", got)
			}
			// The time is what tells a stale claim from a live one, so it has to
			// survive the round trip rather than come back zero.
			if !got.UpdatedAt.Equal(at) {
				t.Errorf("the claim's time read back as %v, want %v", got.UpdatedAt, at)
			}

			// Writing the same task again replaces rather than adds: two claims
			// for one task is two answers to which end may be written.
			mine.Owner = "host-b"
			if err := s.Put(ctx, mine); err != nil {
				t.Fatalf("Put again: %v", err)
			}
			claims, _ = s.Claims(ctx)
			if len(claims) != 1 {
				t.Fatalf("writing the same task twice left %d claims", len(claims))
			}
			if claims[0].Owner != "host-b" {
				t.Errorf("the second write did not replace the first: %+v", claims[0])
			}

			// Another task's claim shares the store and must not be disturbed.
			if err := s.Put(ctx, Claim{TaskID: 42, Role: RoleSource,
				Peer: "osaka:3306", Owner: "host-c", UpdatedAt: at}); err != nil {
				t.Fatalf("Put another task: %v", err)
			}
			if err := s.Remove(ctx, 41); err != nil {
				t.Fatalf("Remove: %v", err)
			}
			claims, err = s.Claims(ctx)
			if err != nil {
				t.Fatalf("Claims after a removal: %v", err)
			}
			if len(claims) != 1 || claims[0].TaskID != 42 {
				t.Errorf("after removing task 41 the store holds %+v", claims)
			}

			// Removing what is not there is not a failure: a task deleted twice
			// must not report one.
			if err := s.Remove(ctx, 999); err != nil {
				t.Errorf("removing a claim that is not there reported %v", err)
			}
		})
	}
}
