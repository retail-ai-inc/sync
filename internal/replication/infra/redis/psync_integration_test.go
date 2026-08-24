//go:build integration

package redis

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// sourceAddr is the Redis to replicate from in these tests.
//
// Set SYNC_REDIS_SOURCE to a cluster-enabled instance owning every slot; a
// single node with CLUSTER ADDSLOTSRANGE 0 16383 is enough, and is what makes
// the same-slot rules for MULTI apply.
func sourceAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("SYNC_REDIS_SOURCE")
	if addr == "" {
		t.Skip("set SYNC_REDIS_SOURCE to a Redis to replicate from")
	}
	return addr
}

func sourceClient(t *testing.T, addr string) *goredis.Client {
	t.Helper()
	client := goredis.NewClient(&goredis.Options{Addr: addr})
	t.Cleanup(func() { client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("ping %s: %v", addr, err)
	}
	return client
}

// masterOffset reads what the source thinks its own offset is.
func masterOffset(t *testing.T, client *goredis.Client) int64 {
	t.Helper()
	info, err := client.Info(context.Background(), "replication").Result()
	if err != nil {
		t.Fatalf("INFO replication: %v", err)
	}
	for _, line := range strings.Split(info, "\n") {
		value, ok := strings.CutPrefix(strings.TrimSpace(line), "master_repl_offset:")
		if !ok {
			continue
		}
		at, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil {
			t.Fatalf("master_repl_offset was %q: %v", value, err)
		}
		return at
	}
	t.Fatal("INFO replication did not report master_repl_offset")
	return 0
}

// TestTheOffsetAgreesWithARealMaster is the test that decides whether any of
// this works.
//
// Every position this package records is a byte count of the replication
// stream, and the source is the authority on what that count is. If this side
// arrives at a different number, resuming asks for the wrong byte and the
// acknowledgements are lies — and both failures are silent. So: replicate a real
// master, apply real commands, and check the two counts match exactly.
func TestTheOffsetAgreesWithARealMaster(t *testing.T) {
	addr := sourceAddr(t)
	client := sourceClient(t, addr)
	ctx := context.Background()

	stream, err := Dial(ctx, StreamOptions{Addr: addr, IdleTimeout: 15 * time.Second})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer stream.Close()

	got, err := stream.Sync(Point{})
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if !got.Full {
		t.Fatalf("a first connection got %+v, want a full resync", got)
	}
	if _, err := stream.SkipRDB(ctx); err != nil {
		t.Fatalf("SkipRDB: %v", err)
	}
	t.Logf("full resync from %s at offset %d", got.ReplID, got.Offset)

	// A mixture that includes the commands the master rewrites on its way out,
	// which is the whole reason for reading the replication stream rather than
	// watching for changes.
	const count = 200
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("offset:{s%d}:%d", i%7, i)
		if err := client.Set(ctx, key, strings.Repeat("v", i%50), 0).Err(); err != nil {
			t.Fatalf("SET: %v", err)
		}
		if err := client.Incr(ctx, fmt.Sprintf("offset:counter:%d", i%3)).Err(); err != nil {
			t.Fatalf("INCR: %v", err)
		}
		if err := client.Expire(ctx, key, time.Hour).Err(); err != nil {
			t.Fatalf("EXPIRE: %v", err)
		}
	}
	if err := client.SAdd(ctx, "offset:set", "a", "b", "c").Err(); err != nil {
		t.Fatalf("SADD: %v", err)
	}
	if err := client.SPop(ctx, "offset:set").Err(); err != nil {
		t.Fatalf("SPOP: %v", err)
	}

	want := masterOffset(t, client)

	// Read until the stream reaches the master's offset. PINGs and the commands
	// this test issued both count.
	deadline := time.Now().Add(20 * time.Second)
	var sawRewrittenSPop, sawAbsoluteExpiry bool
	for stream.Offset() < want && time.Now().Before(deadline) {
		command, err := stream.Next(ctx)
		if err != nil {
			t.Fatalf("Next at offset %d of %d: %v", stream.Offset(), want, err)
		}
		switch command.Name() {
		case "SREM":
			sawRewrittenSPop = true
		case "PEXPIREAT":
			sawAbsoluteExpiry = true
		}
	}

	if stream.Offset() != want {
		t.Fatalf("this side counted %d bytes, the master counted %d. Every recorded "+
			"position is one of these numbers, so a difference means resuming asks "+
			"for the wrong byte.", stream.Offset(), want)
	}

	// The rewriting is what makes command replication safe to apply at all, so
	// it is worth proving rather than assuming.
	if !sawRewrittenSPop {
		t.Error("SPOP did not arrive as SREM; the stream is not the rewritten one")
	}
	if !sawAbsoluteExpiry {
		t.Error("EXPIRE did not arrive as PEXPIREAT; a relative TTL would drift " +
			"against the target's clock")
	}
}

// TestAPartialResyncPicksUpExactlyWhereItStopped covers a reconnect, which is
// what every restart and every network blip looks like.
//
// The offset has to continue rather than restart, and nothing may be delivered
// twice or skipped. This is the property that makes the buffer worth having.
func TestAPartialResyncPicksUpExactlyWhereItStopped(t *testing.T) {
	addr := sourceAddr(t)
	client := sourceClient(t, addr)
	ctx := context.Background()

	// Give the master room to hold history across the disconnect.
	if err := client.ConfigSet(ctx, "repl-backlog-size", "16777216").Err(); err != nil {
		t.Fatalf("widen the backlog: %v", err)
	}

	first, err := Dial(ctx, StreamOptions{Addr: addr, IdleTimeout: 15 * time.Second})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	opened, err := first.Sync(Point{})
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if _, err := first.SkipRDB(ctx); err != nil {
		t.Fatalf("SkipRDB: %v", err)
	}

	for i := 0; i < 20; i++ {
		if err := client.Set(ctx, fmt.Sprintf("resume:before:%d", i), i, 0).Err(); err != nil {
			t.Fatalf("SET: %v", err)
		}
	}

	// Read some of it, then drop the connection mid-stream.
	var stopped int64
	for i := 0; i < 5; i++ {
		command, err := first.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		stopped = command.End
	}
	first.Close()

	// More happens while nothing is connected.
	for i := 0; i < 20; i++ {
		if err := client.Set(ctx, fmt.Sprintf("resume:during:%d", i), i, 0).Err(); err != nil {
			t.Fatalf("SET: %v", err)
		}
	}

	second, err := Dial(ctx, StreamOptions{Addr: addr, IdleTimeout: 15 * time.Second})
	if err != nil {
		t.Fatalf("re-Dial: %v", err)
	}
	defer second.Close()

	resumed, err := second.Sync(Point{ReplID: opened.ReplID, Offset: stopped})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if resumed.Full {
		t.Fatalf("the master would not continue from offset %d and sent everything "+
			"again; the backlog was too small for the test to mean anything", stopped)
	}
	if second.Offset() != stopped {
		t.Errorf("resumed counting at %d, want %d", second.Offset(), stopped)
	}

	// What was written during the gap has to arrive, once each.
	want := masterOffset(t, client)
	seen := map[string]int{}
	deadline := time.Now().Add(20 * time.Second)
	for second.Offset() < want && time.Now().Before(deadline) {
		command, err := second.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if command.Name() == "SET" && len(command.Args) > 1 {
			seen[string(command.Args[1])]++
		}
	}

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("resume:during:%d", i)
		switch seen[key] {
		case 1:
		case 0:
			t.Errorf("%s never arrived, so the gap lost writes", key)
		default:
			t.Errorf("%s arrived %d times", key, seen[key])
		}
	}
}
