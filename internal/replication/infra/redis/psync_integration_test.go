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

// These tests speak the protocol directly rather than through the pipeline, so
// they need one address and one plain client. Set SYNC_REDIS_SOURCE to a
// cluster-enabled instance owning every slot; a single node with
// CLUSTER ADDSLOTSRANGE 0 16383 is enough.
func oneSource(t *testing.T) (string, *goredis.Client) {
	t.Helper()
	addr := addrsFrom(t, "SYNC_REDIS_SOURCE")[0]
	client := goredis.NewClient(&goredis.Options{Addr: addr})
	t.Cleanup(func() { client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("ping %s: %v", addr, err)
	}

	// Start from an empty source.
	//
	// These tests take a full resync, which means the master dumps everything it
	// holds before the stream begins. Run on their own they pass; run after the
	// crash and cluster suites, which leave thousands of keys behind, the dump
	// grows until the read deadline is hit and the failure reads as "the source
	// said nothing" — a connection problem that is not one. The suite has to
	// give each of these a clean server, and the flush guard is already required
	// to run any of this.
	emptyOne(t, client)
	return addr, client
}

// emptyOne clears one server, refusing unless the caller has said it may.
func emptyOne(t *testing.T, client *goredis.Client) {
	t.Helper()
	if os.Getenv("SYNC_REDIS_ALLOW_FLUSH") != "1" {
		t.Skip("set SYNC_REDIS_ALLOW_FLUSH=1 to let these tests empty the server they run against")
	}
	if err := client.FlushAll(context.Background()).Err(); err != nil {
		t.Fatalf("empty the source: %v", err)
	}
}

// masterOffset reads what the source thinks its own offset is.
// keepAcking acknowledges on a ticker, the way the real relay does.
//
// A diskless full resync ends with the master saying "waiting for REPLCONF ACK
// from slave to enable streaming": it will not send a single command until the
// replica acknowledges. The client sends one acknowledgement as soon as it has
// read the data set, and that one can arrive before the master has set the flag
// it is meant to clear — after which the master waits for the next one. In the
// relay there always is a next one, because it acknowledges on a ticker. A test
// driving the Stream directly has to do the same or it waits for ever, which is
// how these two tests failed: "the source said nothing for 15s", against a
// master that had the data and was holding it back.
func keepAcking(t *testing.T, stream *Stream) func() {
	t.Helper()
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				_ = stream.Ack(stream.Offset())
			}
		}
	}()
	return func() { close(done) }
}

// waitOnline waits until the master counts this replica as caught up.
//
// A diskless full resync sends the data set without a length, so the master
// only marks the replica online once it has acknowledged. Writing before that
// happens leaves the commands buffered against a replica the master still
// considers loading, and the read then times out with "the source said nothing"
// — a connection failure that is not one. Two of these tests failed that way on
// redis:7.0, where diskless sync is the default.
func waitOnline(t *testing.T, client *goredis.Client) {
	t.Helper()
	ctx := context.Background()
	for attempt := 0; attempt < 100; attempt++ {
		info, err := client.Info(ctx, "replication").Result()
		if err != nil {
			t.Fatalf("read the source's replication state: %v", err)
		}
		if strings.Contains(info, "state=online") {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("the master never reported this replica online, so anything written " +
		"now would be buffered rather than streamed")
}

// masterOffsetOf reads the source's own write offset.
//
// Named apart from the production masterOffset in link.go: the two do the same
// thing for different callers, and having both in one package broke the
// integration build from the commit that added the production one until this
// one was renamed. Nobody saw it because the integration suite only runs in CI.
func masterOffsetOf(t *testing.T, client *goredis.Client) int64 {
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
	addr, client := oneSource(t)
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
	defer keepAcking(t, stream)()
	waitOnline(t, client)
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

	want := masterOffsetOf(t, client)

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
	addr, client := oneSource(t)
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
	stopAcking := keepAcking(t, first)
	waitOnline(t, client)

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
	stopAcking()
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
	want := masterOffsetOf(t, client)
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
