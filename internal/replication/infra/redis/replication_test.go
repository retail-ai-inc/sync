package redis

import (
	"context"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/sirupsen/logrus"
)

func ctxFor(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func sampleConfig() config.SyncConfig {
	return config.SyncConfig{
		ID:               7,
		Type:             "redis",
		SourceConnection: "redis://127.0.0.1:1/0",
		TargetConnection: "redis://127.0.0.1:1/0",
	}
}

func TestNewRedisSyncerCarriesThePositionPath(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	cfg := sampleConfig()
	cfg.RedisPositionPath = "/var/lib/sync/redis.pos"

	s := NewRedisSyncer(cfg, logger)

	if s.positionPath != cfg.RedisPositionPath {
		t.Errorf("positionPath = %q", s.positionPath)
	}
	if s.cfg.ID != 7 {
		t.Errorf("cfg was not kept: %+v", s.cfg)
	}
}

// ------------------------------------------------------------ key copying

// TestCopyFullKeyDumpsAndRestores records the copy mechanism: the source key is
// serialised with DUMP and written to the target with RESTORE, so the value is
// copied byte for byte whatever its type.
func TestCopyFullKeyDumpsAndRestores(t *testing.T) {
	source := newFakeRedis(t).on("TTL", ":-1\r\n").on("DUMP", bulk("payload"))
	target := newFakeRedis(t).on("RESTORE", "+OK\r\n")
	s := newRedisSyncerWithFakes(t, source, target)

	if err := s.copyFullKey(ctxFor(t), "user:1"); err != nil {
		t.Fatalf("copyFullKey: %v", err)
	}
	if !target.sawCommand("RESTORE") {
		t.Errorf("target commands = %v, want a RESTORE", target.seen())
	}
}

// TestAKeyWithNoTTLIsRestoredWithoutOne records the mapping from Redis's -1
// (no expiry) to RESTORE's 0 (no expiry) — two different sentinels for the same
// thing, and getting it wrong would expire every copied key immediately.
func TestAKeyWithNoTTLIsRestoredWithoutOne(t *testing.T) {
	source := newFakeRedis(t).on("TTL", ":-1\r\n").on("DUMP", bulk("payload"))
	target := newFakeRedis(t).on("RESTORE", "+OK\r\n")
	s := newRedisSyncerWithFakes(t, source, target)

	if err := s.copyFullKey(ctxFor(t), "user:1"); err != nil {
		t.Fatalf("copyFullKey: %v", err)
	}

	for _, cmd := range target.seen() {
		if strings.HasPrefix(cmd, "RESTORE ") {
			fields := strings.Fields(cmd)
			if len(fields) < 3 || fields[2] != "0" {
				t.Errorf("RESTORE ttl = %v, want 0", fields)
			}
			return
		}
	}
	t.Error("no RESTORE was issued")
}

// TestATTLIsCarriedOverInMilliseconds records that the remaining lifetime is
// preserved, converted from the seconds TTL reports to the milliseconds RESTORE
// expects.
func TestATTLIsCarriedOverInMilliseconds(t *testing.T) {
	source := newFakeRedis(t).on("TTL", ":60\r\n").on("DUMP", bulk("payload"))
	target := newFakeRedis(t).on("RESTORE", "+OK\r\n")
	s := newRedisSyncerWithFakes(t, source, target)

	if err := s.copyFullKey(ctxFor(t), "user:1"); err != nil {
		t.Fatalf("copyFullKey: %v", err)
	}

	for _, cmd := range target.seen() {
		if strings.HasPrefix(cmd, "RESTORE ") {
			if fields := strings.Fields(cmd); len(fields) < 3 || fields[2] != "60000" {
				t.Errorf("RESTORE ttl = %v, want 60000", fields)
			}
			return
		}
	}
	t.Error("no RESTORE was issued")
}

// TestAMissingKeyIsSkipped records the guard: TTL answers -2 for a key that does
// not exist, and the copy stops there rather than restoring an empty value.
func TestAMissingKeyIsSkipped(t *testing.T) {
	source := newFakeRedis(t).on("TTL", ":-2\r\n")
	target := newFakeRedis(t)
	s := newRedisSyncerWithFakes(t, source, target)

	if err := s.copyFullKey(ctxFor(t), "gone"); err != nil {
		t.Fatalf("copyFullKey: %v", err)
	}
	if source.sawCommand("DUMP") {
		t.Error("a missing key was dumped anyway")
	}
	if len(target.seen()) != 0 {
		t.Errorf("target commands = %v, want none", target.seen())
	}
}

// TestAnEmptyDumpIsSkipped covers the second guard, for a key that disappeared
// between the TTL and the DUMP.
func TestAnEmptyDumpIsSkipped(t *testing.T) {
	source := newFakeRedis(t).on("TTL", ":-1\r\n").on("DUMP", "$-1\r\n")
	target := newFakeRedis(t)
	s := newRedisSyncerWithFakes(t, source, target)

	if err := s.copyFullKey(ctxFor(t), "gone"); err != nil {
		t.Fatalf("copyFullKey: %v", err)
	}
	if len(target.seen()) != 0 {
		t.Errorf("target commands = %v, want none", target.seen())
	}
}

// TestASyntaxErrorFallsBackToDeleteAndRestore records the compatibility path:
// older Redis rejects RESTORE REPLACE, so the key is deleted and restored
// plainly. The two-step form is not atomic — a reader between them sees no key.
func TestASyntaxErrorFallsBackToDeleteAndRestore(t *testing.T) {
	source := newFakeRedis(t).on("TTL", ":-1\r\n").on("DUMP", bulk("payload"))
	target := newFakeRedis(t)

	// The first RESTORE (with REPLACE) is refused; the retry succeeds. The stub
	// answers per verb, so both attempts get the same reply and the fallback
	// error is what the caller sees — which is enough to pin the DEL.
	target.on("RESTORE", "-ERR syntax error\r\n").on("DEL", ":1\r\n")
	s := newRedisSyncerWithFakes(t, source, target)

	err := s.copyFullKey(ctxFor(t), "user:1")
	if err == nil {
		t.Fatal("copyFullKey reported success for a rejected RESTORE")
	}
	if !target.sawCommand("DEL") {
		t.Errorf("target commands = %v, want the DEL fallback", target.seen())
	}
}

// TestAFailedRestoreIsReported records that a target error other than the syntax
// case is returned rather than swallowed.
func TestAFailedRestoreIsReported(t *testing.T) {
	source := newFakeRedis(t).on("TTL", ":-1\r\n").on("DUMP", bulk("payload"))
	target := newFakeRedis(t).on("RESTORE", "-ERR target is full\r\n")
	s := newRedisSyncerWithFakes(t, source, target)

	err := s.copyFullKey(ctxFor(t), "user:1")
	if err == nil || !strings.Contains(err.Error(), "RESTORE fail") {
		t.Fatalf("err = %v, want a restore failure", err)
	}
	if target.sawCommand("DEL") {
		t.Error("the fallback ran for an unrelated error")
	}
}

func TestCopyFullKeyReportsATTLFailure(t *testing.T) {
	source := newFakeRedis(t).on("TTL", "-ERR nope\r\n")
	s := newRedisSyncerWithFakes(t, source, newFakeRedis(t))

	if err := s.copyFullKey(ctxFor(t), "user:1"); err == nil ||
		!strings.Contains(err.Error(), "get TTL fail") {
		t.Fatalf("err = %v, want a TTL failure", err)
	}
}

// TestCopyKeysKeepsGoingAfterAFailure records the batch contract: one key that
// cannot be copied is logged, the error flag is raised and the remaining keys
// are still attempted. The function itself always reports success, so the flag
// is the only signal.
func TestCopyKeysKeepsGoingAfterAFailure(t *testing.T) {
	source := newFakeRedis(t).on("TTL", "-ERR nope\r\n")
	s := newRedisSyncerWithFakes(t, source, newFakeRedis(t))

	if err := s.copyKeys(ctxFor(t), []string{"a", "b", "c"}); err != nil {
		t.Fatalf("copyKeys reported %v; failures appear to be propagated now, so "+
			"assert that instead", err)
	}
	if atomic.LoadInt32(&s.lastExecErr) != 1 {
		t.Error("the error flag was not raised")
	}

	ttls := 0
	for _, cmd := range source.seen() {
		if strings.HasPrefix(cmd, "TTL ") {
			ttls++
		}
	}
	if ttls != 3 {
		t.Errorf("%d keys attempted, want all 3", ttls)
	}
}

// ------------------------------------------------------ keyspace changes

func TestAKeyspaceDeleteDeletesFromTheTarget(t *testing.T) {
	source := newFakeRedis(t).on("TYPE", "+none\r\n")
	target := newFakeRedis(t).on("DEL", ":1\r\n")
	s := newRedisSyncerWithFakes(t, source, target)

	s.handleKeyspaceChange(ctxFor(t), "__keyspace@0__:user:1", "del")

	if !target.sawCommand("DEL") {
		t.Errorf("target commands = %v, want a DEL", target.seen())
	}
}

// TestTheKeyNameKeepsItsColons records that the channel is split on the first
// colon only, so a key containing colons — the usual Redis convention — arrives
// intact.
func TestTheKeyNameKeepsItsColons(t *testing.T) {
	source := newFakeRedis(t).on("TYPE", "+none\r\n")
	target := newFakeRedis(t).on("DEL", ":1\r\n")
	s := newRedisSyncerWithFakes(t, source, target)

	s.handleKeyspaceChange(ctxFor(t), "__keyspace@0__:user:1:profile", "del")

	for _, cmd := range target.seen() {
		if cmd == "DEL user:1:profile" {
			return
		}
	}
	t.Errorf("target commands = %v, want the whole key", target.seen())
}

func TestAStringSetIsCopiedWithSet(t *testing.T) {
	source := newFakeRedis(t).on("TYPE", "+string\r\n").on("GET", bulk("hello"))
	target := newFakeRedis(t).on("SET", "+OK\r\n")
	s := newRedisSyncerWithFakes(t, source, target)

	s.handleKeyspaceChange(ctxFor(t), "__keyspace@0__:greeting", "set")

	if !target.sawCommand("SET") {
		t.Errorf("target commands = %v, want a SET", target.seen())
	}
}

// TestACopiedStringLosesItsTTL records a real gap in the incremental path: the
// SET carries no expiry, so a key that had one on the source becomes permanent
// on the target. The initial-sync path preserves it; this one does not.
func TestACopiedStringLosesItsTTL(t *testing.T) {
	source := newFakeRedis(t).on("TYPE", "+string\r\n").on("GET", bulk("hello"))
	target := newFakeRedis(t).on("SET", "+OK\r\n")
	s := newRedisSyncerWithFakes(t, source, target)

	s.handleKeyspaceChange(ctxFor(t), "__keyspace@0__:greeting", "set")

	for _, cmd := range target.seen() {
		if strings.HasPrefix(cmd, "SET ") {
			if strings.Contains(strings.ToUpper(cmd), "EX") {
				t.Fatalf("the SET carries an expiry now (%q), so assert that instead", cmd)
			}
			return
		}
	}
	t.Errorf("target commands = %v, want a SET", target.seen())
}

func TestAHashSetIsCopiedWithHSet(t *testing.T) {
	source := newFakeRedis(t).on("TYPE", "+hash\r\n").
		on("HGETALL", "*2\r\n$5\r\nfield\r\n$5\r\nvalue\r\n")
	target := newFakeRedis(t).on("HSET", ":1\r\n")
	s := newRedisSyncerWithFakes(t, source, target)

	s.handleKeyspaceChange(ctxFor(t), "__keyspace@0__:user:1", "set")

	if !target.sawCommand("HSET") {
		t.Errorf("target commands = %v, want an HSET", target.seen())
	}
}

// TestAHashSetIsNotAReplacement records that HSET merges: a field deleted on the
// source stays on the target forever, because nothing ever removes it. The two
// sides drift apart silently.
func TestAHashSetIsNotAReplacement(t *testing.T) {
	source := newFakeRedis(t).on("TYPE", "+hash\r\n").
		on("HGETALL", "*2\r\n$5\r\nfield\r\n$5\r\nvalue\r\n")
	target := newFakeRedis(t).on("HSET", ":1\r\n").on("DEL", ":1\r\n")
	s := newRedisSyncerWithFakes(t, source, target)

	s.handleKeyspaceChange(ctxFor(t), "__keyspace@0__:user:1", "set")

	if target.sawCommand("DEL") {
		t.Fatal("the hash is cleared before the copy now, so assert that instead")
	}
}

// TestAnUnsupportedTypeOnSetIsDropped records the gap in the "set" branch: only
// strings and hashes are handled, so a change to a list, set, sorted set or
// stream is logged at debug level and never replicated. The key stays at
// whatever the initial sync left in the target.
func TestAnUnsupportedTypeOnSetIsDropped(t *testing.T) {
	for _, kind := range []string{"list", "set", "zset", "stream"} {
		t.Run(kind, func(t *testing.T) {
			source := newFakeRedis(t).on("TYPE", "+"+kind+"\r\n")
			target := newFakeRedis(t)
			s := newRedisSyncerWithFakes(t, source, target)

			s.handleKeyspaceChange(ctxFor(t), "__keyspace@0__:items", "set")

			if len(target.seen()) != 0 {
				t.Errorf("target commands = %v; %s appears to be replicated now, so "+
					"assert that instead", target.seen(), kind)
			}
		})
	}
}

// TestAnyOtherOperationTriggersAFullCopy records the catch-all: operations the
// switch does not name — lpush, sadd, expire, rename — fall through to a full
// DUMP and RESTORE of the key, which is correct for every type but copies the
// whole value for every single change.
func TestAnyOtherOperationTriggersAFullCopy(t *testing.T) {
	source := newFakeRedis(t).on("TYPE", "+list\r\n").
		on("TTL", ":-1\r\n").on("DUMP", bulk("payload"))
	target := newFakeRedis(t).on("RESTORE", "+OK\r\n")
	s := newRedisSyncerWithFakes(t, source, target)

	s.handleKeyspaceChange(ctxFor(t), "__keyspace@0__:items", "lpush")

	if !target.sawCommand("RESTORE") {
		t.Errorf("target commands = %v, want a full copy", target.seen())
	}
}

// TestAFullCopyFailureIsDiscardedInTheKeyspacePath records that the fall-through
// branch ignores copyFullKey's error entirely — no log line, no error flag. A
// list that cannot be copied is lost with no trace at all.
func TestAFullCopyFailureIsDiscardedInTheKeyspacePath(t *testing.T) {
	source := newFakeRedis(t).on("TYPE", "+list\r\n").on("TTL", "-ERR nope\r\n")
	s := newRedisSyncerWithFakes(t, source, newFakeRedis(t))

	s.handleKeyspaceChange(ctxFor(t), "__keyspace@0__:items", "lpush")

	if atomic.LoadInt32(&s.lastExecErr) != 0 {
		t.Fatal("the failure is recorded now, so assert that instead")
	}
}

func TestAMalformedKeyspaceChannelIsIgnored(t *testing.T) {
	source := newFakeRedis(t)
	s := newRedisSyncerWithFakes(t, source, newFakeRedis(t))

	s.handleKeyspaceChange(ctxFor(t), "no-colon-here", "set")

	if len(source.seen()) != 0 {
		t.Errorf("source commands = %v, want none", source.seen())
	}
}

func TestAKeyspaceChangeStopsWhenTheTypeCannotBeRead(t *testing.T) {
	source := newFakeRedis(t).on("TYPE", "-ERR nope\r\n")
	target := newFakeRedis(t)
	s := newRedisSyncerWithFakes(t, source, target)

	s.handleKeyspaceChange(ctxFor(t), "__keyspace@0__:user:1", "del")

	if len(target.seen()) != 0 {
		t.Errorf("target commands = %v, want none", target.seen())
	}
}

// TestTheSubscriptionOnlyWatchesDatabaseZero records the hardcoded pattern:
// "__keyspace@0__:*". A task whose source uses any other Redis database gets no
// incremental replication at all — the initial sync runs and then nothing.
func TestTheSubscriptionOnlyWatchesDatabaseZero(t *testing.T) {
	source := newFakeRedis(t).on("TYPE", "+none\r\n")
	target := newFakeRedis(t).on("DEL", ":1\r\n")
	s := newRedisSyncerWithFakes(t, source, target)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.watchKeyspaceChanges(ctx)
		close(done)
	}()

	source.publish("__keyspace@0__:user:1", "del")
	waitFor(t, func() bool { return target.sawCommand("DEL") })
	cancel()
	<-done

	for _, cmd := range source.seen() {
		if strings.HasPrefix(cmd, "PSUBSCRIBE ") {
			if cmd != "PSUBSCRIBE __keyspace@0__:*" {
				t.Errorf("subscription = %q; another database appears to be watched "+
					"now, so assert that instead", cmd)
			}
			return
		}
	}
	t.Errorf("source commands = %v, want a PSUBSCRIBE", source.seen())
}

// TestADroppedSubscriptionIsReconnectedByTheClient records that the "channel
// closed" branch in the watcher is effectively unreachable: go-redis reconnects
// a broken pub/sub connection by itself and keeps the channel open, logging
// "discarding bad PubSub connection" each time. So a source that goes away
// leaves the watcher alive and spinning through reconnects rather than
// returning — and nothing above it learns that notifications have stopped.
func TestADroppedSubscriptionIsReconnectedByTheClient(t *testing.T) {
	source := newFakeRedis(t)
	s := newRedisSyncerWithFakes(t, source, newFakeRedis(t))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.watchKeyspaceChanges(ctx)
		close(done)
	}()

	waitFor(t, func() bool { return source.sawCommand("PSUBSCRIBE") })
	source.closePubSub() // the server hangs up on the subscriber

	// The watcher is still running: only cancelling the context stops it.
	select {
	case <-done:
		t.Fatal("the watcher returned on a dropped connection; the client appears " +
			"to surface the close now, so assert that instead")
	case <-time.After(200 * time.Millisecond):
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("the watcher did not stop when the context was cancelled")
	}
}

// ---------------------------------------------------------- stream events

// TestAStreamMessageIsStoredAsAHash records the stream mapping: each message
// becomes a key named "msg:<id>" in the target, written with the type the
// *source* reports for that key — which for a new message is "none", so nothing
// is written at all.
func TestAStreamMessageIsStoredAsAHash(t *testing.T) {
	source := newFakeRedis(t).on("TYPE", "+hash\r\n")
	target := newFakeRedis(t).on("HSET", ":1\r\n")
	s := newRedisSyncerWithFakes(t, source, target)

	err := s.processStreamMessage(ctxFor(t), goredis.XMessage{
		ID:     "1-1",
		Values: map[string]interface{}{"field": "value"},
	})
	if err != nil {
		t.Fatalf("processStreamMessage: %v", err)
	}
	if !target.sawCommand("HSET") {
		t.Errorf("target commands = %v, want an HSET", target.seen())
	}
}

// TestANewStreamMessageIsRejected records the consequence of asking the source
// for the type of a key the source never had: a brand-new message reports type
// "none", which is neither string nor hash, so the message is rejected, never
// acknowledged, and the stream never advances. The key the code looks up
// ("msg:<id>") is one the *target* would hold, not the source.
func TestANewStreamMessageIsRejected(t *testing.T) {
	source := newFakeRedis(t).on("TYPE", "+none\r\n")
	target := newFakeRedis(t)
	s := newRedisSyncerWithFakes(t, source, target)

	err := s.processStreamMessage(ctxFor(t), goredis.XMessage{
		ID:     "1-1",
		Values: map[string]interface{}{"field": "value"},
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported key type") {
		t.Fatalf("err = %v; the lookup appears to be fixed now, so assert that "+
			"instead", err)
	}
	if len(target.seen()) != 0 {
		t.Errorf("target commands = %v, want none", target.seen())
	}
}

// TestTheStringStreamBranchCanNeverSucceed records a dead branch: it passes the
// whole field map to SET as the value, and go-redis refuses to marshal a
// map — "can't marshal map[string]interface {}". So a stream message whose
// target key is a string always fails, is never acknowledged, and the stream
// stops advancing. Only the hash branch works.
func TestTheStringStreamBranchCanNeverSucceed(t *testing.T) {
	source := newFakeRedis(t).on("TYPE", "+string\r\n")
	target := newFakeRedis(t).on("SET", "+OK\r\n")
	s := newRedisSyncerWithFakes(t, source, target)

	err := s.processStreamMessage(ctxFor(t), goredis.XMessage{
		ID:     "1-1",
		Values: map[string]interface{}{"field": "value"},
	})
	if err == nil {
		t.Fatal("the string branch succeeded; the value appears to be encoded " +
			"properly now, so assert that instead")
	}
	if !strings.Contains(err.Error(), "marshal") {
		t.Errorf("err = %v, want the marshalling failure", err)
	}
	if target.sawCommand("SET") {
		t.Error("a SET reached the server despite the marshalling failure")
	}
}

func TestProcessStreamMessageReportsATypeFailure(t *testing.T) {
	source := newFakeRedis(t).on("TYPE", "-ERR nope\r\n")
	s := newRedisSyncerWithFakes(t, source, newFakeRedis(t))

	if err := s.processStreamMessage(ctxFor(t), goredis.XMessage{ID: "1-1"}); err == nil {
		t.Fatal("processStreamMessage reported success")
	}
}

func TestProcessStreamMessageReportsAWriteFailure(t *testing.T) {
	source := newFakeRedis(t).on("TYPE", "+hash\r\n")
	target := newFakeRedis(t).on("HSET", "-ERR read only replica\r\n")
	s := newRedisSyncerWithFakes(t, source, target)

	if err := s.processStreamMessage(ctxFor(t), goredis.XMessage{
		ID:     "1-1",
		Values: map[string]interface{}{"f": "v"},
	}); err == nil {
		t.Fatal("processStreamMessage reported success for a rejected write")
	}
}

// waitFor polls until the condition holds, so a test does not depend on a fixed
// sleep for the stub server's round trip.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("the condition never held")
}

// ------------------------------------------------------------ initial sync

// TestTheInitialSyncWalksEveryPageOfTheKeyspace records the SCAN loop: the
// cursor is followed until it comes back to zero, and every page is copied.
func TestTheInitialSyncWalksEveryPageOfTheKeyspace(t *testing.T) {
	source := newFakeRedis(t).
		on("SCAN", "*2\r\n$1\r\n0\r\n*2\r\n$5\r\nuser1\r\n$5\r\nuser2\r\n").
		on("TTL", ":-1\r\n").
		on("DUMP", bulk("payload"))
	target := newFakeRedis(t).on("RESTORE", "+OK\r\n")
	s := newRedisSyncerWithFakes(t, source, target)

	if err := s.doInitialSync(ctxFor(t)); err != nil {
		t.Fatalf("doInitialSync: %v", err)
	}

	restores := 0
	for _, cmd := range target.seen() {
		if strings.HasPrefix(cmd, "RESTORE ") {
			restores++
		}
	}
	if restores != 2 {
		t.Errorf("%d keys restored, want 2", restores)
	}
}

// TestTheInitialSyncScansEverything records that the pattern is "*" and the
// batch size 100: there is no way to sync a subset of the keyspace, so the
// mapping's table list is ignored for the initial copy.
func TestTheInitialSyncScansEverything(t *testing.T) {
	source := newFakeRedis(t).on("SCAN", "*2\r\n$1\r\n0\r\n*0\r\n")
	s := newRedisSyncerWithFakes(t, source, newFakeRedis(t))

	if err := s.doInitialSync(ctxFor(t)); err != nil {
		t.Fatalf("doInitialSync: %v", err)
	}

	for _, cmd := range source.seen() {
		if strings.HasPrefix(cmd, "SCAN ") {
			if !strings.Contains(cmd, "match *") && !strings.Contains(cmd, "MATCH *") {
				t.Errorf("scan = %q, want a match-everything pattern", cmd)
			}
			return
		}
	}
	t.Errorf("source commands = %v, want a SCAN", source.seen())
}

func TestTheInitialSyncReportsAScanFailure(t *testing.T) {
	source := newFakeRedis(t).on("SCAN", "-ERR nope\r\n")
	s := newRedisSyncerWithFakes(t, source, newFakeRedis(t))

	if err := s.doInitialSync(ctxFor(t)); err == nil ||
		!strings.Contains(err.Error(), "SCAN fail") {
		t.Fatalf("err = %v, want a scan failure", err)
	}
}

// ---------------------------------------------------------- stream loop

// TestTheStreamLoopAcknowledgesWhatItApplies records the happy path of the
// stream watcher: a message read from the group is written to the target,
// acknowledged, and its id saved as the new position.
func TestTheStreamLoopAcknowledgesWhatItApplies(t *testing.T) {
	source := newFakeRedis(t).
		on("XREADGROUP", "*1\r\n*2\r\n$6\r\nevents\r\n*1\r\n*2\r\n$3\r\n1-1\r\n"+
			"*2\r\n$5\r\nfield\r\n$5\r\nvalue\r\n").
		on("TYPE", "+hash\r\n").
		on("XACK", ":1\r\n")
	target := newFakeRedis(t).on("HSET", ":1\r\n")
	s := newRedisSyncerWithFakes(t, source, target)
	s.positionPath = filepath.Join(t.TempDir(), "redis.pos")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.watchStreamChanges(ctx, "events", "sync_group", "0-0")
		close(done)
	}()

	waitFor(t, func() bool { return source.sawCommand("XACK") })
	cancel()
	<-done

	if got := s.loadStreamPosition(); got != "1-1" {
		t.Errorf("position = %q, want the acknowledged id", got)
	}
}

// TestAnUnappliedStreamMessageIsNotAcknowledged records the other side: a
// message the target rejects is left unacknowledged, so it is redelivered — but
// the loop reads with the same lastID every time, so it also keeps failing on
// the same message, and the error flag is the only trace.
func TestAnUnappliedStreamMessageIsNotAcknowledged(t *testing.T) {
	source := newFakeRedis(t).
		on("XREADGROUP", "*1\r\n*2\r\n$6\r\nevents\r\n*1\r\n*2\r\n$3\r\n1-1\r\n"+
					"*2\r\n$5\r\nfield\r\n$5\r\nvalue\r\n").
		on("TYPE", "+none\r\n") // neither string nor hash, so the write is refused
	s := newRedisSyncerWithFakes(t, source, newFakeRedis(t))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.watchStreamChanges(ctx, "events", "sync_group", "0-0")
		close(done)
	}()

	waitFor(t, func() bool { return atomic.LoadInt32(&s.lastExecErr) == 1 })
	cancel()
	<-done

	if source.sawCommand("XACK") {
		t.Error("a message that was not applied was acknowledged anyway")
	}
}

// TestTheStreamLoopKeepsGoingAfterAReadFailure records that an XREADGROUP error
// is logged and retried immediately — with no backoff, so an unreachable source
// spins the loop as fast as the dial fails.
func TestTheStreamLoopKeepsGoingAfterAReadFailure(t *testing.T) {
	source := newFakeRedis(t).on("XREADGROUP", "-NOGROUP no such group\r\n")
	s := newRedisSyncerWithFakes(t, source, newFakeRedis(t))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.watchStreamChanges(ctx, "events", "sync_group", "0-0")
		close(done)
	}()

	waitFor(t, func() bool {
		reads := 0
		for _, cmd := range source.seen() {
			if strings.HasPrefix(cmd, "XREADGROUP") {
				reads++
			}
		}
		return reads > 2
	})
	cancel()
	<-done
}

// ---------------------------------------------------------------- start

// TestStartRunsTheWholeSequence drives the entry point against stub servers:
// initial sync, keyspace subscription, consumer group creation and the stream
// loop, all stopped by cancelling the context.
func TestStartRunsTheWholeSequence(t *testing.T) {
	source := newFakeRedis(t).
		on("SCAN", "*2\r\n$1\r\n0\r\n*1\r\n$5\r\nuser1\r\n").
		on("TTL", ":-1\r\n").
		on("DUMP", bulk("payload")).
		on("XGROUP", "+OK\r\n").
		on("XREADGROUP", "*0\r\n")
	target := newFakeRedis(t).on("RESTORE", "+OK\r\n")

	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	cfg := sampleConfig()
	cfg.SourceConnection = "redis://" + source.listener.Addr().String() + "/0"
	cfg.TargetConnection = "redis://" + target.listener.Addr().String() + "/0"
	cfg.Mappings = []config.DatabaseMapping{{
		Tables: []config.TableMapping{{SourceTable: "events", TargetTable: "events"}},
	}}
	s := NewRedisSyncer(cfg, logger)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.Start(ctx)
		close(done)
	}()

	waitFor(t, func() bool { return source.sawCommand("XREADGROUP") })
	cancel()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return after the context was cancelled")
	}

	if !target.sawCommand("RESTORE") {
		t.Errorf("the initial sync did not copy anything: %v", target.seen())
	}
	if !source.sawCommand("XGROUP") {
		t.Errorf("no consumer group was created: %v", source.seen())
	}
}

// TestStartPanicsWithNoMappings records that the stream name is read as
// cfg.Mappings[0].Tables[0].SourceTable with no bounds check, so a Redis task
// saved without a table mapping takes the whole process down as soon as it
// starts — after the initial sync has already run.
func TestStartPanicsWithNoMappings(t *testing.T) {
	source := newFakeRedis(t).on("SCAN", "*2\r\n$1\r\n0\r\n*0\r\n")
	target := newFakeRedis(t)

	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	cfg := sampleConfig()
	cfg.SourceConnection = "redis://" + source.listener.Addr().String() + "/0"
	cfg.TargetConnection = "redis://" + target.listener.Addr().String() + "/0"
	s := NewRedisSyncer(cfg, logger)

	recovered := make(chan interface{}, 1)
	go func() {
		defer func() { recovered <- recover() }()
		s.Start(context.Background())
	}()

	select {
	case r := <-recovered:
		if r == nil {
			t.Error("Start returned without panicking; the empty mapping appears to " +
				"be handled now, so assert that instead")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Start neither panicked nor returned")
	}
}

// The matching test for an unreachable source — Start's connection retry budget
// is five attempts with exponential backoff, 62 seconds in total, and the loop
// never consults the context — lives behind the integration tag, because it
// cannot be shortened from outside.
