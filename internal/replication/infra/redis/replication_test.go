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

// TestAChangedKeyIsCopiedWholeWithItsTTL pins the incremental path. It used to
// rebuild the value from its type — GET then SET for a string, HGETALL then
// HSET for a hash — which dropped the expiry, merged rather than replaced a
// hash, and silently ignored every other type. A DUMP and RESTORE copies the
// value byte for byte with its expiry, whatever the type is.
func TestAChangedKeyIsCopiedWholeWithItsTTL(t *testing.T) {
	source := newFakeRedis(t).on("TTL", ":60\r\n").on("DUMP", bulk("payload"))
	target := newFakeRedis(t).on("RESTORE", "+OK\r\n")
	s := newRedisSyncerWithFakes(t, source, target)

	s.handleKeyspaceChange(ctxFor(t), "__keyspace@0__:greeting", "set")

	for _, cmd := range target.seen() {
		if strings.HasPrefix(cmd, "RESTORE ") {
			if !strings.Contains(cmd, "60000") {
				t.Errorf("restore = %q, want the source's expiry in milliseconds", cmd)
			}
			return
		}
	}
	t.Errorf("target commands = %v, want a RESTORE", target.seen())
}

// TestEveryTypeIsReplicatedOnAChange covers the types the old type-by-type
// branch dropped without a word: a change to a list, set, sorted set or stream
// left the target holding whatever the initial copy had put there.
func TestEveryTypeIsReplicatedOnAChange(t *testing.T) {
	for _, op := range []string{"set", "hset", "lpush", "sadd", "zadd", "xadd", "rename_from"} {
		t.Run(op, func(t *testing.T) {
			source := newFakeRedis(t).on("TTL", ":-1\r\n").on("DUMP", bulk("payload"))
			target := newFakeRedis(t).on("RESTORE", "+OK\r\n")
			s := newRedisSyncerWithFakes(t, source, target)

			s.handleKeyspaceChange(ctxFor(t), "__keyspace@0__:items", op)

			if !target.sawCommand("RESTORE") {
				t.Errorf("target commands = %v, want a full copy", target.seen())
			}
		})
	}
}

// TestARestoreReplacesRatherThanMerges is what makes a removed hash field
// disappear from the target too. HSET merged, so a field deleted at the source
// stayed on the target for good and the two sides drifted apart silently.
func TestARestoreReplacesRatherThanMerges(t *testing.T) {
	source := newFakeRedis(t).on("TTL", ":-1\r\n").on("DUMP", bulk("payload"))
	target := newFakeRedis(t).on("RESTORE", "+OK\r\n")
	s := newRedisSyncerWithFakes(t, source, target)

	s.handleKeyspaceChange(ctxFor(t), "__keyspace@0__:user:1", "hset")

	for _, cmd := range target.seen() {
		if strings.HasPrefix(cmd, "RESTORE ") {
			if !strings.Contains(strings.ToUpper(cmd), "REPLACE") {
				t.Errorf("restore = %q, want REPLACE so the old value is not merged", cmd)
			}
			return
		}
	}
	t.Errorf("target commands = %v, want a RESTORE", target.seen())
}

// TestAnExpiryRemovesTheKeyFromTheTarget covers the notifications Redis sends
// when a key goes away by itself. They used to fall through to a full copy of a
// key that no longer exists, which did nothing, so the target kept it.
func TestAnExpiryRemovesTheKeyFromTheTarget(t *testing.T) {
	for _, op := range []string{"del", "expired", "evicted"} {
		t.Run(op, func(t *testing.T) {
			target := newFakeRedis(t).on("DEL", ":1\r\n")
			s := newRedisSyncerWithFakes(t, newFakeRedis(t), target)

			s.handleKeyspaceChange(ctxFor(t), "__keyspace@0__:items", op)

			if !target.sawCommand("DEL") {
				t.Errorf("target commands = %v, want a DEL", target.seen())
			}
		})
	}
}

// TestAFullCopyFailureIsRecorded closes the gap where the fall-through branch
// discarded copyFullKey's error entirely — no log line, no flag, no trace that
// a key had been lost.
func TestAFullCopyFailureIsRecorded(t *testing.T) {
	source := newFakeRedis(t).on("TTL", "-ERR nope\r\n")
	s := newRedisSyncerWithFakes(t, source, newFakeRedis(t))

	s.handleKeyspaceChange(ctxFor(t), "__keyspace@0__:items", "lpush")

	if atomic.LoadInt32(&s.lastExecErr) != 1 {
		t.Error("a key that could not be copied left no trace")
	}
}

func TestAFailedDeleteIsRecorded(t *testing.T) {
	target := newFakeRedis(t).on("DEL", "-ERR read only replica\r\n")
	s := newRedisSyncerWithFakes(t, newFakeRedis(t), target)

	s.handleKeyspaceChange(ctxFor(t), "__keyspace@0__:items", "del")

	if atomic.LoadInt32(&s.lastExecErr) != 1 {
		t.Error("a delete the target refused left no trace")
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

// ------------------------------------------------------ subscription

// TestTheSubscriptionFollowsTheConfiguredDatabase is the fix for a hardcoded
// "__keyspace@0__:*". Keyspace notifications are published per database, so a
// task whose source used any other one got no incremental replication at all:
// the initial copy ran and then nothing happened, with no error anywhere.
func TestTheSubscriptionFollowsTheConfiguredDatabase(t *testing.T) {
	source := newFakeRedis(t)
	s := newRedisSyncerWithFakes(t, source, newFakeRedis(t))
	s.cfg.SourceConnection = "redis://127.0.0.1:6379/3"

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.watchKeyspaceChanges(ctx)
		close(done)
	}()

	waitFor(t, func() bool { return source.sawCommand("PSUBSCRIBE") })
	cancel()
	<-done

	for _, cmd := range source.seen() {
		if strings.HasPrefix(cmd, "PSUBSCRIBE ") {
			if cmd != "PSUBSCRIBE __keyspace@3__:*" {
				t.Errorf("subscription = %q, want database 3", cmd)
			}
			return
		}
	}
	t.Errorf("source commands = %v, want a PSUBSCRIBE", source.seen())
}

func TestADSNWithNoDatabaseWatchesZero(t *testing.T) {
	source := newFakeRedis(t)
	s := newRedisSyncerWithFakes(t, source, newFakeRedis(t))
	s.cfg.SourceConnection = "redis://127.0.0.1:6379"

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.watchKeyspaceChanges(ctx)
		close(done)
	}()

	waitFor(t, func() bool { return source.sawCommand("PSUBSCRIBE") })
	cancel()
	<-done

	for _, cmd := range source.seen() {
		if strings.HasPrefix(cmd, "PSUBSCRIBE ") {
			if cmd != "PSUBSCRIBE __keyspace@0__:*" {
				t.Errorf("subscription = %q, want database 0", cmd)
			}
			return
		}
	}
	t.Errorf("source commands = %v, want a PSUBSCRIBE", source.seen())
}

// TestADroppedSubscriptionIsReconnectedByTheClient records that the "channel
// closed" branch in the watcher is effectively unreachable: go-redis reconnects
// a broken pub/sub connection by itself and keeps the channel open. A source
// that goes away leaves the watcher alive and spinning through reconnects
// rather than returning — which is why the periodic comparison exists.
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

// ------------------------------------------ keyspace notification check

// TestASourceThatPublishesNothingIsReported covers the configuration that makes
// incremental replication silently empty: the subscription succeeds whether or
// not the server publishes anything, so without this check a source with
// notify-keyspace-events unset looks exactly like a source with no traffic.
// Memorystore leaves it unset by default.
func TestASourceThatPublishesNothingIsReported(t *testing.T) {
	source := newFakeRedis(t).on("CONFIG", "*2\r\n$22\r\nnotify-keyspace-events\r\n$0\r\n\r\n")
	s := newRedisSyncerWithFakes(t, source, newFakeRedis(t))

	s.checkKeyspaceNotifications(ctxFor(t))

	if !source.sawCommand("CONFIG") {
		t.Errorf("source commands = %v, want the setting to be read", source.seen())
	}
}

func TestAConfiguredSourceIsAccepted(t *testing.T) {
	source := newFakeRedis(t).on("CONFIG", "*2\r\n$22\r\nnotify-keyspace-events\r\n$3\r\nKEA\r\n")
	s := newRedisSyncerWithFakes(t, source, newFakeRedis(t))

	s.checkKeyspaceNotifications(ctxFor(t)) // must not panic or block
}

// TestAServerThatRefusesConfigGetIsTolerated covers the managed services that
// do not allow CONFIG GET at all: the check is advisory, so it must not stop
// the task.
func TestAServerThatRefusesConfigGetIsTolerated(t *testing.T) {
	source := newFakeRedis(t).on("CONFIG", "-ERR unknown command\r\n")
	s := newRedisSyncerWithFakes(t, source, newFakeRedis(t))

	s.checkKeyspaceNotifications(ctxFor(t))
}

// ---------------------------------------------------------- stream events

// TestAStreamEntryIsAppendedToTheTargetStream is the whole point of the stream
// path. It used to write the entry into a hash called msg:<id> on the target,
// and to decide how by asking the *source* for the type of that hash — a key
// the source has never had. The type came back "none", the write was refused,
// the entry was never acknowledged, and the reader read it forever.
func TestAStreamEntryIsAppendedToTheTargetStream(t *testing.T) {
	target := newFakeRedis(t).on("XADD", bulk("1-1"))
	s := newRedisSyncerWithFakes(t, newFakeRedis(t), target)

	err := s.processStreamMessage(ctxFor(t), "events_copy", goredis.XMessage{
		ID:     "1-1",
		Values: map[string]interface{}{"field": "value"},
	})
	if err != nil {
		t.Fatalf("processStreamMessage: %v", err)
	}

	for _, cmd := range target.seen() {
		if strings.HasPrefix(cmd, "XADD ") {
			if !strings.Contains(cmd, "events_copy") {
				t.Errorf("xadd = %q, want the target stream", cmd)
			}
			if !strings.Contains(cmd, "1-1") {
				t.Errorf("xadd = %q, want the source identifier kept", cmd)
			}
			return
		}
	}
	t.Errorf("target commands = %v, want an XADD", target.seen())
}

// TestAReplayedEntryIsTreatedAsApplied is what lets the reader move on after a
// restart. Redis refuses an identifier that is not greater than the stream's
// last one, and that refusal means the entry is already there.
func TestAReplayedEntryIsTreatedAsApplied(t *testing.T) {
	target := newFakeRedis(t).on("XADD",
		"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
	s := newRedisSyncerWithFakes(t, newFakeRedis(t), target)

	err := s.processStreamMessage(ctxFor(t), "events", goredis.XMessage{ID: "1-1"})
	if err != nil {
		t.Errorf("processStreamMessage: %v; a replayed entry must not stop the reader", err)
	}
}

func TestProcessStreamMessageReportsAWriteFailure(t *testing.T) {
	target := newFakeRedis(t).on("XADD", "-ERR read only replica\r\n")
	s := newRedisSyncerWithFakes(t, newFakeRedis(t), target)

	if err := s.processStreamMessage(ctxFor(t), "events", goredis.XMessage{
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

// ---------------------------------------------------------- reconciliation

// TestReconciliationCopiesTheSourceOver is the safety net under the keyspace
// subscription. Redis publishes notifications with no acknowledgement and no
// replay, so anything published while the subscriber is reconnecting is gone
// and nothing reports it; only a full comparison brings the target back.
func TestReconciliationCopiesTheSourceOver(t *testing.T) {
	source := newFakeRedis(t).
		on("SCAN", "*2\r\n$1\r\n0\r\n*1\r\n$5\r\nuser1\r\n").
		on("TTL", ":-1\r\n").
		on("DUMP", bulk("payload")).
		on("EXISTS", ":1\r\n")
	target := newFakeRedis(t).
		on("SCAN", "*2\r\n$1\r\n0\r\n*1\r\n$5\r\nuser1\r\n").
		on("RESTORE", "+OK\r\n")
	s := newRedisSyncerWithFakes(t, source, target)

	s.reconcile(ctxFor(t))

	if !target.sawCommand("RESTORE") {
		t.Errorf("target commands = %v, want the source copied over", target.seen())
	}
}

// TestReconciliationRemovesWhatTheSourceNoLongerHas is how a delete lost with a
// dropped subscription is corrected.
func TestReconciliationRemovesWhatTheSourceNoLongerHas(t *testing.T) {
	source := newFakeRedis(t).
		on("SCAN", "*2\r\n$1\r\n0\r\n*0\r\n").
		on("EXISTS", ":0\r\n")
	target := newFakeRedis(t).
		on("SCAN", "*2\r\n$1\r\n0\r\n*1\r\n$5\r\nstale\r\n").
		on("DEL", ":1\r\n")
	s := newRedisSyncerWithFakes(t, source, target)

	s.reconcile(ctxFor(t))

	for _, cmd := range target.seen() {
		if cmd == "DEL stale" {
			return
		}
	}
	t.Errorf("target commands = %v, want the stale key removed", target.seen())
}

// TestReconciliationKeepsWhatTheSourceStillHas is the other half: a key present
// on both sides must survive the comparison.
func TestReconciliationKeepsWhatTheSourceStillHas(t *testing.T) {
	source := newFakeRedis(t).
		on("SCAN", "*2\r\n$1\r\n0\r\n*0\r\n").
		on("EXISTS", ":1\r\n")
	target := newFakeRedis(t).
		on("SCAN", "*2\r\n$1\r\n0\r\n*1\r\n$5\r\nalive\r\n").
		on("DEL", ":1\r\n")
	s := newRedisSyncerWithFakes(t, source, target)

	s.reconcile(ctxFor(t))

	if target.sawCommand("DEL") {
		t.Errorf("target commands = %v; a key the source still holds was removed", target.seen())
	}
}

func TestReconciliationIsSkippedWhenDisabled(t *testing.T) {
	source := newFakeRedis(t)
	s := newRedisSyncerWithFakes(t, source, newFakeRedis(t))
	s.reconcileEvery = -1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.reconcileLoop(ctx) // returns at once rather than blocking on a ticker

	if len(source.seen()) != 0 {
		t.Errorf("source commands = %v, want none", source.seen())
	}
}

func TestTheDefaultReconcileIntervalIsApplied(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)

	if got := NewRedisSyncer(sampleConfig(), logger).reconcileEvery; got != defaultReconcileInterval {
		t.Errorf("reconcileEvery = %v, want %v", got, defaultReconcileInterval)
	}

	cfg := sampleConfig()
	cfg.RedisReconcileInterval = 5 * time.Minute
	if got := NewRedisSyncer(cfg, logger).reconcileEvery; got != 5*time.Minute {
		t.Errorf("reconcileEvery = %v, want the configured 5m", got)
	}

	cfg.RedisReconcileInterval = -1
	if got := NewRedisSyncer(cfg, logger).reconcileEvery; got != 0 {
		t.Errorf("reconcileEvery = %v, want it turned off", got)
	}
}

// ---------------------------------------------------------- stream loop

// TestTheStreamLoopAcknowledgesWhatItApplies records the happy path of the
// stream watcher: an entry read from the group is appended to the target
// stream, acknowledged, and its id saved as the new position.
func TestTheStreamLoopAcknowledgesWhatItApplies(t *testing.T) {
	source := newFakeRedis(t).
		on("XREADGROUP", "*1\r\n*2\r\n$6\r\nevents\r\n*1\r\n*2\r\n$3\r\n1-1\r\n"+
			"*2\r\n$5\r\nfield\r\n$5\r\nvalue\r\n").
		on("XACK", ":1\r\n")
	target := newFakeRedis(t).on("XADD", bulk("1-1"))
	s := newRedisSyncerWithFakes(t, source, target)
	s.positionPath = filepath.Join(t.TempDir(), "redis.pos")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.watchStreamChanges(ctx, streamPair{source: "events", target: "events"}, "sync_group", "0-0")
		close(done)
	}()

	waitFor(t, func() bool { return source.sawCommand("XACK") })
	cancel()
	<-done

	if got := s.loadStreamPosition("events"); got != "1-1" {
		t.Errorf("position = %q, want the acknowledged id", got)
	}
}

// TestTheStreamLoopReadsUndeliveredEntries pins the ">" identifier. Reading with
// the stored id instead returns the consumer's already-delivered backlog, so a
// group that had nothing pending read the same empty history forever and never
// saw a new entry.
func TestTheStreamLoopReadsUndeliveredEntries(t *testing.T) {
	source := newFakeRedis(t).on("XREADGROUP", "*0\r\n")
	s := newRedisSyncerWithFakes(t, source, newFakeRedis(t))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.watchStreamChanges(ctx, streamPair{source: "events", target: "events"}, "sync_group", "5-5")
		close(done)
	}()

	waitFor(t, func() bool { return source.sawCommand("XREADGROUP") })
	cancel()
	<-done

	for _, cmd := range source.seen() {
		if strings.HasPrefix(cmd, "XREADGROUP") {
			if !strings.Contains(cmd, ">") {
				t.Errorf("read = %q, want the undelivered-entries identifier", cmd)
			}
			return
		}
	}
}

// TestAnUnappliedStreamEntryIsNotAcknowledged records the other side: an entry
// the target rejects is left unacknowledged, so it is redelivered.
func TestAnUnappliedStreamEntryIsNotAcknowledged(t *testing.T) {
	source := newFakeRedis(t).
		on("XREADGROUP", "*1\r\n*2\r\n$6\r\nevents\r\n*1\r\n*2\r\n$3\r\n1-1\r\n"+
			"*2\r\n$5\r\nfield\r\n$5\r\nvalue\r\n")
	target := newFakeRedis(t).on("XADD", "-ERR read only replica\r\n")
	s := newRedisSyncerWithFakes(t, source, target)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.watchStreamChanges(ctx, streamPair{source: "events", target: "events"}, "sync_group", "0-0")
		close(done)
	}()

	waitFor(t, func() bool { return atomic.LoadInt32(&s.lastExecErr) == 1 })
	cancel()
	<-done

	if source.sawCommand("XACK") {
		t.Error("an entry that was not applied was acknowledged anyway")
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
		s.watchStreamChanges(ctx, streamPair{source: "events", target: "events"}, "sync_group", "0-0")
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

// ------------------------------------------------------ stream mappings

func TestTheStreamMappingsComeFromTheConfiguration(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	cfg := sampleConfig()
	cfg.Mappings = []config.DatabaseMapping{{
		Tables: []config.TableMapping{
			{SourceTable: "events", TargetTable: "events_copy"},
			{SourceTable: "audit"}, // no target named, so the source name is reused
			{},                     // the empty entry the loader inserts
		},
	}}

	got := NewRedisSyncer(cfg, logger).streamMappings()

	want := []streamPair{{"events", "events_copy"}, {"audit", "audit"}}
	if len(got) != len(want) {
		t.Fatalf("mappings = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("mapping %d = %v, want %v", i, got[i], want[i])
		}
	}
}

// TestNoMappingsMeansNoStreams is the fix for a panic. The stream name was read
// as cfg.Mappings[0].Tables[0].SourceTable with no bounds check, and the
// configuration loader inserts a mapping with an empty table list when a task
// has none — so the index was out of range for every Redis task saved without
// tables, and the panic took the whole syncer process down.
func TestNoMappingsMeansNoStreams(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)

	for _, mappings := range [][]config.DatabaseMapping{
		nil,
		{},
		{{Tables: []config.TableMapping{}}},
		{{Tables: []config.TableMapping{{}}}},
	} {
		cfg := sampleConfig()
		cfg.Mappings = mappings
		if got := NewRedisSyncer(cfg, logger).streamMappings(); len(got) != 0 {
			t.Errorf("mappings = %v, want none", got)
		}
	}
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
		on("XREADGROUP", "*0\r\n").
		on("HGETALL", "*0\r\n").
		on("HSET", ":1\r\n")
	target := newFakeRedis(t).on("RESTORE", "+OK\r\n").
		on("HGETALL", "*0\r\n").
		on("HSET", ":1\r\n")

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
		_ = s.Start(ctx)
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

// TestStartWithNoMappingsReplicatesTheKeyspace is the same task that used to
// panic on startup. It now runs the keyspace path and says so.
func TestStartWithNoMappingsReplicatesTheKeyspace(t *testing.T) {
	source := newFakeRedis(t).on("SCAN", "*2\r\n$1\r\n0\r\n*0\r\n").
		on("HGETALL", "*0\r\n").on("HSET", ":1\r\n")
	target := newFakeRedis(t).on("HGETALL", "*0\r\n").on("HSET", ":1\r\n")

	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	cfg := sampleConfig()
	cfg.SourceConnection = "redis://" + source.listener.Addr().String() + "/0"
	cfg.TargetConnection = "redis://" + target.listener.Addr().String() + "/0"
	s := NewRedisSyncer(cfg, logger)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan interface{}, 1)
	go func() {
		defer func() { done <- recover() }()
		_ = s.Start(ctx)
	}()

	waitFor(t, func() bool { return source.sawCommand("PSUBSCRIBE") })
	cancel()

	select {
	case r := <-done:
		if r != nil {
			t.Fatalf("Start panicked on a task with no table mapping: %v", r)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return after the context was cancelled")
	}
}

// TestStartRefusesAPromotedTarget is the direction lock seen from the entry
// point. A target that is being replicated out of is what a promoted replica
// looks like: writing to it would overwrite everything written since the
// promotion, so the task refuses to start rather than catching up.
func TestStartRefusesAPromotedTarget(t *testing.T) {
	claim := `{"task_id":9,"role":"source","peer":"elsewhere","owner":"syncer-tokyo-0",` +
		`"updated_at":"` + time.Now().UTC().Format(time.RFC3339) + `"}`

	source := newFakeRedis(t).on("HGETALL", "*0\r\n").on("HSET", ":1\r\n").
		on("SCAN", "*2\r\n$1\r\n0\r\n*0\r\n")
	target := newFakeRedis(t).
		on("HGETALL", "*2\r\n$1\r\n9\r\n"+bulk(claim)).
		on("HSET", ":1\r\n")

	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	cfg := sampleConfig()
	cfg.SourceConnection = "redis://" + source.listener.Addr().String() + "/0"
	cfg.TargetConnection = "redis://" + target.listener.Addr().String() + "/0"
	s := NewRedisSyncer(cfg, logger)

	done := make(chan struct{})
	go func() {
		_ = s.Start(context.Background())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not refuse a target that is being replicated out of")
	}

	if source.sawCommand("SCAN") {
		t.Error("the initial copy ran despite the refusal")
	}
}

// ------------------------------------------- cluster master rediscovery

// TestASubscriptionIsStartedForEachMaster pins the reconciliation: a cluster
// publishes keyspace events on the node that owns the key, so every master needs
// its own subscription.
func TestASubscriptionIsStartedForEachMaster(t *testing.T) {
	s := newRedisSyncerWithFakes(t, newFakeRedis(t), newFakeRedis(t))
	running := map[string]context.CancelFunc{}
	var started []string

	s.reconcileSubscriptions(context.Background(), running,
		[]string{"a:6379", "b:6379"},
		func(_ context.Context, addr string) { started = append(started, addr) })

	if len(started) != 2 || len(running) != 2 {
		t.Errorf("started %v, tracking %d", started, len(running))
	}
}

// TestAnExistingSubscriptionIsNotRestarted keeps the common tick cheap: nothing
// changed, so nothing happens.
func TestAnExistingSubscriptionIsNotRestarted(t *testing.T) {
	s := newRedisSyncerWithFakes(t, newFakeRedis(t), newFakeRedis(t))
	running := map[string]context.CancelFunc{}
	var started []string
	record := func(_ context.Context, addr string) { started = append(started, addr) }

	s.reconcileSubscriptions(context.Background(), running, []string{"a:6379"}, record)
	s.reconcileSubscriptions(context.Background(), running, []string{"a:6379"}, record)

	if len(started) != 1 {
		t.Errorf("started %v, want one subscription", started)
	}
}

// TestAFailedOverMasterIsReplaced is the fix. go-redis reconnects a dropped
// subscription to the same address, which after a failover serves a replica or
// nothing: the subscription stays open and silent, and the events for that slot
// range simply stop arriving with no error anywhere.
func TestAFailedOverMasterIsReplaced(t *testing.T) {
	s := newRedisSyncerWithFakes(t, newFakeRedis(t), newFakeRedis(t))
	running := map[string]context.CancelFunc{}

	stopped := make(chan string, 4)
	start := func(ctx context.Context, addr string) {
		go func() {
			<-ctx.Done()
			stopped <- addr
		}()
	}

	s.reconcileSubscriptions(context.Background(), running, []string{"a:6379", "b:6379"}, start)
	// b has been replaced by c.
	s.reconcileSubscriptions(context.Background(), running, []string{"a:6379", "c:6379"}, start)

	if _, ok := running["b:6379"]; ok {
		t.Error("the replaced master is still tracked")
	}
	if _, ok := running["c:6379"]; !ok {
		t.Error("the new master has no subscription")
	}
	select {
	case addr := <-stopped:
		if addr != "b:6379" {
			t.Errorf("stopped %q, want the replaced master", addr)
		}
	case <-time.After(2 * time.Second):
		t.Error("the replaced master's subscription was not cancelled")
	}
}

// TestEveryMasterLeavingStopsEverySubscription covers the whole cluster becoming
// unreachable: nothing is left running on an address that no longer serves.
func TestEveryMasterLeavingStopsEverySubscription(t *testing.T) {
	s := newRedisSyncerWithFakes(t, newFakeRedis(t), newFakeRedis(t))
	running := map[string]context.CancelFunc{}
	start := func(context.Context, string) {}

	s.reconcileSubscriptions(context.Background(), running, []string{"a:6379", "b:6379"}, start)
	s.reconcileSubscriptions(context.Background(), running, nil, start)

	if len(running) != 0 {
		t.Errorf("%d subscriptions are still tracked", len(running))
	}
}

// TestTheRediscoveryIntervalIsShorterThanTheReconciliation records the ordering
// that makes the recovery point tolerable: a failover is noticed in seconds,
// long before the hourly full comparison would have papered over it.
func TestTheRediscoveryIntervalIsShorterThanTheReconciliation(t *testing.T) {
	if masterRediscoveryInterval >= defaultReconcileInterval {
		t.Errorf("masters are rediscovered every %v but the keyspace is compared "+
			"every %v, so a failover would be corrected by the comparison rather "+
			"than by resubscribing", masterRediscoveryInterval, defaultReconcileInterval)
	}
}

// clusterOf builds a cluster client whose topology is a fixed set of masters,
// each of them the stub server. The topology is supplied rather than discovered
// so the stub does not have to answer CLUSTER SLOTS.
func clusterOf(t *testing.T, f *fakeRedis, masters int) *goredis.ClusterClient {
	t.Helper()

	addr := f.listener.Addr().String()
	addrs := make([]string, 0, masters)
	for i := 0; i < masters; i++ {
		addrs = append(addrs, addr)
	}

	cluster := goredis.NewClusterClient(&goredis.ClusterOptions{
		Addrs:    addrs,
		Username: "syncer",
		Password: "p4ss",
		ClusterSlots: func(context.Context) ([]goredis.ClusterSlot, error) {
			slots := make([]goredis.ClusterSlot, 0, masters)
			width := 16384 / masters
			for i := 0; i < masters; i++ {
				end := (i+1)*width - 1
				if i == masters-1 {
					end = 16383
				}
				slots = append(slots, goredis.ClusterSlot{
					Start: i * width, End: end,
					Nodes: []goredis.ClusterNode{{Addr: addr}},
				})
			}
			return slots, nil
		},
	})
	t.Cleanup(func() { _ = cluster.Close() })
	return cluster
}

// TestTheMastersAreReadFromTheCluster covers the lookup the rediscovery tick
// depends on: the addresses have to come from the cluster each time, because the
// set of them is exactly what changes after a failover.
func TestTheMastersAreReadFromTheCluster(t *testing.T) {
	f := newFakeRedis(t)
	cluster := clusterOf(t, f, 1)

	masters, err := clusterMasters(ctxFor(t), cluster)
	if err != nil {
		t.Fatalf("clusterMasters: %v", err)
	}
	if len(masters) != 1 || masters[0] != f.listener.Addr().String() {
		t.Errorf("masters = %v, want the stub's address", masters)
	}
}

// TestANodeClientCarriesTheClustersCredentials matters because a subscription
// opened without them authenticates as nobody, and a managed Redis refuses it.
func TestANodeClientCarriesTheClustersCredentials(t *testing.T) {
	f := newFakeRedis(t)
	s := newRedisSyncerWithFakes(t, f, newFakeRedis(t))
	cluster := clusterOf(t, f, 1)

	client := s.nodeClient(cluster, "shard-2:6379")
	defer client.Close()

	opts := client.Options()
	if opts.Addr != "shard-2:6379" {
		t.Errorf("Addr = %q", opts.Addr)
	}
	if opts.Username != "syncer" || opts.Password != "p4ss" {
		t.Errorf("credentials = %q/%q", opts.Username, opts.Password)
	}
}

// TestAClusterIsSubscribedNodeByNode is the whole cluster path end to end: one
// subscription per master rather than one for the cluster, which would only ever
// receive the events of whichever node the client happened to pick.
func TestAClusterIsSubscribedNodeByNode(t *testing.T) {
	f := newFakeRedis(t).on("AUTH", "+OK\r\n")
	s := newRedisSyncerWithFakes(t, f, newFakeRedis(t))
	s.source = clusterOf(t, f, 1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.watchKeyspaceChanges(ctx)
	}()

	deadline := time.After(5 * time.Second)
	for !f.sawCommand("PSUBSCRIBE") {
		select {
		case <-deadline:
			t.Fatalf("no subscription was opened; commands = %v", f.seen())
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("watchKeyspaceChanges did not return when the context was cancelled")
	}
}
