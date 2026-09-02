package redis

import (
	"context"
	"net"
	"strings"
	"testing"

	goredis "github.com/redis/go-redis/v9"
	"github.com/retail-ai-inc/sync/internal/platform/config"
)

// TestAFailoverIsReportedAsAMovedMaster covers the harmless change: the slots
// stayed where they were, so the keys did too, and the position still applies.
func TestAFailoverIsReportedAsAMovedMaster(t *testing.T) {
	before := map[string]string{"0-5460": "a:6379", "5461-10922": "b:6379"}
	after := map[string]string{"0-5460": "c:6379", "5461-10922": "b:6379"}

	got := describe(before, after)
	if !strings.Contains(got, "0-5460 moved from a:6379 to c:6379") {
		t.Errorf("describe = %q, want the moved master named", got)
	}
	if strings.Contains(got, "5461-10922") {
		t.Errorf("describe = %q, want the untouched shard left out", got)
	}
}

// Slots moving between shards is what deletes keys from one master and
// restores them on another, down two connections with no ordering between
// them.
func TestAReshardIsReportedAsNewAndDepartedShards(t *testing.T) {
	before := map[string]string{"0-8191": "a:6379", "8192-16383": "b:6379"}
	after := map[string]string{
		"0-5460": "a:6379", "5461-10922": "b:6379", "10923-16383": "c:6379",
	}

	got := describe(before, after)
	for _, want := range []string{"0-8191", "8192-16383", "0-5460", "10923-16383"} {
		if !strings.Contains(got, want) {
			t.Errorf("describe = %q, want it to mention %s", got, want)
		}
	}
}

// TestNoChangeIsReportedAsNothing keeps the watcher from crying wolf every time
// it looks.
func TestNoChangeIsReportedAsNothing(t *testing.T) {
	shape := map[string]string{"0-5460": "a:6379", "5461-16383": "b:6379"}
	if got := describe(shape, shape); got != "" {
		t.Errorf("describe of an unchanged shape = %q, want nothing", got)
	}
}

// The rig builds its shards from a seed the test supplies, so every existing
// case passes a clean host:port and none of them exercised what production
// passes.
func TestASingleServerSourceIsDialledWithoutItsDatabase(t *testing.T) {
	s := &Syncer{cfg: config.SyncConfig{
		SourceConnection: "redis://10.105.174.243:6379/0",
	}}
	if got := s.sourceAddr(); got != "10.105.174.243:6379" {
		t.Fatalf("sourceAddr = %q, want 10.105.174.243:6379", got)
	}

	shards, err := shardsOf(context.Background(), goredis.NewClient(
		&goredis.Options{Addr: s.sourceAddr()}), s.sourceAddr())
	if err != nil {
		t.Fatalf("shardsOf: %v", err)
	}
	if len(shards) != 1 {
		t.Fatalf("shards = %d, want 1", len(shards))
	}
	if _, _, err := net.SplitHostPort(shards[0].addr); err != nil {
		t.Fatalf("shard address %q is not dialable: %v", shards[0].addr, err)
	}
}

// TestTheSourceOffsetIsReadFromInfoReplication covers the number the lag alarm
// should be built on.
func TestTheSourceOffsetIsReadFromInfoReplication(t *testing.T) {
	// Not a live server: masterOffset only has to read the field out of what
	// INFO returns, and the shape of that reply is what this pins down.
	const reply = "# Replication\r\nrole:master\r\nconnected_slaves:1\r\n" +
		"master_repl_offset:102009797831\r\n"
	got, err := parseMasterOffset(reply)
	if err != nil {
		t.Fatalf("parseMasterOffset: %v", err)
	}
	if got != 102009797831 {
		t.Errorf("offset = %d, want 102009797831", got)
	}
}

func TestASourceThatReportsNoOffsetIsAnError(t *testing.T) {
	if _, err := parseMasterOffset("# Replication\r\nrole:master\r\n"); err == nil {
		t.Fatal("want an error: publishing an unknown lag as zero would hide a stall")
	}
}
