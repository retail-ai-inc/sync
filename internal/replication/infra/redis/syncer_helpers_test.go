package redis

import (
	"strings"
	"testing"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// TestTwoTasksDoNotShareABufferDirectory.
func TestTwoTasksDoNotShareABufferDirectory(t *testing.T) {
	first := &Syncer{cfg: config.SyncConfig{ID: 51, RedisBufferDir: "/var/lib/sync"}}
	second := &Syncer{cfg: config.SyncConfig{ID: 52, RedisBufferDir: "/var/lib/sync"}}

	a, err := first.bufferDir("0-16383")
	if err != nil {
		t.Fatalf("bufferDir: %v", err)
	}
	b, err := second.bufferDir("0-16383")
	if err != nil {
		t.Fatalf("bufferDir: %v", err)
	}
	if a == b {
		t.Fatalf("both tasks buffer into %s", a)
	}
	if !strings.Contains(a, "51") || !strings.Contains(b, "52") {
		t.Errorf("the task id is not in the path: %s / %s", a, b)
	}
}

// TestTwoShardsDoNotShareABufferDirectory, for the same reason within one task.
func TestTwoShardsDoNotShareABufferDirectory(t *testing.T) {
	s := &Syncer{cfg: config.SyncConfig{ID: 51, RedisBufferDir: "/var/lib/sync"}}

	a, _ := s.bufferDir("0-5460")
	b, _ := s.bufferDir("5461-10922")
	if a == b {
		t.Fatalf("both shards buffer into %s", a)
	}
}

// TestNoBufferDirectoryIsRefusedAtStartup rather than silently buffering in
// memory: the whole point of the disk buffer is surviving a target outage, and
// a task that quietly does without it fails the first time it is needed.
func TestNoBufferDirectoryIsRefusedAtStartup(t *testing.T) {
	s := &Syncer{cfg: config.SyncConfig{ID: 51}}

	_, err := s.bufferDir("0-16383")
	if err == nil {
		t.Fatal("a task with no buffer directory was accepted")
	}
	if !domain.IsUnrecoverable(err) {
		t.Errorf("error is %v, want an unrecoverable one — no amount of retrying "+
			"creates a directory nobody configured", err)
	}
}

// TestAShardIdentifierIsSafeAsADirectoryName.
func TestAShardIdentifierIsSafeAsADirectoryName(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"0-16383", "0-16383"},
		{"5461-10922", "5461-10922"},
		{"a/../../etc", "a_______etc"},
		{"10.0.0.1:6379", "10_0_0_1_6379"},
		{"", ""},
	} {
		if got := sanitise(c.in); got != c.want {
			t.Errorf("sanitise(%q) = %q, want %q", c.in, got, c.want)
		}
		if strings.ContainsAny(sanitise(c.in), "/\\.:") {
			t.Errorf("sanitise(%q) = %q, which still carries a path separator",
				c.in, sanitise(c.in))
		}
	}
}

// TestAVersionComparisonOrdersReleasesNumerically is what decides whether a
// source is new enough for the features this relay needs.
func TestAVersionComparisonOrdersReleasesNumerically(t *testing.T) {
	for _, c := range []struct {
		a, b string
		want bool
	}{
		{"6.2.0", "7.0.0", true},
		{"7.0.0", "6.2.0", false},
		{"7.0.9", "7.0.10", true}, // the case string comparison gets wrong
		{"7.0.10", "7.0.9", false},
		{"7.0.0", "7.0.0", false},
		{"7.0", "7.0.1", false}, // a shorter version is not older on its own
	} {
		if got := olderThan(c.a, c.b); got != c.want {
			t.Errorf("olderThan(%q, %q) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}

// TestAVersionThatDoesNotParseIsNotTreatedAsOld.
func TestAVersionThatDoesNotParseIsNotTreatedAsOld(t *testing.T) {
	for _, c := range []struct{ a, b string }{
		{"unstable", "7.0.0"},
		{"7.0.0", "unstable"},
		{"", "7.0.0"},
	} {
		if olderThan(c.a, c.b) {
			t.Errorf("olderThan(%q, %q) reported older", c.a, c.b)
		}
	}
}

// TestCredentialsComeOutOfTheDSN, because the replication connection is made
// straight to a shard rather than through the client that holds the config.
func TestCredentialsComeOutOfTheDSN(t *testing.T) {
	for _, c := range []struct {
		dsn, user, password string
	}{
		{"redis://default:s3cret@10.0.0.1:6379/0", "default", "s3cret"},
		{"redis://10.0.0.1:6379/0", "", ""},
		{"redis://alice@10.0.0.1:6379/0", "alice", ""},
		{"not a url at all", "", ""},
	} {
		user, password := credentials(c.dsn)
		if user != c.user || password != c.password {
			t.Errorf("credentials(%q) = %q/%q, want %q/%q",
				c.dsn, user, password, c.user, c.password)
		}
	}
}
