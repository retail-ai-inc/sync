package domain

import "testing"

// TestATaskWithNoShardsIsNotCaughtUp covers the one wrong answer. A position
// that could not be read has to report "not caught up": answering yes to
// "is Osaka safe to promote" when nothing is known loses data.
func TestATaskWithNoShardsIsNotCaughtUp(t *testing.T) {
	if (Progress{Engine: "redis"}).CaughtUp() {
		t.Error("a task whose position could not be read reported caught up")
	}
}

func TestATaskIsCaughtUpOnlyWhenEveryShardIs(t *testing.T) {
	for name, c := range map[string]struct {
		shards []ShardProgress
		want   bool
	}{
		"every shard": {[]ShardProgress{
			{Shard: "0-5460", CaughtUp: true},
			{Shard: "5461-10922", CaughtUp: true},
			{Shard: "10923-16383", CaughtUp: true},
		}, true},
		"one behind": {[]ShardProgress{
			{Shard: "0-5460", CaughtUp: true},
			{Shard: "5461-10922", CaughtUp: false},
			{Shard: "10923-16383", CaughtUp: true},
		}, false},
		"none":          {[]ShardProgress{{CaughtUp: false}}, false},
		"single stream": {[]ShardProgress{{CaughtUp: true}}, true},
	} {
		t.Run(name, func(t *testing.T) {
			if got := (Progress{Shards: c.shards}).CaughtUp(); got != c.want {
				t.Errorf("CaughtUp() = %v, want %v", got, c.want)
			}
		})
	}
}
